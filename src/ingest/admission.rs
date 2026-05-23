use super::batches::PendingBatch;
use super::{OtlpRequestKind, StorageSignal};
use crate::config::Config;
use crate::metrics::Metrics;
use crate::runtime::memory;
use crate::validation::{ApiError, ApiResult};
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

// Request-byte admission estimates budget compressed/raw payload expansion into
// Arrow buffers; metrics fan out into gauge+sum tables and usually carry more
// label/value overhead than logs or spans.
const NON_METRIC_IDENTITY_EXPANSION: usize = 4;
const NON_METRIC_GZIP_EXPANSION: usize = 8;
const METRIC_IDENTITY_EXPANSION: usize = 6;
const METRIC_GZIP_EXPANSION: usize = 12;
const GZIP_EXPANSION_MAX_BODY_MULTIPLIER: usize = 4;

pub(super) struct RuntimeMemoryReservation {
    reserved_bytes: Arc<AtomicUsize>,
    bytes: usize,
    limit: Option<usize>,
}

impl RuntimeMemoryReservation {
    pub(super) fn disabled(reserved_bytes: Arc<AtomicUsize>) -> Self {
        Self {
            reserved_bytes,
            bytes: 0,
            limit: None,
        }
    }

    pub(super) fn with_limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }

    pub(super) fn reserve_at_least(
        &mut self,
        target_bytes: usize,
        route: OtlpRequestKind,
        metrics: &Metrics,
    ) -> ApiResult<()> {
        let Some(limit) = self.limit else {
            return Ok(());
        };
        if target_bytes <= self.bytes {
            return Ok(());
        }
        let delta = target_bytes - self.bytes;
        loop {
            let Some(rss) = memory::runtime_rss_bytes() else {
                metrics.inc(
                    "canardstack_ingest_runtime_memory_unknown_total",
                    &[("request_kind", route.as_str())],
                    1,
                );
                return Ok(());
            };
            metrics.gauge("canardstack_runtime_rss_bytes", &[], rss as f64);
            metrics.gauge("canardstack_runtime_memory_limit_bytes", &[], limit as f64);

            let current_reserved = self.reserved_bytes.load(Ordering::Acquire);
            let other_reserved = current_reserved.saturating_sub(self.bytes);
            let projected = rss.saturating_add(other_reserved).saturating_add(delta);
            if rss >= limit || projected > limit {
                return Err(ApiError::new(
                    429,
                    "runtime_memory_full",
                    "process runtime memory limit would be exceeded",
                )
                .with_retry_after(5));
            }
            let new_reserved = current_reserved.saturating_add(delta);
            match self.reserved_bytes.compare_exchange(
                current_reserved,
                new_reserved,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    self.bytes = target_bytes;
                    return Ok(());
                }
                Err(_) => continue,
            }
        }
    }
}

impl Drop for RuntimeMemoryReservation {
    fn drop(&mut self) {
        if self.bytes > 0 {
            self.reserved_bytes.fetch_sub(self.bytes, Ordering::AcqRel);
        }
    }
}

/// Per-signal accounting of bytes that have been admitted (durably spooled and
/// handed to an ingest worker) but not yet appended to the Arrow write buffer.
///
/// Freshness-first admission in
/// [`crate::admission_control::AdmissionController::admit_ingest`] is
/// the single authority that sheds ingest under projected-visibility pressure.
/// This tracker only adds a cheap per-signal ceiling so one signal's burst
/// cannot monopolize the in-flight window, and exposes the in-flight total the
/// freshness projection treats as "queued" bytes. It is lock-free: the former
/// watermark/hysteresis credit ledger collapsed into plain atomics.
pub(super) struct InflightBytes {
    counters: [AtomicUsize; StorageSignal::ALL.len()],
    per_signal_capacity_bytes: usize,
}

impl InflightBytes {
    pub(super) fn new(config: &Config) -> Self {
        Self {
            counters: std::array::from_fn(|_| AtomicUsize::new(0)),
            per_signal_capacity_bytes: config.per_signal_inflight_bytes.max(1),
        }
    }

    pub(super) fn total_bytes(&self) -> usize {
        self.counters
            .iter()
            .map(|counter| counter.load(Ordering::Acquire))
            .sum()
    }

    pub(super) fn signal_bytes(&self, signal: StorageSignal) -> usize {
        self.counters[signal_index(signal)].load(Ordering::Acquire)
    }

    pub(super) fn capacity_bytes(&self) -> usize {
        self.per_signal_capacity_bytes
    }

    pub(super) fn estimate_for_request(
        &self,
        route: OtlpRequestKind,
        headers: &HashMap<String, String>,
        compressed_body_bytes: usize,
        max_body_bytes: usize,
    ) -> BTreeMap<StorageSignal, usize> {
        inflight_estimate_by_request(route, headers, compressed_body_bytes, max_body_bytes)
    }

    /// Reserve the per-signal estimate, rejecting with `signal_inflight_full` if a
    /// signal would exceed its in-flight ceiling. The returned guard releases
    /// the reservation on drop, so an ingest worker that panics mid-process
    /// returns its bytes instead of leaking toward a permanent 429.
    pub(super) fn reserve(
        self: &Arc<Self>,
        estimate: BTreeMap<StorageSignal, usize>,
    ) -> ApiResult<InflightReservation> {
        let estimate = normalized_bytes(estimate);
        for (&signal, &bytes) in &estimate {
            let after =
                self.counters[signal_index(signal)].fetch_add(bytes, Ordering::AcqRel) + bytes;
            if after > self.per_signal_capacity_bytes {
                // Roll back this signal and any already added earlier in the
                // (sorted) iteration order, then reject before the durable
                // raw-spool append.
                sub_saturating(&self.counters[signal_index(signal)], bytes);
                for (&done, &done_bytes) in &estimate {
                    if done == signal {
                        break;
                    }
                    sub_saturating(&self.counters[signal_index(done)], done_bytes);
                }
                tracing::warn!(
                    event = "ingest_signal_inflight_full",
                    signal = signal.as_str(),
                    inflight_bytes = after,
                    capacity_bytes = self.per_signal_capacity_bytes
                );
                return Err(ApiError::new(
                    429,
                    "signal_inflight_full",
                    format!("{signal} queue is full"),
                )
                .with_retry_after(5));
            }
        }
        Ok(InflightReservation {
            tracker: Arc::clone(self),
            bytes: estimate,
        })
    }
}

fn signal_index(signal: StorageSignal) -> usize {
    match signal {
        StorageSignal::Logs => 0,
        StorageSignal::Spans => 1,
        StorageSignal::MetricGauge => 2,
        StorageSignal::MetricSum => 3,
    }
}

pub(super) struct InflightReservation {
    tracker: Arc<InflightBytes>,
    bytes: BTreeMap<StorageSignal, usize>,
}

impl InflightReservation {
    /// Correct the admission estimate to the exact buffered Arrow bytes once the
    /// payload has been transformed, keeping the in-flight total accurate while
    /// the rows wait for the worker storage buffer append. Infallible: the request is
    /// already durably spooled, so accurate accounting must not be able to
    /// reject it here.
    pub(super) fn adjust(&mut self, exact: BTreeMap<StorageSignal, usize>) {
        let exact = normalized_bytes(exact);
        for signal in StorageSignal::ALL {
            let current = self.bytes.get(&signal).copied().unwrap_or(0);
            let desired = exact.get(&signal).copied().unwrap_or(0);
            if desired > current {
                self.tracker.counters[signal_index(signal)]
                    .fetch_add(desired - current, Ordering::AcqRel);
            } else if current > desired {
                sub_saturating(
                    &self.tracker.counters[signal_index(signal)],
                    current - desired,
                );
            }
        }
        self.bytes = exact;
    }
}

impl Drop for InflightReservation {
    fn drop(&mut self) {
        for (signal, bytes) in std::mem::take(&mut self.bytes) {
            sub_saturating(&self.tracker.counters[signal_index(signal)], bytes);
        }
    }
}

pub(super) fn inflight_bytes_by_signal(batches: &[PendingBatch]) -> BTreeMap<StorageSignal, usize> {
    let mut bytes_by_signal = BTreeMap::new();
    for batch in batches {
        *bytes_by_signal.entry(batch.signal).or_default() += batch.approx_bytes;
    }
    bytes_by_signal
}

pub(super) fn decode_reservation_bytes(
    headers: &HashMap<String, String>,
    compressed_body_bytes: usize,
    max_body_bytes: usize,
) -> usize {
    let compressed_body_bytes = compressed_body_bytes.max(1);
    match headers
        .get("content-encoding")
        .map(|value| value.to_ascii_lowercase())
        .as_deref()
    {
        Some("gzip") => compressed_body_bytes.saturating_add(max_body_bytes),
        _ => compressed_body_bytes.saturating_mul(2),
    }
}

fn inflight_estimate_bytes(
    headers: &HashMap<String, String>,
    compressed_body_bytes: usize,
    max_body_bytes: usize,
) -> usize {
    let compressed_body_bytes = compressed_body_bytes.max(1);
    match headers
        .get("content-encoding")
        .map(|value| value.to_ascii_lowercase())
        .as_deref()
    {
        Some("gzip") => compressed_body_bytes
            .saturating_mul(NON_METRIC_GZIP_EXPANSION)
            .min(max_body_bytes.saturating_mul(GZIP_EXPANSION_MAX_BODY_MULTIPLIER)),
        _ => compressed_body_bytes.saturating_mul(NON_METRIC_IDENTITY_EXPANSION),
    }
}

fn inflight_estimate_by_request(
    route: OtlpRequestKind,
    headers: &HashMap<String, String>,
    compressed_body_bytes: usize,
    max_body_bytes: usize,
) -> BTreeMap<StorageSignal, usize> {
    match route {
        OtlpRequestKind::Logs => BTreeMap::from([(
            StorageSignal::Logs,
            inflight_estimate_bytes(headers, compressed_body_bytes, max_body_bytes),
        )]),
        OtlpRequestKind::Traces => BTreeMap::from([(
            StorageSignal::Spans,
            inflight_estimate_bytes(headers, compressed_body_bytes, max_body_bytes),
        )]),
        OtlpRequestKind::Metrics => {
            let bytes =
                metric_inflight_estimate_bytes(headers, compressed_body_bytes, max_body_bytes);
            BTreeMap::from([
                (StorageSignal::MetricGauge, bytes),
                (StorageSignal::MetricSum, bytes),
            ])
        }
    }
}

fn metric_inflight_estimate_bytes(
    headers: &HashMap<String, String>,
    compressed_body_bytes: usize,
    max_body_bytes: usize,
) -> usize {
    let compressed_body_bytes = compressed_body_bytes.max(1);
    match headers
        .get("content-encoding")
        .map(|value| value.to_ascii_lowercase())
        .as_deref()
    {
        Some("gzip") => compressed_body_bytes
            .saturating_mul(METRIC_GZIP_EXPANSION)
            .min(max_body_bytes.saturating_mul(GZIP_EXPANSION_MAX_BODY_MULTIPLIER)),
        _ => compressed_body_bytes.saturating_mul(METRIC_IDENTITY_EXPANSION),
    }
}

fn normalized_bytes(
    bytes_by_signal: BTreeMap<StorageSignal, usize>,
) -> BTreeMap<StorageSignal, usize> {
    bytes_by_signal
        .into_iter()
        .filter(|(_, bytes)| *bytes > 0)
        .collect()
}

fn sub_saturating(counter: &AtomicUsize, bytes: usize) {
    let mut current = counter.load(Ordering::Acquire);
    loop {
        let next = current.saturating_sub(bytes);
        match counter.compare_exchange_weak(current, next, Ordering::AcqRel, Ordering::Acquire) {
            Ok(_) => return,
            Err(observed) => current = observed,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metric_inflight_estimate_reserves_both_metric_signals() {
        let estimate =
            inflight_estimate_by_request(OtlpRequestKind::Metrics, &HashMap::new(), 100, 1_000);

        assert_eq!(estimate.get(&StorageSignal::MetricGauge), Some(&600));
        assert_eq!(estimate.get(&StorageSignal::MetricSum), Some(&600));
        assert_eq!(estimate.len(), 2);
    }

    #[test]
    fn non_metric_inflight_estimate_reserves_request_signal_only() {
        let estimate =
            inflight_estimate_by_request(OtlpRequestKind::Logs, &HashMap::new(), 100, 1_000);

        assert_eq!(estimate.get(&StorageSignal::Logs), Some(&400));
        assert_eq!(estimate.len(), 1);
    }

    #[test]
    fn dropping_unreleased_reservation_returns_inflight_bytes() {
        use crate::config::Config;

        let dir = tempfile::tempdir().unwrap();
        let config = Config::test(dir.path().join("canardstack.duckdb"));
        let tracker = Arc::new(InflightBytes::new(&config));
        let reservation = tracker
            .reserve(BTreeMap::from([(StorageSignal::Logs, 1_024)]))
            .unwrap();
        assert_eq!(tracker.total_bytes(), 1_024);

        // Simulate an ingest worker panicking before it can explicitly release:
        // the reservation drops un-released and must return its bytes, or ingest
        // would walk toward a permanent 429.
        drop(reservation);
        assert_eq!(
            tracker.total_bytes(),
            0,
            "dropping an unreleased reservation must return its bytes"
        );
    }

    #[test]
    fn reserve_rejects_when_signal_ceiling_exceeded() {
        use crate::config::Config;

        let dir = tempfile::tempdir().unwrap();
        let mut config = Config::test(dir.path().join("canardstack.duckdb"));
        config.per_signal_inflight_bytes = 16;
        let tracker = Arc::new(InflightBytes::new(&config));

        let err = match tracker.reserve(BTreeMap::from([(StorageSignal::Logs, 64)])) {
            Ok(_) => panic!("reservation above the per-signal ceiling must reject"),
            Err(err) => err,
        };
        assert_eq!(err.status, 429);
        assert_eq!(err.reason, "signal_inflight_full");
        assert_eq!(
            tracker.total_bytes(),
            0,
            "a rejected reservation must not leave bytes reserved"
        );
    }

    #[test]
    fn adjust_then_drop_returns_exact_bytes() {
        use crate::config::Config;

        let dir = tempfile::tempdir().unwrap();
        let config = Config::test(dir.path().join("canardstack.duckdb"));
        let tracker = Arc::new(InflightBytes::new(&config));
        let mut reservation = tracker
            .reserve(BTreeMap::from([(StorageSignal::Logs, 1_000)]))
            .unwrap();
        reservation.adjust(BTreeMap::from([(StorageSignal::Logs, 250)]));
        assert_eq!(tracker.signal_bytes(StorageSignal::Logs), 250);
        drop(reservation);
        assert_eq!(tracker.total_bytes(), 0);
    }
}
