use super::batches::PendingBatch;
use super::OtlpRequestKind;
use crate::config::Config;
use crate::metrics::{MetricName, Metrics};
use crate::runtime::memory;
use crate::signal::StorageSignal;
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
                    MetricName::IngestRuntimeMemoryUnknownTotal,
                    &[("request_kind", route.as_str())],
                    1,
                );
                return Ok(());
            };
            metrics.gauge(MetricName::RuntimeRssBytes, &[], rss as f64);
            metrics.gauge(MetricName::RuntimeMemoryLimitBytes, &[], limit as f64);

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
/// [`crate::admission_control::AdmissionController::admit_ingest`] is the SOLE
/// soft shed for ingest: it projects seal visibility from the in-flight TOTAL
/// this tracker exposes and rejects with 429 when projected visibility exceeds
/// the freshness budget. The optional process RSS limit
/// ([`RuntimeMemoryReservation`]) is the sole hard cap. This tracker is pure
/// per-signal accounting that feeds the freshness in-flight total; it enforces
/// no admission ceiling and never rejects. It is lock-free: the per-signal
/// counters are plain atomics.
pub(super) struct InflightBytes {
    counters: [AtomicUsize; StorageSignal::ALL.len()],
}

impl InflightBytes {
    pub(super) fn new(_config: &Config) -> Self {
        Self {
            counters: std::array::from_fn(|_| AtomicUsize::new(0)),
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

    pub(super) fn estimate_for_request(
        &self,
        route: OtlpRequestKind,
        headers: &HashMap<String, String>,
        compressed_body_bytes: usize,
        max_body_bytes: usize,
    ) -> BTreeMap<StorageSignal, usize> {
        inflight_estimate_by_request(route, headers, compressed_body_bytes, max_body_bytes)
    }

    /// Reserve the per-signal estimate. This is pure accounting and never
    /// rejects: freshness-first admission has already run as the sole soft shed
    /// before this point. The returned guard releases the reservation on drop,
    /// so an ingest worker that panics mid-process returns its bytes instead of
    /// leaking and inflating the freshness projection forever.
    pub(super) fn reserve(
        self: &Arc<Self>,
        estimate: BTreeMap<StorageSignal, usize>,
    ) -> InflightReservation {
        let estimate = normalized_bytes(estimate);
        for (&signal, &bytes) in &estimate {
            self.counters[signal_index(signal)].fetch_add(bytes, Ordering::AcqRel);
        }
        InflightReservation {
            tracker: Arc::clone(self),
            bytes: estimate,
        }
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
    // Metric requests fan out into gauge+sum and carry more per-byte expansion;
    // pick the estimate once, then assign it to each storage signal the request
    // fans out to.
    let bytes = match route {
        OtlpRequestKind::Metrics => {
            metric_inflight_estimate_bytes(headers, compressed_body_bytes, max_body_bytes)
        }
        OtlpRequestKind::Logs | OtlpRequestKind::Traces => {
            inflight_estimate_bytes(headers, compressed_body_bytes, max_body_bytes)
        }
    };
    route
        .storage_signals()
        .iter()
        .map(|&signal| (signal, bytes))
        .collect()
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
    fn storage_signals_fan_out_matches_request_kind() {
        assert_eq!(
            OtlpRequestKind::Logs.storage_signals(),
            &[StorageSignal::Logs]
        );
        assert_eq!(
            OtlpRequestKind::Traces.storage_signals(),
            &[StorageSignal::Spans]
        );
        assert_eq!(
            OtlpRequestKind::Metrics.storage_signals(),
            &[StorageSignal::MetricGauge, StorageSignal::MetricSum]
        );
    }

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
        let reservation = tracker.reserve(BTreeMap::from([(StorageSignal::Logs, 1_024)]));
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
    fn adjust_then_drop_returns_exact_bytes() {
        use crate::config::Config;

        let dir = tempfile::tempdir().unwrap();
        let config = Config::test(dir.path().join("canardstack.duckdb"));
        let tracker = Arc::new(InflightBytes::new(&config));
        let mut reservation = tracker.reserve(BTreeMap::from([(StorageSignal::Logs, 1_000)]));
        reservation.adjust(BTreeMap::from([(StorageSignal::Logs, 250)]));
        assert_eq!(tracker.signal_bytes(StorageSignal::Logs), 250);
        drop(reservation);
        assert_eq!(tracker.total_bytes(), 0);
    }
}
