use super::queue::PendingBatch;
use super::Signal;
use crate::config::Config;
use crate::metrics::Metrics;
use crate::runtime::memory;
use crate::validation::{ApiError, ApiResult};
use crate::LockExt;
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Weak};

const QUEUE_CREDIT_HIGH_WATERMARK_NUMERATOR: usize = 95;
const QUEUE_CREDIT_LOW_WATERMARK_NUMERATOR: usize = 75;
const WATERMARK_DENOMINATOR: usize = 100;

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
        signal: Signal,
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
                    &[("signal", signal.as_str())],
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

#[derive(Clone, Debug)]
pub(super) struct QueueCreditSnapshot {
    pub(super) reserved_bytes: usize,
    pub(super) available_bytes: usize,
    pub(super) capacity_bytes: usize,
    pub(super) flush_debt_seconds: f64,
    pub(super) closed: bool,
}

#[derive(Clone, Debug)]
struct SignalQueueCredits {
    capacity_bytes: usize,
    high_watermark_bytes: usize,
    low_watermark_bytes: usize,
    reserved_bytes: usize,
    closed: bool,
}

impl SignalQueueCredits {
    fn new(capacity_bytes: usize) -> Self {
        let high_watermark_bytes =
            watermark_bytes(capacity_bytes, QUEUE_CREDIT_HIGH_WATERMARK_NUMERATOR).max(1);
        let low_watermark_bytes =
            watermark_bytes(capacity_bytes, QUEUE_CREDIT_LOW_WATERMARK_NUMERATOR)
                .min(high_watermark_bytes.saturating_sub(1));
        Self {
            capacity_bytes,
            high_watermark_bytes,
            low_watermark_bytes,
            reserved_bytes: 0,
            closed: false,
        }
    }

    fn refresh_hysteresis(&mut self) {
        if self.closed && self.reserved_bytes <= self.low_watermark_bytes {
            self.closed = false;
        }
    }

    fn available_bytes(&self) -> usize {
        if self.closed {
            0
        } else {
            self.high_watermark_bytes
                .saturating_sub(self.reserved_bytes)
        }
    }
}

pub(super) struct QueueCreditLedger {
    signals: BTreeMap<Signal, SignalQueueCredits>,
    max_bytes_per_flush: usize,
    flush_interval_seconds: f64,
}

impl QueueCreditLedger {
    pub(super) fn new(config: &Config) -> Self {
        let signals = Signal::ALL
            .into_iter()
            .map(|signal| {
                (
                    signal,
                    SignalQueueCredits::new(config.per_signal_queue_bytes),
                )
            })
            .collect();
        Self {
            signals,
            max_bytes_per_flush: config.max_bytes_per_flush.max(1),
            flush_interval_seconds: config.scheduler_flush_interval.as_secs_f64(),
        }
    }

    pub(super) fn reserve_estimate(
        &mut self,
        signal: Signal,
        headers: &HashMap<String, String>,
        compressed_body_bytes: usize,
        max_body_bytes: usize,
    ) -> ApiResult<QueueCreditReservation> {
        self.reserve_exact(queue_credit_estimate_by_signal(
            signal,
            headers,
            compressed_body_bytes,
            max_body_bytes,
        ))
    }

    pub(super) fn estimate_for_request(
        &self,
        signal: Signal,
        headers: &HashMap<String, String>,
        compressed_body_bytes: usize,
        max_body_bytes: usize,
    ) -> BTreeMap<Signal, usize> {
        queue_credit_estimate_by_signal(signal, headers, compressed_body_bytes, max_body_bytes)
    }

    pub(super) fn projected_reserved_total_bytes(
        &self,
        desired_delta: &BTreeMap<Signal, usize>,
    ) -> usize {
        let current = self
            .signals
            .values()
            .map(|state| state.reserved_bytes)
            .sum::<usize>();
        current.saturating_add(desired_delta.values().sum::<usize>())
    }

    pub(super) fn total_reserved_bytes(&self) -> usize {
        self.signals
            .values()
            .map(|state| state.reserved_bytes)
            .sum()
    }

    pub(super) fn reserve_exact(
        &mut self,
        bytes_by_signal: BTreeMap<Signal, usize>,
    ) -> ApiResult<QueueCreditReservation> {
        let bytes_by_signal = normalized_credit_bytes(bytes_by_signal);
        self.validate_reservation_delta(&bytes_by_signal, &BTreeMap::new())?;
        self.apply_delta(&bytes_by_signal, &BTreeMap::new());
        Ok(QueueCreditReservation {
            credits: bytes_by_signal,
            active: true,
            ledger: Weak::new(),
        })
    }

    pub(super) fn adjust_reservation(
        &mut self,
        reservation: &mut QueueCreditReservation,
        desired: BTreeMap<Signal, usize>,
    ) -> ApiResult<()> {
        let desired = normalized_credit_bytes(desired);
        self.validate_reservation_delta(&desired, &reservation.credits)?;
        self.apply_delta(&desired, &reservation.credits);
        reservation.credits = desired;
        Ok(())
    }

    pub(super) fn release_reservation(&mut self, reservation: &mut QueueCreditReservation) {
        if !reservation.active {
            return;
        }
        self.release_bytes(&reservation.credits);
        reservation.active = false;
        reservation.credits.clear();
    }

    pub(super) fn release_bytes(&mut self, bytes_by_signal: &BTreeMap<Signal, usize>) {
        for (signal, bytes) in bytes_by_signal {
            let Some(state) = self.signals.get_mut(signal) else {
                continue;
            };
            state.reserved_bytes = state.reserved_bytes.saturating_sub(*bytes);
            state.refresh_hysteresis();
        }
    }

    pub(super) fn snapshots(&mut self) -> BTreeMap<Signal, QueueCreditSnapshot> {
        self.signals
            .iter_mut()
            .map(|(signal, state)| {
                state.refresh_hysteresis();
                let flush_debt_seconds = state.reserved_bytes as f64
                    / self.max_bytes_per_flush as f64
                    * self.flush_interval_seconds;
                (
                    *signal,
                    QueueCreditSnapshot {
                        reserved_bytes: state.reserved_bytes,
                        available_bytes: state.available_bytes(),
                        capacity_bytes: state.capacity_bytes,
                        flush_debt_seconds,
                        closed: state.closed,
                    },
                )
            })
            .collect()
    }

    fn validate_reservation_delta(
        &mut self,
        desired: &BTreeMap<Signal, usize>,
        current: &BTreeMap<Signal, usize>,
    ) -> ApiResult<()> {
        for signal in Signal::ALL {
            let desired_bytes = desired.get(&signal).copied().unwrap_or(0);
            let current_bytes = current.get(&signal).copied().unwrap_or(0);
            if desired_bytes <= current_bytes {
                continue;
            }
            let delta = desired_bytes - current_bytes;
            let state = self
                .signals
                .get_mut(&signal)
                .expect("queue credit signal is initialized");
            state.refresh_hysteresis();
            if state.closed
                || state.reserved_bytes.saturating_add(delta) > state.high_watermark_bytes
            {
                let was_closed = state.closed;
                state.closed = true;
                if !was_closed {
                    tracing::warn!(
                        event = "ingest_queue_credit_full",
                        signal = signal.as_str(),
                        reserved_bytes = state.reserved_bytes,
                        incoming_bytes = delta,
                        high_watermark_bytes = state.high_watermark_bytes
                    );
                }
                return Err(ApiError::new(
                    429,
                    "signal_queue_full",
                    format!("{signal} queue is full"),
                )
                .with_retry_after(5));
            }
        }
        Ok(())
    }

    fn apply_delta(
        &mut self,
        desired: &BTreeMap<Signal, usize>,
        current: &BTreeMap<Signal, usize>,
    ) {
        for signal in Signal::ALL {
            let desired_bytes = desired.get(&signal).copied().unwrap_or(0);
            let current_bytes = current.get(&signal).copied().unwrap_or(0);
            let state = self
                .signals
                .get_mut(&signal)
                .expect("queue credit signal is initialized");
            if desired_bytes >= current_bytes {
                state.reserved_bytes = state
                    .reserved_bytes
                    .saturating_add(desired_bytes - current_bytes);
            } else {
                state.reserved_bytes = state
                    .reserved_bytes
                    .saturating_sub(current_bytes - desired_bytes);
                state.refresh_hysteresis();
            }
            if state.reserved_bytes >= state.high_watermark_bytes {
                state.closed = true;
            }
        }
    }
}

pub(super) struct QueueCreditReservation {
    credits: BTreeMap<Signal, usize>,
    active: bool,
    /// Handle back to the owning ledger so an un-released reservation (e.g. a
    /// ingest worker panics mid-process) returns its credits on drop instead
    /// of leaking them toward a permanent 429. Explicit release/adjust paths set
    /// `active = false`, making the drop a no-op.
    ledger: Weak<Mutex<QueueCreditLedger>>,
}

impl QueueCreditReservation {
    pub(super) fn bind_ledger(&mut self, ledger: Weak<Mutex<QueueCreditLedger>>) {
        self.ledger = ledger;
    }
}

impl Drop for QueueCreditReservation {
    fn drop(&mut self) {
        if !self.active || self.credits.is_empty() {
            return;
        }
        if let Some(ledger) = self.ledger.upgrade() {
            ledger.lock_or_poisoned().release_bytes(&self.credits);
        }
    }
}

pub(super) fn credit_bytes_by_signal(batches: &[PendingBatch]) -> BTreeMap<Signal, usize> {
    let mut bytes_by_signal = BTreeMap::new();
    for batch in batches {
        *bytes_by_signal.entry(batch.key.signal).or_default() += batch.credit_bytes;
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

fn queue_credit_estimate_bytes(
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
            .saturating_mul(8)
            .min(max_body_bytes.saturating_mul(4)),
        _ => compressed_body_bytes.saturating_mul(4),
    }
}

fn queue_credit_estimate_by_signal(
    signal: Signal,
    headers: &HashMap<String, String>,
    compressed_body_bytes: usize,
    max_body_bytes: usize,
) -> BTreeMap<Signal, usize> {
    let bytes = if signal.is_metric() {
        metric_queue_credit_estimate_bytes(headers, compressed_body_bytes, max_body_bytes)
    } else {
        queue_credit_estimate_bytes(headers, compressed_body_bytes, max_body_bytes)
    };
    if signal.is_metric() {
        BTreeMap::from([(Signal::MetricGauge, bytes), (Signal::MetricSum, bytes)])
    } else {
        BTreeMap::from([(signal, bytes)])
    }
}

fn metric_queue_credit_estimate_bytes(
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
            .saturating_mul(12)
            .min(max_body_bytes.saturating_mul(4)),
        _ => compressed_body_bytes.saturating_mul(6),
    }
}

fn normalized_credit_bytes(bytes_by_signal: BTreeMap<Signal, usize>) -> BTreeMap<Signal, usize> {
    bytes_by_signal
        .into_iter()
        .filter(|(_, bytes)| *bytes > 0)
        .collect()
}

fn watermark_bytes(capacity: usize, numerator: usize) -> usize {
    capacity.saturating_mul(numerator) / WATERMARK_DENOMINATOR
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metric_queue_credit_estimate_reserves_both_metric_signals() {
        let estimate =
            queue_credit_estimate_by_signal(Signal::MetricGauge, &HashMap::new(), 100, 1_000);

        assert_eq!(estimate.get(&Signal::MetricGauge), Some(&600));
        assert_eq!(estimate.get(&Signal::MetricSum), Some(&600));
        assert_eq!(estimate.len(), 2);
    }

    #[test]
    fn non_metric_queue_credit_estimate_reserves_request_signal_only() {
        let estimate = queue_credit_estimate_by_signal(Signal::Logs, &HashMap::new(), 100, 1_000);

        assert_eq!(estimate.get(&Signal::Logs), Some(&400));
        assert_eq!(estimate.len(), 1);
    }

    #[test]
    fn dropping_unreleased_reservation_returns_credits_to_ledger() {
        use crate::config::Config;

        let dir = tempfile::tempdir().unwrap();
        let config = Config::test(dir.path().join("canardstack.duckdb"));
        let ledger = Arc::new(Mutex::new(QueueCreditLedger::new(&config)));
        let mut reservation = ledger
            .lock()
            .unwrap()
            .reserve_exact(BTreeMap::from([(Signal::Logs, 1_024)]))
            .unwrap();
        reservation.bind_ledger(Arc::downgrade(&ledger));
        assert_eq!(ledger.lock().unwrap().total_reserved_bytes(), 1_024);

        // Simulate a ingest worker panicking before it can explicitly release:
        // the reservation drops un-released and must return its credits, or ingest
        // would walk toward a permanent 429.
        drop(reservation);
        assert_eq!(
            ledger.lock().unwrap().total_reserved_bytes(),
            0,
            "dropping an unreleased reservation must return its credits"
        );
    }
}
