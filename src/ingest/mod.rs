use crate::config::Config;
use crate::metrics::Metrics;
use crate::otlp::{self, Transformed};
use crate::storage::Storage;
use crate::validation::{self, ApiError, ApiResult};
use crate::LockExt;
use serde::Serialize;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

mod admission;
mod flush;
mod queue;

pub use flush::{partial_commit_info, PartialFlushError};
pub use queue::IngestSnapshot;

#[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize)]
pub enum Signal {
    Logs,
    Spans,
    MetricGauge,
    MetricSum,
}

impl Signal {
    pub fn as_str(self) -> &'static str {
        match self {
            Signal::Logs => "logs",
            Signal::Spans => "spans",
            Signal::MetricGauge => "metric_gauge",
            Signal::MetricSum => "metric_sum",
        }
    }

    fn is_metric(self) -> bool {
        matches!(self, Signal::MetricGauge | Signal::MetricSum)
    }
}

impl fmt::Display for Signal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Clone)]
pub struct Ingestor {
    queues: Arc<Mutex<queue::QueueMap>>,
    flush_lock: Arc<Mutex<()>>,
    flush_signal: Arc<FlushSignal>,
    runtime_memory_reserved_bytes: Arc<AtomicUsize>,
    config: Config,
}

#[derive(Default)]
struct FlushSignal {
    requested: Mutex<bool>,
    ready: Condvar,
}

impl Ingestor {
    pub fn new(config: Config) -> Self {
        Self {
            queues: Arc::new(Mutex::new(HashMap::new())),
            flush_lock: Arc::new(Mutex::new(())),
            flush_signal: Arc::new(FlushSignal::default()),
            runtime_memory_reserved_bytes: Arc::new(AtomicUsize::new(0)),
            config,
        }
    }

    pub fn request_flush(&self) {
        let mut requested = self.flush_signal.requested.lock_or_poisoned();
        *requested = true;
        self.flush_signal.ready.notify_one();
    }

    pub fn wait_for_flush_or_timeout(&self, timeout: Duration, stop: &AtomicBool) -> bool {
        let mut requested = self.flush_signal.requested.lock_or_poisoned();
        if !*requested && !stop.load(Ordering::SeqCst) {
            let (guard, _) = self
                .flush_signal
                .ready
                .wait_timeout(requested, timeout)
                .unwrap_or_else(|e| e.into_inner());
            requested = guard;
        }
        let was_requested = *requested;
        *requested = false;
        was_requested
    }

    pub fn ingest(
        &self,
        signal: Signal,
        headers: &HashMap<String, String>,
        compressed_body: &[u8],
        storage: &Storage,
        metrics: &Metrics,
    ) -> ApiResult<Value> {
        validation::validate_body_size(compressed_body.len(), &self.config)?;
        validation::validate_content_type(headers)?;
        if !storage.accepts_memory_ingest() || self.config.force_dependency_unhealthy {
            metrics.ingest_request(signal, 503, "dependency_unhealthy");
            return Err(ApiError::new(
                503,
                "dependency_unhealthy",
                "storage dependency is unhealthy",
            )
            .with_retry_after(10));
        }
        let mut runtime_memory_reservation =
            match self.admit_runtime_memory(signal, headers, compressed_body.len(), metrics) {
                Ok(reservation) => reservation,
                Err(err) => {
                    metrics.ingest_request(signal, err.status, err.reason);
                    return Err(err);
                }
            };

        let started = Instant::now();
        let body_result =
            otlp::decompress_if_needed(headers, compressed_body, self.config.max_body_bytes);
        metrics.observe_phase_seconds(
            signal.as_str(),
            "decompress",
            None,
            started.elapsed().as_secs_f64(),
        );
        let body = body_result?;
        validation::validate_body_size(body.len(), &self.config)?;
        let started = Instant::now();
        #[cfg(feature = "transform-split-instrumentation")]
        let transformed_result = otlp::transform_observed(signal, headers, &body, metrics);
        #[cfg(not(feature = "transform-split-instrumentation"))]
        let transformed_result = otlp::transform(signal, headers, &body);
        metrics.observe_phase_seconds(
            signal.as_str(),
            "otlp_transform",
            None,
            started.elapsed().as_secs_f64(),
        );
        let transformed = transformed_result?;
        let started = Instant::now();
        let skew_result = self.validate_skew(&transformed);
        metrics.observe_phase_seconds(
            signal.as_str(),
            "timestamp_validation",
            None,
            started.elapsed().as_secs_f64(),
        );
        skew_result?;

        let request_bytes = compressed_body.len();
        let unsupported_histograms = transformed.unsupported_histograms;
        let batches = queue::pending_batches(transformed);
        let pending_bytes = batches.iter().map(|b| b.approx_bytes).sum::<usize>();
        let peak_bytes = request_bytes
            .saturating_add(body.len())
            .saturating_add(pending_bytes);
        if let Err(err) = runtime_memory_reservation.reserve_at_least(peak_bytes, signal, metrics) {
            metrics.ingest_request(signal, err.status, err.reason);
            return Err(err);
        }
        let accepted = match self.enqueue(signal, batches, metrics) {
            Ok(accepted) => accepted,
            Err(err) => {
                metrics.ingest_request(signal, err.status, err.reason);
                return Err(err);
            }
        };

        metrics.ingest_request(signal, 202, "accepted");
        metrics.inc(
            "canardstack_ingest_request_bytes_total",
            &[
                ("signal", signal.as_str()),
                (
                    "encoding",
                    headers
                        .get("content-encoding")
                        .map(String::as_str)
                        .unwrap_or("identity"),
                ),
            ],
            request_bytes as u64,
        );
        metrics.inc(
            "canardstack_ingest_records_total",
            &[("signal", signal.as_str())],
            accepted as u64,
        );
        self.record_queue_metrics(metrics);

        Ok(json!({
            "accepted": true,
            "records": accepted,
            "acknowledgement": "accepted_into_process_memory_not_durably_committed",
            "unsupported_histograms": unsupported_histograms
        }))
    }

    fn admit_runtime_memory(
        &self,
        signal: Signal,
        headers: &HashMap<String, String>,
        compressed_body_bytes: usize,
        metrics: &Metrics,
    ) -> ApiResult<admission::RuntimeMemoryReservation> {
        let mut reservation = admission::RuntimeMemoryReservation::disabled(
            self.runtime_memory_reserved_bytes.clone(),
        );
        let Some(limit) = self.config.runtime_memory_limit_bytes else {
            return Ok(reservation);
        };
        reservation = reservation.with_limit(limit);
        reservation.reserve_at_least(
            admission::decode_reservation_bytes(
                headers,
                compressed_body_bytes,
                self.config.max_body_bytes,
            ),
            signal,
            metrics,
        )?;
        Ok(reservation)
    }

    pub fn snapshots(&self) -> Vec<IngestSnapshot> {
        let queues = self.queues.lock_or_poisoned();
        queue::snapshots(&queues, &self.config)
    }

    pub fn record_queue_metrics(&self, metrics: &Metrics) {
        for snapshot in self.snapshots() {
            metrics.gauge(
                "canardstack_ingest_queue_rows",
                &[("signal", snapshot.signal)],
                snapshot.queued_rows as f64,
            );
            metrics.gauge(
                "canardstack_ingest_queue_bytes",
                &[("signal", snapshot.signal)],
                snapshot.queued_bytes as f64,
            );
            metrics.gauge(
                "canardstack_ingest_queue_oldest_age_seconds",
                &[("signal", snapshot.signal)],
                snapshot.oldest_age_seconds,
            );
            metrics.gauge(
                "canardstack_ingest_queue_pressure",
                &[("signal", snapshot.signal)],
                snapshot.pressure,
            );
        }
    }

    fn enqueue(
        &self,
        request_signal: Signal,
        batches: Vec<queue::PendingBatch>,
        metrics: &Metrics,
    ) -> ApiResult<usize> {
        if batches.is_empty() {
            return Ok(0);
        }
        let started = Instant::now();
        let queue_result = {
            let mut queues = self.queues.lock_or_poisoned();
            admission::admit_and_enqueue(&mut queues, batches, &self.config)
        };
        metrics.observe_phase_seconds(
            request_signal.as_str(),
            "queue_admission",
            None,
            started.elapsed().as_secs_f64(),
        );
        let queue_result = queue_result?;
        if queue_result.should_request_flush {
            self.request_flush();
            metrics.inc(
                "canardstack_ingest_flush_requests_total",
                &[("triggered_by", request_signal.as_str())],
                1,
            );
        }
        Ok(queue_result.accepted)
    }

    fn validate_skew(&self, transformed: &Transformed) -> ApiResult<()> {
        if let Some(logs) = &transformed.logs {
            validation::validate_arrow_timestamp_skew(logs, Signal::Logs, &self.config)?;
        }
        if let Some(spans) = &transformed.spans {
            validation::validate_arrow_timestamp_skew(spans, Signal::Spans, &self.config)?;
        }
        if let Some(gauge) = &transformed.gauge {
            validation::validate_arrow_timestamp_skew(gauge, Signal::MetricGauge, &self.config)?;
        }
        if let Some(sum) = &transformed.sum {
            validation::validate_arrow_timestamp_skew(sum, Signal::MetricSum, &self.config)?;
        }
        Ok(())
    }
}
