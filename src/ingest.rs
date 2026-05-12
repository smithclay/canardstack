use crate::config::Config;
use crate::metrics::Metrics;
use crate::otlp::{self, Transformed};
use crate::storage::{InsertRecordsError, Storage};
use crate::validation::{self, ApiError, ApiResult};
use crate::LockExt;
use arrow58::record_batch::RecordBatch;
use serde::Serialize;
use serde_json::{json, Value};
use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash, Serialize)]
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
}

impl fmt::Display for Signal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Clone)]
pub struct Ingestor {
    queues: Arc<Mutex<HashMap<Signal, SignalQueue>>>,
    flush_lock: Arc<Mutex<()>>,
    config: Config,
}

/// Flush failed mid-batch. `committed_rows` were accepted by the catalog
/// before the failure; remote object-storage durability is the lake's
/// responsibility, not this process's.
#[derive(Debug)]
pub struct PartialFlushError {
    pub committed_rows: usize,
    pub signal: Signal,
    pub source: anyhow::Error,
}

impl std::fmt::Display for PartialFlushError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} (signal={}, {} row(s) accepted by catalog before failure)",
            self.source, self.signal, self.committed_rows
        )
    }
}

impl std::error::Error for PartialFlushError {}

#[derive(Clone)]
struct QueuedBatch {
    batch: RecordBatch,
    source_format: &'static str,
    accepted_at: Instant,
    approx_bytes: usize,
}

impl QueuedBatch {
    fn split_at(mut self, take_rows: usize) -> (Self, Self) {
        debug_assert!(take_rows > 0);
        debug_assert!(take_rows < self.len());
        let original_rows = self.len();
        let rest_batch = self.batch.slice(take_rows, original_rows - take_rows);
        self.batch = self.batch.slice(0, take_rows);
        let taken_bytes = proportional_bytes(self.approx_bytes, take_rows, original_rows);
        let rest_bytes = self.approx_bytes.saturating_sub(taken_bytes);
        let accepted_at = self.accepted_at;
        let source_format = self.source_format;
        let taken = Self {
            batch: self.batch,
            source_format,
            accepted_at,
            approx_bytes: taken_bytes,
        };
        let rest = Self {
            batch: rest_batch,
            source_format,
            accepted_at,
            approx_bytes: rest_bytes,
        };
        (taken, rest)
    }

    fn suffix(&self, committed_rows: usize) -> Option<Self> {
        if committed_rows >= self.len() {
            return None;
        }
        let batch = self
            .batch
            .slice(committed_rows, self.batch.num_rows() - committed_rows);
        let approx_bytes = proportional_bytes(self.approx_bytes, batch.num_rows(), self.len());
        Some(Self {
            batch,
            source_format: self.source_format,
            accepted_at: self.accepted_at,
            approx_bytes,
        })
    }

    fn len(&self) -> usize {
        self.batch.num_rows()
    }
}

struct PendingBatch {
    signal: Signal,
    batch: RecordBatch,
    source_format: &'static str,
    approx_bytes: usize,
}

#[derive(Default, Clone)]
struct SignalQueue {
    batches: VecDeque<QueuedBatch>,
    rows: usize,
    bytes: usize,
}

#[derive(Debug, Serialize)]
pub struct IngestSnapshot {
    pub signal: &'static str,
    pub queued_rows: usize,
    pub queued_bytes: usize,
    pub oldest_age_seconds: f64,
    pub pressure: f64,
}

impl Ingestor {
    pub fn new(config: Config) -> Self {
        let queues = HashMap::from([
            (Signal::Logs, SignalQueue::default()),
            (Signal::Spans, SignalQueue::default()),
            (Signal::MetricGauge, SignalQueue::default()),
            (Signal::MetricSum, SignalQueue::default()),
        ]);
        Self {
            queues: Arc::new(Mutex::new(queues)),
            flush_lock: Arc::new(Mutex::new(())),
            config,
        }
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
        let batches = pending_batches(transformed);
        let accepted = match self.enqueue_and_maybe_flush(signal, batches, storage, metrics) {
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

    pub fn flush_all(&self, storage: &Storage) -> anyhow::Result<usize> {
        let _guard = self.flush_lock.lock_or_poisoned();
        let mut total = 0;
        for signal in [
            Signal::Logs,
            Signal::Spans,
            Signal::MetricGauge,
            Signal::MetricSum,
        ] {
            total += self.flush_signal_observed(signal, storage, None)?;
        }
        Ok(total)
    }

    pub fn flush_aged(&self, storage: &Storage) -> anyhow::Result<HashMap<Signal, usize>> {
        let due: Vec<Signal> = {
            let queues = self.queues.lock_or_poisoned();
            queues
                .iter()
                .filter_map(|(signal, q)| {
                    let oldest = q.batches.front()?;
                    let age = oldest.accepted_at.elapsed();
                    (age >= self.flush_age(q.bytes)).then_some(*signal)
                })
                .collect()
        };
        let _guard = self.flush_lock.lock_or_poisoned();
        let mut flushed = HashMap::new();
        for signal in due {
            let rows = self.flush_signal_observed(signal, storage, None)?;
            if rows > 0 {
                flushed.insert(signal, rows);
            }
        }
        Ok(flushed)
    }

    pub fn flush_signal(&self, signal: Signal, storage: &Storage) -> anyhow::Result<usize> {
        let _guard = self.flush_lock.lock_or_poisoned();
        self.flush_signal_observed(signal, storage, None)
    }

    fn flush_signal_observed(
        &self,
        signal: Signal,
        storage: &Storage,
        metrics: Option<&Metrics>,
    ) -> anyhow::Result<usize> {
        let batches = self.drain_flush_batches(signal);

        let mut rows = 0;
        for idx in 0..batches.len() {
            let batch = &batches[idx];
            let started = Instant::now();
            let insert_result =
                storage.insert_arrow_records(signal, &batch.batch, batch.source_format);
            if let Some(metrics) = metrics {
                metrics.observe_phase_seconds(
                    signal.as_str(),
                    "storage_insert",
                    None,
                    started.elapsed().as_secs_f64(),
                );
            }
            match insert_result {
                Ok(committed) => {
                    rows += committed;
                }
                Err(err) => {
                    let committed_in_batch =
                        insert_committed_rows(&err).unwrap_or(0).min(batch.len());
                    rows += committed_in_batch;
                    let mut remaining = Vec::new();
                    if let Some(uncommitted) = batch.suffix(committed_in_batch) {
                        remaining.push(uncommitted);
                    }
                    remaining.extend(batches.into_iter().skip(idx + 1));
                    if let Some(metrics) = metrics {
                        metrics.inc(
                            "canardstack_ingest_flush_failures_total",
                            &[
                                ("signal", signal.as_str()),
                                ("reason", flush_failure_reason(&err)),
                            ],
                            1,
                        );
                    }
                    let remaining_rows: usize = remaining.iter().map(QueuedBatch::len).sum();
                    let committed_str = committed_in_batch.to_string();
                    let remaining_str = remaining_rows.to_string();
                    let err_str = err.to_string();
                    crate::log_event(
                        "error",
                        "ingest_flush_failed",
                        &[
                            ("signal", signal.as_str()),
                            ("committed_rows", &committed_str),
                            ("restored_rows", &remaining_str),
                            ("reason", flush_failure_reason(&err)),
                            ("error", &err_str),
                        ],
                    );
                    self.restore_batches(signal, remaining);
                    return Err(anyhow::Error::new(PartialFlushError {
                        committed_rows: rows,
                        signal,
                        source: err,
                    }));
                }
            }
        }
        if let Some(metrics) = metrics {
            metrics.inc(
                "canardstack_ingest_flush_rows_total",
                &[("signal", signal.as_str())],
                rows as u64,
            );
        }
        Ok(rows)
    }

    fn drain_flush_batches(&self, signal: Signal) -> Vec<QueuedBatch> {
        let mut queues = self.queues.lock_or_poisoned();
        let queue = queues.get_mut(&signal).unwrap();
        let mut drained = Vec::new();
        let mut remaining_rows = self.config.max_rows_per_flush;
        let mut remaining_bytes = self.config.max_bytes_per_flush;

        while remaining_rows > 0 && remaining_bytes > 0 {
            let Some(batch) = queue.batches.pop_front() else {
                break;
            };
            let original_rows = batch.len();
            if original_rows == 0 {
                continue;
            }
            let row_bytes = batch.approx_bytes.div_ceil(original_rows).max(1);
            let rows_by_bytes = (remaining_bytes / row_bytes).max(1);
            let take_rows = original_rows.min(remaining_rows).min(rows_by_bytes);
            if take_rows == original_rows {
                queue.rows = queue.rows.saturating_sub(original_rows);
                queue.bytes = queue.bytes.saturating_sub(batch.approx_bytes);
                remaining_rows = remaining_rows.saturating_sub(original_rows);
                remaining_bytes = remaining_bytes.saturating_sub(batch.approx_bytes);
                drained.push(batch);
            } else {
                let (taken, rest) = batch.split_at(take_rows);
                queue.rows = queue.rows.saturating_sub(taken.len());
                queue.bytes = queue.bytes.saturating_sub(taken.approx_bytes);
                queue.batches.push_front(rest);
                drained.push(taken);
                break;
            }
        }

        drained
    }

    pub fn snapshots(&self) -> Vec<IngestSnapshot> {
        let queues = self.queues.lock_or_poisoned();
        [
            Signal::Logs,
            Signal::Spans,
            Signal::MetricGauge,
            Signal::MetricSum,
        ]
        .into_iter()
        .map(|signal| {
            let q = queues.get(&signal).unwrap();
            let oldest_age_seconds = q
                .batches
                .front()
                .map(|b| b.accepted_at.elapsed().as_secs_f64())
                .unwrap_or(0.0);
            IngestSnapshot {
                signal: signal.as_str(),
                queued_rows: q.rows,
                queued_bytes: q.bytes,
                oldest_age_seconds,
                pressure: q.bytes as f64 / self.config.per_signal_queue_bytes as f64,
            }
        })
        .collect()
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

    fn enqueue_and_maybe_flush(
        &self,
        request_signal: Signal,
        batches: Vec<PendingBatch>,
        storage: &Storage,
        metrics: &Metrics,
    ) -> ApiResult<usize> {
        if batches.is_empty() {
            return Ok(0);
        }
        let accepted = batches.iter().map(|b| b.batch.num_rows()).sum();
        let started = Instant::now();
        let queue_result: ApiResult<Vec<Signal>> = (|| {
            let mut queues = self.queues.lock_or_poisoned();
            let process_bytes: usize = queues.values().map(|q| q.bytes).sum();
            let added_process_bytes = batches.iter().map(|b| b.approx_bytes).sum::<usize>();
            let mut added_by_signal: HashMap<Signal, usize> = HashMap::new();
            for batch in &batches {
                *added_by_signal.entry(batch.signal).or_default() += batch.approx_bytes;
            }
            for (signal, added_bytes) in &added_by_signal {
                let queue = queues.get(signal).unwrap();
                if queue.bytes + added_bytes > self.config.per_signal_queue_bytes {
                    let queued_bytes_str = queue.bytes.to_string();
                    let added_str = added_bytes.to_string();
                    let cap_str = self.config.per_signal_queue_bytes.to_string();
                    crate::log_event(
                        "warn",
                        "ingest_queue_full",
                        &[
                            ("signal", signal.as_str()),
                            ("queued_bytes", &queued_bytes_str),
                            ("incoming_bytes", &added_str),
                            ("cap_bytes", &cap_str),
                        ],
                    );
                    // 5s ≈ one flush tick.
                    return Err(ApiError::new(
                        429,
                        "signal_queue_full",
                        format!("{signal} queue is full"),
                    )
                    .with_retry_after(5));
                }
            }
            if process_bytes + added_process_bytes > self.config.process_ingest_bytes {
                let process_str = process_bytes.to_string();
                let added_str = added_process_bytes.to_string();
                let cap_str = self.config.process_ingest_bytes.to_string();
                crate::log_event(
                    "warn",
                    "ingest_process_memory_full",
                    &[
                        ("process_bytes", &process_str),
                        ("incoming_bytes", &added_str),
                        ("cap_bytes", &cap_str),
                    ],
                );
                return Err(ApiError::new(
                    429,
                    "process_ingest_memory_full",
                    "process ingest memory cap would be exceeded",
                )
                .with_retry_after(5));
            }
            for batch in batches {
                let queue = queues.get_mut(&batch.signal).unwrap();
                queue.rows += batch.batch.num_rows();
                queue.bytes += batch.approx_bytes;
                queue.batches.push_back(QueuedBatch {
                    batch: batch.batch,
                    source_format: batch.source_format,
                    accepted_at: Instant::now(),
                    approx_bytes: batch.approx_bytes,
                });
            }
            Ok([
                Signal::Logs,
                Signal::Spans,
                Signal::MetricGauge,
                Signal::MetricSum,
            ]
            .into_iter()
            .filter(|signal| {
                let queue = queues.get(signal).unwrap();
                queue.rows >= self.config.max_rows_per_flush
                    || queue.bytes >= self.config.max_bytes_per_flush
                    || queue
                        .batches
                        .front()
                        .map(|b| b.accepted_at.elapsed())
                        .unwrap_or(Duration::ZERO)
                        >= self.flush_age(queue.bytes)
            })
            .collect::<Vec<_>>())
        })();
        metrics.observe_phase_seconds(
            request_signal.as_str(),
            "queue_admission",
            None,
            started.elapsed().as_secs_f64(),
        );
        let flush_signals = queue_result?;

        if !flush_signals.is_empty() {
            match self.flush_lock.try_lock() {
                Ok(_guard) => {
                    for signal in flush_signals {
                        if let Err(err) = self.flush_signal_observed(signal, storage, Some(metrics))
                        {
                            if let Some((partial_signal, committed)) = partial_commit_info(&err) {
                                if committed > 0 {
                                    metrics.inc(
                                        "canardstack_ingest_partial_commit_rows_total",
                                        &[
                                            ("signal", partial_signal.as_str()),
                                            ("triggered_by", request_signal.as_str()),
                                        ],
                                        committed as u64,
                                    );
                                }
                            }
                            return Err(flush_error_to_api(err));
                        }
                    }
                }
                Err(_) => {
                    metrics.inc(
                        "canardstack_ingest_flush_skipped_total",
                        &[("reason", "flush_in_progress")],
                        1,
                    );
                }
            }
        }
        Ok(accepted)
    }

    fn restore_batches(&self, signal: Signal, batches: Vec<QueuedBatch>) {
        let mut queues = self.queues.lock_or_poisoned();
        let queue = queues.get_mut(&signal).unwrap();
        for batch in batches.into_iter().rev() {
            queue.rows += batch.len();
            queue.bytes += batch.approx_bytes;
            queue.batches.push_front(batch);
        }
    }

    fn flush_age(&self, queue_bytes: usize) -> Duration {
        let pressure = queue_bytes as f64 / self.config.per_signal_queue_bytes as f64;
        if pressure >= 0.70 {
            self.config.high_pressure_max_age
        } else {
            self.config.max_age
        }
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

fn flush_error_to_api(err: anyhow::Error) -> ApiError {
    if let Some(partial) = err.downcast_ref::<PartialFlushError>() {
        return ApiError::new(
            503,
            "storage_flush_failed",
            format!(
                "{} (best-effort: {} row(s) for signal={} accepted by catalog before failure; remote durability is not guaranteed in-process)",
                partial.source, partial.committed_rows, partial.signal
            ),
        );
    }
    ApiError::new(503, "storage_flush_failed", err.to_string())
}

fn insert_committed_rows(err: &anyhow::Error) -> Option<usize> {
    err.downcast_ref::<InsertRecordsError>()
        .map(|insert| insert.committed_rows)
}

fn flush_failure_reason(err: &anyhow::Error) -> &'static str {
    let lower = err.to_string().to_ascii_lowercase();
    if lower.contains("failed to pin block")
        || lower.contains("memory limit")
        || lower.contains("out of memory")
    {
        "duckdb_memory_limit"
    } else if lower.contains("database is locked") || lower.contains("conflict") {
        "duckdb_lock_conflict"
    } else {
        "storage_error"
    }
}

/// Inspect an error returned by `flush_signal` / `flush_all` / `flush_aged` and
/// return (signal, committed_rows) when the failure was a partial commit. The
/// signal lets callers label metrics correctly even when the failure surfaces
/// far from the original ingest call (e.g. admin-triggered flush, scheduler).
pub fn partial_commit_info(err: &anyhow::Error) -> Option<(Signal, usize)> {
    err.downcast_ref::<PartialFlushError>()
        .map(|p| (p.signal, p.committed_rows))
}

fn proportional_bytes(total_bytes: usize, rows: usize, total_rows: usize) -> usize {
    if rows == 0 || total_rows == 0 {
        return 0;
    }
    total_bytes.saturating_mul(rows).div_ceil(total_rows).max(1)
}

fn pending_batches(transformed: Transformed) -> Vec<PendingBatch> {
    let source_format = transformed.source_format;
    let mut batches = Vec::new();
    push_pending_arrow(&mut batches, Signal::Logs, transformed.logs, source_format);
    push_pending_arrow(
        &mut batches,
        Signal::Spans,
        transformed.spans,
        source_format,
    );
    push_pending_arrow(
        &mut batches,
        Signal::MetricGauge,
        transformed.gauge,
        source_format,
    );
    push_pending_arrow(
        &mut batches,
        Signal::MetricSum,
        transformed.sum,
        source_format,
    );
    batches
}

fn push_pending_arrow(
    batches: &mut Vec<PendingBatch>,
    signal: Signal,
    batch: Option<RecordBatch>,
    source_format: &'static str,
) {
    let Some(batch) = batch else {
        return;
    };
    if batch.num_rows() == 0 {
        return;
    }
    let approx_bytes = batch.get_array_memory_size().max(batch.num_rows());
    batches.push(PendingBatch {
        signal,
        batch,
        source_format,
        approx_bytes,
    });
}
