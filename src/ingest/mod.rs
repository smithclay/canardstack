use crate::config::Config;
use crate::metrics::Metrics;
use crate::otlp::{self, Transformed};
use crate::storage::Storage;
use crate::validation::{self, ApiError, ApiResult};
use crate::LockExt;
use anyhow::{Context, Result};
use serde::Serialize;
use serde_json::{json, Value};
use spool::{
    raw_spool_full_info, RawSpoolOptions, RawSpoolRecord, RawSpoolRecordId, RawSpoolWriter,
};
use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

mod admission;
mod flush;
mod queue;
pub mod spool;

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
    raw_spool: RawSpoolWriter,
    raw_spool_flush_refs: Arc<Mutex<BTreeMap<RawSpoolRecordId, RawSpoolFlushRef>>>,
    config: Config,
}

#[derive(Default)]
struct FlushSignal {
    requested: Mutex<bool>,
    ready: Condvar,
}

#[derive(Clone, Copy, Debug)]
struct RawSpoolFlushRef {
    signal: Signal,
    remaining_rows: usize,
}

impl Ingestor {
    pub fn new(config: Config) -> Result<Self> {
        let raw_spool = RawSpoolWriter::spawn(
            RawSpoolOptions {
                dir: config.raw_spool_dir.clone(),
                max_segment_bytes: config.raw_spool_max_segment_bytes as u64,
                max_record_bytes: config.raw_spool_max_record_bytes as u64,
                max_total_bytes: config.raw_spool_max_total_bytes as u64,
            },
            config.raw_spool_writer_queue_capacity,
            config.raw_spool_group_commit_records,
            config.raw_spool_group_commit_delay,
        )?;
        Ok(Self {
            queues: Arc::new(Mutex::new(HashMap::new())),
            flush_lock: Arc::new(Mutex::new(())),
            flush_signal: Arc::new(FlushSignal::default()),
            runtime_memory_reserved_bytes: Arc::new(AtomicUsize::new(0)),
            raw_spool,
            raw_spool_flush_refs: Arc::new(Mutex::new(BTreeMap::new())),
            config,
        })
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
        let raw_spool_id = match self.append_raw_spool(signal, headers, compressed_body, metrics) {
            Ok(id) => id,
            Err(err) => {
                metrics.ingest_request(signal, err.status, err.reason);
                return Err(err);
            }
        };
        let mut runtime_memory_reservation =
            match self.admit_runtime_memory(signal, headers, compressed_body.len(), metrics) {
                Ok(reservation) => reservation,
                Err(err) => {
                    self.checkpoint_raw_spool_terminal(
                        raw_spool_id,
                        signal,
                        "memory_rejected",
                        metrics,
                    )?;
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
        let body = match body_result {
            Ok(body) => body,
            Err(err) => {
                self.checkpoint_raw_spool_terminal(
                    raw_spool_id,
                    signal,
                    "decompress_rejected",
                    metrics,
                )?;
                return Err(err);
            }
        };
        if let Err(err) = validation::validate_body_size(body.len(), &self.config) {
            self.checkpoint_raw_spool_terminal(
                raw_spool_id,
                signal,
                "decoded_size_rejected",
                metrics,
            )?;
            return Err(err);
        }
        let started = Instant::now();
        #[cfg(feature = "otlp2records-observer")]
        let transformed_result = otlp::transform_observed(signal, headers, &body, metrics);
        #[cfg(not(feature = "otlp2records-observer"))]
        let transformed_result = otlp::transform(signal, headers, &body);
        metrics.observe_phase_seconds(
            signal.as_str(),
            "otlp_transform",
            None,
            started.elapsed().as_secs_f64(),
        );
        let transformed = match transformed_result {
            Ok(transformed) => transformed,
            Err(err) => {
                self.checkpoint_raw_spool_terminal(
                    raw_spool_id,
                    signal,
                    "transform_rejected",
                    metrics,
                )?;
                return Err(err);
            }
        };
        for (output_signal, rows) in transformed_rows_by_signal(&transformed) {
            metrics.inc(
                "canardstack_ingest_transformed_rows_total",
                &[
                    ("signal", output_signal.as_str()),
                    ("request_signal", signal.as_str()),
                ],
                rows as u64,
            );
        }
        let started = Instant::now();
        let skew_result = self.validate_skew(&transformed);
        metrics.observe_phase_seconds(
            signal.as_str(),
            "timestamp_validation",
            None,
            started.elapsed().as_secs_f64(),
        );
        if let Err(err) = skew_result {
            self.checkpoint_raw_spool_terminal(
                raw_spool_id,
                signal,
                "timestamp_rejected",
                metrics,
            )?;
            return Err(err);
        }

        let request_bytes = compressed_body.len();
        let unsupported_histograms = transformed.unsupported_histograms;
        let mut batches = queue::pending_batches(transformed);
        let enqueued_totals = pending_batch_totals(&batches);
        let pending_bytes = batches.iter().map(|b| b.approx_bytes).sum::<usize>();
        let peak_bytes = request_bytes
            .saturating_add(body.len())
            .saturating_add(pending_bytes);
        if let Err(err) = runtime_memory_reservation.reserve_at_least(peak_bytes, signal, metrics) {
            self.checkpoint_raw_spool_terminal(raw_spool_id, signal, "memory_rejected", metrics)?;
            metrics.ingest_request(signal, err.status, err.reason);
            return Err(err);
        }
        if batches.is_empty() {
            self.checkpoint_raw_spool_terminal(raw_spool_id, signal, "transform_empty", metrics)?;
        } else {
            self.track_raw_spool_batches(raw_spool_id, signal, &mut batches);
        }
        let accepted = match self.enqueue(signal, batches, metrics) {
            Ok(accepted) => accepted,
            Err(err) => {
                self.untrack_raw_spool_record(raw_spool_id);
                self.checkpoint_raw_spool_terminal(
                    raw_spool_id,
                    signal,
                    "queue_rejected",
                    metrics,
                )?;
                self.record_queue_metrics(metrics);
                metrics.ingest_request(signal, err.status, err.reason);
                return Err(err);
            }
        };
        metrics.ingest_request(signal, 202, "accepted");
        self.record_accepted_body_metrics(signal, headers, request_bytes, body.len(), metrics);
        metrics.inc(
            "canardstack_ingest_records_total",
            &[("signal", signal.as_str())],
            accepted as u64,
        );
        for (output_signal, (rows, bytes)) in enqueued_totals {
            metrics.inc(
                "canardstack_ingest_enqueued_rows_total",
                &[("signal", output_signal.as_str())],
                rows as u64,
            );
            metrics.inc(
                "canardstack_ingest_enqueued_bytes_total",
                &[("signal", output_signal.as_str())],
                bytes as u64,
            );
        }
        self.record_queue_metrics(metrics);

        Ok(json!({
            "accepted": true,
            "records": accepted,
            "acknowledgement": "durably_spooled_locally_at_least_once",
            "unsupported_histograms": unsupported_histograms
        }))
    }

    pub fn replay_raw_spool(&self, storage: &Storage, metrics: &Metrics) -> Result<usize> {
        let pending = self
            .raw_spool
            .recover_pending()
            .context("recover raw spool pending records")?;
        let mut replayed = 0usize;
        for recovered in pending {
            let mut headers = HashMap::new();
            headers.insert(
                "content-type".to_string(),
                recovered.record.content_type.clone(),
            );
            if let Some(encoding) = &recovered.record.content_encoding {
                headers.insert("content-encoding".to_string(), encoding.clone());
            }
            metrics.inc(
                "canardstack_raw_spool_replayed_records_total",
                &[
                    ("signal", recovered.record.signal.as_str()),
                    ("status", "attempted"),
                ],
                1,
            );
            match self.ingest_replayed_raw_record(
                recovered.id,
                recovered.record.signal,
                &headers,
                &recovered.record.compressed_body,
                storage,
                metrics,
            ) {
                Ok(()) => {
                    replayed += 1;
                    metrics.inc(
                        "canardstack_raw_spool_replayed_records_total",
                        &[
                            ("signal", recovered.record.signal.as_str()),
                            ("status", "ok"),
                        ],
                        1,
                    );
                }
                Err(err) => {
                    metrics.inc(
                        "canardstack_raw_spool_replayed_records_total",
                        &[
                            ("signal", recovered.record.signal.as_str()),
                            ("status", "failed"),
                        ],
                        1,
                    );
                    return Err(err).context("replay raw spool record");
                }
            }
        }
        self.record_raw_spool_metrics(metrics);
        Ok(replayed)
    }

    fn ingest_replayed_raw_record(
        &self,
        raw_spool_id: RawSpoolRecordId,
        signal: Signal,
        headers: &HashMap<String, String>,
        compressed_body: &[u8],
        storage: &Storage,
        metrics: &Metrics,
    ) -> Result<()> {
        validation::validate_body_size(compressed_body.len(), &self.config)
            .map_err(|err| anyhow::anyhow!(err.message.clone()))?;
        validation::validate_content_type(headers)
            .map_err(|err| anyhow::anyhow!(err.message.clone()))?;
        if !storage.accepts_memory_ingest() || self.config.force_dependency_unhealthy {
            anyhow::bail!("storage dependency is unhealthy");
        }
        let mut runtime_memory_reservation = self
            .admit_runtime_memory(signal, headers, compressed_body.len(), metrics)
            .map_err(|err| anyhow::anyhow!(err.message.clone()))?;
        let body = otlp::decompress_if_needed(headers, compressed_body, self.config.max_body_bytes)
            .map_err(|err| anyhow::anyhow!(err.message.clone()))?;
        validation::validate_body_size(body.len(), &self.config)
            .map_err(|err| anyhow::anyhow!(err.message.clone()))?;
        #[cfg(feature = "otlp2records-observer")]
        let transformed = otlp::transform_observed(signal, headers, &body, metrics)
            .map_err(|err| anyhow::anyhow!(err.message.clone()))?;
        #[cfg(not(feature = "otlp2records-observer"))]
        let transformed = otlp::transform(signal, headers, &body)
            .map_err(|err| anyhow::anyhow!(err.message.clone()))?;
        self.validate_skew(&transformed)
            .map_err(|err| anyhow::anyhow!(err.message.clone()))?;
        let request_bytes = compressed_body.len();
        let mut batches = queue::pending_batches(transformed);
        let pending_bytes = batches.iter().map(|b| b.approx_bytes).sum::<usize>();
        let peak_bytes = request_bytes
            .saturating_add(body.len())
            .saturating_add(pending_bytes);
        runtime_memory_reservation
            .reserve_at_least(peak_bytes, signal, metrics)
            .map_err(|err| anyhow::anyhow!(err.message.clone()))?;
        if batches.is_empty() {
            self.checkpoint_raw_spool(raw_spool_id, signal, "replay_empty", Some(metrics))?;
            return Ok(());
        }
        self.track_raw_spool_batches(raw_spool_id, signal, &mut batches);
        self.enqueue(signal, batches, metrics).map_err(|err| {
            self.untrack_raw_spool_record(raw_spool_id);
            anyhow::anyhow!(err.message.clone())
        })?;
        Ok(())
    }

    fn track_raw_spool_batches(
        &self,
        id: RawSpoolRecordId,
        signal: Signal,
        batches: &mut [queue::PendingBatch],
    ) {
        if batches.is_empty() {
            return;
        }
        for batch in batches.iter_mut() {
            batch.raw_spool_id = Some(id);
        }
        self.raw_spool_flush_refs.lock_or_poisoned().insert(
            id,
            RawSpoolFlushRef {
                signal,
                remaining_rows: batches.iter().map(|batch| batch.batch.num_rows()).sum(),
            },
        );
    }

    fn untrack_raw_spool_record(&self, id: RawSpoolRecordId) {
        self.raw_spool_flush_refs.lock_or_poisoned().remove(&id);
    }

    fn mark_raw_spool_batches_storage_committed(
        &self,
        sets: &[(queue::QueueKey, Vec<queue::QueuedBatch>)],
        metrics: Option<&Metrics>,
    ) -> Result<()> {
        let mut committed_counts = BTreeMap::<RawSpoolRecordId, usize>::new();
        for (_, batches) in sets {
            for batch in batches {
                if let Some(id) = batch.raw_spool_id {
                    *committed_counts.entry(id).or_default() += batch.len();
                }
            }
        }
        if committed_counts.is_empty() {
            return Ok(());
        }

        let mut ready_to_checkpoint = Vec::new();
        {
            let mut refs = self.raw_spool_flush_refs.lock_or_poisoned();
            for (id, committed_rows) in committed_counts {
                let Some(tracked) = refs.get_mut(&id) else {
                    continue;
                };
                if committed_rows >= tracked.remaining_rows {
                    ready_to_checkpoint.push((id, tracked.signal));
                } else {
                    tracked.remaining_rows -= committed_rows;
                }
            }
        }

        for (id, signal) in &ready_to_checkpoint {
            self.checkpoint_raw_spool(*id, *signal, "storage_committed", metrics)?;
        }
        if !ready_to_checkpoint.is_empty() {
            let mut refs = self.raw_spool_flush_refs.lock_or_poisoned();
            for (id, _) in ready_to_checkpoint {
                refs.remove(&id);
            }
        }
        Ok(())
    }

    fn append_raw_spool(
        &self,
        signal: Signal,
        headers: &HashMap<String, String>,
        compressed_body: &[u8],
        metrics: &Metrics,
    ) -> ApiResult<RawSpoolRecordId> {
        let content_type = headers.get("content-type").cloned().unwrap_or_default();
        let content_encoding = headers.get("content-encoding").cloned();
        let started = Instant::now();
        let record = RawSpoolRecord::new(signal, content_type, content_encoding, compressed_body);
        let result = self.raw_spool.append(record);
        metrics.observe_phase_seconds(
            signal.as_str(),
            "raw_spool_append",
            None,
            started.elapsed().as_secs_f64(),
        );
        match result {
            Ok(id) => {
                metrics.inc(
                    "canardstack_raw_spool_records_total",
                    &[("signal", signal.as_str()), ("status", "spooled")],
                    1,
                );
                metrics.inc(
                    "canardstack_raw_spool_bytes_total",
                    &[("signal", signal.as_str())],
                    compressed_body.len() as u64,
                );
                Ok(id)
            }
            Err(err) => {
                if raw_spool_full_info(&err).is_some() {
                    metrics.inc(
                        "canardstack_raw_spool_records_total",
                        &[("signal", signal.as_str()), ("status", "full")],
                        1,
                    );
                    Err(ApiError::new(
                        429,
                        "raw_spool_full",
                        "raw ingest spool is full",
                    ))
                } else {
                    metrics.inc(
                        "canardstack_raw_spool_records_total",
                        &[("signal", signal.as_str()), ("status", "error")],
                        1,
                    );
                    Err(ApiError::new(
                        503,
                        "raw_spool_unavailable",
                        "raw ingest spool is unavailable",
                    )
                    .with_retry_after(10))
                }
            }
        }
    }

    fn checkpoint_raw_spool_terminal(
        &self,
        id: RawSpoolRecordId,
        signal: Signal,
        reason: &'static str,
        metrics: &Metrics,
    ) -> ApiResult<()> {
        self.checkpoint_raw_spool(id, signal, reason, Some(metrics))
            .map_err(|err| {
                ApiError::new(
                    503,
                    "raw_spool_checkpoint_failed",
                    format!("raw ingest spool checkpoint failed: {err}"),
                )
                .with_retry_after(10)
            })
    }

    fn checkpoint_raw_spool(
        &self,
        id: RawSpoolRecordId,
        signal: Signal,
        reason: &'static str,
        metrics: Option<&Metrics>,
    ) -> Result<()> {
        self.raw_spool
            .mark_committed(id)
            .context("checkpoint raw spool record")?;
        if let Some(metrics) = metrics {
            metrics.inc(
                "canardstack_raw_spool_checkpointed_records_total",
                &[("signal", signal.as_str()), ("reason", reason)],
                1,
            );
        }
        Ok(())
    }

    pub fn raw_spool_stats(&self) -> Result<spool::RawSpoolStats> {
        self.raw_spool.stats()
    }

    pub fn record_raw_spool_metrics(&self, metrics: &Metrics) {
        if let Ok(stats) = self.raw_spool.stats() {
            metrics.gauge(
                "canardstack_raw_spool_segment_bytes",
                &[],
                stats.segment_bytes as f64,
            );
            metrics.gauge(
                "canardstack_raw_spool_segments",
                &[],
                stats.segment_count as f64,
            );
            metrics.gauge(
                "canardstack_raw_spool_pending_records",
                &[],
                stats.pending_records as f64,
            );
            metrics.gauge(
                "canardstack_raw_spool_pending_bytes",
                &[],
                stats.pending_bytes as f64,
            );
        }
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
            metrics.gauge_max(
                "canardstack_ingest_queue_rows_max",
                &[("signal", snapshot.signal)],
                snapshot.queued_rows as f64,
            );
            metrics.gauge(
                "canardstack_ingest_queue_bytes",
                &[("signal", snapshot.signal)],
                snapshot.queued_bytes as f64,
            );
            metrics.gauge_max(
                "canardstack_ingest_queue_bytes_max",
                &[("signal", snapshot.signal)],
                snapshot.queued_bytes as f64,
            );
            metrics.gauge(
                "canardstack_ingest_queue_oldest_age_seconds",
                &[("signal", snapshot.signal)],
                snapshot.oldest_age_seconds,
            );
            metrics.gauge_max(
                "canardstack_ingest_queue_oldest_age_seconds_max",
                &[("signal", snapshot.signal)],
                snapshot.oldest_age_seconds,
            );
            metrics.gauge(
                "canardstack_ingest_queue_pressure",
                &[("signal", snapshot.signal)],
                snapshot.pressure,
            );
            metrics.gauge_max(
                "canardstack_ingest_queue_pressure_max",
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

    fn record_accepted_body_metrics(
        &self,
        signal: Signal,
        headers: &HashMap<String, String>,
        request_bytes: usize,
        decoded_bytes: usize,
        metrics: &Metrics,
    ) {
        let encoding = headers
            .get("content-encoding")
            .map(String::as_str)
            .unwrap_or("identity");
        metrics.inc(
            "canardstack_ingest_request_bytes_total",
            &[("signal", signal.as_str()), ("encoding", encoding)],
            request_bytes as u64,
        );
        metrics.inc(
            "canardstack_ingest_decoded_bytes_total",
            &[("signal", signal.as_str()), ("encoding", encoding)],
            decoded_bytes as u64,
        );
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

fn transformed_rows_by_signal(transformed: &Transformed) -> Vec<(Signal, usize)> {
    [
        (Signal::Logs, transformed.logs.as_ref()),
        (Signal::Spans, transformed.spans.as_ref()),
        (Signal::MetricGauge, transformed.gauge.as_ref()),
        (Signal::MetricSum, transformed.sum.as_ref()),
    ]
    .into_iter()
    .filter_map(|(signal, batch)| {
        let rows = batch.map(|batch| batch.num_rows()).unwrap_or(0);
        (rows > 0).then_some((signal, rows))
    })
    .collect()
}

fn pending_batch_totals(batches: &[queue::PendingBatch]) -> BTreeMap<Signal, (usize, usize)> {
    let mut totals = BTreeMap::new();
    for batch in batches {
        let (rows, bytes) = totals.entry(batch.key.signal).or_insert((0, 0));
        *rows += batch.batch.num_rows();
        *bytes += batch.approx_bytes;
    }
    totals
}
