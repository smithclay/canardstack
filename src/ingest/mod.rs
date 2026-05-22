use crate::config::Config;
use crate::lanes::{FreshnessInputs, LaneController};
use crate::metrics::Metrics;
use crate::otlp::{self, Transformed};
use crate::storage::{ArrowBatchInsert, ArrowBatchInsertTiming, Storage};
use crate::validation::{self, ApiError, ApiResult};
use crate::LockExt;
use anyhow::{Context, Result};
use serde::Serialize;
use serde_json::{json, Value};
use spool::{FlushRef, Options, RecordId, Writer};
use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::sync::atomic::AtomicUsize;
use std::sync::mpsc::TrySendError;
use std::sync::{Arc, Mutex};
use std::time::Instant;

mod admission;
mod batches;
pub mod spool;
mod worker;

pub use batches::IngestSnapshot;
use worker::IngestWorkerPool;

#[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize)]
pub enum Signal {
    Logs,
    Spans,
    MetricGauge,
    MetricSum,
}

impl Signal {
    pub const ALL: [Signal; 4] = [
        Signal::Logs,
        Signal::Spans,
        Signal::MetricGauge,
        Signal::MetricSum,
    ];

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

pub struct Ingestor {
    runtime_memory_reserved_bytes: Arc<AtomicUsize>,
    inflight: Arc<admission::InflightBytes>,
    raw_spools: BTreeMap<Signal, Writer>,
    metric_raw_spool_next: AtomicUsize,
    raw_spool_flush_refs: Arc<Mutex<BTreeMap<(Signal, RecordId), FlushRef>>>,
    ingest_workers: Mutex<Option<IngestWorkerPool>>,
    worker_queue_slots: Arc<worker::WorkerQueueSlots>,
    config: Config,
}

pub(in crate::ingest) struct SpooledIngestWork {
    pub(in crate::ingest) signal: Signal,
    headers: HashMap<String, String>,
    compressed_body: Vec<u8>,
    raw_spool_ref: spool::AppendRef,
    inflight_reservation: admission::InflightReservation,
    runtime_memory_reservation: admission::RuntimeMemoryReservation,
    worker_queue_reservation: worker::WorkerQueueReservation,
    pub(in crate::ingest) metrics: Arc<Metrics>,
}

impl Ingestor {
    pub fn new(config: Config) -> Result<Self> {
        let mut raw_spools = BTreeMap::new();
        for signal in all_signals() {
            raw_spools.insert(signal, spawn_raw_spool_writer(&config, signal)?);
        }
        Ok(Self {
            runtime_memory_reserved_bytes: Arc::new(AtomicUsize::new(0)),
            inflight: Arc::new(admission::InflightBytes::new(&config)),
            raw_spools,
            metric_raw_spool_next: AtomicUsize::new(0),
            raw_spool_flush_refs: Arc::new(Mutex::new(BTreeMap::new())),
            ingest_workers: Mutex::new(None),
            worker_queue_slots: Arc::new(worker::WorkerQueueSlots::new(
                config.ingest_buffer_capacity,
            )),
            config,
        })
    }

    pub fn ingest(
        &self,
        signal: Signal,
        headers: &HashMap<String, String>,
        compressed_body: Vec<u8>,
        storage: &Storage,
        lanes: &LaneController,
        metrics: Arc<Metrics>,
    ) -> ApiResult<Value> {
        let async_metrics = Arc::clone(&metrics);
        let metrics = metrics.as_ref();
        if let Err(err) = validation::validate_body_size(compressed_body.len(), &self.config) {
            metrics.ingest_request(signal, err.status, err.reason);
            return Err(err);
        }
        if let Err(err) = validation::validate_content_type(headers) {
            metrics.ingest_request(signal, err.status, err.reason);
            return Err(err);
        }
        if !storage.accepts_memory_ingest() || self.config.force_dependency_unhealthy {
            metrics.ingest_request(signal, 503, "dependency_unhealthy");
            return Err(ApiError::new(
                503,
                "dependency_unhealthy",
                "storage dependency is unhealthy",
            )
            .with_retry_after(10));
        }
        let mut inflight_reservation = match self.reserve_inflight(
            signal,
            headers,
            compressed_body.len(),
            storage,
            lanes,
            metrics,
        ) {
            Ok(reservation) => reservation,
            Err(err) => {
                self.record_inflight_metrics(metrics);
                metrics.ingest_request(signal, err.status, err.reason);
                return Err(err);
            }
        };
        let runtime_memory_reservation =
            match self.admit_runtime_memory(signal, headers, compressed_body.len(), metrics) {
                Ok(reservation) => reservation,
                Err(err) => {
                    inflight_reservation.release();
                    self.record_inflight_metrics(metrics);
                    metrics.ingest_request(signal, err.status, err.reason);
                    return Err(err);
                }
            };
        let mut worker_queue_reservation = match self.reserve_worker_queue(signal, metrics) {
            Ok(reservation) => reservation,
            Err(err) => {
                inflight_reservation.release();
                self.record_inflight_metrics(metrics);
                metrics.ingest_request(signal, err.status, err.reason);
                return Err(err);
            }
        };
        let (raw_spool_ref, compressed_body) =
            match self.append_raw_spool(signal, headers, compressed_body, metrics) {
                Ok(appended) => appended,
                Err(err) => {
                    inflight_reservation.release();
                    worker_queue_reservation.release();
                    self.record_worker_queue_metrics(metrics);
                    metrics.ingest_request(signal, err.status, err.reason);
                    return Err(err);
                }
            };
        let spooled = SpooledIngestWork {
            signal,
            headers: headers.clone(),
            compressed_body,
            raw_spool_ref,
            inflight_reservation,
            runtime_memory_reservation,
            worker_queue_reservation,
            metrics: async_metrics,
        };
        self.dispatch_ingest_work(spooled, metrics, true)
    }

    pub(in crate::ingest) fn process_spooled_ingest(
        &self,
        work: SpooledIngestWork,
        storage: &Storage,
    ) -> ApiResult<()> {
        let SpooledIngestWork {
            signal,
            headers,
            compressed_body,
            raw_spool_ref,
            mut inflight_reservation,
            mut runtime_memory_reservation,
            worker_queue_reservation: _worker_queue_reservation,
            metrics,
        } = work;
        let metrics_arc = metrics;
        let metrics = metrics_arc.as_ref();
        let started = Instant::now();
        let body_result =
            otlp::decompress_if_needed(&headers, &compressed_body, self.config.max_body_bytes);
        metrics.observe_phase_seconds(
            signal.as_str(),
            "decompress",
            None,
            started.elapsed().as_secs_f64(),
        );
        let body = match body_result {
            Ok(body) => body,
            Err(err) => {
                inflight_reservation.release();
                let reason = if err.reason == "payload_too_large" {
                    "body_size_invalid"
                } else {
                    "decode_failed"
                };
                self.checkpoint_raw_spool_terminal(raw_spool_ref, signal, reason, metrics)?;
                return Err(err);
            }
        };
        let decoded_body_materialized_bytes = match &body {
            std::borrow::Cow::Borrowed(_) => 0,
            std::borrow::Cow::Owned(bytes) => bytes.len(),
        };
        if let Err(err) = validation::validate_body_size(body.len(), &self.config) {
            inflight_reservation.release();
            self.checkpoint_raw_spool_terminal(
                raw_spool_ref,
                signal,
                "body_size_invalid",
                metrics,
            )?;
            return Err(err);
        }
        let started = Instant::now();
        #[cfg(feature = "otlp2records-observer")]
        let transformed_result = otlp::transform_observed(signal, &headers, &body, metrics);
        #[cfg(not(feature = "otlp2records-observer"))]
        let transformed_result = otlp::transform(signal, &headers, &body);
        metrics.observe_phase_seconds(
            signal.as_str(),
            "otlp_transform",
            None,
            started.elapsed().as_secs_f64(),
        );
        let transformed = match transformed_result {
            Ok(transformed) => transformed,
            Err(err) => {
                inflight_reservation.release();
                self.checkpoint_raw_spool_terminal(
                    raw_spool_ref,
                    signal,
                    "transform_failed",
                    metrics,
                )?;
                return Err(err);
            }
        };
        let unsupported_histograms = transformed.unsupported_histograms;
        let started = Instant::now();
        let skew_result = self.validate_skew(&transformed);
        metrics.observe_phase_seconds(
            signal.as_str(),
            "timestamp_validation",
            None,
            started.elapsed().as_secs_f64(),
        );
        if let Err(err) = skew_result {
            inflight_reservation.release();
            self.checkpoint_raw_spool_terminal(
                raw_spool_ref,
                signal,
                "timestamp_rejected",
                metrics,
            )?;
            return Err(err);
        }
        let decoded_body_len = body.len();
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

        let request_bytes = compressed_body.len();
        let batches = batches::pending_batches(transformed);
        let buffered_totals = pending_batch_totals(&batches);
        // Correct the admission estimate to the exact buffered Arrow bytes. The
        // request is already durably spooled, so this is infallible accounting,
        // never a late rejection.
        inflight_reservation.adjust(admission::inflight_bytes_by_signal(&batches));
        let pending_bytes = batches.iter().map(|b| b.approx_bytes).sum::<usize>();
        let peak_bytes = request_bytes
            .saturating_add(decoded_body_materialized_bytes)
            .saturating_add(pending_bytes);
        if let Err(err) = runtime_memory_reservation.reserve_at_least(peak_bytes, signal, metrics) {
            inflight_reservation.release();
            self.checkpoint_raw_spool_terminal(raw_spool_ref, signal, "memory_rejected", metrics)?;
            return Err(err);
        }
        if batches.is_empty() {
            inflight_reservation.release();
            self.checkpoint_raw_spool_terminal(raw_spool_ref, signal, "transform_empty", metrics)?;
            return Ok(());
        }

        let inserts = batches
            .iter()
            .filter(|batch| batch.batch.num_rows() > 0)
            .map(|batch| ArrowBatchInsert {
                table: batch.signal,
                batch: &batch.batch,
                source_format: batch.source_format,
            })
            .collect::<Vec<_>>();
        let insert_started = Instant::now();
        let insert_result = storage.insert_arrow_batches(&inserts);
        metrics.observe_phase_seconds(
            signal.as_str(),
            "storage_insert",
            None,
            insert_started.elapsed().as_secs_f64(),
        );
        let insert = match insert_result {
            Ok(result) => result,
            Err(err) => {
                // The rows never reached the immutable buffer and the raw-spool
                // record was not tracked, so it stays pending and replays on a
                // future restart (at-least-once). Release the admission credit
                // and surface a retryable dependency error.
                inflight_reservation.release();
                self.record_inflight_metrics(metrics);
                metrics.inc(
                    "canardstack_ingest_storage_insert_total",
                    &[("signal", signal.as_str()), ("status", "error")],
                    1,
                );
                tracing::warn!(
                    event = "ingest_storage_insert_failed",
                    signal = signal.as_str(),
                    error = %err
                );
                return Err(
                    ApiError::new(503, "storage_insert_failed", "storage insert failed")
                        .with_retry_after(5),
                );
            }
        };
        observe_storage_timings(metrics, &insert.timings);
        metrics.inc(
            "canardstack_ingest_storage_insert_total",
            &[("signal", signal.as_str()), ("status", "ok")],
            1,
        );

        // Rows are now in the immutable buffer. Track the raw-spool record so the
        // scheduler checkpoints it after the next durable seal, then release the
        // admission credit (buffer occupancy is now reflected as buffered bytes
        // for freshness, not as a held queue credit).
        self.track_raw_spool_record(raw_spool_ref, signal);
        inflight_reservation.release();

        let accepted = buffered_totals
            .values()
            .map(|(rows, _)| *rows)
            .sum::<usize>();
        self.record_accepted_body_metrics(
            signal,
            &headers,
            request_bytes,
            decoded_body_len,
            decoded_body_materialized_bytes,
            metrics,
        );
        metrics.inc(
            "canardstack_ingest_records_total",
            &[("signal", signal.as_str())],
            accepted as u64,
        );
        if unsupported_histograms > 0 {
            metrics.inc(
                "canardstack_ingest_unsupported_histograms_total",
                &[("signal", signal.as_str())],
                unsupported_histograms as u64,
            );
        }
        for (output_signal, (rows, bytes)) in buffered_totals {
            metrics.inc(
                "canardstack_ingest_buffered_rows_total",
                &[("signal", output_signal.as_str())],
                rows as u64,
            );
            metrics.inc(
                "canardstack_ingest_buffered_bytes_total",
                &[("signal", output_signal.as_str())],
                bytes as u64,
            );
        }

        Ok(())
    }

    fn dispatch_ingest_work(
        &self,
        work: SpooledIngestWork,
        metrics: &Metrics,
        accept_after_spool: bool,
    ) -> ApiResult<Value> {
        let signal = work.signal;
        // Round-robin to the first worker with buffer space. On a successful send
        // the function returns directly from inside the loop; only the
        // all-workers-full / all-disconnected paths fall through to the rejection
        // handling below (so `work` is always still owned there).
        let send_err = {
            let mut pool = self.ingest_workers.lock_or_poisoned();
            let Some(dispatcher) = pool.as_mut() else {
                return Err(ApiError::new(
                    503,
                    "ingest_workers_unavailable",
                    "ingest workers are not available",
                )
                .with_retry_after(5));
            };
            if dispatcher.commands.is_empty() {
                return Err(ApiError::new(
                    503,
                    "ingest_workers_unavailable",
                    "ingest workers are stopped",
                )
                .with_retry_after(5));
            }
            let start = dispatcher.next_worker % dispatcher.commands.len();
            let mut work = work;
            let mut disconnected = false;
            for offset in 0..dispatcher.commands.len() {
                let worker_idx = (start + offset) % dispatcher.commands.len();
                match dispatcher.commands[worker_idx].try_send(work) {
                    Ok(()) => {
                        dispatcher.next_worker = worker_idx.wrapping_add(1);
                        metrics.ingest_request(signal, 202, "accepted");
                        self.record_worker_queue_metrics(metrics);
                        metrics.inc(
                            "canardstack_ingest_requests_queued_total",
                            &[("signal", signal.as_str()), ("status", "queued")],
                            1,
                        );
                        return Ok(json!({
                            "accepted": true,
                            "acknowledgement": "locally_spooled"
                        }));
                    }
                    Err(TrySendError::Full(returned)) => {
                        work = returned;
                    }
                    Err(TrySendError::Disconnected(returned)) => {
                        disconnected = true;
                        work = returned;
                    }
                }
            }
            if disconnected {
                TrySendError::Disconnected(work)
            } else {
                TrySendError::Full(work)
            }
        };
        match send_err {
            TrySendError::Full(work) => {
                let SpooledIngestWork {
                    mut inflight_reservation,
                    mut worker_queue_reservation,
                    ..
                } = work;
                inflight_reservation.release();
                worker_queue_reservation.release();
                self.record_inflight_metrics(metrics);
                self.record_worker_queue_metrics(metrics);
                metrics.inc(
                    "canardstack_ingest_requests_queued_total",
                    &[("signal", signal.as_str()), ("status", "buffer_full")],
                    1,
                );
                if accept_after_spool {
                    metrics.ingest_request(signal, 202, "accepted_pending_replay");
                    Ok(json!({
                        "accepted": true,
                        "acknowledgement": "locally_spooled"
                    }))
                } else {
                    Err(
                        ApiError::new(429, "ingest_buffer_full", "ingest worker buffer is full")
                            .with_retry_after(5),
                    )
                }
            }
            TrySendError::Disconnected(work) => {
                let SpooledIngestWork {
                    mut inflight_reservation,
                    mut worker_queue_reservation,
                    ..
                } = work;
                inflight_reservation.release();
                worker_queue_reservation.release();
                self.record_worker_queue_metrics(metrics);
                metrics.inc(
                    "canardstack_ingest_requests_queued_total",
                    &[("signal", signal.as_str()), ("status", "disconnected")],
                    1,
                );
                if accept_after_spool {
                    metrics.ingest_request(signal, 202, "accepted_pending_replay");
                    Ok(json!({
                        "accepted": true,
                        "acknowledgement": "locally_spooled"
                    }))
                } else {
                    Err(ApiError::new(
                        503,
                        "ingest_workers_unavailable",
                        "ingest workers are stopped",
                    )
                    .with_retry_after(5))
                }
            }
        }
    }

    fn reserve_worker_queue(
        &self,
        signal: Signal,
        metrics: &Metrics,
    ) -> ApiResult<worker::WorkerQueueReservation> {
        {
            let pool = self.ingest_workers.lock_or_poisoned();
            let Some(dispatcher) = pool.as_ref() else {
                metrics.inc(
                    "canardstack_ingest_requests_queued_total",
                    &[
                        ("signal", signal.as_str()),
                        ("status", "workers_unavailable"),
                    ],
                    1,
                );
                return Err(ApiError::new(
                    503,
                    "ingest_workers_unavailable",
                    "ingest workers are not available",
                )
                .with_retry_after(5));
            };
            if dispatcher.commands.is_empty() {
                metrics.inc(
                    "canardstack_ingest_requests_queued_total",
                    &[
                        ("signal", signal.as_str()),
                        ("status", "workers_unavailable"),
                    ],
                    1,
                );
                return Err(ApiError::new(
                    503,
                    "ingest_workers_unavailable",
                    "ingest workers are stopped",
                )
                .with_retry_after(5));
            }
        }
        let reservation = self.worker_queue_slots.reserve()?;
        self.record_worker_queue_metrics(metrics);
        Ok(reservation)
    }

    /// Single ingest admission gate: project visibility through the lane
    /// controller (the freshness-first authority), then take a per-signal
    /// in-flight reservation as a cheap isolation ceiling. Both run before the
    /// durable raw-spool append, so a rejection never spools.
    fn reserve_inflight(
        &self,
        signal: Signal,
        headers: &HashMap<String, String>,
        compressed_body_bytes: usize,
        storage: &Storage,
        lanes: &LaneController,
        metrics: &Metrics,
    ) -> ApiResult<admission::InflightReservation> {
        let estimate = self.inflight.estimate_for_request(
            signal,
            headers,
            compressed_body_bytes,
            self.config.max_body_bytes,
        );
        let mut inputs = self.lane_freshness_inputs(storage);
        inputs.incoming_bytes = estimate.values().sum::<usize>();
        lanes.admit_ingest(inputs, metrics)?;
        self.inflight.reserve(estimate)
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
        let capacity = self.inflight.capacity_bytes();
        Signal::ALL
            .into_iter()
            .map(|signal| {
                let inflight_bytes = self.inflight.signal_bytes(signal);
                IngestSnapshot {
                    signal: signal.as_str(),
                    inflight_bytes,
                    inflight_capacity_bytes: capacity,
                    pressure: if capacity == 0 {
                        0.0
                    } else {
                        inflight_bytes as f64 / capacity as f64
                    },
                }
            })
            .collect()
    }

    pub fn record_inflight_metrics(&self, metrics: &Metrics) {
        for snapshot in self.snapshots() {
            metrics.gauge(
                "canardstack_ingest_inflight_bytes",
                &[("signal", snapshot.signal)],
                snapshot.inflight_bytes as f64,
            );
            metrics.gauge_max(
                "canardstack_ingest_inflight_bytes_max",
                &[("signal", snapshot.signal)],
                snapshot.inflight_bytes as f64,
            );
            metrics.gauge(
                "canardstack_ingest_inflight_pressure",
                &[("signal", snapshot.signal)],
                snapshot.pressure,
            );
            metrics.gauge_max(
                "canardstack_ingest_inflight_pressure_max",
                &[("signal", snapshot.signal)],
                snapshot.pressure,
            );
            metrics.gauge(
                "canardstack_ingest_inflight_capacity_bytes",
                &[("signal", snapshot.signal)],
                snapshot.inflight_capacity_bytes as f64,
            );
        }
        self.record_worker_queue_metrics(metrics);
    }

    pub fn record_worker_queue_metrics(&self, metrics: &Metrics) {
        metrics.gauge(
            "canardstack_ingest_worker_queue_slots",
            &[("state", "used")],
            self.worker_queue_slots.used() as f64,
        );
        metrics.gauge(
            "canardstack_ingest_worker_queue_slots",
            &[("state", "capacity")],
            self.worker_queue_slots.capacity() as f64,
        );
    }

    pub fn lane_freshness_inputs(&self, storage: &Storage) -> FreshnessInputs {
        let (buffered_bytes, buffered_active_count, oldest_buffer_age_seconds) = storage
            .immutable_buffer_metrics()
            .into_iter()
            .fold((0usize, 0usize, 0.0f64), |(bytes, count, age), metric| {
                (
                    bytes.saturating_add(metric.bytes),
                    count.saturating_add(usize::from(metric.bytes > 0)),
                    age.max(metric.age_seconds),
                )
            });
        FreshnessInputs {
            queued_bytes: self.inflight_bytes(),
            incoming_bytes: 0,
            // Admitted bytes move straight from the worker into the immutable
            // buffer; there is no separate in-memory queue to age out, so queue
            // dwell is zero and buffer age is carried by oldest_buffer_age_seconds.
            oldest_queue_age_seconds: 0.0,
            buffered_bytes,
            buffered_active_count,
            oldest_buffer_age_seconds,
        }
    }

    pub fn inflight_bytes(&self) -> usize {
        self.inflight.total_bytes()
    }

    fn record_accepted_body_metrics(
        &self,
        signal: Signal,
        headers: &HashMap<String, String>,
        request_bytes: usize,
        decoded_bytes: usize,
        _decoded_body_materialized_bytes: usize,
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
        for (signal, batch) in transformed.signal_batches() {
            if let Some(batch) = batch {
                validation::validate_arrow_timestamp_skew(batch, signal, &self.config)?;
            }
        }
        Ok(())
    }
}

fn observe_storage_timings(metrics: &Metrics, timings: &[ArrowBatchInsertTiming]) {
    for timing in timings {
        metrics.observe_phase_seconds(
            timing.table.as_str(),
            timing.phase.as_str(),
            None,
            timing.seconds,
        );
    }
}

fn transformed_rows_by_signal(transformed: &Transformed) -> Vec<(Signal, usize)> {
    transformed
        .signal_batches()
        .into_iter()
        .filter_map(|(signal, batch)| {
            let rows = batch.map(|batch| batch.num_rows()).unwrap_or(0);
            (rows > 0).then_some((signal, rows))
        })
        .collect()
}

fn all_signals() -> [Signal; 4] {
    Signal::ALL
}

fn spawn_raw_spool_writer(config: &Config, signal: Signal) -> Result<Writer> {
    Writer::spawn(
        Options {
            dir: config.raw_spool_dir.join(signal.as_str()),
            max_segment_bytes: config.raw_spool_max_segment_bytes as u64,
            max_record_bytes: config.raw_spool_max_record_bytes as u64,
            max_total_bytes: config.raw_spool_max_total_bytes as u64,
            append_sync_interval: config.raw_spool_append_sync_interval,
            append_sync_bytes: config.raw_spool_append_sync_bytes as u64,
            checkpoint_fsync_records: config.raw_spool_checkpoint_fsync_records,
            checkpoint_fsync_delay: config.raw_spool_checkpoint_fsync_delay,
        },
        config.raw_spool_writer_queue_capacity,
        config.raw_spool_group_commit_records,
        config.raw_spool_group_commit_delay,
    )
    .with_context(|| format!("spawn {signal} raw spool writer"))
}

fn pending_batch_totals(batches: &[batches::PendingBatch]) -> BTreeMap<Signal, (usize, usize)> {
    let mut totals = BTreeMap::new();
    for batch in batches {
        let (rows, bytes) = totals.entry(batch.signal).or_insert((0, 0));
        *rows += batch.batch.num_rows();
        *bytes += batch.approx_bytes;
    }
    totals
}
