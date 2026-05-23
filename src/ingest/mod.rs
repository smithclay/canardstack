use crate::admission_control::{AdmissionController, FreshnessBudgetInputs};
use crate::config::Config;
use crate::metrics::Metrics;
use crate::otlp::{self, Transformed};
use crate::signal::StorageSignal;
use crate::storage::{ArrowBatchBuffer, ArrowBatchBufferTiming, Storage};
use crate::validation::{self, ApiError, ApiResult};
use crate::LockExt;
use anyhow::{Context, Result};
use serde::Serialize;
use serde_json::{json, Value};
use spool::{Options, RecordId, SealRef, Writer};
use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicUsize};
use std::sync::mpsc::TrySendError;
use std::sync::{Arc, Mutex};
use std::time::Instant;

mod admission;
mod batches;
mod lifecycle;
pub mod spool;
mod worker;

pub use batches::IngestSnapshot;
pub(in crate::ingest) use lifecycle::IngestStage;
use worker::IngestWorkerPool;
pub(crate) use worker::INGEST_WORKER_CHANNEL_CAPACITY;

#[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize)]
pub enum OtlpRequestKind {
    Logs,
    Traces,
    Metrics,
}

impl OtlpRequestKind {
    pub const ALL: [OtlpRequestKind; 3] = [Self::Logs, Self::Traces, Self::Metrics];

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Logs => "logs",
            Self::Traces => "traces",
            Self::Metrics => "metrics",
        }
    }

    /// The storage signal(s) a request of this kind fans out to. A metrics
    /// request splits across both metric tables; logs and traces map 1:1.
    pub fn storage_signals(self) -> &'static [StorageSignal] {
        match self {
            Self::Logs => &[StorageSignal::Logs],
            Self::Traces => &[StorageSignal::Spans],
            Self::Metrics => &[StorageSignal::MetricGauge, StorageSignal::MetricSum],
        }
    }
}

impl fmt::Display for OtlpRequestKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

pub struct Ingestor {
    runtime_memory_reserved_bytes: Arc<AtomicUsize>,
    inflight: Arc<admission::InflightBytes>,
    raw_spools: BTreeMap<OtlpRequestKind, Writer>,
    raw_spool_seal_refs: Arc<Mutex<BTreeMap<(OtlpRequestKind, RecordId), SealRef>>>,
    ingest_workers: Mutex<Option<IngestWorkerPool>>,
    /// First-transition latch so worker-pool saturation (caller-runs fallback)
    /// logs once per episode rather than once per saturated request. Set on the
    /// caller-runs path, cleared on the next successful queued dispatch.
    worker_pool_saturated: AtomicBool,
    config: Config,
}

pub(in crate::ingest) struct SpooledIngestWork {
    pub(in crate::ingest) route: OtlpRequestKind,
    headers: HashMap<String, String>,
    compressed_body: Vec<u8>,
    raw_spool_ref: spool::AppendRef,
    inflight_reservation: admission::InflightReservation,
    runtime_memory_reservation: admission::RuntimeMemoryReservation,
    pub(in crate::ingest) metrics: Arc<Metrics>,
    /// Explicit lifecycle stage for tracing and the stage counters. See
    /// [`crate::ingest::lifecycle`] for the authoritative stage map. Advancing it
    /// never changes control flow.
    pub(in crate::ingest) stage: IngestStage,
}

impl Ingestor {
    pub fn new(config: Config) -> Result<Self> {
        let mut raw_spools = BTreeMap::new();
        for request_kind in OtlpRequestKind::ALL {
            raw_spools.insert(request_kind, spawn_raw_spool_writer(&config, request_kind)?);
        }
        Ok(Self {
            runtime_memory_reserved_bytes: Arc::new(AtomicUsize::new(0)),
            inflight: Arc::new(admission::InflightBytes::new(&config)),
            raw_spools,
            raw_spool_seal_refs: Arc::new(Mutex::new(BTreeMap::new())),
            ingest_workers: Mutex::new(None),
            worker_pool_saturated: AtomicBool::new(false),
            config,
        })
    }

    pub fn ingest(
        &self,
        route: OtlpRequestKind,
        headers: &HashMap<String, String>,
        compressed_body: Vec<u8>,
        storage: &Storage,
        admission: &AdmissionController,
        metrics: Arc<Metrics>,
    ) -> ApiResult<Value> {
        let async_metrics = Arc::clone(&metrics);
        let metrics = metrics.as_ref();
        if let Err(err) = validation::validate_body_size(compressed_body.len(), &self.config) {
            metrics.ingest_request(route.as_str(), err.status, err.reason);
            return Err(err);
        }
        if let Err(err) = validation::validate_content_type(headers) {
            metrics.ingest_request(route.as_str(), err.status, err.reason);
            return Err(err);
        }
        if !storage.accepts_memory_ingest() {
            metrics.ingest_request(route.as_str(), 503, "dependency_unhealthy");
            return Err(ApiError::new(
                503,
                "dependency_unhealthy",
                "storage dependency is unhealthy",
            )
            .with_retry_after(10));
        }
        let inflight_reservation = match self.admit_and_reserve_inflight(
            route,
            headers,
            compressed_body.len(),
            storage,
            admission,
            metrics,
        ) {
            Ok(reservation) => reservation,
            Err(err) => {
                self.record_inflight_metrics(metrics);
                metrics.ingest_request(route.as_str(), err.status, err.reason);
                return Err(err);
            }
        };
        let runtime_memory_reservation =
            match self.admit_runtime_memory(route, headers, compressed_body.len(), metrics) {
                Ok(reservation) => reservation,
                Err(err) => {
                    metrics.ingest_request(route.as_str(), err.status, err.reason);
                    return Err(err);
                }
            };
        if let Err(err) = self.ensure_ingest_workers_available(route, metrics) {
            metrics.ingest_request(route.as_str(), err.status, err.reason);
            return Err(err);
        }
        // Every request-path gate passed; the request is admitted but not yet
        // durably spooled. Enter the lifecycle through the centralized chokepoint.
        // See `crate::ingest::lifecycle`.
        let mut stage = lifecycle::enter_admitted(metrics, route);
        let (raw_spool_ref, compressed_body) =
            match self.append_raw_spool(route, headers, compressed_body, metrics) {
                Ok(appended) => appended,
                Err(err) => {
                    metrics.ingest_request(route.as_str(), err.status, err.reason);
                    return Err(err);
                }
            };
        // `append_raw_spool` succeeded, so the raw request is fsynced.
        lifecycle::advance(&mut stage, metrics, route, IngestStage::DurablySpooled);
        let spooled = SpooledIngestWork {
            route,
            headers: headers.clone(),
            compressed_body,
            raw_spool_ref,
            inflight_reservation,
            runtime_memory_reservation,
            metrics: async_metrics,
            // Carries the local lifecycle stage advanced above.
            stage,
        };
        self.dispatch_ingest_work(spooled, storage, metrics)
    }

    pub(in crate::ingest) fn process_spooled_ingest(
        &self,
        work: SpooledIngestWork,
        storage: &Storage,
    ) -> ApiResult<()> {
        let SpooledIngestWork {
            route,
            headers,
            compressed_body,
            raw_spool_ref,
            mut inflight_reservation,
            mut runtime_memory_reservation,
            metrics,
            // Lifecycle stage on entry: `DurablySpooled` (set at construction),
            // or `WorkerDispatched` / `InlineProcessed` if `dispatch_ingest_work`
            // stamped a dispatch decision. Advanced below as the request walks
            // toward `ArrowBuffered`; see `crate::ingest::lifecycle`.
            mut stage,
        } = work;
        let metrics_arc = metrics;
        let metrics = metrics_arc.as_ref();
        // Record the dispatch decision (`WorkerDispatched` / `InlineProcessed`,
        // or `DurablySpooled` on replay) before it is advanced through the
        // per-request stages. See `crate::ingest::lifecycle`.
        tracing::trace!(
            event = "ingest_processing_started",
            request_kind = route.as_str(),
            stage = stage.as_str(),
        );
        let started = Instant::now();
        let body_result =
            otlp::decompress_if_needed(&headers, &compressed_body, self.config.max_body_bytes);
        metrics.observe_request_phase_seconds(
            route.as_str(),
            "decompress",
            started.elapsed().as_secs_f64(),
        );
        let body = match body_result {
            Ok(body) => body,
            Err(err) => {
                let reason = if err.reason == "payload_too_large" {
                    "body_size_invalid"
                } else {
                    "decode_failed"
                };
                self.checkpoint_raw_spool_terminal(
                    &mut stage,
                    raw_spool_ref,
                    route,
                    reason,
                    metrics,
                )?;
                return Err(err);
            }
        };
        let decoded_body_materialized_bytes = match &body {
            std::borrow::Cow::Borrowed(_) => 0,
            std::borrow::Cow::Owned(bytes) => bytes.len(),
        };
        if let Err(err) = validation::validate_body_size(body.len(), &self.config) {
            self.checkpoint_raw_spool_terminal(
                &mut stage,
                raw_spool_ref,
                route,
                "body_size_invalid",
                metrics,
            )?;
            return Err(err);
        }
        let started = Instant::now();
        #[cfg(feature = "otlp2records-observer")]
        let transformed_result = otlp::transform_observed(route, &headers, &body, metrics);
        #[cfg(not(feature = "otlp2records-observer"))]
        let transformed_result = otlp::transform(route, &headers, &body);
        metrics.observe_request_phase_seconds(
            route.as_str(),
            "otlp_transform",
            started.elapsed().as_secs_f64(),
        );
        let transformed = match transformed_result {
            Ok(transformed) => transformed,
            Err(err) => {
                self.checkpoint_raw_spool_terminal(
                    &mut stage,
                    raw_spool_ref,
                    route,
                    "transform_failed",
                    metrics,
                )?;
                return Err(err);
            }
        };
        // otlp2records produced Arrow batches; see `crate::ingest::lifecycle`.
        lifecycle::advance(&mut stage, metrics, route, IngestStage::Transformed);
        let unsupported_histograms = transformed.unsupported_histograms;
        let started = Instant::now();
        let skew_result = self.validate_skew(&transformed);
        metrics.observe_request_phase_seconds(
            route.as_str(),
            "timestamp_validation",
            started.elapsed().as_secs_f64(),
        );
        if let Err(err) = skew_result {
            self.checkpoint_raw_spool_terminal(
                &mut stage,
                raw_spool_ref,
                route,
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
                    ("storage_signal", output_signal.as_str()),
                    ("request_kind", route.as_str()),
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
        if let Err(err) = runtime_memory_reservation.reserve_at_least(peak_bytes, route, metrics) {
            self.checkpoint_raw_spool_terminal(
                &mut stage,
                raw_spool_ref,
                route,
                "memory_rejected",
                metrics,
            )?;
            return Err(err);
        }
        if batches.is_empty() {
            self.checkpoint_raw_spool_terminal(
                &mut stage,
                raw_spool_ref,
                route,
                "transform_empty",
                metrics,
            )?;
            return Ok(());
        }

        let buffers = batches
            .iter()
            .filter(|batch| batch.batch.num_rows() > 0)
            .map(|batch| ArrowBatchBuffer {
                table: batch.signal,
                batch: &batch.batch,
                source_format: batch.source_format,
            })
            .collect::<Vec<_>>();
        let buffer_started = Instant::now();
        let buffer_result = storage.buffer_arrow_batches(&buffers);
        metrics.observe_request_phase_seconds(
            route.as_str(),
            "storage_buffer",
            buffer_started.elapsed().as_secs_f64(),
        );
        let buffered = match buffer_result {
            Ok(result) => result,
            Err(err) => {
                // The rows never reached the Arrow write buffer and the raw-spool
                // record was not tracked, so it stays pending and replays on a
                // future restart (at-least-once). Surface a retryable
                // dependency error; the admission credit drops with this scope.
                metrics.inc(
                    "canardstack_ingest_storage_insert_total",
                    &[("request_kind", route.as_str()), ("status", "error")],
                    1,
                );
                tracing::warn!(
                    event = "ingest_storage_insert_failed",
                    request_kind = route.as_str(),
                    stage = stage.as_str(),
                    error = %err
                );
                return Err(
                    ApiError::new(503, "storage_insert_failed", "storage insert failed")
                        .with_retry_after(5),
                );
            }
        };
        // Rows reached the Arrow write buffer; the per-request phase terminus.
        // See `crate::ingest::lifecycle`.
        lifecycle::advance(&mut stage, metrics, route, IngestStage::ArrowBuffered);
        observe_storage_timings(metrics, &buffered.timings);
        metrics.inc(
            "canardstack_ingest_storage_insert_total",
            &[("request_kind", route.as_str()), ("status", "ok")],
            1,
        );

        // Rows are now in the Arrow write buffer. Track the raw-spool record so the
        // scheduler checkpoints it after the next durable DuckLake commit, then release the
        // admission credit (buffer occupancy is now reflected as buffered bytes
        // for freshness, not as a held queue credit). See `crate::ingest::lifecycle`
        // for the seal-side hops that take the tracked record to a checkpoint.
        self.track_raw_spool_record(raw_spool_ref, route);
        drop(inflight_reservation);
        tracing::debug!(
            event = "ingest_buffered",
            request_kind = route.as_str(),
            stage = stage.as_str(),
        );

        let accepted = buffered_totals
            .values()
            .map(|(rows, _)| *rows)
            .sum::<usize>();
        self.record_accepted_body_metrics(
            route.as_str(),
            &headers,
            request_bytes,
            decoded_body_len,
            decoded_body_materialized_bytes,
            metrics,
        );
        metrics.inc(
            "canardstack_ingest_records_total",
            &[("request_kind", route.as_str())],
            accepted as u64,
        );
        if unsupported_histograms > 0 {
            metrics.inc(
                "canardstack_ingest_unsupported_histograms_total",
                &[("request_kind", route.as_str())],
                unsupported_histograms as u64,
            );
        }
        for (output_signal, (rows, bytes)) in buffered_totals {
            metrics.inc(
                "canardstack_ingest_buffered_rows_total",
                &[("storage_signal", output_signal.as_str())],
                rows as u64,
            );
            metrics.inc(
                "canardstack_ingest_buffered_bytes_total",
                &[("storage_signal", output_signal.as_str())],
                bytes as u64,
            );
        }

        Ok(())
    }

    fn dispatch_ingest_work(
        &self,
        work: SpooledIngestWork,
        storage: &Storage,
        metrics: &Metrics,
    ) -> ApiResult<Value> {
        let route = work.route;
        // Round-robin to the first worker with buffer space. On a successful send
        // the function returns directly from inside the loop; if every worker is
        // full (or the pool is gone) the still-owned `work` falls through to the
        // caller-runs path below. The single authoritative `work.stage` carries the
        // dispatch hop: the receiving worker advances `WorkerDispatched` on receipt
        // (`run_ingest_worker`), and the caller-runs path below advances
        // `InlineProcessed`. See `crate::ingest::lifecycle`.
        let mut work = {
            let mut pool = self.ingest_workers.lock_or_poisoned();
            let mut work = work;
            if let Some(dispatcher) = pool.as_mut() {
                let worker_count = dispatcher.commands.len();
                if worker_count > 0 {
                    let start = dispatcher.next_worker % worker_count;
                    for offset in 0..worker_count {
                        let worker_idx = (start + offset) % worker_count;
                        match dispatcher.commands[worker_idx].try_send(work) {
                            Ok(()) => {
                                dispatcher.next_worker = worker_idx.wrapping_add(1);
                                // A worker accepted the handoff: clear the
                                // saturation latch so a later caller-runs episode
                                // logs again on its first transition.
                                self.worker_pool_saturated
                                    .store(false, std::sync::atomic::Ordering::Release);
                                metrics.ingest_request(route.as_str(), 202, "accepted");
                                self.record_worker_queue_metrics(metrics);
                                metrics.inc(
                                    "canardstack_ingest_worker_dispatch_total",
                                    &[("request_kind", route.as_str()), ("outcome", "queued")],
                                    1,
                                );
                                // Lifecycle funnel: the receiving worker advances the
                                // single authoritative `work.stage` to
                                // `WorkerDispatched` on receipt (`run_ingest_worker`),
                                // so the stage travels with the work rather than a
                                // mirror. See `crate::ingest::lifecycle`.
                                return Ok(json!({
                                    "accepted": true,
                                    "acknowledgement": "locally_spooled"
                                }));
                            }
                            Err(TrySendError::Full(returned)) => work = returned,
                            Err(TrySendError::Disconnected(returned)) => work = returned,
                        }
                    }
                }
            }
            work
        };

        // Caller-runs: no worker could take the handoff, so process the already
        // durably-spooled work inline on this thread instead of dropping it for
        // restart replay. This keeps the 202 honest (the rows reach the Arrow write
        // buffer now, not only after the next process restart) and applies
        // natural backpressure: request latency rises under worker saturation.
        metrics.inc(
            "canardstack_ingest_worker_dispatch_total",
            &[
                ("request_kind", route.as_str()),
                ("outcome", "processed_inline"),
            ],
            1,
        );
        // Loud-once on the first transition into saturation so the inline
        // degrade mode is visible in logs, not only in the `processed_inline`
        // counter. Mirrors the raw-spool fatal first-transition latch; the next
        // successful queued dispatch clears it.
        if self
            .worker_pool_saturated
            .compare_exchange(
                false,
                true,
                std::sync::atomic::Ordering::AcqRel,
                std::sync::atomic::Ordering::Acquire,
            )
            .is_ok()
        {
            tracing::warn!(
                event = "ingest_worker_pool_saturated",
                request_kind = route.as_str(),
                "ingest worker pool saturated; processing inline on the connection thread (back-pressure via latency)"
            );
        }
        // Caller-runs path: no worker took the handoff, so advance the single
        // authoritative `work.stage` to `InlineProcessed` through the centralized
        // chokepoint (the `DurablySpooled -> InlineProcessed` hop) and process
        // inline. See `crate::ingest::lifecycle`.
        lifecycle::advance(
            &mut work.stage,
            metrics,
            route,
            IngestStage::InlineProcessed,
        );
        let result = self.process_spooled_ingest(work, storage);
        self.record_inflight_metrics(metrics);
        // The raw request is durably spooled, so the 202 acknowledgement holds
        // regardless of the inline transform outcome — matching the async worker
        // contract where 202 means spooled-and-accepted, not transformed.
        // `process_spooled_ingest` has already disposed of the record (terminal
        // checkpoint for bad payloads, left pending for retryable storage faults).
        if let Err(err) = result {
            tracing::warn!(
                event = "ingest_inline_process_failed",
                request_kind = route.as_str(),
                status = err.status,
                reason = err.reason,
                message = %err.message,
                "inline ingest processing failed after worker saturation; raw request stays durably spooled"
            );
        }
        metrics.ingest_request(route.as_str(), 202, "accepted_inline");
        Ok(json!({
            "accepted": true,
            "acknowledgement": "locally_spooled"
        }))
    }

    fn ensure_ingest_workers_available(
        &self,
        route: OtlpRequestKind,
        metrics: &Metrics,
    ) -> ApiResult<()> {
        {
            let pool = self.ingest_workers.lock_or_poisoned();
            let Some(dispatcher) = pool.as_ref() else {
                metrics.inc(
                    "canardstack_ingest_worker_dispatch_total",
                    &[
                        ("request_kind", route.as_str()),
                        ("outcome", "workers_unavailable"),
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
                    "canardstack_ingest_worker_dispatch_total",
                    &[
                        ("request_kind", route.as_str()),
                        ("outcome", "workers_unavailable"),
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
        Ok(())
    }

    /// Single ingest admission gate: project visibility through the freshness
    /// budget (the sole soft shed) before the durable raw-spool append, so a
    /// rejection never spools. On admission, take the per-storage-signal
    /// in-flight reservation — pure accounting that feeds the freshness total;
    /// it never rejects.
    fn admit_and_reserve_inflight(
        &self,
        route: OtlpRequestKind,
        headers: &HashMap<String, String>,
        compressed_body_bytes: usize,
        storage: &Storage,
        admission: &AdmissionController,
        metrics: &Metrics,
    ) -> ApiResult<admission::InflightReservation> {
        let estimate = self.inflight.estimate_for_request(
            route,
            headers,
            compressed_body_bytes,
            self.config.max_body_bytes,
        );
        let mut inputs = self.freshness_budget_inputs(storage);
        inputs.incoming_bytes = estimate.values().sum::<usize>();
        admission.admit_ingest(inputs, metrics)?;
        Ok(self.inflight.reserve(estimate))
    }

    fn admit_runtime_memory(
        &self,
        route: OtlpRequestKind,
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
            route,
            metrics,
        )?;
        Ok(reservation)
    }

    pub fn snapshots(&self) -> Vec<IngestSnapshot> {
        StorageSignal::ALL
            .into_iter()
            .map(|signal| IngestSnapshot {
                storage_signal: signal.as_str(),
                inflight_bytes: self.inflight.signal_bytes(signal),
            })
            .collect()
    }

    pub fn record_inflight_metrics(&self, metrics: &Metrics) {
        for snapshot in self.snapshots() {
            metrics.gauge(
                "canardstack_ingest_inflight_bytes",
                &[("storage_signal", snapshot.storage_signal)],
                snapshot.inflight_bytes as f64,
            );
        }
    }

    pub fn record_worker_queue_metrics(&self, metrics: &Metrics) {
        metrics.gauge(
            "canardstack_ingest_worker_queue_capacity",
            &[("state", "capacity")],
            self.config.ingest_worker_channel_capacity as f64,
        );
    }

    pub fn freshness_budget_inputs(&self, storage: &Storage) -> FreshnessBudgetInputs {
        let (buffered_bytes, buffered_active_count, oldest_buffer_age_seconds) = storage
            .arrow_write_buffer_metrics()
            .into_iter()
            .fold((0usize, 0usize, 0.0f64), |(bytes, count, age), metric| {
                (
                    bytes.saturating_add(metric.bytes),
                    count.saturating_add(usize::from(metric.bytes > 0)),
                    age.max(metric.age_seconds),
                )
            });
        FreshnessBudgetInputs {
            inflight_bytes: self.inflight_bytes(),
            incoming_bytes: 0,
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
        route: &str,
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
            &[("request_kind", route), ("encoding", encoding)],
            request_bytes as u64,
        );
        metrics.inc(
            "canardstack_ingest_decoded_bytes_total",
            &[("request_kind", route), ("encoding", encoding)],
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

fn observe_storage_timings(metrics: &Metrics, timings: &[ArrowBatchBufferTiming]) {
    for timing in timings {
        metrics.observe_storage_signal_phase_seconds(
            timing.table.as_str(),
            timing.phase.as_str(),
            timing.seconds,
        );
    }
}

fn transformed_rows_by_signal(transformed: &Transformed) -> Vec<(StorageSignal, usize)> {
    transformed
        .signal_batches()
        .into_iter()
        .filter_map(|(signal, batch)| {
            let rows = batch.map(|batch| batch.num_rows()).unwrap_or(0);
            (rows > 0).then_some((signal, rows))
        })
        .collect()
}

fn spawn_raw_spool_writer(config: &Config, request_kind: OtlpRequestKind) -> Result<Writer> {
    Writer::spawn(
        Options {
            dir: config.raw_spool_dir.join(request_kind.as_str()),
            max_segment_bytes: config.raw_spool_max_segment_bytes as u64,
            max_record_bytes: config.raw_spool_max_record_bytes as u64,
            max_total_bytes: config.raw_spool_max_total_bytes as u64,
            append_sync_interval: config.raw_spool_append_sync_interval,
            append_sync_bytes: config.raw_spool_append_sync_bytes as u64,
            checkpoint_fsync_records: spool::RAW_SPOOL_CHECKPOINT_FSYNC_RECORDS,
            checkpoint_fsync_delay: spool::RAW_SPOOL_CHECKPOINT_FSYNC_DELAY,
        },
        spool::RAW_SPOOL_WRITER_QUEUE_CAPACITY,
        spool::RAW_SPOOL_GROUP_COMMIT_RECORDS,
        config.raw_spool_group_commit_delay,
    )
    .with_context(|| format!("spawn {request_kind} raw spool writer"))
}

fn pending_batch_totals(
    batches: &[batches::PendingBatch],
) -> BTreeMap<StorageSignal, (usize, usize)> {
    let mut totals = BTreeMap::new();
    for batch in batches {
        let (rows, bytes) = totals.entry(batch.signal).or_insert((0, 0));
        *rows += batch.batch.num_rows();
        *bytes += batch.approx_bytes;
    }
    totals
}
