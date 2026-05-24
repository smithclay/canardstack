use crate::admission_control::{AdmissionController, FreshnessBudgetInputs};
use crate::config::Config;
use crate::ingest::raw_spool::{RawSpool, RawSpoolAppendRef};
use crate::metrics::{MetricName, Metrics};
use crate::otlp::{self, Transformed};
use crate::signal::StorageSignal;
use crate::storage::{
    ArrowBatchBufferResult, ArrowBatchBufferTiming, CommittedReplayRefs, ReplayBackedArrowBatch,
    Storage,
};
use crate::validation::{self, ApiError, ApiResult};
use crate::LockExt;
use anyhow::Result;
use serde::Serialize;
use serde_json::{json, Value};
use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicUsize};
use std::sync::mpsc::TrySendError;
use std::sync::{Arc, Mutex};
use std::time::Instant;

mod admission;
mod batches;
pub(crate) mod lifecycle;
pub(crate) mod raw_spool;
pub mod spool;
mod worker;

pub use batches::IngestSnapshot;
pub(in crate::ingest) use lifecycle::IngestStage;
pub(crate) use lifecycle::SealStage;
pub(crate) use raw_spool::ReplayBackedRecordRef;
use worker::IngestWorkerPool;
pub(crate) use worker::INGEST_WORKER_CHANNEL_CAPACITY;

/// The OTLP ingress vocabulary, and the dimension the raw spool is partitioned
/// by: one durable raw-spool writer per variant (see [`raw_spool::RawSpool`]). A
/// request is spooled and checkpointed under one `OtlpRequestKind` identity even
/// when it fans out to several [`StorageSignal`]s via [`Self::storage_signals`]
/// (metrics -> `metric_gauge` + `metric_sum`). Adding a `StorageSignal` does not
/// add a raw-spool writer; only adding a request kind here does.
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
    pipeline: Arc<IngestPipeline>,
}

pub(crate) struct IngestPipeline {
    runtime_memory_reserved_bytes: Arc<AtomicUsize>,
    inflight: Arc<admission::InflightBytes>,
    raw_spool: Arc<RawSpool>,
    ingest_workers: Mutex<Option<IngestWorkerPool>>,
    /// First-transition latch so worker-pool saturation (caller-runs fallback)
    /// logs once per episode rather than once per saturated request. Set on the
    /// caller-runs path, cleared on the next successful queued dispatch.
    worker_pool_saturated: AtomicBool,
    config: Config,
}

pub(in crate::ingest) struct SpooledIngestWork {
    pub(in crate::ingest) request_kind: OtlpRequestKind,
    headers: HashMap<String, String>,
    compressed_body: Vec<u8>,
    raw_spool_ref: RawSpoolAppendRef,
    inflight_reservation: admission::InflightReservation,
    runtime_memory_reservation: admission::RuntimeMemoryReservation,
    pub(in crate::ingest) metrics: Arc<Metrics>,
}

#[derive(Clone, Copy)]
struct SpooledRequestContext<'a> {
    route: OtlpRequestKind,
    raw_spool_ref: RawSpoolAppendRef,
    metrics: &'a Metrics,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(in crate::ingest) enum SpooledIngestDisposition {
    Buffered,
    TerminallyDisposed,
    LeftPendingForReplay,
}

impl SpooledIngestDisposition {
    pub(in crate::ingest) fn as_str(self) -> &'static str {
        match self {
            Self::Buffered => "buffered",
            Self::TerminallyDisposed => "terminally_disposed",
            Self::LeftPendingForReplay => "left_pending_for_replay",
        }
    }
}

pub(in crate::ingest) struct SpooledIngestError {
    pub(in crate::ingest) error: ApiError,
    pub(in crate::ingest) disposition: SpooledIngestDisposition,
}

struct ReplayError {
    request_kind: OtlpRequestKind,
    raw_spool_ref: RawSpoolAppendRef,
    error: anyhow::Error,
}

impl ReplayError {
    fn new(pending: &raw_spool::PendingRawRecord, message: String) -> Self {
        Self::new_raw(
            pending.raw_spool_ref,
            pending.request_kind,
            anyhow::anyhow!(message),
        )
    }

    fn new_raw(
        raw_spool_ref: RawSpoolAppendRef,
        request_kind: OtlpRequestKind,
        error: impl Into<anyhow::Error>,
    ) -> Self {
        Self {
            request_kind,
            raw_spool_ref,
            error: error.into(),
        }
    }
}

impl SpooledIngestError {
    fn terminal(error: ApiError) -> Self {
        Self {
            error,
            disposition: SpooledIngestDisposition::TerminallyDisposed,
        }
    }

    fn pending_replay(error: ApiError) -> Self {
        Self {
            error,
            disposition: SpooledIngestDisposition::LeftPendingForReplay,
        }
    }
}

impl Ingestor {
    pub fn new(config: Config) -> Result<Self> {
        let raw_spool = Arc::new(RawSpool::open(&config)?);
        let pipeline = Arc::new(IngestPipeline::new(config, raw_spool));
        Ok(Self { pipeline })
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
        self.pipeline
            .ingest(route, headers, compressed_body, storage, admission, metrics)
    }

    pub fn replay_raw_spool(
        &self,
        storage: &Storage,
        admission: &AdmissionController,
        metrics: Arc<Metrics>,
    ) -> Result<usize> {
        self.pipeline.replay_raw_spool(storage, admission, metrics)
    }

    pub fn raw_spool_stats(&self) -> Result<spool::Stats> {
        self.pipeline.raw_spool_stats()
    }

    pub fn raw_spool_stats_by_request_kind(&self) -> Result<BTreeMap<&'static str, spool::Stats>> {
        self.pipeline.raw_spool_stats_by_request_kind()
    }

    pub fn raw_spool_healthy(&self) -> bool {
        self.pipeline.raw_spool_healthy()
    }

    #[doc(hidden)]
    pub fn force_raw_spool_unhealthy(
        &self,
        request_kind: OtlpRequestKind,
        message: impl Into<String>,
    ) -> Result<()> {
        self.pipeline
            .force_raw_spool_unhealthy(request_kind, message)
    }

    pub fn raw_spool_health_by_request_kind(
        &self,
    ) -> BTreeMap<&'static str, (bool, Option<String>)> {
        self.pipeline.raw_spool_health_by_request_kind()
    }

    pub fn record_raw_spool_metrics(&self, metrics: &Metrics) {
        self.pipeline.record_raw_spool_metrics(metrics);
    }

    pub(crate) fn checkpoint_replay_backed_records(
        &self,
        records: CommittedReplayRefs,
        reason: &'static str,
        metrics: Option<&Metrics>,
    ) -> Result<()> {
        self.pipeline
            .checkpoint_replay_backed_records(records, reason, metrics)
    }

    pub fn snapshots(&self) -> Vec<IngestSnapshot> {
        self.pipeline.snapshots()
    }

    pub fn record_inflight_metrics(&self, metrics: &Metrics) {
        self.pipeline.record_inflight_metrics(metrics);
    }

    pub fn record_worker_queue_metrics(&self, metrics: &Metrics) {
        self.pipeline.record_worker_queue_metrics(metrics);
    }

    pub fn freshness_budget_inputs(&self, storage: &Storage) -> FreshnessBudgetInputs {
        self.pipeline.freshness_budget_inputs(storage)
    }

    pub fn inflight_bytes(&self) -> usize {
        self.pipeline.inflight_bytes()
    }

    pub(crate) fn pipeline(&self) -> Arc<IngestPipeline> {
        Arc::clone(&self.pipeline)
    }
}

impl IngestPipeline {
    fn new(config: Config, raw_spool: Arc<RawSpool>) -> Self {
        Self {
            runtime_memory_reserved_bytes: Arc::new(AtomicUsize::new(0)),
            inflight: Arc::new(admission::InflightBytes::new(&config)),
            raw_spool,
            ingest_workers: Mutex::new(None),
            worker_pool_saturated: AtomicBool::new(false),
            config,
        }
    }

    fn raw_spool_stats(&self) -> Result<spool::Stats> {
        self.raw_spool.stats()
    }

    fn raw_spool_stats_by_request_kind(&self) -> Result<BTreeMap<&'static str, spool::Stats>> {
        self.raw_spool.stats_by_request_kind()
    }

    fn raw_spool_healthy(&self) -> bool {
        self.raw_spool.healthy()
    }

    fn force_raw_spool_unhealthy(
        &self,
        request_kind: OtlpRequestKind,
        message: impl Into<String>,
    ) -> Result<()> {
        self.raw_spool.force_unhealthy(request_kind, message)
    }

    fn raw_spool_health_by_request_kind(&self) -> BTreeMap<&'static str, (bool, Option<String>)> {
        self.raw_spool.health_by_request_kind()
    }

    fn record_raw_spool_metrics(&self, metrics: &Metrics) {
        self.raw_spool.record_metrics(metrics);
    }

    fn checkpoint_replay_backed_records(
        &self,
        records: CommittedReplayRefs,
        reason: &'static str,
        metrics: Option<&Metrics>,
    ) -> Result<()> {
        self.raw_spool
            .checkpoint_replay_backed_records(records, reason, metrics)
    }

    pub(crate) fn replay_raw_spool(
        &self,
        storage: &Storage,
        admission: &AdmissionController,
        metrics: Arc<Metrics>,
    ) -> Result<usize> {
        let mut replayed = 0usize;
        for pending in self.raw_spool.recover_pending()? {
            let request_kind = pending.request_kind;
            metrics.inc(
                MetricName::RawSpoolReplayedRecordsTotal,
                &[
                    ("request_kind", request_kind.as_str()),
                    ("status", "attempted"),
                ],
                1,
            );
            match self.ingest_replayed_raw_record(pending, storage, admission, metrics.clone()) {
                Ok(()) => {
                    replayed += 1;
                    metrics.inc(
                        MetricName::RawSpoolReplayedRecordsTotal,
                        &[("request_kind", request_kind.as_str()), ("status", "ok")],
                        1,
                    );
                }
                Err(err) => {
                    metrics.inc(
                        MetricName::RawSpoolReplayedRecordsTotal,
                        &[
                            ("request_kind", err.request_kind.as_str()),
                            ("status", "failed"),
                        ],
                        1,
                    );
                    // Startup replay is tolerant: a single failing record must
                    // never abort boot. The record stays un-checkpointed (still
                    // pending) and is retried on a future startup, preserving
                    // at-least-once delivery.
                    tracing::warn!(
                        event = "raw_spool_replay_record_failed",
                        request_kind = err.request_kind.as_str(),
                        record_segment = err.raw_spool_ref.record_id.segment,
                        record_sequence = err.raw_spool_ref.record_id.sequence,
                        error = %err.error,
                        "skipping raw spool replay record; left pending for retry"
                    );
                    continue;
                }
            }
        }
        self.raw_spool.record_metrics(metrics.as_ref());
        Ok(replayed)
    }

    fn ingest_replayed_raw_record(
        &self,
        pending: raw_spool::PendingRawRecord,
        storage: &Storage,
        admission: &AdmissionController,
        metrics: Arc<Metrics>,
    ) -> std::result::Result<(), ReplayError> {
        let request_kind = pending.request_kind;
        validation::validate_body_size(pending.compressed_body.len(), &self.config)
            .map_err(|err| ReplayError::new(&pending, err.message.clone()))?;
        validation::validate_content_type(&pending.headers)
            .map_err(|err| ReplayError::new(&pending, err.message.clone()))?;
        if !storage.accepts_memory_ingest() {
            return Err(ReplayError::new(
                &pending,
                "storage dependency is unhealthy".to_string(),
            ));
        }
        let inflight_reservation = self
            .admit_and_reserve_inflight(
                request_kind,
                &pending.headers,
                pending.compressed_body.len(),
                storage,
                admission,
                metrics.as_ref(),
            )
            .map_err(|err| ReplayError::new(&pending, err.message.clone()))?;
        let runtime_memory_reservation = self
            .admit_runtime_memory(
                request_kind,
                &pending.headers,
                pending.compressed_body.len(),
                metrics.as_ref(),
            )
            .map_err(|err| ReplayError::new(&pending, err.message.clone()))?;
        self.ensure_ingest_workers_available(request_kind, metrics.as_ref())
            .map_err(|err| ReplayError::new(&pending, err.message.clone()))?;
        // The recovered record is already durably spooled; emit the `spooled`
        // boundary for funnel consistency. See `crate::ingest::lifecycle`.
        lifecycle::record(metrics.as_ref(), request_kind, IngestStage::Spooled);
        let work = SpooledIngestWork {
            request_kind,
            headers: pending.headers.clone(),
            compressed_body: pending.compressed_body,
            raw_spool_ref: pending.raw_spool_ref,
            inflight_reservation,
            runtime_memory_reservation,
            metrics: metrics.clone(),
        };
        self.dispatch_ingest_work(work, storage, metrics.as_ref())
            .map(|_| ())
            .map_err(|err| {
                ReplayError::new_raw(
                    pending.raw_spool_ref,
                    request_kind,
                    anyhow::anyhow!(err.message),
                )
            })
    }

    pub(crate) fn ingest(
        &self,
        route: OtlpRequestKind,
        headers: &HashMap<String, String>,
        compressed_body: Vec<u8>,
        storage: &Storage,
        admission: &AdmissionController,
        metrics: Arc<Metrics>,
    ) -> ApiResult<Value> {
        let worker_metrics = Arc::clone(&metrics);
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
        // Every request-path gate passed: the request is accepted. See
        // `crate::ingest::lifecycle`.
        lifecycle::record(metrics, route, IngestStage::Accepted);
        let (raw_spool_ref, compressed_body) =
            match self
                .raw_spool
                .append(route, headers, compressed_body, metrics)
            {
                Ok(appended) => appended,
                Err(err) => {
                    metrics.ingest_request(route.as_str(), err.status, err.reason);
                    return Err(err);
                }
            };
        // `RawSpool::append` succeeded, so the raw request is fsynced.
        lifecycle::record(metrics, route, IngestStage::Spooled);
        let spooled = SpooledIngestWork {
            request_kind: route,
            headers: headers.clone(),
            compressed_body,
            raw_spool_ref,
            inflight_reservation,
            runtime_memory_reservation,
            metrics: worker_metrics,
        };
        self.dispatch_ingest_work(spooled, storage, metrics)
    }

    /// At-least-once transaction script for one durably-spooled request, read as
    /// the lifecycle funnel: spooled -> transformed -> buffered. Each phase is a
    /// named helper that either transforms the payload or produces an explicit
    /// disposition; every terminal/retryable raw-spool decision stays visible
    /// here. Phase helpers never decide 202 — the raw request is already spooled,
    /// so a failure only chooses how the spooled record is disposed (terminally
    /// checkpointed, or left pending for replay).
    pub(in crate::ingest) fn process_spooled_ingest(
        &self,
        work: SpooledIngestWork,
        storage: &Storage,
    ) -> std::result::Result<SpooledIngestDisposition, SpooledIngestError> {
        let SpooledIngestWork {
            request_kind: route,
            headers,
            compressed_body,
            raw_spool_ref,
            mut inflight_reservation,
            mut runtime_memory_reservation,
            metrics,
        } = work;
        let metrics_arc = metrics;
        let metrics = metrics_arc.as_ref();
        let request_context = SpooledRequestContext {
            route,
            raw_spool_ref,
            metrics,
        };
        tracing::trace!(
            event = "ingest_processing_started",
            request_kind = route.as_str(),
        );

        // spooled -> decoded: decompress and re-check the decoded body size.
        let body = self.decode_spooled_body(&headers, &compressed_body, request_context)?;
        let decoded_body_materialized_bytes = match &body {
            std::borrow::Cow::Borrowed(_) => 0,
            std::borrow::Cow::Owned(bytes) => bytes.len(),
        };
        let decoded_body_len = body.len();

        // decoded -> transformed: OTLP -> Arrow batches, then timestamp-skew gate.
        let transformed = self.transform_spooled_body(&headers, &body, request_context)?;
        lifecycle::record(metrics, route, IngestStage::Transformed);
        let unsupported_histograms = transformed.unsupported_histograms;
        self.validate_spooled_skew(&transformed, request_context)?;
        for (output_signal, rows) in transformed_rows_by_signal(&transformed) {
            metrics.inc(
                MetricName::IngestTransformedRowsTotal,
                &[
                    ("storage_signal", output_signal.as_str()),
                    ("request_kind", route.as_str()),
                ],
                rows as u64,
            );
        }

        // transformed -> accounted: exact in-flight correction + peak memory cap.
        let request_bytes = compressed_body.len();
        let batches = batches::pending_batches(transformed);
        let buffered_totals = pending_batch_totals(&batches);
        self.account_buffer_demand(
            &batches,
            request_bytes,
            decoded_body_materialized_bytes,
            &mut inflight_reservation,
            &mut runtime_memory_reservation,
            request_context,
        )?;
        if batches.is_empty() {
            // Nothing to buffer (e.g. all rows were unsupported): terminally
            // dispose so the spooled record will not replay.
            return match self.raw_spool.checkpoint_terminal(
                request_context.raw_spool_ref,
                request_context.route,
                "transform_empty",
                request_context.metrics,
            ) {
                Ok(()) => Ok(SpooledIngestDisposition::TerminallyDisposed),
                Err(checkpoint_err) => Err(SpooledIngestError::pending_replay(checkpoint_err)),
            };
        }

        // accounted -> buffered: append replay-backed rows to the write buffer.
        let buffered = self.buffer_spooled_batches(storage, &batches, request_context)?;
        // Rows reached the Arrow write buffer; the per-request phase terminus.
        // See `crate::ingest::lifecycle`.
        lifecycle::record(metrics, route, IngestStage::Buffered);
        observe_storage_timings(metrics, &buffered.timings);
        metrics.inc(
            MetricName::IngestStorageInsertTotal,
            &[("request_kind", route.as_str()), ("status", "ok")],
            1,
        );

        // Rows are now in the Arrow write buffer together with their replay ref,
        // so the seal snapshot owns the commit->checkpoint binding.
        drop(inflight_reservation);
        tracing::debug!(event = "ingest_buffered", request_kind = route.as_str(),);

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
            MetricName::IngestRecordsTotal,
            &[("request_kind", route.as_str())],
            accepted as u64,
        );
        if unsupported_histograms > 0 {
            metrics.inc(
                MetricName::IngestUnsupportedHistogramsTotal,
                &[("request_kind", route.as_str())],
                unsupported_histograms as u64,
            );
        }
        for (output_signal, (rows, bytes)) in buffered_totals {
            metrics.inc(
                MetricName::IngestBufferedRowsTotal,
                &[("storage_signal", output_signal.as_str())],
                rows as u64,
            );
            metrics.inc(
                MetricName::IngestBufferedBytesTotal,
                &[("storage_signal", output_signal.as_str())],
                bytes as u64,
            );
        }

        Ok(SpooledIngestDisposition::Buffered)
    }

    /// Terminal checkpoint: the spooled record can never succeed, so checkpoint it
    /// (mark committed so it will not replay) and surface the original rejection.
    /// If the checkpoint itself fails, the record is left pending for replay
    /// instead. The single place the terminal-vs-retryable raw-spool decision is
    /// made on the worker path.
    fn dispose_terminal(
        &self,
        context: SpooledRequestContext<'_>,
        reason: &'static str,
        err: ApiError,
    ) -> SpooledIngestError {
        match self.raw_spool.checkpoint_terminal(
            context.raw_spool_ref,
            context.route,
            reason,
            context.metrics,
        ) {
            Ok(()) => SpooledIngestError::terminal(err),
            Err(checkpoint_err) => SpooledIngestError::pending_replay(checkpoint_err),
        }
    }

    /// Decode phase: decompress (if needed) and re-check the decoded body size.
    /// Returns the decoded body borrowed from `compressed_body`, or a terminal
    /// disposition for an undecodable / oversized payload.
    fn decode_spooled_body<'b>(
        &self,
        headers: &HashMap<String, String>,
        compressed_body: &'b [u8],
        context: SpooledRequestContext<'_>,
    ) -> std::result::Result<std::borrow::Cow<'b, [u8]>, SpooledIngestError> {
        let started = Instant::now();
        let body_result = otlp::decompress_if_needed(
            headers,
            compressed_body,
            self.config.operator.max_body_bytes,
        );
        context.metrics.observe_request_phase_seconds(
            context.route.as_str(),
            "decompress",
            started.elapsed().as_secs_f64(),
        );
        let body = body_result.map_err(|err| {
            let reason = if err.reason == "payload_too_large" {
                "body_size_invalid"
            } else {
                "decode_failed"
            };
            self.dispose_terminal(context, reason, err)
        })?;
        if let Err(err) = validation::validate_body_size(body.len(), &self.config) {
            return Err(self.dispose_terminal(context, "body_size_invalid", err));
        }
        Ok(body)
    }

    /// Transform phase: turn the decoded OTLP body into Arrow `RecordBatch`es, or
    /// a terminal disposition for a payload that cannot be transformed.
    fn transform_spooled_body(
        &self,
        headers: &HashMap<String, String>,
        body: &[u8],
        context: SpooledRequestContext<'_>,
    ) -> std::result::Result<Transformed, SpooledIngestError> {
        let started = Instant::now();
        #[cfg(feature = "otlp2records-observer")]
        let transformed_result =
            otlp::transform_observed(context.route, headers, body, context.metrics);
        #[cfg(not(feature = "otlp2records-observer"))]
        let transformed_result = otlp::transform(context.route, headers, body);
        context.metrics.observe_request_phase_seconds(
            context.route.as_str(),
            "otlp_transform",
            started.elapsed().as_secs_f64(),
        );
        transformed_result.map_err(|err| self.dispose_terminal(context, "transform_failed", err))
    }

    /// Timestamp-validation phase: reject batches whose timestamps fall outside
    /// the configured skew window, as a terminal disposition.
    fn validate_spooled_skew(
        &self,
        transformed: &Transformed,
        context: SpooledRequestContext<'_>,
    ) -> std::result::Result<(), SpooledIngestError> {
        let started = Instant::now();
        let skew_result = self.validate_skew(transformed);
        context.metrics.observe_request_phase_seconds(
            context.route.as_str(),
            "timestamp_validation",
            started.elapsed().as_secs_f64(),
        );
        skew_result.map_err(|err| self.dispose_terminal(context, "timestamp_rejected", err))
    }

    /// Exact-accounting phase: correct the in-flight reservation to the real
    /// buffered Arrow byte size (infallible — the request is already spooled) and
    /// reserve the peak runtime memory the buffer will demand. Only the optional
    /// runtime-memory cap can reject, as a terminal disposition.
    fn account_buffer_demand(
        &self,
        batches: &[batches::PendingBatch],
        request_bytes: usize,
        decoded_body_materialized_bytes: usize,
        inflight_reservation: &mut admission::InflightReservation,
        runtime_memory_reservation: &mut admission::RuntimeMemoryReservation,
        context: SpooledRequestContext<'_>,
    ) -> std::result::Result<(), SpooledIngestError> {
        inflight_reservation.adjust(admission::inflight_bytes_by_signal(batches));
        let pending_bytes = batches.iter().map(|b| b.approx_bytes).sum::<usize>();
        let peak_bytes = request_bytes
            .saturating_add(decoded_body_materialized_bytes)
            .saturating_add(pending_bytes);
        runtime_memory_reservation
            .reserve_at_least(peak_bytes, context.route, context.metrics)
            .map_err(|err| self.dispose_terminal(context, "memory_rejected", err))
    }

    /// Buffer phase: append the transformed rows to the storage Arrow write
    /// buffer, bound to their raw-spool replay ref so the seal owns the
    /// commit->checkpoint binding. A storage fault leaves the record pending for
    /// replay (retryable); the caller then stamps the `buffered` lifecycle stage.
    fn buffer_spooled_batches(
        &self,
        storage: &Storage,
        batches: &[batches::PendingBatch],
        context: SpooledRequestContext<'_>,
    ) -> std::result::Result<ArrowBatchBufferResult, SpooledIngestError> {
        let replay_ref = ReplayBackedRecordRef::new(context.raw_spool_ref);
        let buffers = batches
            .iter()
            .filter(|batch| batch.batch.num_rows() > 0)
            .map(|batch| ReplayBackedArrowBatch {
                storage_signal: batch.signal,
                batch: &batch.batch,
                source_format: batch.source_format,
                replay_ref,
            })
            .collect::<Vec<_>>();
        let buffer_started = Instant::now();
        let buffer_result = storage.buffer_replay_backed_arrow_batches(&buffers);
        context.metrics.observe_request_phase_seconds(
            context.route.as_str(),
            "storage_buffer",
            buffer_started.elapsed().as_secs_f64(),
        );
        buffer_result.map_err(|err| {
            // The rows never reached the Arrow write buffer and the raw-spool
            // record was not tracked, so it stays pending and replays on a future
            // restart (at-least-once). Surface a retryable dependency error; the
            // admission credit drops with this scope.
            context.metrics.inc(
                MetricName::IngestStorageInsertTotal,
                &[
                    ("request_kind", context.route.as_str()),
                    ("status", "error"),
                ],
                1,
            );
            tracing::warn!(
                event = "ingest_storage_insert_failed",
                request_kind = context.route.as_str(),
                error = %err
            );
            SpooledIngestError::pending_replay(
                ApiError::new(503, "storage_insert_failed", "storage insert failed")
                    .with_retry_after(5),
            )
        })
    }

    fn dispatch_ingest_work(
        &self,
        work: SpooledIngestWork,
        storage: &Storage,
        metrics: &Metrics,
    ) -> ApiResult<Value> {
        let route = work.request_kind;
        // Round-robin to the first worker with buffer space. On a successful send
        // the function returns directly from inside the loop; if every worker is
        // full (or the pool is gone) the still-owned `work` falls through to the
        // caller-runs path below.
        let work = {
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
                                    MetricName::IngestWorkerDispatchTotal,
                                    &[("request_kind", route.as_str()), ("outcome", "queued")],
                                    1,
                                );
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
            MetricName::IngestWorkerDispatchTotal,
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
        // Caller-runs path: no worker took the handoff, so process the work inline
        // on this thread. The per-request boundary counters fire inside
        // `process_spooled_ingest` itself. See `crate::ingest::lifecycle`.
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
                status = err.error.status,
                reason = err.error.reason,
                disposition = err.disposition.as_str(),
                message = %err.error.message,
                "inline ingest processing failed after worker saturation; raw request stays durably spooled"
            );
        }
        metrics.ingest_request(route.as_str(), 202, "accepted_inline");
        Ok(json!({
            "accepted": true,
            "acknowledgement": "locally_spooled"
        }))
    }

    /// Fail-fast worker-availability gate, run on the request path BEFORE the
    /// durable raw-spool append: if the ingest worker pool is absent or empty
    /// (e.g. a query-only role, or every worker stopped), shed with 503 rather
    /// than spool the request and silently process it inline on the connection
    /// thread. This is deliberately a separate, earlier check from the re-lock in
    /// [`Self::dispatch_ingest_work`]; the window between the two is benign
    /// because the only state transition there is pool teardown at shutdown,
    /// which the dispatch caller-runs fallback already absorbs (it processes the
    /// already-spooled work inline instead of dropping it).
    fn ensure_ingest_workers_available(
        &self,
        route: OtlpRequestKind,
        metrics: &Metrics,
    ) -> ApiResult<()> {
        {
            let pool = self.ingest_workers.lock_or_poisoned();
            let Some(dispatcher) = pool.as_ref() else {
                metrics.inc(
                    MetricName::IngestWorkerDispatchTotal,
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
                    MetricName::IngestWorkerDispatchTotal,
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
            self.config.operator.max_body_bytes,
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
        let Some(limit) = self.config.operator.runtime_memory_limit_bytes else {
            return Ok(reservation);
        };
        reservation = reservation.with_limit(limit);
        reservation.reserve_at_least(
            admission::decode_reservation_bytes(
                headers,
                compressed_body_bytes,
                self.config.operator.max_body_bytes,
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
                MetricName::IngestInflightBytes,
                &[("storage_signal", snapshot.storage_signal)],
                snapshot.inflight_bytes as f64,
            );
        }
    }

    pub fn record_worker_queue_metrics(&self, metrics: &Metrics) {
        metrics.gauge(
            MetricName::IngestWorkerQueueCapacity,
            &[("state", "capacity")],
            self.config.test_overrides.ingest_worker_channel_capacity as f64,
        );
    }

    pub fn freshness_budget_inputs(&self, storage: &Storage) -> FreshnessBudgetInputs {
        // Ingest hot path: ask storage for exactly the three buffer scalars the
        // freshness projection takes, folded under one lock with no per-signal
        // vec allocation. The per-signal `arrow_write_buffer_metrics` detail is
        // reserved for the scheduler/admin paths.
        let buffers = storage.arrow_write_buffer_freshness();
        FreshnessBudgetInputs {
            inflight_bytes: self.inflight_bytes(),
            incoming_bytes: 0,
            buffered_bytes: buffers.buffered_bytes,
            buffered_active_count: buffers.buffered_active_count,
            oldest_buffer_age_seconds: buffers.oldest_buffer_age_seconds,
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
            MetricName::IngestRequestBytesTotal,
            &[("request_kind", route), ("encoding", encoding)],
            request_bytes as u64,
        );
        metrics.inc(
            MetricName::IngestDecodedBytesTotal,
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
            timing.storage_signal.as_str(),
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
