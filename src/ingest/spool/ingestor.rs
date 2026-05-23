use super::{full_info, AppendBatchStats, CheckpointBatchStats, Record, RecordId, Writer};
use crate::admission_control::AdmissionController;
use crate::ingest::{lifecycle, IngestStage, Ingestor, OtlpRequestKind, SealStage};
use crate::metrics::Metrics;
use crate::storage::{ArrowFlushOutcome, Storage, TimingPhase};
use crate::validation::{self, ApiError, ApiResult};
use crate::LockExt;
use anyhow::{Context, Result};
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::Instant;

#[derive(Clone, Copy, Debug)]
pub(in crate::ingest) struct SealRef {
    pub(in crate::ingest) request_kind: OtlpRequestKind,
}

#[derive(Clone, Copy, Debug)]
pub(in crate::ingest) struct AppendRef {
    pub(in crate::ingest) spool: OtlpRequestKind,
    pub(in crate::ingest) id: RecordId,
}

struct RecoveredWork {
    raw_spool_ref: AppendRef,
    route: OtlpRequestKind,
    headers: HashMap<String, String>,
    compressed_body: Vec<u8>,
}

impl Ingestor {
    pub fn replay_raw_spool(
        &self,
        storage: &Storage,
        admission: &AdmissionController,
        metrics: Arc<Metrics>,
    ) -> Result<usize> {
        let mut replayed = 0usize;
        for request_kind in OtlpRequestKind::ALL {
            let pending = self
                .raw_spool_for(request_kind)?
                .recover_pending()
                .with_context(|| format!("recover {request_kind} raw spool pending records"))?;
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
                        ("request_kind", recovered.record.request_kind.as_str()),
                        ("status", "attempted"),
                    ],
                    1,
                );
                match self.ingest_replayed_raw_record(
                    RecoveredWork {
                        raw_spool_ref: AppendRef {
                            spool: request_kind,
                            id: recovered.id,
                        },
                        route: recovered.record.request_kind,
                        headers,
                        compressed_body: recovered.record.compressed_body,
                    },
                    storage,
                    admission,
                    metrics.clone(),
                ) {
                    Ok(()) => {
                        replayed += 1;
                        metrics.inc(
                            "canardstack_raw_spool_replayed_records_total",
                            &[
                                ("request_kind", recovered.record.request_kind.as_str()),
                                ("status", "ok"),
                            ],
                            1,
                        );
                    }
                    Err(err) => {
                        metrics.inc(
                            "canardstack_raw_spool_replayed_records_total",
                            &[
                                ("request_kind", recovered.record.request_kind.as_str()),
                                ("status", "failed"),
                            ],
                            1,
                        );
                        // Best-effort replay: a single failing record must never abort
                        // boot. The record stays un-checkpointed (still pending) and is
                        // retried on a future startup, preserving at-least-once delivery.
                        tracing::warn!(
                            event = "raw_spool_replay_record_failed",
                            request_kind = recovered.record.request_kind.as_str(),
                            record_segment = recovered.id.segment,
                            record_sequence = recovered.id.sequence,
                            error = %err,
                            "skipping raw spool replay record; left pending for retry"
                        );
                        continue;
                    }
                }
            }
        }
        self.record_raw_spool_metrics(metrics.as_ref());
        Ok(replayed)
    }

    fn ingest_replayed_raw_record(
        &self,
        recovered: RecoveredWork,
        storage: &Storage,
        admission: &AdmissionController,
        metrics: Arc<Metrics>,
    ) -> Result<()> {
        let RecoveredWork {
            raw_spool_ref,
            route,
            headers,
            compressed_body,
        } = recovered;
        validation::validate_body_size(compressed_body.len(), &self.config)
            .map_err(|err| anyhow::anyhow!(err.message.clone()))?;
        validation::validate_content_type(&headers)
            .map_err(|err| anyhow::anyhow!(err.message.clone()))?;
        if !storage.accepts_memory_ingest() {
            anyhow::bail!("storage dependency is unhealthy");
        }
        let inflight_reservation = self
            .admit_and_reserve_inflight(
                route,
                &headers,
                compressed_body.len(),
                storage,
                admission,
                metrics.as_ref(),
            )
            .map_err(|err| anyhow::anyhow!(err.message.clone()))?;
        let runtime_memory_reservation = self
            .admit_runtime_memory(route, &headers, compressed_body.len(), metrics.as_ref())
            .map_err(|err| anyhow::anyhow!(err.message.clone()))?;
        self.ensure_ingest_workers_available(route, metrics.as_ref())
            .map_err(|err| anyhow::anyhow!(err.message.clone()))?;
        // The recovered record is already durably spooled; emit the `spooled`
        // boundary for funnel consistency. See `crate::ingest::lifecycle`.
        lifecycle::record(metrics.as_ref(), route, IngestStage::Spooled);
        let work = crate::ingest::SpooledIngestWork {
            route,
            headers: headers.clone(),
            compressed_body,
            raw_spool_ref,
            inflight_reservation,
            runtime_memory_reservation,
            metrics: metrics.clone(),
        };
        self.dispatch_ingest_work(work, storage, metrics.as_ref())
            .map(|_| ())
            .map_err(|err| anyhow::anyhow!(err.message.clone()))
    }

    /// Record that a durably-spooled request's rows are now in the Arrow write
    /// buffer (the [`IngestStage::Buffered`] boundary in
    /// [`crate::ingest::lifecycle`]). The scheduler checkpoints the record after
    /// the next durable DuckLake commit (see
    /// [`Ingestor::seal_committed_to_storage`]). Called only after a successful
    /// buffer append so a tracked ref always implies buffered rows.
    pub(in crate::ingest) fn track_raw_spool_record(
        &self,
        raw_spool_ref: AppendRef,
        route: OtlpRequestKind,
    ) {
        self.raw_spool_seal_refs.lock_or_poisoned().insert(
            (raw_spool_ref.spool, raw_spool_ref.id),
            SealRef {
                request_kind: route,
            },
        );
    }

    /// Single seal driver, emitting the seal-phase boundaries in
    /// [`crate::ingest::lifecycle`]: capture the records to checkpoint, force-flush
    /// the whole Arrow write buffer to durable DuckLake storage
    /// ([`SealStage::Committed`]), then checkpoint exactly the captured records
    /// ([`SealStage::Checkpointed`]), or mark [`SealStage::DuplicateRisk`] if the
    /// checkpoint fails after the commit. Capturing before flushing is
    /// load-bearing for at-least-once: a record appended after the capture is not
    /// checkpointed until a later seal, so we never checkpoint rows that were not
    /// storage-committed.
    pub fn seal_committed_to_storage(
        &self,
        storage: &Storage,
        metrics: &Metrics,
    ) -> Result<ArrowFlushOutcome> {
        let captured = self.capture_committed_refs();
        // The seal counters are guarded on a non-empty capture so the periodic
        // scheduler seal that finds nothing buffered does not inflate the funnel:
        // an idle seal still flushes/commits an empty buffer but checkpoints no
        // records.
        let seal_records = !captured.is_empty();
        tracing::debug!(
            event = "seal_captured_refs",
            captured_records = captured.len(),
        );
        let outcome = match storage.flush_arrow_write_buffer(true) {
            Ok(outcome) => outcome,
            Err(err) => {
                self.restore_committed_refs(captured);
                return Err(err);
            }
        };
        tracing::debug!(
            event = "seal_ducklake_committed",
            flushed_rows = outcome.flushed_rows,
        );
        if seal_records {
            lifecycle::record_seal(metrics, SealStage::Committed);
        }
        observe_arrow_flush(metrics, &outcome);
        match self.checkpoint_raw_spool_batch(&captured, "storage_committed", Some(metrics)) {
            Ok(()) => {
                tracing::debug!(
                    event = "seal_raw_spool_checkpointed",
                    checkpointed_records = captured.len(),
                );
                if seal_records {
                    lifecycle::record_seal(metrics, SealStage::Checkpointed);
                }
            }
            Err(err) => {
                // Rows are durably committed; only the raw-spool checkpoint
                // failed. The records stay pending and replay as duplicate ROWS in
                // storage on a future restart. The checkpoint deliberately
                // follows the DuckLake COMMIT (capture before flush, checkpoint
                // after commit), so any crash or checkpoint failure between commit
                // and checkpoint re-ingests already-committed records. v0 does NOT
                // dedup, so those duplicate rows are surfaced verbatim to queries
                // after crash-replay. This branch only runs for a non-empty
                // capture (an empty checkpoint batch returns Ok), so it is always a
                // real per-seal duplicate-risk event. See `crate::ingest::lifecycle`.
                tracing::error!(
                    event = "raw_spool_checkpoint_failed",
                    error = %err,
                    "Arrow flush committed but raw spool checkpoint failed; records left pending"
                );
                lifecycle::record_seal(metrics, SealStage::DuplicateRisk);
            }
        }
        Ok(outcome)
    }

    fn capture_committed_refs(&self) -> Vec<(OtlpRequestKind, AppendRef)> {
        let mut refs = self.raw_spool_seal_refs.lock_or_poisoned();
        let captured = refs
            .iter()
            .map(|((spool, id), seal_ref)| {
                (
                    seal_ref.request_kind,
                    AppendRef {
                        spool: *spool,
                        id: *id,
                    },
                )
            })
            .collect::<Vec<_>>();
        refs.clear();
        captured
    }

    fn restore_committed_refs(&self, captured: Vec<(OtlpRequestKind, AppendRef)>) {
        let mut refs = self.raw_spool_seal_refs.lock_or_poisoned();
        for (request_kind, append_ref) in captured {
            refs.entry((append_ref.spool, append_ref.id))
                .or_insert(SealRef { request_kind });
        }
    }

    pub(in crate::ingest) fn append_raw_spool(
        &self,
        route: OtlpRequestKind,
        headers: &HashMap<String, String>,
        compressed_body: Vec<u8>,
        metrics: &Metrics,
    ) -> ApiResult<(AppendRef, Vec<u8>)> {
        let spool = route;
        let content_type = headers.get("content-type").cloned().unwrap_or_default();
        let content_encoding = headers.get("content-encoding").cloned();
        let compressed_body_len = compressed_body.len();
        let started = Instant::now();
        let record = Record::new(route, content_type, content_encoding, compressed_body);
        let result = self
            .raw_spool_for(spool)
            .and_then(|raw_spool| raw_spool.append(record));
        metrics.observe_request_phase_seconds(
            spool.as_str(),
            "raw_spool_append",
            started.elapsed().as_secs_f64(),
        );
        match result {
            Ok(ack) => {
                if let Some(stats) = ack.batch_stats {
                    Self::record_raw_spool_append_batch_metrics(metrics, spool, stats);
                }
                metrics.inc(
                    "canardstack_raw_spool_records_total",
                    &[("request_kind", spool.as_str()), ("status", "spooled")],
                    1,
                );
                metrics.inc(
                    "canardstack_raw_spool_bytes_total",
                    &[("request_kind", spool.as_str())],
                    compressed_body_len as u64,
                );
                Ok((AppendRef { spool, id: ack.id }, ack.compressed_body))
            }
            Err(err) => {
                if full_info(&err).is_some() {
                    metrics.inc(
                        "canardstack_raw_spool_records_total",
                        &[("request_kind", spool.as_str()), ("status", "full")],
                        1,
                    );
                    Err(ApiError::new(
                        429,
                        "raw_spool_full",
                        "raw ingest spool is full",
                    ))
                } else if err.to_string().contains("raw spool writer queue is full") {
                    metrics.inc(
                        "canardstack_raw_spool_records_total",
                        &[("request_kind", spool.as_str()), ("status", "queue_full")],
                        1,
                    );
                    Err(ApiError::new(
                        429,
                        "raw_spool_queue_full",
                        "raw ingest spool writer queue is full",
                    )
                    .with_retry_after(5))
                } else {
                    metrics.inc(
                        "canardstack_raw_spool_records_total",
                        &[("request_kind", spool.as_str()), ("status", "error")],
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

    fn record_raw_spool_append_batch_metrics(
        metrics: &Metrics,
        request_kind: OtlpRequestKind,
        stats: AppendBatchStats,
    ) {
        metrics.inc(
            "canardstack_raw_spool_append_batches_total",
            &[("request_kind", request_kind.as_str())],
            1,
        );
        metrics.inc(
            "canardstack_raw_spool_append_batch_records_total",
            &[("request_kind", request_kind.as_str())],
            stats.records as u64,
        );
        metrics.inc(
            "canardstack_raw_spool_append_batch_encoded_bytes_total",
            &[("request_kind", request_kind.as_str())],
            stats.encoded_bytes,
        );
        metrics.inc(
            "canardstack_raw_spool_append_file_fsyncs_total",
            &[("request_kind", request_kind.as_str())],
            stats.fsync_count,
        );
        // Fine-grained spool append phase micro-timings are gated behind the
        // `detailed-metrics` feature: the coarse `raw_spool_append` phase
        // (emitted in `append_raw_spool`) stays always-on for visibility.
        #[cfg(feature = "detailed-metrics")]
        {
            metrics.observe_request_phase_seconds_n(
                request_kind.as_str(),
                "raw_spool_append_queue_wait",
                stats.records as u64,
                stats.queue_seconds,
            );
            metrics.observe_request_phase_seconds(
                request_kind.as_str(),
                "raw_spool_append_batch_wait",
                stats.wait_seconds,
            );
            metrics.observe_request_phase_seconds(
                request_kind.as_str(),
                "raw_spool_append_encode",
                stats.encode_seconds,
            );
            metrics.observe_request_phase_seconds(
                request_kind.as_str(),
                "raw_spool_append_write",
                stats.write_seconds,
            );
            metrics.observe_request_phase_seconds(
                request_kind.as_str(),
                "raw_spool_append_fsync",
                stats.fsync_seconds,
            );
        }
    }

    fn record_raw_spool_checkpoint_batch_metrics(
        metrics: &Metrics,
        request_kind: OtlpRequestKind,
        stats: CheckpointBatchStats,
    ) {
        if stats.records == 0 {
            return;
        }
        metrics.inc(
            "canardstack_raw_spool_checkpoint_batches_total",
            &[("request_kind", request_kind.as_str())],
            1,
        );
        metrics.inc(
            "canardstack_raw_spool_checkpoint_batch_records_total",
            &[("request_kind", request_kind.as_str())],
            stats.records as u64,
        );
        metrics.inc(
            "canardstack_raw_spool_checkpoint_batch_commands_total",
            &[("request_kind", request_kind.as_str())],
            stats.commands as u64,
        );
        // Gated like the append micro-timings; the coarse
        // `raw_spool_checkpoint` phase remains always-on.
        #[cfg(feature = "detailed-metrics")]
        {
            metrics.observe_request_phase_seconds_n(
                request_kind.as_str(),
                "raw_spool_checkpoint_queue_wait",
                stats.records as u64,
                stats.queue_seconds,
            );
            metrics.observe_request_phase_seconds(
                request_kind.as_str(),
                "raw_spool_checkpoint_batch_wait",
                stats.wait_seconds,
            );
        }
    }

    /// Terminal disposition for a payload that can never succeed: checkpoint the
    /// raw-spool record so it will not replay; the caller returns the rejection
    /// afterward. (Retryable storage faults do NOT take this path — they leave the
    /// record pending so it replays on restart.) A terminally-rejected request is
    /// dropped from the lifecycle funnel and stays covered by
    /// `canardstack_raw_spool_checkpointed_records_total{reason}` and
    /// `canardstack_ingest_requests_total{reason}`; no stage counter is emitted.
    pub(in crate::ingest) fn checkpoint_raw_spool_terminal(
        &self,
        raw_spool_ref: AppendRef,
        route: OtlpRequestKind,
        reason: &'static str,
        metrics: &Metrics,
    ) -> ApiResult<()> {
        tracing::debug!(
            event = "ingest_terminally_rejected",
            request_kind = route.as_str(),
            reason,
        );
        self.checkpoint_raw_spool(raw_spool_ref, route, reason, Some(metrics))
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
        raw_spool_ref: AppendRef,
        route: OtlpRequestKind,
        reason: &'static str,
        metrics: Option<&Metrics>,
    ) -> Result<()> {
        let started = Instant::now();
        let stats = self
            .raw_spool_for(raw_spool_ref.spool)?
            .mark_committed(raw_spool_ref.id)
            .context("checkpoint raw spool record")?;
        if let Some(metrics) = metrics {
            let seconds = started.elapsed().as_secs_f64();
            Self::record_raw_spool_checkpoint_batch_metrics(metrics, raw_spool_ref.spool, stats);
            metrics.observe_seconds(
                "canardstack_phase_duration_seconds",
                &[
                    ("request_kind", route.as_str()),
                    ("phase", "raw_spool_terminal_checkpoint"),
                    ("reason", reason),
                ],
                seconds,
            );
            metrics.observe_request_phase_seconds(route.as_str(), "raw_spool_checkpoint", seconds);
            metrics.inc(
                "canardstack_raw_spool_checkpointed_records_total",
                &[("request_kind", route.as_str()), ("reason", reason)],
                1,
            );
        }
        Ok(())
    }

    fn checkpoint_raw_spool_batch(
        &self,
        records: &[(OtlpRequestKind, AppendRef)],
        reason: &'static str,
        metrics: Option<&Metrics>,
    ) -> Result<()> {
        if records.is_empty() {
            return Ok(());
        }
        let started = Instant::now();
        let mut by_request_kind_ids = BTreeMap::<OtlpRequestKind, Vec<RecordId>>::new();
        for (_, raw_spool_ref) in records {
            by_request_kind_ids
                .entry(raw_spool_ref.spool)
                .or_default()
                .push(raw_spool_ref.id);
        }
        for (request_kind, ids) in by_request_kind_ids {
            let stats = self
                .raw_spool_for(request_kind)?
                .mark_committed_batch(&ids)
                .with_context(|| format!("checkpoint {request_kind} raw spool records"))?;
            if let Some(metrics) = metrics {
                Self::record_raw_spool_checkpoint_batch_metrics(metrics, request_kind, stats);
            }
        }
        if let Some(metrics) = metrics {
            metrics.observe_seconds(
                "canardstack_phase_duration_seconds",
                &[("phase", "raw_spool_checkpoint")],
                started.elapsed().as_secs_f64(),
            );
            let mut by_request_kind = BTreeMap::<OtlpRequestKind, u64>::new();
            for (request_kind, _) in records {
                *by_request_kind.entry(*request_kind).or_default() += 1;
            }
            for (request_kind, count) in by_request_kind {
                metrics.inc(
                    "canardstack_raw_spool_checkpointed_records_total",
                    &[("request_kind", request_kind.as_str()), ("reason", reason)],
                    count,
                );
            }
        }
        Ok(())
    }

    pub fn raw_spool_stats(&self) -> Result<super::Stats> {
        let mut aggregate = super::Stats {
            healthy: true,
            ..Default::default()
        };
        for request_kind in OtlpRequestKind::ALL {
            let stats = self
                .raw_spool_for(request_kind)?
                .stats()
                .with_context(|| format!("read {request_kind} raw spool stats"))?;
            merge_raw_spool_stats(&mut aggregate, &stats);
        }
        Ok(aggregate)
    }

    pub fn raw_spool_stats_by_request_kind(&self) -> Result<BTreeMap<&'static str, super::Stats>> {
        let mut stats_by_request_kind = BTreeMap::new();
        for request_kind in OtlpRequestKind::ALL {
            stats_by_request_kind.insert(
                request_kind.as_str(),
                self.raw_spool_for(request_kind)?
                    .stats()
                    .with_context(|| format!("read {request_kind} raw spool stats"))?,
            );
        }
        Ok(stats_by_request_kind)
    }

    /// True only when every raw-spool request-kind writer is healthy. A writer
    /// that cannot read its stats (thread stopped/poisoned) or is in the fatal
    /// append/fsync latch counts as unhealthy so readiness reports NOT ready.
    pub fn raw_spool_healthy(&self) -> bool {
        OtlpRequestKind::ALL.into_iter().all(|request_kind| {
            self.raw_spool_for(request_kind)
                .and_then(|spool| spool.stats())
                .map(|stats| stats.healthy)
                .unwrap_or(false)
        })
    }

    /// Force a single raw-spool request-kind writer into the fatal/unhealthy latch,
    /// mirroring a real append/fsync failure. Intended for tests that exercise
    /// readiness wiring; gated to debug builds.
    #[doc(hidden)]
    pub fn force_raw_spool_unhealthy(
        &self,
        request_kind: OtlpRequestKind,
        message: impl Into<String>,
    ) -> Result<()> {
        self.raw_spool_for(request_kind)?.inject_fatal(message)
    }

    /// Per-request-kind raw-spool writer health, with the latched error message
    /// for any unhealthy request kind so the health JSON can show which writer is
    /// wedged.
    pub fn raw_spool_health_by_request_kind(
        &self,
    ) -> BTreeMap<&'static str, (bool, Option<String>)> {
        let mut health = BTreeMap::new();
        for request_kind in OtlpRequestKind::ALL {
            let entry = match self
                .raw_spool_for(request_kind)
                .and_then(|spool| spool.stats())
            {
                Ok(stats) => (stats.healthy, stats.error),
                Err(err) => (false, Some(err.to_string())),
            };
            health.insert(request_kind.as_str(), entry);
        }
        health
    }

    pub fn record_raw_spool_metrics(&self, metrics: &Metrics) {
        // Only per-request-kind series are emitted; the aggregate is derivable
        // as `sum without(request_kind)` and was dropped in the metrics diet.
        for request_kind in OtlpRequestKind::ALL {
            let Ok(stats) = self
                .raw_spool_for(request_kind)
                .and_then(|spool| spool.stats())
            else {
                continue;
            };
            metrics.gauge(
                "canardstack_raw_spool_segment_bytes",
                &[("request_kind", request_kind.as_str())],
                stats.segment_bytes as f64,
            );
            metrics.gauge(
                "canardstack_raw_spool_segments",
                &[("request_kind", request_kind.as_str())],
                stats.segment_count as f64,
            );
            metrics.gauge(
                "canardstack_raw_spool_pending_records",
                &[("request_kind", request_kind.as_str())],
                stats.pending_records as f64,
            );
            metrics.gauge(
                "canardstack_raw_spool_pending_bytes",
                &[("request_kind", request_kind.as_str())],
                stats.pending_bytes as f64,
            );
            metrics.gauge(
                "canardstack_raw_spool_unsynced_records",
                &[("request_kind", request_kind.as_str())],
                stats.unsynced_records as f64,
            );
            metrics.gauge(
                "canardstack_raw_spool_unsynced_bytes",
                &[("request_kind", request_kind.as_str())],
                stats.unsynced_bytes as f64,
            );
            metrics.gauge(
                "canardstack_raw_spool_unsynced_age_seconds",
                &[("request_kind", request_kind.as_str())],
                stats.unsynced_age_seconds,
            );
            metrics.gauge(
                "canardstack_raw_spool_healthy",
                &[("request_kind", request_kind.as_str())],
                if stats.healthy { 1.0 } else { 0.0 },
            );
            metrics.set_counter(
                "canardstack_raw_spool_append_syncs_total",
                &[("request_kind", request_kind.as_str())],
                stats.append_syncs_total,
            );
            metrics.set_counter(
                "canardstack_raw_spool_append_sync_failures_total",
                &[("request_kind", request_kind.as_str())],
                stats.append_sync_failures_total,
            );
            metrics.set_counter(
                "canardstack_raw_spool_append_file_fsyncs_total",
                &[("request_kind", request_kind.as_str())],
                stats.append_sync_file_fsyncs_total,
            );
            // The per-request-kind fsync phase observation is a fine
            // micro-timing; gate it behind `detailed-metrics` alongside the
            // other spool internals.
            #[cfg(feature = "detailed-metrics")]
            metrics.set_observation(
                "canardstack_phase_duration_seconds",
                &[
                    ("request_kind", request_kind.as_str()),
                    ("phase", "raw_spool_append_fsync"),
                ],
                stats.append_syncs_total,
                stats.append_sync_seconds_total,
            );
        }
    }

    fn raw_spool_for(&self, request_kind: OtlpRequestKind) -> Result<&Writer> {
        self.raw_spools
            .get(&request_kind)
            .with_context(|| format!("raw spool writer for {request_kind} is unavailable"))
    }
}

fn observe_arrow_flush(metrics: &Metrics, outcome: &ArrowFlushOutcome) {
    for timing in &outcome.timings {
        metrics.observe_storage_signal_phase_seconds(
            timing.table.as_str(),
            timing.phase.as_str(),
            timing.seconds,
        );
    }
    if outcome.flushed_rows == 0 {
        return;
    }
    for timing in &outcome.timings {
        if timing.phase == TimingPhase::DuckdbArrowAppend {
            metrics.inc(
                "canardstack_duckdb_arrow_appends_total",
                &[("storage_signal", timing.table.as_str())],
                1,
            );
            metrics.inc(
                "canardstack_duckdb_arrow_appended_rows_total",
                &[("storage_signal", timing.table.as_str())],
                timing.rows as u64,
            );
        } else if timing.phase == TimingPhase::DucklakeCommit {
            metrics.inc(
                "canardstack_arrow_flush_rows_total",
                &[("storage_signal", timing.table.as_str())],
                timing.rows as u64,
            );
            metrics.inc(
                "canardstack_arrow_flushes_total",
                &[("storage_signal", timing.table.as_str())],
                1,
            );
        }
    }
}

fn merge_raw_spool_stats(aggregate: &mut super::Stats, stats: &super::Stats) {
    aggregate.segment_count += stats.segment_count;
    aggregate.segment_bytes = aggregate.segment_bytes.saturating_add(stats.segment_bytes);
    aggregate.pending_records += stats.pending_records;
    aggregate.pending_bytes = aggregate.pending_bytes.saturating_add(stats.pending_bytes);
    aggregate.unsynced_records += stats.unsynced_records;
    aggregate.unsynced_bytes = aggregate
        .unsynced_bytes
        .saturating_add(stats.unsynced_bytes);
    aggregate.unsynced_age_seconds = aggregate
        .unsynced_age_seconds
        .max(stats.unsynced_age_seconds);
    aggregate.append_syncs_total = aggregate
        .append_syncs_total
        .saturating_add(stats.append_syncs_total);
    aggregate.append_sync_failures_total = aggregate
        .append_sync_failures_total
        .saturating_add(stats.append_sync_failures_total);
    aggregate.append_sync_seconds_total += stats.append_sync_seconds_total;
    aggregate.append_sync_file_fsyncs_total = aggregate
        .append_sync_file_fsyncs_total
        .saturating_add(stats.append_sync_file_fsyncs_total);
    aggregate.healthy &= stats.healthy;
    if let Some(error) = &stats.error {
        aggregate.error = Some(match aggregate.error.take() {
            Some(existing) => format!("{existing}; {error}"),
            None => error.clone(),
        });
    }
}
