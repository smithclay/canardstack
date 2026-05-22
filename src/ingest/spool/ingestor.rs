use super::{full_info, AppendBatchStats, CheckpointBatchStats, Record, RecordId, Writer};
use crate::ingest::{all_signals, IngestRoute, Ingestor, Signal};
use crate::lanes::LaneController;
use crate::metrics::Metrics;
use crate::storage::{ImmutableFlushOutcome, Storage, TimingPhase};
use crate::validation::{self, ApiError, ApiResult};
use crate::LockExt;
use anyhow::{Context, Result};
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Instant;

#[derive(Clone, Copy, Debug)]
pub(in crate::ingest) struct FlushRef {
    pub(in crate::ingest) signal: Signal,
}

#[derive(Clone, Copy, Debug)]
pub(in crate::ingest) struct AppendRef {
    pub(in crate::ingest) shard: Signal,
    pub(in crate::ingest) id: RecordId,
}

struct RecoveredWork {
    raw_spool_ref: AppendRef,
    signal: Signal,
    headers: HashMap<String, String>,
    compressed_body: Vec<u8>,
}

impl Ingestor {
    pub fn replay_raw_spool(
        &self,
        storage: &Storage,
        lanes: &LaneController,
        metrics: Arc<Metrics>,
    ) -> Result<usize> {
        let mut replayed = 0usize;
        for signal in all_signals() {
            let pending = self
                .raw_spool_for(signal)?
                .recover_pending()
                .with_context(|| format!("recover {signal} raw spool pending records"))?;
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
                    RecoveredWork {
                        raw_spool_ref: AppendRef {
                            shard: signal,
                            id: recovered.id,
                        },
                        signal: recovered.record.signal,
                        headers,
                        compressed_body: recovered.record.compressed_body,
                    },
                    storage,
                    lanes,
                    metrics.clone(),
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
                        // Best-effort replay: a single failing record must never abort
                        // boot. The record stays un-checkpointed (still pending) and is
                        // retried on a future startup, preserving at-least-once delivery.
                        tracing::warn!(
                            event = "raw_spool_replay_record_failed",
                            signal = recovered.record.signal.as_str(),
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
        lanes: &LaneController,
        metrics: Arc<Metrics>,
    ) -> Result<()> {
        let RecoveredWork {
            raw_spool_ref,
            signal,
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
            .reserve_inflight(
                signal,
                &headers,
                compressed_body.len(),
                storage,
                lanes,
                metrics.as_ref(),
            )
            .map_err(|err| anyhow::anyhow!(err.message.clone()))?;
        let runtime_memory_reservation = self
            .admit_runtime_memory(signal, &headers, compressed_body.len(), metrics.as_ref())
            .map_err(|err| anyhow::anyhow!(err.message.clone()))?;
        self.ensure_ingest_workers_available(signal, metrics.as_ref())
            .map_err(|err| anyhow::anyhow!(err.message.clone()))?;
        let work = crate::ingest::SpooledIngestWork {
            route: IngestRoute::from_spool_record_signal(signal),
            signal,
            headers: headers.clone(),
            compressed_body,
            raw_spool_ref,
            inflight_reservation,
            runtime_memory_reservation,
            metrics: metrics.clone(),
        };
        self.dispatch_ingest_work(work, metrics.as_ref(), false)
            .map(|_| ())
            .map_err(|err| anyhow::anyhow!(err.message.clone()))
    }

    /// Record that a durably-spooled request's rows are now in the storage
    /// immutable buffer. The scheduler checkpoints the record after the next
    /// durable seal (see [`Ingestor::flush_committed_to_storage`]). Called only
    /// after a successful buffer append so a tracked ref always implies buffered rows.
    pub(in crate::ingest) fn track_raw_spool_record(
        &self,
        raw_spool_ref: AppendRef,
        signal: Signal,
    ) {
        self.raw_spool_flush_refs
            .lock_or_poisoned()
            .insert((raw_spool_ref.shard, raw_spool_ref.id), FlushRef { signal });
    }

    /// Single seal driver: capture the records to checkpoint, force-seal the
    /// whole immutable buffer to durable storage, then checkpoint exactly the
    /// captured records. Capturing before sealing is load-bearing for
    /// at-least-once: a record appended after the capture is not checkpointed
    /// until a later flush, so we never checkpoint rows that were not sealed.
    pub fn flush_committed_to_storage(
        &self,
        storage: &Storage,
        metrics: &Metrics,
    ) -> Result<ImmutableFlushOutcome> {
        let captured = self.capture_committed_refs();
        let outcome = match storage.flush_immutable_segments(true) {
            Ok(outcome) => outcome,
            Err(err) => {
                self.restore_committed_refs(captured);
                return Err(err);
            }
        };
        observe_immutable_flush(metrics, &outcome);
        if let Err(err) =
            self.checkpoint_raw_spool_batch(&captured, "storage_committed", Some(metrics))
        {
            // Rows are durably sealed; only the raw-spool checkpoint failed. The
            // records stay pending and replay (as duplicates) on a future
            // restart, which at-least-once allows.
            tracing::error!(
                event = "raw_spool_checkpoint_failed",
                error = %err,
                "segments sealed but raw spool checkpoint failed; records left pending"
            );
        }
        Ok(outcome)
    }

    fn capture_committed_refs(&self) -> Vec<(Signal, AppendRef)> {
        let mut refs = self.raw_spool_flush_refs.lock_or_poisoned();
        let captured = refs
            .iter()
            .map(|((lane, id), flush_ref)| {
                (
                    flush_ref.signal,
                    AppendRef {
                        shard: *lane,
                        id: *id,
                    },
                )
            })
            .collect::<Vec<_>>();
        refs.clear();
        captured
    }

    fn restore_committed_refs(&self, captured: Vec<(Signal, AppendRef)>) {
        let mut refs = self.raw_spool_flush_refs.lock_or_poisoned();
        for (signal, append_ref) in captured {
            refs.entry((append_ref.shard, append_ref.id))
                .or_insert(FlushRef { signal });
        }
    }

    pub(in crate::ingest) fn append_raw_spool(
        &self,
        signal: Signal,
        headers: &HashMap<String, String>,
        compressed_body: Vec<u8>,
        metrics: &Metrics,
    ) -> ApiResult<(AppendRef, Vec<u8>)> {
        let shard = self.raw_spool_shard_for_append(signal);
        let content_type = headers.get("content-type").cloned().unwrap_or_default();
        let content_encoding = headers.get("content-encoding").cloned();
        let compressed_body_len = compressed_body.len();
        let started = Instant::now();
        let record = Record::new(signal, content_type, content_encoding, compressed_body);
        let result = self
            .raw_spool_for(shard)
            .and_then(|raw_spool| raw_spool.append(record));
        metrics.observe_phase_seconds(
            shard.as_str(),
            "raw_spool_append",
            None,
            started.elapsed().as_secs_f64(),
        );
        match result {
            Ok(ack) => {
                if let Some(stats) = ack.batch_stats {
                    Self::record_raw_spool_append_batch_metrics(metrics, shard, stats);
                }
                metrics.inc(
                    "canardstack_raw_spool_records_total",
                    &[("signal", shard.as_str()), ("status", "spooled")],
                    1,
                );
                metrics.inc(
                    "canardstack_raw_spool_bytes_total",
                    &[("signal", shard.as_str())],
                    compressed_body_len as u64,
                );
                Ok((AppendRef { shard, id: ack.id }, ack.compressed_body))
            }
            Err(err) => {
                if full_info(&err).is_some() {
                    metrics.inc(
                        "canardstack_raw_spool_records_total",
                        &[("signal", shard.as_str()), ("status", "full")],
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
                        &[("signal", shard.as_str()), ("status", "queue_full")],
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
                        &[("signal", shard.as_str()), ("status", "error")],
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
        signal: Signal,
        stats: AppendBatchStats,
    ) {
        metrics.inc(
            "canardstack_raw_spool_append_batches_total",
            &[("signal", signal.as_str())],
            1,
        );
        metrics.inc(
            "canardstack_raw_spool_append_batch_records_total",
            &[("signal", signal.as_str())],
            stats.records as u64,
        );
        metrics.inc(
            "canardstack_raw_spool_append_batch_encoded_bytes_total",
            &[("signal", signal.as_str())],
            stats.encoded_bytes,
        );
        metrics.inc(
            "canardstack_raw_spool_append_file_fsyncs_total",
            &[("signal", signal.as_str())],
            stats.fsync_count,
        );
        metrics.gauge(
            "canardstack_raw_spool_append_batch_records",
            &[("signal", signal.as_str()), ("stat", "last")],
            stats.records as f64,
        );
        metrics.gauge_max(
            "canardstack_raw_spool_append_batch_records",
            &[("signal", signal.as_str()), ("stat", "max")],
            stats.records as f64,
        );
        metrics.gauge(
            "canardstack_raw_spool_append_batch_encoded_bytes",
            &[("signal", signal.as_str()), ("stat", "last")],
            stats.encoded_bytes as f64,
        );
        metrics.gauge_max(
            "canardstack_raw_spool_append_batch_encoded_bytes",
            &[("signal", signal.as_str()), ("stat", "max")],
            stats.encoded_bytes as f64,
        );
        metrics.observe_phase_seconds_n(
            signal.as_str(),
            "raw_spool_append_queue_wait",
            None,
            stats.records as u64,
            stats.queue_seconds,
        );
        metrics.observe_phase_seconds(
            signal.as_str(),
            "raw_spool_append_batch_wait",
            None,
            stats.wait_seconds,
        );
        metrics.observe_phase_seconds(
            signal.as_str(),
            "raw_spool_append_encode",
            None,
            stats.encode_seconds,
        );
        metrics.observe_phase_seconds(
            signal.as_str(),
            "raw_spool_append_write",
            None,
            stats.write_seconds,
        );
        metrics.observe_phase_seconds(
            signal.as_str(),
            "raw_spool_append_fsync",
            None,
            stats.fsync_seconds,
        );
    }

    fn record_raw_spool_checkpoint_batch_metrics(
        metrics: &Metrics,
        signal: Signal,
        stats: CheckpointBatchStats,
    ) {
        if stats.records == 0 {
            return;
        }
        metrics.inc(
            "canardstack_raw_spool_checkpoint_batches_total",
            &[("signal", signal.as_str())],
            1,
        );
        metrics.inc(
            "canardstack_raw_spool_checkpoint_batch_records_total",
            &[("signal", signal.as_str())],
            stats.records as u64,
        );
        metrics.inc(
            "canardstack_raw_spool_checkpoint_batch_commands_total",
            &[("signal", signal.as_str())],
            stats.commands as u64,
        );
        metrics.observe_phase_seconds_n(
            signal.as_str(),
            "raw_spool_checkpoint_queue_wait",
            None,
            stats.records as u64,
            stats.queue_seconds,
        );
        metrics.observe_phase_seconds(
            signal.as_str(),
            "raw_spool_checkpoint_batch_wait",
            None,
            stats.wait_seconds,
        );
    }

    pub(in crate::ingest) fn checkpoint_raw_spool_terminal(
        &self,
        raw_spool_ref: AppendRef,
        signal: Signal,
        reason: &'static str,
        metrics: &Metrics,
    ) -> ApiResult<()> {
        self.checkpoint_raw_spool(raw_spool_ref, signal, reason, Some(metrics))
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
        signal: Signal,
        reason: &'static str,
        metrics: Option<&Metrics>,
    ) -> Result<()> {
        let started = Instant::now();
        let stats = self
            .raw_spool_for(raw_spool_ref.shard)?
            .mark_committed(raw_spool_ref.id)
            .context("checkpoint raw spool record")?;
        if let Some(metrics) = metrics {
            let seconds = started.elapsed().as_secs_f64();
            Self::record_raw_spool_checkpoint_batch_metrics(metrics, raw_spool_ref.shard, stats);
            metrics.observe_seconds(
                "canardstack_phase_duration_seconds",
                &[
                    ("signal", signal.as_str()),
                    ("phase", "raw_spool_terminal_checkpoint"),
                    ("reason", reason),
                ],
                seconds,
            );
            metrics.observe_phase_seconds(signal.as_str(), "raw_spool_checkpoint", None, seconds);
            metrics.inc(
                "canardstack_raw_spool_checkpointed_records_total",
                &[("signal", signal.as_str()), ("reason", reason)],
                1,
            );
        }
        Ok(())
    }

    fn checkpoint_raw_spool_batch(
        &self,
        records: &[(Signal, AppendRef)],
        reason: &'static str,
        metrics: Option<&Metrics>,
    ) -> Result<()> {
        if records.is_empty() {
            return Ok(());
        }
        let started = Instant::now();
        let mut by_signal_ids = BTreeMap::<Signal, Vec<RecordId>>::new();
        for (_, raw_spool_ref) in records {
            by_signal_ids
                .entry(raw_spool_ref.shard)
                .or_default()
                .push(raw_spool_ref.id);
        }
        for (signal, ids) in by_signal_ids {
            let stats = self
                .raw_spool_for(signal)?
                .mark_committed_batch(&ids)
                .with_context(|| format!("checkpoint {signal} raw spool records"))?;
            if let Some(metrics) = metrics {
                Self::record_raw_spool_checkpoint_batch_metrics(metrics, signal, stats);
            }
        }
        if let Some(metrics) = metrics {
            metrics.observe_phase_seconds(
                "all",
                "raw_spool_checkpoint",
                None,
                started.elapsed().as_secs_f64(),
            );
            let mut by_signal = BTreeMap::<Signal, u64>::new();
            for (signal, _) in records {
                *by_signal.entry(*signal).or_default() += 1;
            }
            for (signal, count) in by_signal {
                metrics.inc(
                    "canardstack_raw_spool_checkpointed_records_total",
                    &[("signal", signal.as_str()), ("reason", reason)],
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
        for signal in all_signals() {
            let stats = self
                .raw_spool_for(signal)?
                .stats()
                .with_context(|| format!("read {signal} raw spool stats"))?;
            merge_raw_spool_stats(&mut aggregate, &stats);
        }
        Ok(aggregate)
    }

    pub fn raw_spool_stats_by_signal(&self) -> Result<BTreeMap<&'static str, super::Stats>> {
        let mut stats_by_signal = BTreeMap::new();
        for signal in all_signals() {
            stats_by_signal.insert(
                signal.as_str(),
                self.raw_spool_for(signal)?
                    .stats()
                    .with_context(|| format!("read {signal} raw spool stats"))?,
            );
        }
        Ok(stats_by_signal)
    }

    /// True only when every per-signal raw-spool writer is healthy. A writer
    /// that cannot read its stats (thread stopped/poisoned) or is in the fatal
    /// append/fsync latch counts as unhealthy so readiness reports NOT ready.
    pub fn raw_spool_healthy(&self) -> bool {
        all_signals().into_iter().all(|signal| {
            self.raw_spool_for(signal)
                .and_then(|spool| spool.stats())
                .map(|stats| stats.healthy)
                .unwrap_or(false)
        })
    }

    /// Force a single signal's raw-spool writer into the fatal/unhealthy latch,
    /// mirroring a real append/fsync failure. Intended for tests that exercise
    /// readiness wiring; gated to debug builds.
    #[doc(hidden)]
    pub fn force_raw_spool_unhealthy(
        &self,
        signal: Signal,
        message: impl Into<String>,
    ) -> Result<()> {
        self.raw_spool_for(signal)?.inject_fatal(message)
    }

    /// Per-signal raw-spool writer health, with the latched error message for
    /// any unhealthy signal so the health JSON can show which signal is wedged.
    pub fn raw_spool_health_by_signal(&self) -> BTreeMap<&'static str, (bool, Option<String>)> {
        let mut health = BTreeMap::new();
        for signal in all_signals() {
            let entry = match self.raw_spool_for(signal).and_then(|spool| spool.stats()) {
                Ok(stats) => (stats.healthy, stats.error),
                Err(err) => (false, Some(err.to_string())),
            };
            health.insert(signal.as_str(), entry);
        }
        health
    }

    pub fn record_raw_spool_metrics(&self, metrics: &Metrics) {
        if let Ok(stats) = self.raw_spool_stats() {
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
            metrics.gauge(
                "canardstack_raw_spool_unsynced_records",
                &[],
                stats.unsynced_records as f64,
            );
            metrics.gauge(
                "canardstack_raw_spool_unsynced_bytes",
                &[],
                stats.unsynced_bytes as f64,
            );
            metrics.gauge(
                "canardstack_raw_spool_unsynced_age_seconds",
                &[],
                stats.unsynced_age_seconds,
            );
            metrics.gauge(
                "canardstack_raw_spool_healthy",
                &[],
                if stats.healthy { 1.0 } else { 0.0 },
            );
            metrics.set_counter(
                "canardstack_raw_spool_append_syncs_total",
                &[],
                stats.append_syncs_total,
            );
            metrics.set_counter(
                "canardstack_raw_spool_append_sync_failures_total",
                &[],
                stats.append_sync_failures_total,
            );
            metrics.set_counter(
                "canardstack_raw_spool_append_file_fsyncs_total",
                &[],
                stats.append_sync_file_fsyncs_total,
            );
            metrics.set_observation(
                "canardstack_phase_duration_seconds",
                &[("signal", "all"), ("phase", "raw_spool_append_fsync")],
                stats.append_syncs_total,
                stats.append_sync_seconds_total,
            );
        }
        for signal in all_signals() {
            let Ok(stats) = self.raw_spool_for(signal).and_then(|spool| spool.stats()) else {
                continue;
            };
            metrics.gauge(
                "canardstack_raw_spool_segment_bytes",
                &[("signal", signal.as_str())],
                stats.segment_bytes as f64,
            );
            metrics.gauge(
                "canardstack_raw_spool_segments",
                &[("signal", signal.as_str())],
                stats.segment_count as f64,
            );
            metrics.gauge(
                "canardstack_raw_spool_pending_records",
                &[("signal", signal.as_str())],
                stats.pending_records as f64,
            );
            metrics.gauge(
                "canardstack_raw_spool_pending_bytes",
                &[("signal", signal.as_str())],
                stats.pending_bytes as f64,
            );
            metrics.gauge(
                "canardstack_raw_spool_unsynced_records",
                &[("signal", signal.as_str())],
                stats.unsynced_records as f64,
            );
            metrics.gauge(
                "canardstack_raw_spool_unsynced_bytes",
                &[("signal", signal.as_str())],
                stats.unsynced_bytes as f64,
            );
            metrics.gauge(
                "canardstack_raw_spool_unsynced_age_seconds",
                &[("signal", signal.as_str())],
                stats.unsynced_age_seconds,
            );
            metrics.gauge(
                "canardstack_raw_spool_healthy",
                &[("signal", signal.as_str())],
                if stats.healthy { 1.0 } else { 0.0 },
            );
            metrics.set_counter(
                "canardstack_raw_spool_append_syncs_total",
                &[("signal", signal.as_str())],
                stats.append_syncs_total,
            );
            metrics.set_counter(
                "canardstack_raw_spool_append_sync_failures_total",
                &[("signal", signal.as_str())],
                stats.append_sync_failures_total,
            );
            metrics.set_counter(
                "canardstack_raw_spool_append_file_fsyncs_total",
                &[("signal", signal.as_str())],
                stats.append_sync_file_fsyncs_total,
            );
            metrics.set_observation(
                "canardstack_phase_duration_seconds",
                &[
                    ("signal", signal.as_str()),
                    ("phase", "raw_spool_append_fsync"),
                ],
                stats.append_syncs_total,
                stats.append_sync_seconds_total,
            );
        }
    }

    fn raw_spool_for(&self, signal: Signal) -> Result<&Writer> {
        self.raw_spools
            .get(&signal)
            .with_context(|| format!("raw spool writer for {signal} is unavailable"))
    }

    fn raw_spool_shard_for_append(&self, signal: Signal) -> Signal {
        if signal.is_metric() {
            if self
                .metric_raw_spool_next
                .fetch_add(1, Ordering::Relaxed)
                .is_multiple_of(2)
            {
                Signal::MetricGauge
            } else {
                Signal::MetricSum
            }
        } else {
            signal
        }
    }
}

fn observe_immutable_flush(metrics: &Metrics, outcome: &ImmutableFlushOutcome) {
    for timing in &outcome.timings {
        metrics.observe_phase_seconds(
            timing.table.as_str(),
            timing.phase.as_str(),
            None,
            timing.seconds,
        );
    }
    if outcome.sealed_rows == 0 && outcome.sealed_files == 0 {
        return;
    }
    for timing in &outcome.timings {
        if timing.phase == TimingPhase::ParquetEncode {
            metrics.inc(
                "canardstack_immutable_segments_sealed_files_total",
                &[("signal", timing.table.as_str())],
                1,
            );
            metrics.inc(
                "canardstack_immutable_segments_sealed_rows_total",
                &[("signal", timing.table.as_str())],
                timing.rows as u64,
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
