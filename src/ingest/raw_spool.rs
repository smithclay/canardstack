use super::spool::{
    self, raw_spool_full_info, RawSpoolAppendBatchStats, RawSpoolCheckpointBatchStats,
    RawSpoolRecord, RawSpoolRecordId, RawSpoolWriter,
};
use super::{all_signals, queue, Ingestor, Signal};
use crate::lanes::LaneController;
use crate::metrics::Metrics;
use crate::storage::Storage;
use crate::validation::{self, ApiError, ApiResult};
use crate::LockExt;
use anyhow::{Context, Result};
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Instant;

#[derive(Clone, Copy, Debug)]
pub(super) struct RawSpoolFlushRef {
    pub(super) signal: Signal,
    pub(super) remaining_rows: usize,
}

#[derive(Clone, Copy, Debug)]
pub(super) struct RawSpoolAppendRef {
    pub(super) lane: Signal,
    pub(super) id: RawSpoolRecordId,
}

struct RecoveredRawSpoolWork {
    raw_spool_ref: RawSpoolAppendRef,
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
                    RecoveredRawSpoolWork {
                        raw_spool_ref: RawSpoolAppendRef {
                            lane: signal,
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
                        return Err(err).context("replay raw spool record");
                    }
                }
            }
        }
        self.record_raw_spool_metrics(metrics.as_ref());
        Ok(replayed)
    }

    fn ingest_replayed_raw_record(
        &self,
        recovered: RecoveredRawSpoolWork,
        storage: &Storage,
        lanes: &LaneController,
        metrics: Arc<Metrics>,
    ) -> Result<()> {
        let RecoveredRawSpoolWork {
            raw_spool_ref,
            signal,
            headers,
            compressed_body,
        } = recovered;
        validation::validate_body_size(compressed_body.len(), &self.config)
            .map_err(|err| anyhow::anyhow!(err.message.clone()))?;
        validation::validate_content_type(&headers)
            .map_err(|err| anyhow::anyhow!(err.message.clone()))?;
        if !storage.accepts_memory_ingest() || self.config.force_dependency_unhealthy {
            anyhow::bail!("storage dependency is unhealthy");
        }
        let queue_credit_reservation = self
            .reserve_queue_credit_estimate(
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
        let work = super::SpooledIngestWork {
            signal,
            headers: headers.clone(),
            compressed_body,
            raw_spool_ref,
            queue_credit_reservation,
            runtime_memory_reservation,
            metrics: metrics.clone(),
        };
        self.dispatch_topology_work(work, metrics.as_ref())
            .map(|_| ())
            .map_err(|err| anyhow::anyhow!(err.message.clone()))
    }

    pub(super) fn track_raw_spool_batches(
        &self,
        raw_spool_ref: RawSpoolAppendRef,
        signal: Signal,
        batches: &mut [queue::PendingBatch],
    ) {
        if batches.is_empty() {
            return;
        }
        for batch in batches.iter_mut() {
            batch.raw_spool_id = Some(raw_spool_ref.id);
            batch.raw_spool_lane = Some(raw_spool_ref.lane);
        }
        self.raw_spool_flush_refs.lock_or_poisoned().insert(
            (raw_spool_ref.lane, raw_spool_ref.id),
            RawSpoolFlushRef {
                signal,
                remaining_rows: batches.iter().map(|batch| batch.batch.num_rows()).sum(),
            },
        );
    }

    pub(super) fn untrack_raw_spool_record(&self, raw_spool_ref: RawSpoolAppendRef) {
        self.raw_spool_flush_refs
            .lock_or_poisoned()
            .remove(&(raw_spool_ref.lane, raw_spool_ref.id));
    }

    pub(super) fn mark_raw_spool_batches_storage_committed(
        &self,
        sets: &[(queue::QueueKey, Vec<queue::QueuedBatch>)],
        metrics: Option<&Metrics>,
    ) -> Result<()> {
        let mut committed_counts = BTreeMap::<(Signal, RawSpoolRecordId), usize>::new();
        for (key, batches) in sets {
            for batch in batches {
                if let Some(id) = batch.raw_spool_id {
                    let lane = batch.raw_spool_lane.unwrap_or(key.signal);
                    *committed_counts.entry((lane, id)).or_default() += batch.len();
                }
            }
        }
        if committed_counts.is_empty() {
            return Ok(());
        }

        let mut ready_to_checkpoint = Vec::new();
        {
            let mut refs = self.raw_spool_flush_refs.lock_or_poisoned();
            for ((signal, id), committed_rows) in committed_counts {
                let Some(tracked) = refs.get_mut(&(signal, id)) else {
                    continue;
                };
                if committed_rows >= tracked.remaining_rows {
                    ready_to_checkpoint
                        .push((tracked.signal, RawSpoolAppendRef { lane: signal, id }));
                } else {
                    tracked.remaining_rows -= committed_rows;
                }
            }
        }

        self.checkpoint_raw_spool_batch(&ready_to_checkpoint, "storage_committed", metrics)?;
        if !ready_to_checkpoint.is_empty() {
            let mut refs = self.raw_spool_flush_refs.lock_or_poisoned();
            for (_, raw_spool_ref) in ready_to_checkpoint {
                refs.remove(&(raw_spool_ref.lane, raw_spool_ref.id));
            }
        }
        Ok(())
    }

    pub(super) fn append_raw_spool(
        &self,
        signal: Signal,
        headers: &HashMap<String, String>,
        compressed_body: Vec<u8>,
        metrics: &Metrics,
    ) -> ApiResult<(RawSpoolAppendRef, Vec<u8>)> {
        let lane = self.raw_spool_lane_for_append(signal);
        let content_type = headers.get("content-type").cloned().unwrap_or_default();
        let content_encoding = headers.get("content-encoding").cloned();
        let compressed_body_len = compressed_body.len();
        let started = Instant::now();
        let record = RawSpoolRecord::new(signal, content_type, content_encoding, compressed_body);
        metrics.inc(
            "canardstack_ingest_materialized_bytes_total",
            &[
                ("signal", lane.as_str()),
                ("component", "raw_spool_record"),
                ("kind", "body_clone"),
            ],
            0,
        );
        let result = self
            .raw_spool_for(lane)
            .and_then(|raw_spool| raw_spool.append(record));
        metrics.observe_phase_seconds(
            lane.as_str(),
            "raw_spool_append",
            None,
            started.elapsed().as_secs_f64(),
        );
        match result {
            Ok(ack) => {
                if let Some(stats) = ack.batch_stats {
                    Self::record_raw_spool_append_batch_metrics(metrics, lane, stats);
                }
                metrics.inc(
                    "canardstack_raw_spool_records_total",
                    &[("signal", lane.as_str()), ("status", "spooled")],
                    1,
                );
                metrics.inc(
                    "canardstack_raw_spool_bytes_total",
                    &[("signal", lane.as_str())],
                    compressed_body_len as u64,
                );
                Ok((RawSpoolAppendRef { lane, id: ack.id }, ack.compressed_body))
            }
            Err(err) => {
                if raw_spool_full_info(&err).is_some() {
                    metrics.inc(
                        "canardstack_raw_spool_records_total",
                        &[("signal", lane.as_str()), ("status", "full")],
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
                        &[("signal", lane.as_str()), ("status", "error")],
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
        stats: RawSpoolAppendBatchStats,
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
            "canardstack_ingest_materialized_bytes_total",
            &[
                ("signal", signal.as_str()),
                ("component", "raw_spool_encode"),
                ("kind", "record_frame"),
            ],
            stats.encoded_bytes,
        );
        metrics.inc(
            "canardstack_ingest_materialized_bytes_total",
            &[
                ("signal", signal.as_str()),
                ("component", "raw_spool_group"),
                ("kind", "encoded_copy"),
            ],
            stats.encoded_bytes,
        );
        metrics.inc(
            "canardstack_raw_spool_append_syncs_total",
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
        Self::record_raw_spool_batch_enqueue_depth_metrics(
            metrics,
            "canardstack_raw_spool_append_batch_enqueue_pending_commands",
            signal,
            stats.max_pending_commands_at_enqueue,
            stats.max_pending_append_commands_at_enqueue,
            stats.max_pending_checkpoint_commands_at_enqueue,
        );
        metrics.inc(
            "canardstack_raw_spool_append_batch_deferred_commands_total",
            &[("signal", signal.as_str()), ("kind", "checkpoint")],
            stats.deferred_checkpoint_commands as u64,
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
        stats: RawSpoolCheckpointBatchStats,
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
        Self::record_raw_spool_batch_enqueue_depth_metrics(
            metrics,
            "canardstack_raw_spool_checkpoint_batch_enqueue_pending_commands",
            signal,
            stats.max_pending_commands_at_enqueue,
            stats.max_pending_append_commands_at_enqueue,
            stats.max_pending_checkpoint_commands_at_enqueue,
        );
        metrics.inc(
            "canardstack_raw_spool_checkpoint_batch_deferred_commands_total",
            &[("signal", signal.as_str()), ("kind", "append")],
            stats.deferred_append_commands as u64,
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

    fn record_raw_spool_batch_enqueue_depth_metrics(
        metrics: &Metrics,
        metric_name: &str,
        signal: Signal,
        pending_commands: usize,
        pending_append_commands: usize,
        pending_checkpoint_commands: usize,
    ) {
        for (kind, value) in [
            ("all", pending_commands),
            ("append", pending_append_commands),
            ("checkpoint", pending_checkpoint_commands),
        ] {
            metrics.gauge(
                metric_name,
                &[
                    ("signal", signal.as_str()),
                    ("kind", kind),
                    ("stat", "last"),
                ],
                value as f64,
            );
            metrics.gauge_max(
                metric_name,
                &[("signal", signal.as_str()), ("kind", kind), ("stat", "max")],
                value as f64,
            );
        }
    }

    pub(super) fn checkpoint_raw_spool_terminal(
        &self,
        raw_spool_ref: RawSpoolAppendRef,
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
        raw_spool_ref: RawSpoolAppendRef,
        signal: Signal,
        reason: &'static str,
        metrics: Option<&Metrics>,
    ) -> Result<()> {
        let started = Instant::now();
        let stats = self
            .raw_spool_for(raw_spool_ref.lane)?
            .mark_committed(raw_spool_ref.id)
            .context("checkpoint raw spool record")?;
        if let Some(metrics) = metrics {
            let seconds = started.elapsed().as_secs_f64();
            Self::record_raw_spool_checkpoint_batch_metrics(metrics, raw_spool_ref.lane, stats);
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
        records: &[(Signal, RawSpoolAppendRef)],
        reason: &'static str,
        metrics: Option<&Metrics>,
    ) -> Result<()> {
        if records.is_empty() {
            return Ok(());
        }
        let started = Instant::now();
        let mut by_signal_ids = BTreeMap::<Signal, Vec<RawSpoolRecordId>>::new();
        for (_, raw_spool_ref) in records {
            by_signal_ids
                .entry(raw_spool_ref.lane)
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

    pub fn raw_spool_stats(&self) -> Result<spool::RawSpoolStats> {
        let mut aggregate = spool::RawSpoolStats {
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

    pub fn raw_spool_stats_by_signal(
        &self,
    ) -> Result<BTreeMap<&'static str, spool::RawSpoolStats>> {
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
            metrics.set_observation(
                "canardstack_phase_duration_seconds",
                &[("signal", "all"), ("phase", "raw_spool_append_fsync")],
                stats.append_syncs_total,
                stats.append_sync_seconds_total,
            );
            Self::record_raw_spool_writer_metrics(metrics, None, &stats);
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
            metrics.set_observation(
                "canardstack_phase_duration_seconds",
                &[
                    ("signal", signal.as_str()),
                    ("phase", "raw_spool_append_fsync"),
                ],
                stats.append_syncs_total,
                stats.append_sync_seconds_total,
            );
            Self::record_raw_spool_writer_metrics(metrics, Some(signal.as_str()), &stats);
        }
    }

    fn record_raw_spool_writer_metrics(
        metrics: &Metrics,
        signal: Option<&str>,
        stats: &spool::RawSpoolStats,
    ) {
        for (kind, current, max) in [
            (
                "all",
                stats.writer_pending_commands,
                stats.writer_pending_commands_max,
            ),
            (
                "append",
                stats.writer_pending_append_commands,
                stats.writer_pending_append_commands_max,
            ),
            (
                "checkpoint",
                stats.writer_pending_checkpoint_commands,
                stats.writer_pending_checkpoint_commands_max,
            ),
        ] {
            if let Some(signal) = signal {
                metrics.gauge(
                    "canardstack_raw_spool_writer_pending_commands",
                    &[("signal", signal), ("kind", kind), ("stat", "current")],
                    current as f64,
                );
                metrics.gauge(
                    "canardstack_raw_spool_writer_pending_commands",
                    &[("signal", signal), ("kind", kind), ("stat", "max")],
                    max as f64,
                );
            } else {
                metrics.gauge(
                    "canardstack_raw_spool_writer_pending_commands",
                    &[("kind", kind), ("stat", "current")],
                    current as f64,
                );
                metrics.gauge(
                    "canardstack_raw_spool_writer_pending_commands",
                    &[("kind", kind), ("stat", "max")],
                    max as f64,
                );
            }
        }

        for (kind, total) in [
            ("append", stats.writer_append_commands_total),
            ("checkpoint", stats.writer_checkpoint_commands_total),
            ("recover", stats.writer_recover_commands_total),
            ("stats", stats.writer_stats_commands_total),
        ] {
            if let Some(signal) = signal {
                metrics.set_counter(
                    "canardstack_raw_spool_writer_commands_total",
                    &[("signal", signal), ("kind", kind)],
                    total,
                );
            } else {
                metrics.set_counter(
                    "canardstack_raw_spool_writer_commands_total",
                    &[("kind", kind)],
                    total,
                );
            }
        }
    }

    fn raw_spool_for(&self, signal: Signal) -> Result<&RawSpoolWriter> {
        self.raw_spools
            .get(&signal)
            .with_context(|| format!("raw spool writer for {signal} is unavailable"))
    }

    fn raw_spool_lane_for_append(&self, signal: Signal) -> Signal {
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

fn merge_raw_spool_stats(aggregate: &mut spool::RawSpoolStats, stats: &spool::RawSpoolStats) {
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
    aggregate.writer_pending_commands += stats.writer_pending_commands;
    aggregate.writer_pending_append_commands += stats.writer_pending_append_commands;
    aggregate.writer_pending_checkpoint_commands += stats.writer_pending_checkpoint_commands;
    aggregate.writer_pending_commands_max = aggregate
        .writer_pending_commands_max
        .max(stats.writer_pending_commands_max);
    aggregate.writer_pending_append_commands_max = aggregate
        .writer_pending_append_commands_max
        .max(stats.writer_pending_append_commands_max);
    aggregate.writer_pending_checkpoint_commands_max = aggregate
        .writer_pending_checkpoint_commands_max
        .max(stats.writer_pending_checkpoint_commands_max);
    aggregate.writer_append_commands_total = aggregate
        .writer_append_commands_total
        .saturating_add(stats.writer_append_commands_total);
    aggregate.writer_checkpoint_commands_total = aggregate
        .writer_checkpoint_commands_total
        .saturating_add(stats.writer_checkpoint_commands_total);
    aggregate.writer_recover_commands_total = aggregate
        .writer_recover_commands_total
        .saturating_add(stats.writer_recover_commands_total);
    aggregate.writer_stats_commands_total = aggregate
        .writer_stats_commands_total
        .saturating_add(stats.writer_stats_commands_total);
    aggregate.healthy &= stats.healthy;
    if let Some(error) = &stats.error {
        aggregate.error = Some(match aggregate.error.take() {
            Some(existing) => format!("{existing}; {error}"),
            None => error.clone(),
        });
    }
}
