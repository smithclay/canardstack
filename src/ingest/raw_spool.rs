use super::spool::{
    self, raw_spool_full_info, RawSpoolAppendBatchStats, RawSpoolRecord, RawSpoolRecordId,
    RawSpoolWriter,
};
use super::{admission, all_signals, queue, Ingestor, Signal};
use crate::metrics::Metrics;
use crate::otlp;
use crate::storage::Storage;
use crate::validation::{self, ApiError, ApiResult};
use crate::LockExt;
use anyhow::{Context, Result};
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::Ordering;
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

impl Ingestor {
    pub fn replay_raw_spool(&self, storage: &Storage, metrics: &Metrics) -> Result<usize> {
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
                    RawSpoolAppendRef {
                        lane: signal,
                        id: recovered.id,
                    },
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
        }
        self.record_raw_spool_metrics(metrics);
        Ok(replayed)
    }

    fn ingest_replayed_raw_record(
        &self,
        raw_spool_ref: RawSpoolAppendRef,
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
        let mut queue_credit_reservation = self
            .reserve_queue_credit_exact(admission::credit_bytes_by_signal(&batches))
            .map_err(|err| anyhow::anyhow!(err.message.clone()))?;
        let pending_bytes = batches.iter().map(|b| b.approx_bytes).sum::<usize>();
        let peak_bytes = request_bytes
            .saturating_add(body.len())
            .saturating_add(pending_bytes);
        runtime_memory_reservation
            .reserve_at_least(peak_bytes, signal, metrics)
            .map_err(|err| anyhow::anyhow!(err.message.clone()))?;
        if batches.is_empty() {
            self.release_queue_credit_reservation(&mut queue_credit_reservation);
            self.checkpoint_raw_spool(raw_spool_ref, signal, "replay_empty", Some(metrics))?;
            return Ok(());
        }
        self.track_raw_spool_batches(raw_spool_ref, signal, &mut batches);
        self.enqueue(signal, batches, metrics).map_err(|err| {
            self.untrack_raw_spool_record(raw_spool_ref);
            self.release_queue_credit_reservation(&mut queue_credit_reservation);
            anyhow::anyhow!(err.message.clone())
        })?;
        queue_credit_reservation.commit_to_queue();
        Ok(())
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
        compressed_body: &[u8],
        metrics: &Metrics,
    ) -> ApiResult<RawSpoolAppendRef> {
        let lane = self.raw_spool_lane_for_append(signal);
        let content_type = headers.get("content-type").cloned().unwrap_or_default();
        let content_encoding = headers.get("content-encoding").cloned();
        let started = Instant::now();
        let record = RawSpoolRecord::new(signal, content_type, content_encoding, compressed_body);
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
                    compressed_body.len() as u64,
                );
                Ok(RawSpoolAppendRef { lane, id: ack.id })
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
        self.raw_spool_for(raw_spool_ref.lane)?
            .mark_committed(raw_spool_ref.id)
            .context("checkpoint raw spool record")?;
        if let Some(metrics) = metrics {
            metrics.observe_phase_seconds(
                signal.as_str(),
                "raw_spool_checkpoint",
                None,
                started.elapsed().as_secs_f64(),
            );
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
            self.raw_spool_for(signal)?
                .mark_committed_batch(&ids)
                .with_context(|| format!("checkpoint {signal} raw spool records"))?;
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
    aggregate.healthy &= stats.healthy;
    if let Some(error) = &stats.error {
        aggregate.error = Some(match aggregate.error.take() {
            Some(existing) => format!("{existing}; {error}"),
            None => error.clone(),
        });
    }
}
