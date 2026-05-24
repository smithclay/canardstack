use crate::config::Config;
use crate::ingest::spool::{
    self, full_info, AppendBatchStats, CheckpointBatchStats, Record, RecordId, Stats, Writer,
};
use crate::ingest::OtlpRequestKind;
use crate::metrics::Metrics;
use crate::validation::{ApiError, ApiResult};
use anyhow::{Context, Result};
use std::collections::{BTreeMap, HashMap};
use std::time::Instant;

#[derive(Clone, Copy, Debug)]
pub(crate) struct RawSpoolAppendRef {
    pub(crate) request_kind: OtlpRequestKind,
    pub(crate) record_id: RecordId,
}

/// Raw-spool record to checkpoint after its associated buffered rows commit.
///
/// `request_kind` is both the record's request kind and the raw-spool writer it
/// lives in; they were always equal, so a single field drives checkpoint routing
/// and the `request_kind` metric label.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub(crate) struct ReplayBackedRecordRef {
    pub(crate) request_kind: OtlpRequestKind,
    pub(crate) raw_record_id: RecordId,
}

impl ReplayBackedRecordRef {
    pub(crate) fn new(append_ref: RawSpoolAppendRef) -> Self {
        Self {
            request_kind: append_ref.request_kind,
            raw_record_id: append_ref.record_id,
        }
    }
}

pub(crate) struct PendingRawRecord {
    pub(crate) raw_spool_ref: RawSpoolAppendRef,
    pub(crate) request_kind: OtlpRequestKind,
    pub(crate) headers: HashMap<String, String>,
    pub(crate) compressed_body: Vec<u8>,
}

pub(crate) struct RawSpool {
    writers: BTreeMap<OtlpRequestKind, Writer>,
}

impl RawSpool {
    pub(crate) fn open(config: &Config) -> Result<Self> {
        let mut writers = BTreeMap::new();
        for request_kind in OtlpRequestKind::ALL {
            writers.insert(request_kind, spawn_raw_spool_writer(config, request_kind)?);
        }
        Ok(Self { writers })
    }

    pub(crate) fn recover_pending(&self) -> Result<Vec<PendingRawRecord>> {
        let mut pending = Vec::new();
        for raw_spool_request_kind in OtlpRequestKind::ALL {
            let recovered = self
                .raw_spool_for(raw_spool_request_kind)?
                .recover_pending()
                .with_context(|| {
                    format!("recover {raw_spool_request_kind} raw spool pending records")
                })?;
            for recovered in recovered {
                let mut headers = HashMap::new();
                headers.insert(
                    "content-type".to_string(),
                    recovered.record.content_type.clone(),
                );
                if let Some(encoding) = &recovered.record.content_encoding {
                    headers.insert("content-encoding".to_string(), encoding.clone());
                }
                pending.push(PendingRawRecord {
                    raw_spool_ref: RawSpoolAppendRef {
                        request_kind: raw_spool_request_kind,
                        record_id: recovered.id,
                    },
                    request_kind: recovered.record.request_kind,
                    headers,
                    compressed_body: recovered.record.compressed_body,
                });
            }
        }
        Ok(pending)
    }

    pub(crate) fn append(
        &self,
        request_kind: OtlpRequestKind,
        headers: &HashMap<String, String>,
        compressed_body: Vec<u8>,
        metrics: &Metrics,
    ) -> ApiResult<(RawSpoolAppendRef, Vec<u8>)> {
        let content_type = headers.get("content-type").cloned().unwrap_or_default();
        let content_encoding = headers.get("content-encoding").cloned();
        let compressed_body_len = compressed_body.len();
        let started = Instant::now();
        let record = Record::new(
            request_kind,
            content_type,
            content_encoding,
            compressed_body,
        );
        let result = self
            .raw_spool_for(request_kind)
            .and_then(|raw_spool| raw_spool.append(record));
        metrics.observe_request_phase_seconds(
            request_kind.as_str(),
            "raw_spool_append",
            started.elapsed().as_secs_f64(),
        );
        match result {
            Ok(ack) => {
                if let Some(stats) = ack.batch_stats {
                    Self::record_raw_spool_append_batch_metrics(metrics, request_kind, stats);
                }
                metrics.inc(
                    "canardstack_raw_spool_records_total",
                    &[
                        ("request_kind", request_kind.as_str()),
                        ("status", "spooled"),
                    ],
                    1,
                );
                metrics.inc(
                    "canardstack_raw_spool_bytes_total",
                    &[("request_kind", request_kind.as_str())],
                    compressed_body_len as u64,
                );
                Ok((
                    RawSpoolAppendRef {
                        request_kind,
                        record_id: ack.id,
                    },
                    ack.compressed_body,
                ))
            }
            Err(err) => {
                if full_info(&err).is_some() {
                    metrics.inc(
                        "canardstack_raw_spool_records_total",
                        &[("request_kind", request_kind.as_str()), ("status", "full")],
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
                        &[
                            ("request_kind", request_kind.as_str()),
                            ("status", "queue_full"),
                        ],
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
                        &[("request_kind", request_kind.as_str()), ("status", "error")],
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
    pub(crate) fn checkpoint_terminal(
        &self,
        raw_spool_ref: RawSpoolAppendRef,
        request_kind: OtlpRequestKind,
        reason: &'static str,
        metrics: &Metrics,
    ) -> ApiResult<()> {
        tracing::debug!(
            event = "ingest_terminally_rejected",
            request_kind = request_kind.as_str(),
            reason,
        );
        self.checkpoint_raw_record(raw_spool_ref, request_kind, reason, Some(metrics))
            .map_err(|err| {
                ApiError::new(
                    503,
                    "raw_spool_checkpoint_failed",
                    format!("raw ingest spool checkpoint failed: {err}"),
                )
                .with_retry_after(10)
            })
    }

    fn checkpoint_raw_record(
        &self,
        raw_spool_ref: RawSpoolAppendRef,
        request_kind: OtlpRequestKind,
        reason: &'static str,
        metrics: Option<&Metrics>,
    ) -> Result<()> {
        let started = Instant::now();
        let stats = self
            .raw_spool_for(raw_spool_ref.request_kind)?
            .mark_committed(raw_spool_ref.record_id)
            .context("checkpoint raw spool record")?;
        if let Some(metrics) = metrics {
            let seconds = started.elapsed().as_secs_f64();
            Self::record_raw_spool_checkpoint_batch_metrics(
                metrics,
                raw_spool_ref.request_kind,
                stats,
            );
            metrics.observe_seconds(
                "canardstack_phase_duration_seconds",
                &[
                    ("request_kind", request_kind.as_str()),
                    ("phase", "raw_spool_terminal_checkpoint"),
                    ("reason", reason),
                ],
                seconds,
            );
            metrics.observe_request_phase_seconds(
                request_kind.as_str(),
                "raw_spool_checkpoint",
                seconds,
            );
            metrics.inc(
                "canardstack_raw_spool_checkpointed_records_total",
                &[("request_kind", request_kind.as_str()), ("reason", reason)],
                1,
            );
        }
        Ok(())
    }

    pub(crate) fn checkpoint_replay_backed_records(
        &self,
        records: &[ReplayBackedRecordRef],
        reason: &'static str,
        metrics: Option<&Metrics>,
    ) -> Result<()> {
        if records.is_empty() {
            return Ok(());
        }
        let started = Instant::now();
        let mut by_request_kind_ids = BTreeMap::<OtlpRequestKind, Vec<RecordId>>::new();
        for replay_ref in records {
            by_request_kind_ids
                .entry(replay_ref.request_kind)
                .or_default()
                .push(replay_ref.raw_record_id);
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
            for replay_ref in records {
                *by_request_kind.entry(replay_ref.request_kind).or_default() += 1;
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

    pub(crate) fn stats(&self) -> Result<Stats> {
        let mut aggregate = Stats {
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

    pub(crate) fn stats_by_request_kind(&self) -> Result<BTreeMap<&'static str, Stats>> {
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
    pub(crate) fn healthy(&self) -> bool {
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
    pub(crate) fn force_unhealthy(
        &self,
        request_kind: OtlpRequestKind,
        message: impl Into<String>,
    ) -> Result<()> {
        self.raw_spool_for(request_kind)?.inject_fatal(message)
    }

    /// Per-request-kind raw-spool writer health, with the latched error message
    /// for any unhealthy request kind so the health JSON can show which writer is
    /// wedged.
    pub(crate) fn health_by_request_kind(&self) -> BTreeMap<&'static str, (bool, Option<String>)> {
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

    pub(crate) fn record_metrics(&self, metrics: &Metrics) {
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
        self.writers
            .get(&request_kind)
            .with_context(|| format!("raw spool writer for {request_kind} is unavailable"))
    }
}

fn merge_raw_spool_stats(aggregate: &mut Stats, stats: &Stats) {
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

fn spawn_raw_spool_writer(config: &Config, request_kind: OtlpRequestKind) -> Result<Writer> {
    Writer::spawn(
        spool::Options {
            dir: config.mechanics.raw_spool_dir.join(request_kind.as_str()),
            max_segment_bytes: config.mechanics.raw_spool_max_segment_bytes as u64,
            max_record_bytes: config.mechanics.raw_spool_max_record_bytes as u64,
            max_total_bytes: config.mechanics.raw_spool_max_total_bytes as u64,
            checkpoint_fsync_records: spool::RAW_SPOOL_CHECKPOINT_FSYNC_RECORDS,
            checkpoint_fsync_delay: spool::RAW_SPOOL_CHECKPOINT_FSYNC_DELAY,
        },
        spool::RAW_SPOOL_WRITER_QUEUE_CAPACITY,
        spool::RAW_SPOOL_GROUP_COMMIT_RECORDS,
        config.mechanics.raw_spool_group_commit_delay,
    )
    .with_context(|| format!("spawn {request_kind} raw spool writer"))
}
