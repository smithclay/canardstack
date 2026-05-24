use crate::app::AppState;
use crate::config::Config;
use crate::ingest::{lifecycle, Ingestor, SealStage};
use crate::metrics::{MetricName, Metrics};
use crate::storage::{ArrowFlushOutcome, Storage, TimingPhase};
use crate::validation::{ApiError, ApiResult};
use serde_json::{json, Value};
use std::time::{Duration, Instant};

#[derive(Clone, Copy, Debug)]
pub(crate) struct SealDriverTick {
    pub(crate) max_buffer_age_seconds: f64,
}

pub(crate) struct SealDriver {
    cadence: Duration,
    buffer_target_bytes: usize,
    buffer_max_age_seconds: f64,
    next_seal: Instant,
    backoff_until: Instant,
}

impl SealDriver {
    pub(crate) fn new(config: &Config, now: Instant) -> Self {
        let cadence = config.mechanics.scheduler_seal_interval;
        Self {
            cadence,
            buffer_target_bytes: config.mechanics.arrow_write_buffer_target_bytes,
            buffer_max_age_seconds: config.mechanics.arrow_write_buffer_max_age.as_secs_f64(),
            next_seal: now + cadence,
            backoff_until: now,
        }
    }

    /// Single seal trigger policy. Flush when any buffered storage signal reaches
    /// its size/age threshold, or on the seal cadence. A failed seal backs off
    /// one cadence so a broken catalog cannot spin the writer.
    pub(crate) fn tick<F>(
        &mut self,
        state: &AppState,
        now: Instant,
        mut run_seal: F,
    ) -> SealDriverTick
    where
        F: FnMut(&AppState) -> bool,
    {
        let buffers = state.storage.arrow_write_buffer_metrics();
        let max_buffer_age_seconds = buffers
            .iter()
            .map(|metric| metric.age_seconds)
            .fold(0.0, f64::max);
        if now < self.backoff_until {
            return SealDriverTick {
                max_buffer_age_seconds,
            };
        }

        let buffered = buffers.iter().any(|metric| metric.bytes > 0);
        let threshold_due = buffers.iter().any(|metric| {
            Storage::size_or_age_due(
                metric.bytes,
                metric.age_seconds,
                self.buffer_target_bytes,
                self.buffer_max_age_seconds,
            )
        });
        if buffered && (threshold_due || now >= self.next_seal) {
            let ok = run_seal(state);
            self.next_seal = now + self.cadence;
            if !ok {
                self.backoff_until = now + self.cadence;
            }
            return SealDriverTick {
                max_buffer_age_seconds,
            };
        }
        if now >= self.next_seal {
            self.next_seal = now + self.cadence;
        }
        SealDriverTick {
            max_buffer_age_seconds,
        }
    }
}

/// The whole seal, in one place. In order: reserve seal capacity; commit the
/// Arrow write buffer to DuckLake (append buffered rows and COMMIT the
/// transaction so they become query-visible); checkpoint the now-durable
/// raw-spool records (mark them committed so they will not replay); feed the
/// observed throughput back into the admission EWMA; and record the run. The
/// single named entry point for "perform a seal", used by both the scheduler tick
/// and the admin maintenance route.
///
/// Delivery semantics: the raw-spool checkpoint happens AFTER the DuckLake COMMIT
/// (capture before flush, checkpoint after commit). This ordering is deliberate
/// and load-bearing for at-least-once — we never checkpoint rows that were not
/// storage-committed. The consequence is that a crash between COMMIT and
/// checkpoint replays those records on restart, producing duplicate ROWS in
/// storage, which v0 surfaces to queries without dedup.
pub fn run(state: &AppState) -> ApiResult<Value> {
    let started = Instant::now();
    let pending_bytes: usize = state
        .storage
        .arrow_write_buffer_metrics()
        .iter()
        .map(|metric| metric.bytes)
        .sum();
    let mut guard = state.admission.reserve_seal(&state.metrics)?;
    guard.record_bytes(pending_bytes);
    let arrow_flush = commit_buffered_rows(state)
        .map_err(|err| ApiError::new(503, "storage_operation_failed", err.to_string()));
    guard.finish(&state.metrics);
    let arrow_flush = arrow_flush?;
    state.maintenance.record_seal_run();
    Ok(json!({
        "status": "ok",
        "arrow_flush": arrow_flush.to_json(),
        "duration_ms": started.elapsed().as_millis()
    }))
}

/// Commit the typed Arrow write-buffer snapshot and checkpoint exactly the
/// replay-backed records in that committed snapshot.
pub fn commit_buffered_rows(state: &AppState) -> anyhow::Result<ArrowFlushOutcome> {
    commit_buffered_rows_with(&state.ingestor, &state.storage, &state.metrics)
}

fn commit_buffered_rows_with(
    ingestor: &Ingestor,
    storage: &Storage,
    metrics: &Metrics,
) -> anyhow::Result<ArrowFlushOutcome> {
    let mut outcome = storage.commit_arrow_write_buffer()?;
    let committed_replay_refs = outcome.take_committed_replay_refs();
    let replay_backed_records = committed_replay_refs.len();
    let seal_records = replay_backed_records != 0;
    tracing::debug!(
        event = "seal_committed_buffer_snapshot",
        replay_backed_records,
    );
    if seal_records {
        lifecycle::record_seal(metrics, SealStage::Committed);
    }
    observe_arrow_flush(metrics, &outcome);
    match ingestor.checkpoint_replay_backed_records(
        committed_replay_refs,
        "storage_committed",
        Some(metrics),
    ) {
        Ok(()) => {
            tracing::debug!(
                event = "seal_raw_spool_checkpointed",
                checkpointed_records = replay_backed_records,
            );
            if seal_records {
                lifecycle::record_seal(metrics, SealStage::Checkpointed);
            }
        }
        Err(err) => {
            tracing::error!(
                event = "raw_spool_checkpoint_failed",
                error = %err,
                "Arrow flush committed but raw spool checkpoint failed; replay-backed records left pending"
            );
            if seal_records {
                lifecycle::record_seal(metrics, SealStage::DuplicateRisk);
            }
        }
    }
    Ok(outcome)
}

fn observe_arrow_flush(metrics: &Metrics, outcome: &ArrowFlushOutcome) {
    for timing in &outcome.timings {
        metrics.observe_storage_signal_phase_seconds(
            timing.storage_signal.as_str(),
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
                MetricName::DuckdbArrowAppendsTotal,
                &[("storage_signal", timing.storage_signal.as_str())],
                1,
            );
            metrics.inc(
                MetricName::DuckdbArrowAppendedRowsTotal,
                &[("storage_signal", timing.storage_signal.as_str())],
                timing.rows as u64,
            );
        } else if timing.phase == TimingPhase::DucklakeCommit {
            metrics.inc(
                MetricName::ArrowFlushRowsTotal,
                &[("storage_signal", timing.storage_signal.as_str())],
                timing.rows as u64,
            );
            metrics.inc(
                MetricName::ArrowFlushesTotal,
                &[("storage_signal", timing.storage_signal.as_str())],
                1,
            );
        }
    }
}
