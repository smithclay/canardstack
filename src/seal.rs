use crate::app::AppState;
use crate::ingest::spool::{AppendRef, RecordId};
use crate::ingest::{lifecycle, Ingestor, OtlpRequestKind, SealStage};
use crate::metrics::Metrics;
use crate::storage::{ArrowFlushOutcome, Storage};
use crate::validation::{ApiError, ApiResult};
use serde_json::{json, Value};
use std::collections::BTreeSet;
use std::time::Instant;

/// Raw-spool record to checkpoint after its associated buffered rows commit.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub(crate) struct ReplayRef {
    pub(crate) request_kind: OtlpRequestKind,
    pub(crate) spool: OtlpRequestKind,
    pub(crate) id: RecordId,
}

impl ReplayRef {
    pub(crate) fn new(request_kind: OtlpRequestKind, append_ref: AppendRef) -> Self {
        Self {
            request_kind,
            spool: append_ref.spool,
            id: append_ref.id,
        }
    }
}

/// Durability contract carried by buffered rows.
///
/// A buffer may legally contain replay-backed ingest rows, best-effort internal
/// rows, or both. Replay-backed refs are checkpointed only after the rows commit
/// to DuckLake; best-effort rows have no raw-spool record to checkpoint.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BufferDurability {
    replay_refs: BTreeSet<ReplayRef>,
    best_effort: bool,
}

impl BufferDurability {
    pub(crate) fn empty() -> Self {
        Self {
            replay_refs: BTreeSet::new(),
            best_effort: false,
        }
    }

    pub fn best_effort() -> Self {
        Self {
            replay_refs: BTreeSet::new(),
            best_effort: true,
        }
    }

    pub(crate) fn replay_backed(replay_ref: ReplayRef) -> Self {
        Self {
            replay_refs: BTreeSet::from([replay_ref]),
            best_effort: false,
        }
    }

    pub(crate) fn merge(&mut self, mut other: Self) {
        self.replay_refs.append(&mut other.replay_refs);
        self.best_effort |= other.best_effort;
    }

    pub(crate) fn replay_refs(&self) -> Vec<ReplayRef> {
        self.replay_refs.iter().copied().collect()
    }

    pub(crate) fn has_best_effort(&self) -> bool {
        self.best_effort
    }

    pub(crate) fn is_declared(&self) -> bool {
        self.best_effort || !self.replay_refs.is_empty()
    }
}

/// The whole seal, in one place: reserve seal capacity, flush+commit+checkpoint
/// the Arrow write buffer to DuckLake, feed the observed throughput back into the
/// admission EWMA, and record the run. The single named entry point for "perform
/// a seal", used by both the scheduler tick and the admin maintenance route.
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
    let outcome = storage.flush_arrow_write_buffer(true)?;
    let replay_refs = outcome.replay_refs();
    let seal_records = !replay_refs.is_empty();
    tracing::debug!(
        event = "seal_committed_buffer_snapshot",
        replay_backed_records = replay_refs.len(),
        best_effort_rows = outcome.best_effort_rows()
    );
    if seal_records {
        lifecycle::record_seal(metrics, SealStage::Committed);
    }
    ingestor.observe_arrow_flush(metrics, &outcome);
    match ingestor.checkpoint_replay_refs(&replay_refs, "storage_committed", Some(metrics)) {
        Ok(()) => {
            tracing::debug!(
                event = "seal_raw_spool_checkpointed",
                checkpointed_records = replay_refs.len(),
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
