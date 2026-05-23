//! Coarse durable-boundary counters for the ingest funnel. Each stage is a real
//! durability/visibility boundary, not an observability convenience step:
//!
//! - per-request (`canardstack_ingest_stage_total{request_kind, stage}`):
//!   accepted -> spooled -> transformed -> buffered
//! - seal (`canardstack_ingest_seal_stage_total{stage}`):
//!   committed -> checkpointed, with `duplicate_risk` as the at-least-once hazard
//!   marker (committed to DuckLake but the raw-spool checkpoint failed, so the
//!   records replay as duplicate rows on a future restart; v0 does not dedup).
//!
//! These counters carry NO control flow: emitting a stage never changes what the
//! pipeline does, only how the funnel is described in metrics.

use crate::ingest::OtlpRequestKind;
use crate::metrics::Metrics;

/// Per-request durable-boundary stage. Emitted as the `stage` label on
/// `canardstack_ingest_stage_total{request_kind, stage}` via [`record`].
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(in crate::ingest) enum IngestStage {
    /// Passed every request-path admission gate.
    Accepted,
    /// Raw request bytes fsynced to the local raw spool (the 202 durability
    /// point).
    Spooled,
    /// Decompressed, validated, and turned into Arrow `RecordBatch`es.
    Transformed,
    /// Transformed batches appended to the storage Arrow write buffer.
    Buffered,
}

/// Seal durable-boundary stage. Emitted as the `stage` label on
/// `canardstack_ingest_seal_stage_total{stage}` via [`record_seal`].
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(in crate::ingest) enum SealStage {
    /// Arrow write buffer flushed and committed to durable DuckLake storage.
    Committed,
    /// Captured raw-spool refs checkpointed after the commit (will not replay).
    Checkpointed,
    /// Committed but the raw-spool checkpoint failed — records replay as
    /// duplicate rows on restart (the at-least-once hazard; v0 does not dedup).
    DuplicateRisk,
}

impl IngestStage {
    /// Stable snake_case label value. Operators key dashboards off these, so the
    /// [`as_str_is_stable`](tests::as_str_is_stable) test pins them.
    pub(in crate::ingest) fn as_str(self) -> &'static str {
        match self {
            Self::Accepted => "accepted",
            Self::Spooled => "spooled",
            Self::Transformed => "transformed",
            Self::Buffered => "buffered",
        }
    }
}

impl SealStage {
    /// Stable snake_case label value; pinned by
    /// [`as_str_is_stable`](tests::as_str_is_stable).
    pub(in crate::ingest) fn as_str(self) -> &'static str {
        match self {
            Self::Committed => "committed",
            Self::Checkpointed => "checkpointed",
            Self::DuplicateRisk => "duplicate_risk",
        }
    }
}

/// Increment `canardstack_ingest_stage_total{request_kind, stage}`. The single
/// place this counter is emitted.
pub(in crate::ingest) fn record(
    metrics: &Metrics,
    request_kind: OtlpRequestKind,
    stage: IngestStage,
) {
    metrics.inc(
        "canardstack_ingest_stage_total",
        &[
            ("request_kind", request_kind.as_str()),
            ("stage", stage.as_str()),
        ],
        1,
    );
}

/// Increment `canardstack_ingest_seal_stage_total{stage}`. The single place this
/// counter is emitted.
pub(in crate::ingest) fn record_seal(metrics: &Metrics, stage: SealStage) {
    metrics.inc(
        "canardstack_ingest_seal_stage_total",
        &[("stage", stage.as_str())],
        1,
    );
}

#[cfg(test)]
mod tests {
    use super::{IngestStage, SealStage};

    /// Cheap guard against an accidental rename of a stage's stable label value.
    /// If a variant's snake_case identifier changes, update operator dashboards
    /// deliberately rather than letting the rename slip through.
    #[test]
    fn as_str_is_stable() {
        assert_eq!(IngestStage::Accepted.as_str(), "accepted");
        assert_eq!(IngestStage::Spooled.as_str(), "spooled");
        assert_eq!(IngestStage::Transformed.as_str(), "transformed");
        assert_eq!(IngestStage::Buffered.as_str(), "buffered");
        assert_eq!(SealStage::Committed.as_str(), "committed");
        assert_eq!(SealStage::Checkpointed.as_str(), "checkpointed");
        assert_eq!(SealStage::DuplicateRisk.as_str(), "duplicate_risk");
    }
}
