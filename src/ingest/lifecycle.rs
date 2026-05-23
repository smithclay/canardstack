//! Authoritative map of the ingest lifecycle: the explicit state machine a
//! single OTLP request walks from admission through durable DuckLake commit and
//! raw-spool checkpoint. This module is the SINGLE SOURCE OF TRUTH for the
//! lifecycle vocabulary; inline comments elsewhere point here rather than
//! re-describing the hops.
//!
//! [`IngestStage`] is an observability-only annotation: it is stamped on the
//! in-flight work value and emitted on existing `tracing` events AND as the
//! `stage` label on two counters that share this vocabulary via
//! [`IngestStage::as_str`] — `canardstack_ingest_stage_total{request_kind,stage}`
//! (per-request funnel) and `canardstack_ingest_seal_stage_total{stage}`
//! (per-seal-operation funnel). It carries NO control flow: advancing the stage
//! never changes what the pipeline does, only how it is described in logs and
//! metrics.
//!
//! # Enforced transition chokepoint
//!
//! This module is the centralized, ENFORCED transition chokepoint for the
//! lifecycle. Every advance goes through one of the functions here — [`advance`]
//! for the per-request phase and [`seal_advance`] for the seal phase — and each
//! illegal hop `debug_assert!`-fails against [`is_legal`] / [`is_legal_seal`].
//! The transition tables ([`is_legal`], [`is_legal_seal`]) are the single
//! authoritative encoding of the legal graph below. Each stage counter is
//! emitted from EXACTLY ONE place (the entry/advance functions here), so there
//! is no scattered, drift-prone `inc` for the two stage counters anywhere else.
//! The `debug_assert!` is the only runtime cost added over the prior hand-rolled
//! advances and is compiled out of release builds; release behavior (counter
//! names, labels, per-stage values, and the stage `tracing` events) is identical.
//!
//! # The two phases
//!
//! Ingest is at-least-once after the durable raw-spool append: a 202 means the
//! raw request was fsynced to the local raw spool and accepted for bounded
//! processing, NOT that rows are DuckLake-committed or query-visible. The
//! lifecycle therefore splits into a per-request phase (admission → Arrow write
//! buffer) and a scheduler-driven seal phase (Arrow write buffer → durable
//! DuckLake commit → raw-spool checkpoint).
//!
//! ## Per-request phase
//!
//! - [`IngestStage::AdmittedNotSpooled`] — the request passed every request-path
//!   gate (body size, content type, storage health, freshness-budget ingest
//!   admission, in-flight + runtime-memory reservations, worker availability) but
//!   has not yet been written to the durable raw spool. Owned by
//!   [`Ingestor::ingest`](crate::ingest::Ingestor::ingest) up to the
//!   `append_raw_spool` call. A rejection here never spools.
//! - [`IngestStage::DurablySpooled`] — the raw request bytes are fsynced to the
//!   local raw spool; this is the durability point behind the 202 contract.
//!   Entered by `append_raw_spool` succeeding. Stamped at the two
//!   `SpooledIngestWork` construction sites: the live path in `Ingestor::ingest`
//!   and the restart-replay path in `ingest_replayed_raw_record`.
//! - [`IngestStage::WorkerDispatched`] — a worker thread accepted the handoff via
//!   a successful `try_send`. Owned by `dispatch_ingest_work`. The work moves to a
//!   worker thread, which calls `process_spooled_ingest`.
//! - [`IngestStage::InlineProcessed`] — every worker channel was full (or the
//!   pool is gone), so `dispatch_ingest_work` runs `process_spooled_ingest`
//!   inline on the connection thread (caller-runs back-pressure). The work does
//!   NOT move to a worker. This keeps the 202 honest under worker saturation.
//! - [`IngestStage::Transformed`] — inside `process_spooled_ingest`, the raw
//!   request was decompressed, validated, and run through `otlp2records` into
//!   Arrow `RecordBatch`es. Set after the transform succeeds.
//! - [`IngestStage::ArrowBuffered`] — `process_spooled_ingest` appended the
//!   transformed batches into the storage Arrow write buffer
//!   (`storage.buffer_arrow_batches`). The raw-spool record is then tracked via
//!   `track_raw_spool_record` so the scheduler checkpoints it after the next
//!   durable commit. This is the per-request phase terminus on the happy path.
//! - [`IngestStage::TerminallyRejectedCheckpointed`] — `process_spooled_ingest`
//!   hit a terminal payload fault (decode/body-size/transform/timestamp/memory
//!   rejection, or an empty transform) and checkpointed the raw-spool record via
//!   `checkpoint_raw_spool_terminal`. The record is disposed of and will not
//!   replay. (Retryable storage faults are NOT this stage: they leave the record
//!   pending and return without checkpointing, so it replays on restart.)
//!
//! ## Seal phase
//!
//! Driven by the scheduler's single seal driver: `seal::run` →
//! [`Ingestor::seal_committed_to_storage`](crate::ingest::Ingestor::seal_committed_to_storage).
//! Capturing the records to checkpoint BEFORE flushing is load-bearing for
//! at-least-once: a record buffered after the capture is not checkpointed until a
//! later seal, so we never checkpoint rows that were not storage-committed.
//!
//! - [`IngestStage::CapturedForSeal`] — `capture_committed_refs` snapshotted the
//!   set of tracked raw-spool refs to checkpoint and cleared the tracking map.
//!   Entered before the Arrow write buffer is flushed.
//! - [`IngestStage::DuckLakeCommitted`] — `flush_arrow_write_buffer(true)`
//!   returned Ok: the captured rows are durably committed to DuckLake.
//! - [`IngestStage::RawSpoolCheckpointed`] — `checkpoint_raw_spool_batch`
//!   succeeded for the captured refs after the commit. The records are disposed
//!   of and will not replay. This is the seal-phase happy-path terminus.
//! - [`IngestStage::CommittedNotCheckpointed`] — the DuckLake COMMIT succeeded
//!   but the subsequent raw-spool checkpoint failed (the
//!   `raw_spool_checkpoint_failed` branch). The records stay pending and replay
//!   on a future restart as duplicate ROWS in storage. This is the duplicate-risk
//!   state and the explicit at-least-once consequence; v0 does NOT dedup.

/// Explicit ingest lifecycle stage threaded through the per-request path and
/// stamped on seal-side `tracing` events. See the module-level docs for the
/// authoritative description of each stage and the function that owns the hop
/// into it. Observability-only (tracing + the two stage counters): no control
/// flow. Transitions between stages are enforced through [`advance`] /
/// [`seal_advance`], which `debug_assert!` against the legal graph.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(crate) enum IngestStage {
    /// Passed request-path admission; not yet written to the durable raw spool.
    AdmittedNotSpooled,
    /// Raw request bytes fsynced to the local raw spool (the 202 durability
    /// point).
    DurablySpooled,
    /// A worker thread accepted the handoff via a successful `try_send`.
    WorkerDispatched,
    /// Workers saturated; processed inline on the connection thread
    /// (caller-runs).
    InlineProcessed,
    /// Decompressed, validated, and turned into Arrow `RecordBatch`es.
    Transformed,
    /// Terminal payload fault: raw-spool record checkpointed (will not replay).
    TerminallyRejectedCheckpointed,
    /// Transformed batches appended to the storage Arrow write buffer.
    ArrowBuffered,
    /// Seal: tracked raw-spool refs snapshotted and tracking map cleared
    /// (capture before flush).
    CapturedForSeal,
    /// Seal: Arrow write buffer flushed and committed to durable DuckLake
    /// storage.
    DuckLakeCommitted,
    /// Seal: captured raw-spool refs checkpointed after the commit (will not
    /// replay).
    RawSpoolCheckpointed,
    /// Seal: committed but the raw-spool checkpoint failed — the duplicate-risk
    /// state; records replay as duplicate rows on restart (v0 does not dedup).
    CommittedNotCheckpointed,
}

use crate::ingest::OtlpRequestKind;
use crate::metrics::Metrics;

impl IngestStage {
    /// Stable snake_case identifier for `tracing` fields. The strings are a
    /// public-ish surface (operators key dashboards/log queries off them), so
    /// the [`as_str_is_stable`](tests::as_str_is_stable) test pins them.
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::AdmittedNotSpooled => "admitted_not_spooled",
            Self::DurablySpooled => "durably_spooled",
            Self::WorkerDispatched => "worker_dispatched",
            Self::InlineProcessed => "inline_processed",
            Self::Transformed => "transformed",
            Self::TerminallyRejectedCheckpointed => "terminally_rejected_checkpointed",
            Self::ArrowBuffered => "arrow_buffered",
            Self::CapturedForSeal => "captured_for_seal",
            Self::DuckLakeCommitted => "ducklake_committed",
            Self::RawSpoolCheckpointed => "raw_spool_checkpointed",
            Self::CommittedNotCheckpointed => "committed_not_checkpointed",
        }
    }
}

/// Authoritative per-request transition table. Returns whether `from -> to` is a
/// legal hop in the per-request phase (admission → Arrow write buffer). This is
/// the single encoding of the per-request graph; [`advance`] `debug_assert!`s
/// against it on every hop.
fn is_legal(from: IngestStage, to: IngestStage) -> bool {
    use IngestStage::*;
    matches!(
        (from, to),
        (AdmittedNotSpooled, DurablySpooled)
            | (DurablySpooled, WorkerDispatched)
            | (DurablySpooled, InlineProcessed)
            | (WorkerDispatched, Transformed)
            | (WorkerDispatched, TerminallyRejectedCheckpointed)
            | (InlineProcessed, Transformed)
            | (InlineProcessed, TerminallyRejectedCheckpointed)
            | (Transformed, ArrowBuffered)
            | (Transformed, TerminallyRejectedCheckpointed)
    )
}

/// Authoritative seal-phase transition table. Returns whether `from -> to` is a
/// legal hop in the seal phase (Arrow write buffer → durable DuckLake commit →
/// raw-spool checkpoint). [`seal_advance`] `debug_assert!`s against it.
fn is_legal_seal(from: IngestStage, to: IngestStage) -> bool {
    use IngestStage::*;
    matches!(
        (from, to),
        (CapturedForSeal, DuckLakeCommitted)
            | (DuckLakeCommitted, RawSpoolCheckpointed)
            | (DuckLakeCommitted, CommittedNotCheckpointed)
    )
}

/// Emit the per-request stage counter and the per-request stage `tracing` event
/// for `stage`. The ONLY place `canardstack_ingest_stage_total` is incremented;
/// every per-request hop funnels through here (via [`enter_admitted`],
/// [`enter_spooled`], and [`advance`]).
fn emit_request_stage(metrics: &Metrics, request_kind: OtlpRequestKind, stage: IngestStage) {
    tracing::trace!(
        event = "ingest_stage",
        request_kind = request_kind.as_str(),
        stage = stage.as_str(),
    );
    metrics.inc(
        "canardstack_ingest_stage_total",
        &[
            ("request_kind", request_kind.as_str()),
            ("stage", stage.as_str()),
        ],
        1,
    );
}

/// Emit the per-seal-operation stage counter and the seal stage `tracing` event
/// for `stage`. The ONLY place `canardstack_ingest_seal_stage_total` is
/// incremented; every seal hop funnels through here (via [`seal_enter`] and
/// [`seal_advance`]). Callers keep the existing non-empty-capture guard.
fn emit_seal_stage(metrics: &Metrics, stage: IngestStage) {
    tracing::debug!(event = "ingest_seal_stage", stage = stage.as_str());
    metrics.inc(
        "canardstack_ingest_seal_stage_total",
        &[("stage", stage.as_str())],
        1,
    );
}

/// Per-request phase entry: the request passed every request-path gate but is
/// not yet durably spooled. Emits the `admitted_not_spooled` counter + stage
/// event and returns the starting [`IngestStage::AdmittedNotSpooled`]. No prior
/// stage exists, so there is nothing to validate.
pub(in crate::ingest) fn enter_admitted(
    metrics: &Metrics,
    request_kind: OtlpRequestKind,
) -> IngestStage {
    emit_request_stage(metrics, request_kind, IngestStage::AdmittedNotSpooled);
    IngestStage::AdmittedNotSpooled
}

/// Per-request phase entry for the restart-replay path, which begins already
/// durably spooled (the record is recovered from the fsynced raw spool). Emits
/// the `durably_spooled` counter + stage event and returns
/// [`IngestStage::DurablySpooled`].
pub(in crate::ingest) fn enter_spooled(
    metrics: &Metrics,
    request_kind: OtlpRequestKind,
) -> IngestStage {
    emit_request_stage(metrics, request_kind, IngestStage::DurablySpooled);
    IngestStage::DurablySpooled
}

/// Advance the per-request lifecycle to `to`: `debug_assert!` the hop is legal
/// against [`is_legal`], emit the stage counter + stage event, then store the
/// new stage. The single chokepoint for per-request stage transitions.
pub(in crate::ingest) fn advance(
    stage: &mut IngestStage,
    metrics: &Metrics,
    request_kind: OtlpRequestKind,
    to: IngestStage,
) {
    debug_assert!(
        is_legal(*stage, to),
        "illegal ingest transition {:?} -> {:?}",
        *stage,
        to
    );
    emit_request_stage(metrics, request_kind, to);
    *stage = to;
}

/// Seal phase entry: the tracked raw-spool refs were captured and the tracking
/// map cleared (capture before flush). Emits the `captured_for_seal` counter +
/// stage event and returns the starting [`IngestStage::CapturedForSeal`]. The
/// caller wraps this in the existing non-empty-capture guard.
pub(in crate::ingest) fn seal_enter(metrics: &Metrics) -> IngestStage {
    emit_seal_stage(metrics, IngestStage::CapturedForSeal);
    IngestStage::CapturedForSeal
}

/// Advance the seal lifecycle to `to`: `debug_assert!` the hop is legal against
/// [`is_legal_seal`], emit the seal stage counter + stage event, then store the
/// new stage. The single chokepoint for seal stage transitions. The caller
/// wraps this in the existing non-empty-capture guard.
pub(in crate::ingest) fn seal_advance(stage: &mut IngestStage, metrics: &Metrics, to: IngestStage) {
    debug_assert!(
        is_legal_seal(*stage, to),
        "illegal ingest seal transition {:?} -> {:?}",
        *stage,
        to
    );
    emit_seal_stage(metrics, to);
    *stage = to;
}

#[cfg(test)]
mod tests {
    use super::{is_legal, is_legal_seal, IngestStage};

    /// The complete set of legal per-request hops, pinned independently of the
    /// matcher in [`is_legal`] so the enforced graph cannot silently drift.
    const LEGAL_REQUEST: &[(IngestStage, IngestStage)] = {
        use IngestStage::*;
        &[
            (AdmittedNotSpooled, DurablySpooled),
            (DurablySpooled, WorkerDispatched),
            (DurablySpooled, InlineProcessed),
            (WorkerDispatched, Transformed),
            (WorkerDispatched, TerminallyRejectedCheckpointed),
            (InlineProcessed, Transformed),
            (InlineProcessed, TerminallyRejectedCheckpointed),
            (Transformed, ArrowBuffered),
            (Transformed, TerminallyRejectedCheckpointed),
        ]
    };

    /// The complete set of legal seal hops, pinned independently of
    /// [`is_legal_seal`].
    const LEGAL_SEAL: &[(IngestStage, IngestStage)] = {
        use IngestStage::*;
        &[
            (CapturedForSeal, DuckLakeCommitted),
            (DuckLakeCommitted, RawSpoolCheckpointed),
            (DuckLakeCommitted, CommittedNotCheckpointed),
        ]
    };

    const ALL_STAGES: &[IngestStage] = {
        use IngestStage::*;
        &[
            AdmittedNotSpooled,
            DurablySpooled,
            WorkerDispatched,
            InlineProcessed,
            Transformed,
            TerminallyRejectedCheckpointed,
            ArrowBuffered,
            CapturedForSeal,
            DuckLakeCommitted,
            RawSpoolCheckpointed,
            CommittedNotCheckpointed,
        ]
    };

    /// `is_legal` accepts exactly the per-request graph and rejects every other
    /// ordered pair. This is the table the `debug_assert!` in `advance` enforces;
    /// the integration tests then prove production walks only these hops.
    #[test]
    fn is_legal_matches_request_table() {
        for &from in ALL_STAGES {
            for &to in ALL_STAGES {
                let expected = LEGAL_REQUEST.contains(&(from, to));
                assert_eq!(
                    is_legal(from, to),
                    expected,
                    "is_legal({from:?}, {to:?}) should be {expected}"
                );
            }
        }
    }

    /// `is_legal_seal` accepts exactly the seal graph and rejects every other
    /// ordered pair.
    #[test]
    fn is_legal_seal_matches_seal_table() {
        for &from in ALL_STAGES {
            for &to in ALL_STAGES {
                let expected = LEGAL_SEAL.contains(&(from, to));
                assert_eq!(
                    is_legal_seal(from, to),
                    expected,
                    "is_legal_seal({from:?}, {to:?}) should be {expected}"
                );
            }
        }
    }

    /// Cheap guard against an accidental rename of a stage's stable string. If a
    /// variant's snake_case identifier changes, update operator dashboards/log
    /// queries deliberately rather than letting the rename slip through.
    #[test]
    fn as_str_is_stable() {
        let cases = [
            (IngestStage::AdmittedNotSpooled, "admitted_not_spooled"),
            (IngestStage::DurablySpooled, "durably_spooled"),
            (IngestStage::WorkerDispatched, "worker_dispatched"),
            (IngestStage::InlineProcessed, "inline_processed"),
            (IngestStage::Transformed, "transformed"),
            (
                IngestStage::TerminallyRejectedCheckpointed,
                "terminally_rejected_checkpointed",
            ),
            (IngestStage::ArrowBuffered, "arrow_buffered"),
            (IngestStage::CapturedForSeal, "captured_for_seal"),
            (IngestStage::DuckLakeCommitted, "ducklake_committed"),
            (IngestStage::RawSpoolCheckpointed, "raw_spool_checkpointed"),
            (
                IngestStage::CommittedNotCheckpointed,
                "committed_not_checkpointed",
            ),
        ];
        for (stage, expected) in cases {
            assert_eq!(stage.as_str(), expected, "stage string drifted: {stage:?}");
        }
    }
}
