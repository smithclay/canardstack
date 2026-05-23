//! Authoritative map of the ingest lifecycle: the explicit state machine a
//! single OTLP request walks from admission through durable DuckLake commit and
//! raw-spool checkpoint. This module is the SINGLE SOURCE OF TRUTH for the
//! lifecycle vocabulary; inline comments elsewhere point here rather than
//! re-describing the hops.
//!
//! [`IngestStage`] is a tracing-only annotation: it is stamped on the in-flight
//! work value and emitted on existing `tracing` events. It carries NO control
//! flow, NO metrics, and NO transition validation — advancing the stage never
//! changes what the pipeline does, only how it is described in logs.
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
/// into it. Tracing-only: no control flow, no metrics, no transition validation.
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

#[cfg(test)]
mod tests {
    use super::IngestStage;

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
