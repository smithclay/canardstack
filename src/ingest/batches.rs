use crate::otlp::Transformed;
use crate::signal::StorageSignal;
use arrow58::record_batch::RecordBatch;
use serde::Serialize;

pub(super) struct PendingBatch {
    pub(super) signal: StorageSignal,
    pub(super) batch: RecordBatch,
    pub(super) source_format: &'static str,
    pub(super) approx_bytes: usize,
}

/// Per-signal view of ingest in-flight accounting: bytes admitted (durably
/// spooled, handed to a worker) but not yet appended to the Arrow write buffer.
/// There is no separate in-memory queue; freshness/visibility debt lives in the
/// admission snapshot.
#[derive(Debug, Serialize)]
pub struct IngestSnapshot {
    pub storage_signal: &'static str,
    pub inflight_bytes: usize,
}

pub(super) fn pending_batches(transformed: Transformed) -> Vec<PendingBatch> {
    let source_format = transformed.source_format;
    let mut batches = Vec::new();
    push_pending_arrow(
        &mut batches,
        StorageSignal::Logs,
        transformed.logs,
        source_format,
    );
    push_pending_arrow(
        &mut batches,
        StorageSignal::Spans,
        transformed.spans,
        source_format,
    );
    push_pending_arrow(
        &mut batches,
        StorageSignal::MetricGauge,
        transformed.gauge,
        source_format,
    );
    push_pending_arrow(
        &mut batches,
        StorageSignal::MetricSum,
        transformed.sum,
        source_format,
    );
    batches
}

fn push_pending_arrow(
    batches: &mut Vec<PendingBatch>,
    signal: StorageSignal,
    batch: Option<RecordBatch>,
    source_format: &'static str,
) {
    let Some(batch) = batch else {
        return;
    };
    if batch.num_rows() == 0 {
        return;
    }
    let approx_bytes = batch.get_array_memory_size().max(batch.num_rows());
    batches.push(PendingBatch {
        signal,
        batch,
        source_format,
        approx_bytes,
    });
}
