use super::Signal;
use crate::otlp::Transformed;
use arrow58::record_batch::RecordBatch;
use serde::Serialize;

pub(super) struct PendingBatch {
    pub(super) signal: Signal,
    pub(super) batch: RecordBatch,
    pub(super) source_format: &'static str,
    pub(super) approx_bytes: usize,
}

/// Per-signal view of ingest in-flight pressure: bytes that have been admitted
/// (durably spooled, handed to a worker) but not yet appended to the immutable
/// buffer. There is no separate in-memory queue, so this is the only "queue"
/// depth ingest exposes; freshness/visibility debt lives in the lane snapshot.
#[derive(Debug, Serialize)]
pub struct IngestSnapshot {
    pub signal: &'static str,
    pub inflight_bytes: usize,
    pub inflight_capacity_bytes: usize,
    pub pressure: f64,
}

pub(super) fn pending_batches(transformed: Transformed) -> Vec<PendingBatch> {
    let source_format = transformed.source_format;
    let mut batches = Vec::new();
    push_pending_arrow(&mut batches, Signal::Logs, transformed.logs, source_format);
    push_pending_arrow(
        &mut batches,
        Signal::Spans,
        transformed.spans,
        source_format,
    );
    push_pending_arrow(
        &mut batches,
        Signal::MetricGauge,
        transformed.gauge,
        source_format,
    );
    push_pending_arrow(
        &mut batches,
        Signal::MetricSum,
        transformed.sum,
        source_format,
    );
    batches
}

fn push_pending_arrow(
    batches: &mut Vec<PendingBatch>,
    signal: Signal,
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
