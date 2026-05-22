use super::Signal;
use crate::otlp::Transformed;
use arrow58::record_batch::RecordBatch;
use serde::Serialize;

#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash)]
pub(super) struct QueueKey {
    pub(super) signal: Signal,
    pub(super) partition: BatchPartition,
}

impl QueueKey {
    pub(super) fn new(signal: Signal, source_format: &'static str) -> Self {
        Self {
            signal,
            partition: BatchPartition::from_source_format(source_format),
        }
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash)]
pub(super) enum BatchPartition {
    Json,
    Protobuf,
}

impl BatchPartition {
    fn from_source_format(source_format: &'static str) -> Self {
        match source_format {
            "json" | "otlp_json" => Self::Json,
            _ => Self::Protobuf,
        }
    }
}

pub(super) struct PendingBatch {
    pub(super) key: QueueKey,
    pub(super) batch: RecordBatch,
    pub(super) source_format: &'static str,
    pub(super) approx_bytes: usize,
    pub(super) credit_bytes: usize,
}

#[derive(Debug, Serialize)]
pub struct IngestSnapshot {
    pub signal: &'static str,
    pub buffered_rows: usize,
    pub buffered_bytes: usize,
    pub queue_credit_reserved_bytes: usize,
    pub queue_credit_available_bytes: usize,
    pub queue_credit_capacity_bytes: usize,
    pub queue_credit_closed: bool,
    pub visibility_debt_seconds: f64,
    pub oldest_age_seconds: f64,
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
        key: QueueKey::new(signal, source_format),
        batch,
        source_format,
        approx_bytes,
        credit_bytes: approx_bytes,
    });
}
