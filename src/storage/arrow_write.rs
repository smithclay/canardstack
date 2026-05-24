use super::{ArrowBatchBufferTiming, PreparedArrowBatch};
use crate::ingest::ReplayBackedRecordRef;
use crate::signal::StorageSignal;
use anyhow::{Context, Result};
use arrow58::array as arrow58_array;
use arrow58::array::Array as _;
use arrow58::compute::concat_batches;
use arrow58::record_batch::RecordBatch;
use chrono::{DateTime, Utc};
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::time::Instant;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct BufferDurability {
    replay_refs: BTreeSet<ReplayBackedRecordRef>,
    best_effort: bool,
}

impl BufferDurability {
    pub(super) fn empty() -> Self {
        Self {
            replay_refs: BTreeSet::new(),
            best_effort: false,
        }
    }

    pub(super) fn best_effort() -> Self {
        Self {
            replay_refs: BTreeSet::new(),
            best_effort: true,
        }
    }

    pub(super) fn replay_backed(replay_ref: ReplayBackedRecordRef) -> Self {
        Self {
            replay_refs: BTreeSet::from([replay_ref]),
            best_effort: false,
        }
    }

    pub(super) fn merge(&mut self, mut other: Self) {
        self.replay_refs.append(&mut other.replay_refs);
        self.best_effort |= other.best_effort;
    }

    pub(super) fn replay_refs(&self) -> Vec<ReplayBackedRecordRef> {
        self.replay_refs.iter().copied().collect()
    }
}

#[derive(Clone)]
pub(super) struct ArrowWriteBuffer {
    pub(super) batches: Vec<RecordBatch>,
    pub(super) rows: usize,
    pub(super) bytes: usize,
    pub(super) timestamp_days: BTreeSet<String>,
    pub(super) durability: BufferDurability,
    pub(super) best_effort_rows: usize,
    pub(super) opened_at: Instant,
}

pub(super) struct ArrowFlushResult {
    pub(super) rows: usize,
    pub(super) buffers: usize,
    pub(super) timings: Vec<ArrowBatchBufferTiming>,
    pub(super) affected: BTreeMap<StorageSignal, BTreeSet<String>>,
    pub(super) replay_refs: Vec<ReplayBackedRecordRef>,
    pub(super) best_effort_rows: usize,
}

pub struct ArrowFlushOutcome {
    pub flushed_rows: usize,
    pub flushed_buffers: usize,
    pub timings: Vec<ArrowBatchBufferTiming>,
    pub active_write_buffers: Value,
    pub(crate) replay_refs: Vec<ReplayBackedRecordRef>,
    pub(crate) best_effort_rows: usize,
}

impl ArrowFlushOutcome {
    pub(crate) fn replay_refs(&self) -> Vec<ReplayBackedRecordRef> {
        self.replay_refs.clone()
    }

    pub(crate) fn best_effort_rows(&self) -> usize {
        self.best_effort_rows
    }

    pub fn to_json(&self) -> Value {
        json!({
            "supported": true,
            "flushed_rows": self.flushed_rows,
            "flushed_buffers": self.flushed_buffers,
            "replay_backed_records": self.replay_refs.len(),
            "best_effort_rows": self.best_effort_rows,
            "timings": arrow_write_timing_snapshot(&self.timings),
            "active_write_buffers": self.active_write_buffers,
        })
    }
}

impl ArrowWriteBuffer {
    pub(super) fn new(now: Instant) -> Self {
        Self {
            batches: Vec::new(),
            rows: 0,
            bytes: 0,
            timestamp_days: BTreeSet::new(),
            durability: BufferDurability::empty(),
            best_effort_rows: 0,
            opened_at: now,
        }
    }

    pub(super) fn push(&mut self, prepared: PreparedArrowBatch) {
        self.rows += prepared.rows;
        self.bytes += prepared.batch.get_array_memory_size().max(prepared.rows);
        self.timestamp_days.extend(prepared.timestamp_days);
        self.durability.merge(prepared.durability);
        self.best_effort_rows += prepared.best_effort_rows;
        self.batches.push(prepared.batch);
    }

    pub(super) fn append_buffer(&mut self, mut other: ArrowWriteBuffer) {
        self.rows += other.rows;
        self.bytes += other.bytes;
        self.timestamp_days.append(&mut other.timestamp_days);
        self.durability.merge(other.durability);
        self.best_effort_rows += other.best_effort_rows;
        if other.opened_at < self.opened_at {
            self.opened_at = other.opened_at;
        }
        self.batches.append(&mut other.batches);
    }

    pub(super) fn record_batch(&self, storage_signal: StorageSignal) -> Result<RecordBatch> {
        match self.batches.as_slice() {
            [] => anyhow::bail!("Arrow write buffer for {storage_signal} is empty"),
            [batch] => Ok(batch.clone()),
            batches => {
                let schema = batches[0].schema();
                let refs = batches.iter().collect::<Vec<_>>();
                concat_batches(&schema, refs)
                    .with_context(|| format!("coalesce Arrow write buffer for {storage_signal}"))
            }
        }
    }
}

/// Single source of truth for the Arrow write-buffer size/age flush threshold:
/// a buffer is due when its byte size reaches `target_bytes` or its age reaches
/// `max_age_seconds`. The `SealDriver` seal-cadence decision delegates here (via
/// [`super::Storage::size_or_age_due`]); the seal then flushes everything
/// buffered through `flush_arrow_write_buffer`.
pub(crate) fn size_or_age_due(
    bytes: usize,
    age_seconds: f64,
    target_bytes: usize,
    max_age_seconds: f64,
) -> bool {
    bytes >= target_bytes || age_seconds >= max_age_seconds
}

pub(super) fn arrow_write_buffer_snapshot(
    buffers: &BTreeMap<StorageSignal, ArrowWriteBuffer>,
) -> Value {
    let mut map = serde_json::Map::new();
    for (table, buffer) in buffers {
        map.insert(
            table.as_str().to_string(),
            json!({
                "rows": buffer.rows,
                "bytes": buffer.bytes,
                "age_seconds": buffer.opened_at.elapsed().as_secs_f64(),
            }),
        );
    }
    Value::Object(map)
}

pub(super) fn arrow_write_timing_snapshot(timings: &[ArrowBatchBufferTiming]) -> Value {
    Value::Array(
        timings
            .iter()
            .map(|timing| {
                json!({
                    "storage_signal": timing.storage_signal.as_str(),
                    "table": timing.storage_signal.as_str(),
                    "phase": timing.phase.as_str(),
                    "rows": timing.rows,
                    "seconds": timing.seconds,
                })
            })
            .collect(),
    )
}

pub(super) fn timestamp_day(
    timestamps: &arrow58_array::TimestampMicrosecondArray,
    row: usize,
) -> Option<String> {
    timestamp_utc(timestamps, row).map(|timestamp| timestamp.date_naive().to_string())
}

fn timestamp_utc(
    timestamps: &arrow58_array::TimestampMicrosecondArray,
    row: usize,
) -> Option<DateTime<Utc>> {
    if timestamps.is_null(row) {
        return None;
    }
    let micros = timestamps.value(row);
    let secs = micros.div_euclid(1_000_000);
    let nanos = micros.rem_euclid(1_000_000) as u32 * 1_000;
    DateTime::<Utc>::from_timestamp(secs, nanos)
}
