use super::{ArrowBatchBufferTiming, CommittedReplayRefs, PreparedArrowBatch};
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

#[derive(Clone)]
pub(super) struct ArrowWriteBuffer {
    pub(super) batches: Vec<RecordBatch>,
    pub(super) rows: usize,
    pub(super) bytes: usize,
    pub(super) timestamp_days: BTreeSet<String>,
    pub(super) replay_refs: BTreeSet<ReplayBackedRecordRef>,
    pub(super) opened_at: Instant,
}

/// Result record of a durable Arrow write-buffer commit
/// ([`super::Storage::commit_arrow_write_buffer`]). The `flushed_*` field names
/// (and the matching `flushed_rows`/`flushed_buffers` JSON keys plus the
/// `canardstack_arrow_flush*` metrics) are a stable operator contract, so the
/// noun "flush" is kept on the result even though the operation verb is "commit".
pub struct ArrowFlushOutcome {
    pub flushed_rows: usize,
    pub flushed_buffers: usize,
    pub timings: Vec<ArrowBatchBufferTiming>,
    pub active_write_buffers: Value,
    pub(super) replay_backed_records: usize,
    pub(super) committed_replay_refs: CommittedReplayRefs,
}

impl ArrowFlushOutcome {
    pub(crate) fn take_committed_replay_refs(&mut self) -> CommittedReplayRefs {
        std::mem::replace(
            &mut self.committed_replay_refs,
            CommittedReplayRefs::empty(),
        )
    }

    pub fn to_json(&self) -> Value {
        json!({
            "supported": true,
            "flushed_rows": self.flushed_rows,
            "flushed_buffers": self.flushed_buffers,
            "replay_backed_records": self.replay_backed_records,
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
            replay_refs: BTreeSet::new(),
            opened_at: now,
        }
    }

    pub(super) fn push(&mut self, prepared: PreparedArrowBatch) {
        self.rows += prepared.rows;
        self.bytes += prepared.batch.get_array_memory_size().max(prepared.rows);
        self.timestamp_days.extend(prepared.timestamp_days);
        self.replay_refs.extend(prepared.replay_refs);
        self.batches.push(prepared.batch);
    }

    pub(super) fn append_buffer(&mut self, mut other: ArrowWriteBuffer) {
        self.rows += other.rows;
        self.bytes += other.bytes;
        self.timestamp_days.append(&mut other.timestamp_days);
        self.replay_refs.append(&mut other.replay_refs);
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
/// [`super::Storage::size_or_age_due`]); the seal then commits everything
/// buffered through `commit_arrow_write_buffer`.
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
                    "phase": timing.phase.as_str(),
                    "rows": timing.rows,
                    "seconds": timing.seconds,
                })
            })
            .collect(),
    )
}

pub(super) fn timestamp_day(
    timestamps: &arrow58_array::TimestampNanosecondArray,
    row: usize,
) -> Option<String> {
    timestamp_utc(timestamps, row).map(|timestamp| timestamp.date_naive().to_string())
}

fn timestamp_utc(
    timestamps: &arrow58_array::TimestampNanosecondArray,
    row: usize,
) -> Option<DateTime<Utc>> {
    if timestamps.is_null(row) {
        return None;
    }
    let nanos = timestamps.value(row);
    let secs = nanos.div_euclid(1_000_000_000);
    let subsec_nanos = nanos.rem_euclid(1_000_000_000) as u32;
    DateTime::<Utc>::from_timestamp(secs, subsec_nanos)
}
