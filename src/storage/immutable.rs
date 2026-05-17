use super::{ArrowBatchInsertTiming, PreparedArrowBatch, TimingPhase};
use crate::db::sql::quote as sql_quote;
use crate::ingest::Signal;
use crate::storage::arrow::timestamp_column;
use anyhow::{Context, Result};
use arrow58::array as arrow58_array;
use arrow58::array::Array as _;
use arrow58::compute::{concat_batches, take};
use arrow58::record_batch::RecordBatch;
use chrono::{DateTime, Datelike, NaiveDate, Timelike, Utc};
use duckdb::Connection;
use otlp2records::to_parquet;
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

static IMMUTABLE_SEGMENT_COUNTER: AtomicU64 = AtomicU64::new(0);

#[derive(Clone)]
pub(super) struct ImmutableSegmentBuffer {
    pub(super) batches: Vec<RecordBatch>,
    pub(super) rows: usize,
    pub(super) bytes: usize,
    pub(super) timestamp_days: BTreeSet<String>,
    pub(super) opened_at: Instant,
}

pub(super) struct ImmutableSealResult {
    pub(super) rows: usize,
    pub(super) files: usize,
    pub(super) timings: Vec<ArrowBatchInsertTiming>,
    pub(super) affected: BTreeMap<Signal, BTreeSet<String>>,
}

pub struct ImmutableFlushOutcome {
    pub force: bool,
    pub sealed_files: usize,
    pub sealed_rows: usize,
    pub timings: Vec<ArrowBatchInsertTiming>,
    pub active_buffers: Value,
}

impl ImmutableFlushOutcome {
    pub fn to_json(&self) -> Value {
        json!({
            "supported": true,
            "force": self.force,
            "sealed_files": self.sealed_files,
            "sealed_rows": self.sealed_rows,
            "timings": immutable_timing_snapshot(&self.timings),
            "active_buffers": self.active_buffers,
        })
    }
}

pub(super) struct ImmutableSegmentWrite {
    pub(super) segment: SealedSegment,
    pub(super) timings: Vec<ArrowBatchInsertTiming>,
}

impl ImmutableSegmentBuffer {
    pub(super) fn new(now: Instant) -> Self {
        Self {
            batches: Vec::new(),
            rows: 0,
            bytes: 0,
            timestamp_days: BTreeSet::new(),
            opened_at: now,
        }
    }

    pub(super) fn push(&mut self, prepared: PreparedArrowBatch) {
        self.rows += prepared.rows;
        self.bytes += prepared.batch.get_array_memory_size().max(prepared.rows);
        self.timestamp_days.extend(prepared.timestamp_days);
        self.batches.push(prepared.batch);
    }

    pub(super) fn append_buffer(&mut self, mut other: ImmutableSegmentBuffer) {
        self.rows += other.rows;
        self.bytes += other.bytes;
        self.timestamp_days.append(&mut other.timestamp_days);
        if other.opened_at < self.opened_at {
            self.opened_at = other.opened_at;
        }
        self.batches.append(&mut other.batches);
    }

    pub(super) fn should_seal(&self, target_bytes: usize, max_age: Duration, now: Instant) -> bool {
        self.rows > 0
            && (self.bytes >= target_bytes || now.duration_since(self.opened_at) >= max_age)
    }

    pub(super) fn record_batch(&self, table: Signal) -> Result<RecordBatch> {
        match self.batches.as_slice() {
            [] => anyhow::bail!("immutable {table} buffer is empty"),
            [batch] => Ok(batch.clone()),
            batches => {
                let schema = batches[0].schema();
                let refs = batches.iter().collect::<Vec<_>>();
                concat_batches(&schema, refs)
                    .with_context(|| format!("coalesce immutable {table} segment buffer"))
            }
        }
    }
}
pub(super) fn distribute_commit_seconds(
    timings: &mut [ArrowBatchInsertTiming],
    commit_seconds: f64,
) {
    if timings.is_empty() || commit_seconds <= 0.0 {
        return;
    }
    let insert_timings = timings
        .iter_mut()
        .filter(|timing| timing.phase == TimingPhase::Insert)
        .collect::<Vec<_>>();
    if insert_timings.is_empty() {
        return;
    }
    let total_rows: usize = insert_timings.iter().map(|timing| timing.rows).sum();
    if total_rows == 0 {
        let each = commit_seconds / insert_timings.len() as f64;
        for timing in insert_timings {
            timing.seconds += each;
        }
        return;
    }
    for timing in insert_timings {
        timing.seconds += commit_seconds * timing.rows as f64 / total_rows as f64;
    }
}

pub(super) fn distributed_segment_timing(
    phase: TimingPhase,
    sealed: &[SealedSegment],
    seconds: f64,
) -> Vec<ArrowBatchInsertTiming> {
    if sealed.is_empty() {
        return Vec::new();
    }
    let total_rows = sealed.iter().map(|segment| segment.rows).sum::<usize>();
    sealed
        .iter()
        .map(|segment| {
            let segment_seconds = if total_rows == 0 {
                seconds / sealed.len() as f64
            } else {
                seconds * segment.rows as f64 / total_rows as f64
            };
            ArrowBatchInsertTiming {
                table: segment.table,
                phase,
                rows: segment.rows,
                seconds: segment_seconds,
            }
        })
        .collect()
}

pub(super) struct SealedSegment {
    pub(super) table: Signal,
    pub(super) path: PathBuf,
    pub(super) rows: usize,
}

pub(super) fn write_immutable_segment(
    storage_dir: &Path,
    table: Signal,
    partition: ImmutableSegmentPartition,
    batch: &RecordBatch,
) -> Result<ImmutableSegmentWrite> {
    let final_path = immutable_segment_path(storage_dir, table, partition)?;
    let parent = final_path
        .parent()
        .context("immutable segment path has no parent")?;
    fs::create_dir_all(parent)
        .with_context(|| format!("create immutable segment directory {}", parent.display()))?;

    let tmp_path = final_path.with_extension("parquet.tmp");
    let mut timings = Vec::new();
    let rows = batch.num_rows();

    let started = Instant::now();
    let parquet_bytes = to_parquet(batch).context("encode immutable segment parquet")?;
    timings.push(ArrowBatchInsertTiming {
        table,
        phase: TimingPhase::ParquetEncode,
        rows,
        seconds: started.elapsed().as_secs_f64(),
    });

    let started = Instant::now();
    let mut file = BufWriter::new(
        File::create(&tmp_path)
            .with_context(|| format!("create immutable segment {}", tmp_path.display()))?,
    );
    file.write_all(&parquet_bytes)
        .with_context(|| format!("write immutable segment {}", tmp_path.display()))?;
    let file = file
        .into_inner()
        .context("flush immutable segment writer before seal")?;
    timings.push(ArrowBatchInsertTiming {
        table,
        phase: TimingPhase::FileWrite,
        rows,
        seconds: started.elapsed().as_secs_f64(),
    });

    let started = Instant::now();
    file.sync_all()
        .with_context(|| format!("fsync immutable segment {}", tmp_path.display()))?;
    timings.push(ArrowBatchInsertTiming {
        table,
        phase: TimingPhase::FileFsync,
        rows,
        seconds: started.elapsed().as_secs_f64(),
    });
    drop(file);

    let started = Instant::now();
    fs::rename(&tmp_path, &final_path).with_context(|| {
        format!(
            "seal immutable segment {} -> {}",
            tmp_path.display(),
            final_path.display()
        )
    })?;
    timings.push(ArrowBatchInsertTiming {
        table,
        phase: TimingPhase::FileRename,
        rows,
        seconds: started.elapsed().as_secs_f64(),
    });

    Ok(ImmutableSegmentWrite {
        segment: SealedSegment {
            table,
            path: final_path,
            rows,
        },
        timings,
    })
}

pub(super) fn immutable_segment_path(
    storage_dir: &Path,
    table: Signal,
    partition: ImmutableSegmentPartition,
) -> Result<PathBuf> {
    let sequence = IMMUTABLE_SEGMENT_COUNTER.fetch_add(1, Ordering::SeqCst);
    let suffix = format!("{}-{sequence}.parquet", Utc::now().timestamp_micros());
    let day =
        NaiveDate::parse_from_str(&partition.timestamp_day, "%Y-%m-%d").with_context(|| {
            format!(
                "parse immutable segment timestamp day {}",
                partition.timestamp_day
            )
        })?;
    Ok(storage_dir
        .join("main")
        .join(table.as_str())
        .join(format!("year={}", day.format("%Y")))
        .join(format!("month={}", day.month()))
        .join(format!("day={}", day.day()))
        .join(suffix))
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(super) struct ImmutableSegmentPartition {
    pub(super) timestamp_day: String,
    pub(super) hour: u32,
}

pub(super) fn split_batch_by_immutable_partition(
    batch: &RecordBatch,
) -> Result<Vec<(ImmutableSegmentPartition, RecordBatch)>> {
    if batch.num_rows() == 0 {
        return Ok(Vec::new());
    }

    let mut rows_by_partition: BTreeMap<ImmutableSegmentPartition, Vec<u32>> = BTreeMap::new();
    let timestamps = timestamp_column(batch)?;
    for row in 0..batch.num_rows() {
        rows_by_partition
            .entry(immutable_row_partition(timestamps, row))
            .or_default()
            .push(row as u32);
    }

    if rows_by_partition.len() == 1 {
        let partition = rows_by_partition
            .into_keys()
            .next()
            .expect("partition exists for non-empty batch");
        return Ok(vec![(partition, batch.clone())]);
    }

    let schema = batch.schema();
    rows_by_partition
        .into_iter()
        .map(|(partition, rows)| {
            let indices = arrow58_array::UInt32Array::from(rows);
            let columns = batch
                .columns()
                .iter()
                .map(|column| take(column.as_ref(), &indices, None))
                .collect::<arrow58::error::Result<Vec<_>>>()?;
            let batch = RecordBatch::try_new(schema.clone(), columns)
                .context("build immutable partition RecordBatch")?;
            Ok((partition, batch))
        })
        .collect()
}

pub(super) fn immutable_row_partition(
    timestamps: &arrow58_array::TimestampMicrosecondArray,
    row: usize,
) -> ImmutableSegmentPartition {
    let now = Utc::now();
    ImmutableSegmentPartition {
        timestamp_day: timestamp_day(timestamps, row)
            .unwrap_or_else(|| now.date_naive().to_string()),
        hour: timestamp_hour(timestamps, row).unwrap_or_else(|| now.hour()),
    }
}

pub(super) fn timestamp_day(
    timestamps: &arrow58_array::TimestampMicrosecondArray,
    row: usize,
) -> Option<String> {
    timestamp_utc(timestamps, row).map(|timestamp| timestamp.date_naive().to_string())
}

pub(super) fn timestamp_hour(
    timestamps: &arrow58_array::TimestampMicrosecondArray,
    row: usize,
) -> Option<u32> {
    timestamp_utc(timestamps, row).map(|timestamp| timestamp.hour())
}

pub(super) fn timestamp_utc(
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

pub(super) fn register_ducklake_data_file(
    conn: &Connection,
    catalog_name: &str,
    table: Signal,
    path: &Path,
) -> Result<()> {
    let sql = format!(
        "CALL ducklake_add_data_files({}, {}, {}, schema = 'main')",
        sql_quote(catalog_name),
        sql_quote(table.as_str()),
        sql_quote(&path.to_string_lossy())
    );
    conn.execute_batch(&sql)
        .with_context(|| format!("register immutable segment {}", path.display()))?;
    Ok(())
}

pub(super) fn immutable_buffer_snapshot(
    buffers: &BTreeMap<Signal, ImmutableSegmentBuffer>,
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

pub(super) fn immutable_timing_snapshot(timings: &[ArrowBatchInsertTiming]) -> Value {
    Value::Array(
        timings
            .iter()
            .map(|timing| {
                json!({
                    "table": timing.table.as_str(),
                    "phase": timing.phase.as_str(),
                    "rows": timing.rows,
                    "seconds": timing.seconds,
                })
            })
            .collect(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow58::array::{Int64Array, TimestampMicrosecondArray};
    use arrow58::datatypes::{DataType, Field, Schema, TimeUnit};
    use std::sync::Arc;
    use tempfile::tempdir;

    #[test]
    fn immutable_segment_write_reports_detailed_timings() {
        let dir = tempdir().unwrap();
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                false,
            ),
            Field::new("value", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(TimestampMicrosecondArray::from(vec![1_000_000, 2_000_000])),
                Arc::new(Int64Array::from(vec![10, 20])),
            ],
        )
        .unwrap();
        let partition = ImmutableSegmentPartition {
            timestamp_day: "2026-05-16".to_string(),
            hour: 0,
        };

        let written = write_immutable_segment(dir.path(), Signal::Logs, partition, &batch).unwrap();

        assert_eq!(written.segment.table, Signal::Logs);
        assert_eq!(written.segment.rows, 2);
        assert!(written.segment.path.exists());

        let phases = written
            .timings
            .iter()
            .map(|timing| timing.phase)
            .collect::<Vec<_>>();
        assert_eq!(
            phases,
            vec![
                TimingPhase::ParquetEncode,
                TimingPhase::FileWrite,
                TimingPhase::FileFsync,
                TimingPhase::FileRename,
            ]
        );
        assert!(written
            .timings
            .iter()
            .all(|timing| timing.table == Signal::Logs && timing.rows == 2));
    }
}
