use super::arrow::{batch_timestamp_days, storage_duckdb_batch};
use super::ducklake::configure_write_connection;
use super::immutable::{
    distribute_commit_seconds, distributed_segment_timing, immutable_buffer_snapshot,
    register_ducklake_data_file, split_batch_by_immutable_partition, write_immutable_segment,
    ImmutableFlushOutcome, ImmutableSealResult, SealedSegment,
};
use super::{
    ArrowBatchInsert, ArrowBatchInsertResult, ArrowBatchInsertTiming, ImmutableBufferMetric,
    ImmutableSegmentBuffer, PreparedArrowBatch, Signal, Storage, TimingPhase,
};
use crate::LockExt;
use anyhow::{Context, Result};
use arrow58::compute::concat_batches;
use arrow58::record_batch::RecordBatch;
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::thread;
use std::time::Instant;

struct SealedBuffer {
    segments: Vec<SealedSegment>,
    timings: Vec<ArrowBatchInsertTiming>,
    affected_days: BTreeSet<String>,
}

impl Storage {
    pub fn immutable_buffer_metrics(&self) -> Vec<ImmutableBufferMetric> {
        let buffers = self.immutable_buffers.lock_or_poisoned();
        buffers
            .iter()
            .map(|(table, buffer)| ImmutableBufferMetric {
                table: *table,
                rows: buffer.rows,
                bytes: buffer.bytes,
                age_seconds: buffer.opened_at.elapsed().as_secs_f64(),
            })
            .collect()
    }

    pub fn insert_arrow_records(
        &self,
        table: Signal,
        batch: &RecordBatch,
        source_format: &str,
    ) -> Result<usize> {
        let result = self.insert_arrow_batches(&[ArrowBatchInsert {
            table,
            batch,
            source_format,
        }])?;
        Ok(result.rows)
    }

    pub fn insert_arrow_batches(
        &self,
        batches: &[ArrowBatchInsert<'_>],
    ) -> Result<ArrowBatchInsertResult> {
        let mut prepared = Vec::new();
        let mut prepare_timings = Vec::new();
        let mut attempted_rows = 0;
        let mut grouped = BTreeMap::<(Signal, &str), Vec<&RecordBatch>>::new();
        for batch in batches {
            if batch.batch.num_rows() == 0 {
                continue;
            }
            attempted_rows += batch.batch.num_rows();
            grouped
                .entry((batch.table, batch.source_format))
                .or_default()
                .push(batch.batch);
        }
        for ((table, source_format), batches) in grouped {
            let rows = batches.iter().map(|batch| batch.num_rows()).sum();
            let coalesce_started = Instant::now();
            let batch = coalesce_storage_batches(table, &batches)?;
            prepare_timings.push(ArrowBatchInsertTiming {
                table,
                phase: TimingPhase::Coalesce,
                rows,
                seconds: coalesce_started.elapsed().as_secs_f64(),
            });
            let prepare_started = Instant::now();
            let prepared_batch = storage_duckdb_batch(table, &batch, source_format)?;
            let timestamp_days = batch_timestamp_days(&prepared_batch)?;
            let prepare_seconds = prepare_started.elapsed().as_secs_f64();
            prepared.push(PreparedArrowBatch {
                table,
                batch: prepared_batch,
                rows,
                timestamp_days,
            });
            prepare_timings.push(ArrowBatchInsertTiming {
                table,
                phase: TimingPhase::Prepare,
                rows,
                seconds: prepare_seconds,
            });
        }

        if prepared.is_empty() {
            return Ok(ArrowBatchInsertResult {
                rows: 0,
                timings: Vec::new(),
            });
        }

        self.insert_immutable_segments(prepared, prepare_timings, attempted_rows)
    }

    fn insert_immutable_segments(
        &self,
        prepared: Vec<PreparedArrowBatch>,
        prepare_timings: Vec<ArrowBatchInsertTiming>,
        attempted_rows: usize,
    ) -> Result<ArrowBatchInsertResult> {
        if !self.ducklake_available {
            anyhow::bail!("immutable segment ingest requires DuckLake storage");
        }

        let error_table = prepared
            .first()
            .map(|batch| batch.table)
            .unwrap_or(Signal::Logs);
        let mut timings = prepare_timings;

        {
            let mut buffers = self.immutable_buffers.lock_or_poisoned();
            let started = Instant::now();
            for batch in prepared {
                buffers
                    .entry(batch.table)
                    .or_insert_with(|| ImmutableSegmentBuffer::new(started))
                    .push(batch);
            }
            timings.push(ArrowBatchInsertTiming {
                table: error_table,
                phase: TimingPhase::Buffer,
                rows: attempted_rows,
                seconds: started.elapsed().as_secs_f64(),
            });
        }
        *self.last_error.lock_or_poisoned() = None;
        Ok(ArrowBatchInsertResult {
            rows: attempted_rows,
            timings,
        })
    }

    pub fn flush_immutable_segments(&self, force: bool) -> Result<ImmutableFlushOutcome> {
        let mut to_seal = BTreeMap::new();
        {
            let mut buffers = self.immutable_buffers.lock_or_poisoned();
            let now = Instant::now();
            let tables_to_seal = buffers
                .iter()
                .filter_map(|(table, buffer)| {
                    (force
                        || buffer.should_seal(
                            self.immutable_segment_target_bytes,
                            self.immutable_segment_max_age,
                            now,
                        ))
                    .then_some(*table)
                })
                .collect::<Vec<_>>();

            if tables_to_seal.is_empty() {
                return Ok(ImmutableFlushOutcome {
                    force,
                    sealed_files: 0,
                    sealed_rows: 0,
                    timings: Vec::new(),
                    active_buffers: immutable_buffer_snapshot(&buffers),
                });
            }

            for table in tables_to_seal {
                if let Some(buffer) = buffers.remove(&table) {
                    to_seal.insert(table, buffer);
                }
            }
        }
        let seal_result = match self.seal_immutable_buffers(&to_seal) {
            Ok(result) => result,
            Err(err) => {
                self.restore_immutable_buffers(to_seal);
                return Err(err);
            }
        };
        let active_buffers = immutable_buffer_snapshot(&self.immutable_buffers.lock_or_poisoned());
        *self.last_error.lock_or_poisoned() = None;
        self.mark_metadata_dirty(seal_result.affected);

        Ok(ImmutableFlushOutcome {
            force,
            sealed_files: seal_result.files,
            sealed_rows: seal_result.rows,
            timings: seal_result.timings,
            active_buffers,
        })
    }

    fn seal_immutable_buffers(
        &self,
        buffers: &BTreeMap<Signal, ImmutableSegmentBuffer>,
    ) -> Result<ImmutableSealResult> {
        let mut timings = Vec::new();
        let mut sealed = Vec::with_capacity(buffers.len());
        let mut affected = BTreeMap::new();

        let mut handles = Vec::with_capacity(buffers.len());
        for (&table, buffer) in buffers {
            let storage_dir = self.local_storage_dir.clone();
            let buffer = buffer.clone();
            handles.push((
                table,
                thread::spawn(move || seal_immutable_buffer(&storage_dir, table, &buffer)),
            ));
        }

        let mut seal_error = None;
        for (table, handle) in handles {
            match handle.join() {
                Ok(Ok(buffer)) => {
                    if seal_error.is_none() {
                        timings.extend(buffer.timings);
                        sealed.extend(buffer.segments);
                        affected.insert(table, buffer.affected_days);
                    }
                }
                Ok(Err(err)) => {
                    if seal_error.is_none() {
                        seal_error = Some(err);
                    }
                }
                Err(_) => {
                    if seal_error.is_none() {
                        seal_error = Some(anyhow::anyhow!(
                            "immutable segment writer panicked for {table}"
                        ));
                    }
                }
            }
        }
        if let Some(err) = seal_error {
            return Err(err);
        }

        let conn = self.writer.lock_or_poisoned();
        configure_write_connection(&conn, &self.write_memory_limit)?;
        let register_result = (|| -> Result<()> {
            conn.execute_batch("BEGIN TRANSACTION;")?;
            for segment in &sealed {
                let started = Instant::now();
                register_ducklake_data_file(
                    &conn,
                    &self.catalog_name,
                    segment.table,
                    &segment.path,
                )?;
                let register_seconds = started.elapsed().as_secs_f64();
                timings.push(ArrowBatchInsertTiming {
                    table: segment.table,
                    phase: TimingPhase::DucklakeRegister,
                    rows: segment.rows,
                    seconds: register_seconds,
                });
                timings.push(ArrowBatchInsertTiming {
                    table: segment.table,
                    phase: TimingPhase::Insert,
                    rows: segment.rows,
                    seconds: register_seconds,
                });
            }
            let commit_started = Instant::now();
            conn.execute_batch("COMMIT;")?;
            let commit_seconds = commit_started.elapsed().as_secs_f64();
            timings.extend(distributed_segment_timing(
                TimingPhase::DucklakeCommit,
                &sealed,
                commit_seconds,
            ));
            distribute_commit_seconds(&mut timings, commit_seconds);
            Ok(())
        })();

        if let Err(err) = register_result {
            let _ = conn.execute_batch("ROLLBACK;");
            return Err(err);
        }

        Ok(ImmutableSealResult {
            rows: sealed.iter().map(|segment| segment.rows).sum(),
            files: sealed.len(),
            timings,
            affected,
        })
    }

    fn restore_immutable_buffers(&self, detached: BTreeMap<Signal, ImmutableSegmentBuffer>) {
        let mut buffers = self.immutable_buffers.lock_or_poisoned();
        for (table, mut detached_buffer) in detached {
            if let Some(current) = buffers.remove(&table) {
                detached_buffer.append_buffer(current);
            }
            buffers.insert(table, detached_buffer);
        }
    }
}

fn coalesce_storage_batches(table: Signal, batches: &[&RecordBatch]) -> Result<RecordBatch> {
    match batches {
        [] => anyhow::bail!("cannot coalesce empty {table} storage batch group"),
        [batch] => Ok((*batch).clone()),
        [first, ..] => {
            let schema = first.schema();
            concat_batches(&schema, batches.iter().copied())
                .with_context(|| format!("coalesce {table} storage batches"))
        }
    }
}

fn seal_immutable_buffer(
    storage_dir: &Path,
    table: Signal,
    buffer: &ImmutableSegmentBuffer,
) -> Result<SealedBuffer> {
    let coalesce_started = Instant::now();
    let batch = buffer.record_batch(table)?;
    let mut timings = vec![ArrowBatchInsertTiming {
        table,
        phase: TimingPhase::ImmutableCoalesce,
        rows: buffer.rows,
        seconds: coalesce_started.elapsed().as_secs_f64(),
    }];
    let started = Instant::now();
    let partitions = split_batch_by_immutable_partition(&batch)?;
    timings.push(ArrowBatchInsertTiming {
        table,
        phase: TimingPhase::PartitionSplit,
        rows: buffer.rows,
        seconds: started.elapsed().as_secs_f64(),
    });
    let mut segments = Vec::with_capacity(partitions.len());
    for (partition, batch) in partitions {
        let write = write_immutable_segment(storage_dir, table, partition, &batch)?;
        timings.extend(write.timings);
        segments.push(write.segment);
    }
    timings.push(ArrowBatchInsertTiming {
        table,
        phase: TimingPhase::ParquetWrite,
        rows: buffer.rows,
        seconds: started.elapsed().as_secs_f64(),
    });

    Ok(SealedBuffer {
        segments,
        timings,
        affected_days: buffer.timestamp_days.clone(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow58::array::{Int64Array, TimestampMicrosecondArray};
    use arrow58::datatypes::{DataType, Field, Schema, TimeUnit};
    use std::sync::Arc;
    use tempfile::tempdir;

    #[test]
    fn coalesce_storage_batches_concats_rows() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )]));
        let first =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![1, 2]))])
                .unwrap();
        let second =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![3]))]).unwrap();

        let coalesced = coalesce_storage_batches(Signal::MetricGauge, &[&first, &second]).unwrap();

        assert_eq!(coalesced.num_rows(), 3);
    }

    #[test]
    fn seal_immutable_buffer_writes_segments_and_tracks_days() {
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
        let mut buffer = ImmutableSegmentBuffer::new(Instant::now());
        buffer.push(PreparedArrowBatch {
            table: Signal::MetricGauge,
            batch,
            rows: 2,
            timestamp_days: vec!["1970-01-01".to_string()],
        });

        let sealed = seal_immutable_buffer(dir.path(), Signal::MetricGauge, &buffer).unwrap();

        assert_eq!(sealed.segments.len(), 1);
        assert_eq!(sealed.segments[0].table, Signal::MetricGauge);
        assert_eq!(sealed.segments[0].rows, 2);
        assert!(sealed.segments[0].path.exists());
        assert!(sealed.affected_days.contains("1970-01-01"));

        let phases = sealed
            .timings
            .iter()
            .map(|timing| timing.phase)
            .collect::<Vec<_>>();
        assert_eq!(
            phases,
            vec![
                TimingPhase::ImmutableCoalesce,
                TimingPhase::PartitionSplit,
                TimingPhase::ParquetEncode,
                TimingPhase::FileWrite,
                TimingPhase::FileFsync,
                TimingPhase::FileRename,
                TimingPhase::ParquetWrite,
            ]
        );
    }
}
