use super::arrow::{batch_timestamp_days, storage_duckdb_batch};
use super::ducklake::configure_write_connection;
use super::immutable::{
    distribute_commit_seconds, immutable_buffer_snapshot, immutable_timing_snapshot,
    register_ducklake_data_file, split_batch_by_immutable_partition, write_immutable_segment,
    ImmutableSealResult,
};
use super::{
    ArrowBatchInsert, ArrowBatchInsertResult, ArrowBatchInsertTiming, ImmutableSegmentBuffer,
    PreparedArrowBatch, Signal, Storage,
};
use crate::LockExt;
use anyhow::Result;
use arrow58::record_batch::RecordBatch;
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::time::Instant;

impl Storage {
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
        for batch in batches {
            if batch.batch.num_rows() == 0 {
                continue;
            }
            let rows = batch.batch.num_rows();
            let prepare_started = Instant::now();
            let prepared_batch =
                storage_duckdb_batch(batch.table, batch.batch, batch.source_format)?;
            let timestamp_days = batch_timestamp_days(&prepared_batch)?;
            let prepare_seconds = prepare_started.elapsed().as_secs_f64();
            attempted_rows += rows;
            prepared.push(PreparedArrowBatch {
                table: batch.table,
                batch: prepared_batch,
                rows,
                timestamp_days,
            });
            prepare_timings.push(ArrowBatchInsertTiming {
                table: batch.table,
                phase: "storage_prepare",
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
                phase: "storage_buffer",
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

    pub fn flush_immutable_segments(&self, force: bool) -> Result<Value> {
        let mut to_seal = BTreeMap::new();
        let no_seal_snapshot;
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
                no_seal_snapshot = immutable_buffer_snapshot(&buffers);
                return Ok(json!({
                    "supported": true,
                    "force": force,
                    "sealed_files": 0,
                    "sealed_rows": 0,
                    "timings": [],
                    "active_buffers": no_seal_snapshot,
                }));
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

        Ok(json!({
            "supported": true,
            "force": force,
            "sealed_files": seal_result.files,
            "sealed_rows": seal_result.rows,
            "timings": immutable_timing_snapshot(&seal_result.timings),
            "active_buffers": active_buffers,
        }))
    }

    fn seal_immutable_buffers(
        &self,
        buffers: &BTreeMap<Signal, ImmutableSegmentBuffer>,
    ) -> Result<ImmutableSealResult> {
        let mut timings = Vec::new();
        let mut sealed = Vec::with_capacity(buffers.len());
        let mut affected = BTreeMap::new();

        for (&table, buffer) in buffers {
            let batch = buffer.record_batch(table)?;
            let started = Instant::now();
            let segments = split_batch_by_immutable_partition(&batch)?
                .into_iter()
                .map(|(partition, batch)| {
                    write_immutable_segment(&self.local_storage_dir, table, partition, &batch)
                })
                .collect::<Result<Vec<_>>>()?;
            sealed.extend(segments);
            timings.push(ArrowBatchInsertTiming {
                table,
                phase: "storage_parquet_write",
                rows: buffer.rows,
                seconds: started.elapsed().as_secs_f64(),
            });
            affected.insert(table, buffer.timestamp_days.clone());
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
                timings.push(ArrowBatchInsertTiming {
                    table: segment.table,
                    phase: "storage_insert",
                    rows: segment.rows,
                    seconds: started.elapsed().as_secs_f64(),
                });
            }
            let commit_started = Instant::now();
            conn.execute_batch("COMMIT;")?;
            distribute_commit_seconds(&mut timings, commit_started.elapsed().as_secs_f64());
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
