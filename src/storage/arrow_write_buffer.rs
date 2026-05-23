use super::arrow::{batch_timestamp_days, storage_duckdb_batch};
use super::arrow_write::{
    arrow_write_buffer_snapshot, ArrowFlushOutcome, ArrowFlushResult, ArrowWriteBuffer,
};
use super::ducklake::configure_write_connection;
use super::{
    ArrowBatchBuffer, ArrowBatchBufferResult, ArrowBatchBufferTiming, ArrowWriteBufferMetric,
    PreparedArrowBatch, Storage, StorageSignal, TimingPhase,
};
use crate::LockExt;
use anyhow::{Context, Result};
use arrow58::compute::concat_batches;
use arrow58::record_batch::RecordBatch;
use duckdb::Connection;
use std::collections::BTreeMap;
use std::time::Instant;

impl Storage {
    pub fn arrow_write_buffer_metrics(&self) -> Vec<ArrowWriteBufferMetric> {
        let buffers = self.arrow_write_buffers.lock_or_poisoned();
        buffers
            .iter()
            .map(|(table, buffer)| ArrowWriteBufferMetric {
                table: *table,
                rows: buffer.rows,
                bytes: buffer.bytes,
                age_seconds: buffer.opened_at.elapsed().as_secs_f64(),
            })
            .collect()
    }

    pub fn buffer_arrow_records(
        &self,
        table: StorageSignal,
        batch: &RecordBatch,
        source_format: &str,
    ) -> Result<usize> {
        let result = self.buffer_arrow_batches(&[ArrowBatchBuffer {
            table,
            batch,
            source_format,
        }])?;
        Ok(result.rows)
    }

    pub fn buffer_arrow_batches(
        &self,
        batches: &[ArrowBatchBuffer<'_>],
    ) -> Result<ArrowBatchBufferResult> {
        let mut prepared = Vec::new();
        let mut prepare_timings = Vec::new();
        let mut attempted_rows = 0;
        let mut grouped = BTreeMap::<(StorageSignal, &str), Vec<&RecordBatch>>::new();
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
            prepare_timings.push(ArrowBatchBufferTiming {
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
            prepare_timings.push(ArrowBatchBufferTiming {
                table,
                phase: TimingPhase::Prepare,
                rows,
                seconds: prepare_seconds,
            });
        }

        if prepared.is_empty() {
            return Ok(ArrowBatchBufferResult {
                rows: 0,
                timings: Vec::new(),
            });
        }

        self.buffer_arrow_write_batches(prepared, prepare_timings, attempted_rows)
    }

    fn buffer_arrow_write_batches(
        &self,
        prepared: Vec<PreparedArrowBatch>,
        prepare_timings: Vec<ArrowBatchBufferTiming>,
        attempted_rows: usize,
    ) -> Result<ArrowBatchBufferResult> {
        if !self.ducklake_available {
            anyhow::bail!("Arrow write buffer ingest requires DuckLake storage");
        }

        let error_table = prepared
            .first()
            .map(|batch| batch.table)
            .unwrap_or(StorageSignal::Logs);
        let mut timings = prepare_timings;

        {
            let mut buffers = self.arrow_write_buffers.lock_or_poisoned();
            let started = Instant::now();
            for batch in prepared {
                buffers
                    .entry(batch.table)
                    .or_insert_with(|| ArrowWriteBuffer::new(started))
                    .push(batch);
            }
            timings.push(ArrowBatchBufferTiming {
                table: error_table,
                phase: TimingPhase::ArrowWriteBuffer,
                rows: attempted_rows,
                seconds: started.elapsed().as_secs_f64(),
            });
        }
        *self.last_error.lock_or_poisoned() = None;
        Ok(ArrowBatchBufferResult {
            rows: attempted_rows,
            timings,
        })
    }

    pub fn flush_arrow_write_buffer(&self, force: bool) -> Result<ArrowFlushOutcome> {
        let mut to_flush = BTreeMap::new();
        {
            let mut buffers = self.arrow_write_buffers.lock_or_poisoned();
            let now = Instant::now();
            let tables_to_flush = buffers
                .iter()
                .filter_map(|(table, buffer)| {
                    (force
                        || buffer.should_flush(
                            self.arrow_write_buffer_target_bytes,
                            self.arrow_write_buffer_max_age,
                            now,
                        ))
                    .then_some(*table)
                })
                .collect::<Vec<_>>();

            if tables_to_flush.is_empty() {
                return Ok(ArrowFlushOutcome {
                    force,
                    flushed_rows: 0,
                    flushed_buffers: 0,
                    timings: Vec::new(),
                    active_write_buffers: arrow_write_buffer_snapshot(&buffers),
                });
            }

            for table in tables_to_flush {
                if let Some(buffer) = buffers.remove(&table) {
                    to_flush.insert(table, buffer);
                }
            }
        }
        let flush_result = match self.flush_arrow_write_buffers(&to_flush) {
            Ok(result) => result,
            Err(err) => {
                self.restore_arrow_write_buffers(to_flush);
                return Err(err);
            }
        };
        let active_write_buffers =
            arrow_write_buffer_snapshot(&self.arrow_write_buffers.lock_or_poisoned());
        *self.last_error.lock_or_poisoned() = None;
        self.mark_metadata_dirty(flush_result.affected);

        Ok(ArrowFlushOutcome {
            force,
            flushed_rows: flush_result.rows,
            flushed_buffers: flush_result.buffers,
            timings: flush_result.timings,
            active_write_buffers,
        })
    }

    fn flush_arrow_write_buffers(
        &self,
        buffers: &BTreeMap<StorageSignal, ArrowWriteBuffer>,
    ) -> Result<ArrowFlushResult> {
        let rows = buffers.values().map(|buffer| buffer.rows).sum();
        let affected = buffers
            .iter()
            .map(|(&table, buffer)| (table, buffer.timestamp_days.clone()))
            .collect::<BTreeMap<_, _>>();
        let conn = self.writer.lock_or_poisoned();
        configure_write_connection(&conn, &self.write_memory_limit)?;

        let mut timings = Vec::new();
        append_buffers_to_ducklake(&conn, &self.catalog_name, buffers, &mut timings).with_context(
            || "DuckDB Arrow appender flush failed; no fallback write path is enabled",
        )?;

        Ok(ArrowFlushResult {
            rows,
            buffers: buffers.len(),
            timings,
            affected,
        })
    }

    fn restore_arrow_write_buffers(&self, detached: BTreeMap<StorageSignal, ArrowWriteBuffer>) {
        let mut buffers = self.arrow_write_buffers.lock_or_poisoned();
        for (table, mut detached_buffer) in detached {
            if let Some(current) = buffers.remove(&table) {
                detached_buffer.append_buffer(current);
            }
            buffers.insert(table, detached_buffer);
        }
    }
}

fn append_buffers_to_ducklake(
    conn: &Connection,
    catalog_name: &str,
    buffers: &BTreeMap<StorageSignal, ArrowWriteBuffer>,
    timings: &mut Vec<ArrowBatchBufferTiming>,
) -> Result<()> {
    conn.execute_batch("BEGIN TRANSACTION;")?;
    let result = (|| -> Result<()> {
        for (&table, buffer) in buffers {
            let coalesce_started = Instant::now();
            let batch = buffer.record_batch(table)?;
            timings.push(ArrowBatchBufferTiming {
                table,
                phase: TimingPhase::ArrowWriteCoalesce,
                rows: buffer.rows,
                seconds: coalesce_started.elapsed().as_secs_f64(),
            });

            let append_started = Instant::now();
            append_record_batch_to_ducklake(conn, catalog_name, table, batch)?;
            timings.push(ArrowBatchBufferTiming {
                table,
                phase: TimingPhase::DuckdbArrowAppend,
                rows: buffer.rows,
                seconds: append_started.elapsed().as_secs_f64(),
            });
        }

        let commit_started = Instant::now();
        conn.execute_batch("COMMIT;")?;
        let commit_seconds = commit_started.elapsed().as_secs_f64();
        timings.extend(distributed_buffer_timing(
            TimingPhase::DucklakeCommit,
            buffers,
            commit_seconds,
        ));
        Ok(())
    })();

    if let Err(err) = result {
        let _ = conn.execute_batch("ROLLBACK;");
        return Err(err);
    }
    Ok(())
}

fn append_record_batch_to_ducklake(
    conn: &Connection,
    catalog_name: &str,
    table: StorageSignal,
    batch: RecordBatch,
) -> Result<()> {
    let mut appender = conn
        .appender_to_catalog_and_db(table.as_str(), catalog_name, "main")
        .with_context(|| format!("open DuckDB Arrow appender for {catalog_name}.main.{table}"))?;
    appender
        .append_record_batch(batch)
        .with_context(|| format!("append Arrow RecordBatch to {catalog_name}.main.{table}"))?;
    appender
        .flush()
        .with_context(|| format!("flush DuckDB Arrow appender for {catalog_name}.main.{table}"))?;
    Ok(())
}

fn distributed_buffer_timing(
    phase: TimingPhase,
    buffers: &BTreeMap<StorageSignal, ArrowWriteBuffer>,
    seconds: f64,
) -> Vec<ArrowBatchBufferTiming> {
    if buffers.is_empty() {
        return Vec::new();
    }
    let total_rows = buffers.values().map(|buffer| buffer.rows).sum::<usize>();
    buffers
        .iter()
        .map(|(&table, buffer)| {
            let buffer_seconds = if total_rows == 0 {
                seconds / buffers.len() as f64
            } else {
                seconds * buffer.rows as f64 / total_rows as f64
            };
            ArrowBatchBufferTiming {
                table,
                phase,
                rows: buffer.rows,
                seconds: buffer_seconds,
            }
        })
        .collect()
}

fn coalesce_storage_batches(table: StorageSignal, batches: &[&RecordBatch]) -> Result<RecordBatch> {
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

#[cfg(test)]
mod tests {
    use super::*;
    use arrow58::array::Int64Array;
    use arrow58::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

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

        let coalesced =
            coalesce_storage_batches(StorageSignal::MetricGauge, &[&first, &second]).unwrap();

        assert_eq!(coalesced.num_rows(), 3);
    }
}
