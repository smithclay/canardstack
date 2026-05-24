use super::arrow::{batch_timestamp_days, storage_duckdb_batch};
use super::arrow_write::{
    arrow_write_buffer_snapshot, size_or_age_due, ArrowFlushOutcome, ArrowFlushResult,
    ArrowWriteBuffer,
};
use super::ducklake::configure_write_connection;
use super::{
    ArrowBatchBufferResult, ArrowBatchBufferTiming, ArrowWriteBufferFreshness,
    ArrowWriteBufferMetric, CommittedReplayRefs, InternalTelemetryCommitResult, PreparedArrowBatch,
    ReplayBackedArrowBatch, Storage, StorageSignal, TimingPhase,
};
use crate::LockExt;
use anyhow::{Context, Result};
use arrow58::compute::concat_batches;
use arrow58::record_batch::RecordBatch;
use duckdb::Connection;
use std::collections::{BTreeMap, BTreeSet};
use std::time::Instant;

impl Storage {
    /// The single size/age flush-threshold predicate, exposed so the
    /// `SealDriver` seal-cadence decision delegates to one definition. See
    /// [`size_or_age_due`].
    pub(crate) fn size_or_age_due(
        bytes: usize,
        age_seconds: f64,
        target_bytes: usize,
        max_age_seconds: f64,
    ) -> bool {
        size_or_age_due(bytes, age_seconds, target_bytes, max_age_seconds)
    }

    pub fn arrow_write_buffer_metrics(&self) -> Vec<ArrowWriteBufferMetric> {
        let buffers = self.arrow_write_buffers.lock_or_poisoned();
        metrics_from_buffers(&buffers)
    }

    /// Folded Arrow write-buffer aggregates the ingest admission freshness
    /// projection consumes, read under one lock without allocating the
    /// per-signal metric vec. This is the dedicated ingest hot-path accessor: the
    /// request path needs only the three scalars [`FreshnessBudgetInputs`] takes,
    /// so it asks for exactly those instead of piggybacking on
    /// [`Self::arrow_write_buffer_metrics`] (which builds a per-signal vec for the
    /// scheduler/admin detail paths). Equivalent to folding that vec.
    pub fn arrow_write_buffer_freshness(&self) -> ArrowWriteBufferFreshness {
        let buffers = self.arrow_write_buffers.lock_or_poisoned();
        freshness_from_buffers(&buffers)
    }

    /// Commit a sanctioned internal operator-metrics batch directly to DuckLake.
    ///
    /// This is deliberately not an Arrow write-buffer entry point: external OTLP
    /// ingest reaches the buffer only through replay-backed batches, while internal
    /// self-telemetry has no raw-spool record to checkpoint and commits on its own
    /// clearly named path.
    pub(crate) fn commit_operator_metrics_snapshot(
        &self,
        storage_signal: StorageSignal,
        batch: &RecordBatch,
        source_format: &str,
    ) -> Result<InternalTelemetryCommitResult> {
        self.commit_internal_telemetry_batch(storage_signal, batch, source_format)
    }

    /// Direct internal telemetry commit helper for in-repo tests and benches.
    /// Production operator metrics route through
    /// [`Storage::commit_operator_metrics_snapshot`]. This helper does not touch
    /// the ingest write buffer and must not be used for external OTLP ingest.
    #[doc(hidden)]
    pub fn commit_internal_telemetry_batch(
        &self,
        storage_signal: StorageSignal,
        batch: &RecordBatch,
        source_format: &str,
    ) -> Result<InternalTelemetryCommitResult> {
        self.commit_internal_telemetry_records(storage_signal, batch, source_format)
    }

    pub(crate) fn buffer_replay_backed_arrow_batches(
        &self,
        batches: &[ReplayBackedArrowBatch<'_>],
    ) -> Result<ArrowBatchBufferResult> {
        let mut prepared = Vec::new();
        let mut prepare_timings = Vec::new();
        let mut attempted_rows = 0;
        let mut grouped =
            BTreeMap::<(StorageSignal, &str), (Vec<&RecordBatch>, BTreeSet<_>)>::new();
        for batch in batches {
            let storage_signal = batch.storage_signal;
            let record_batch = batch.batch;
            if record_batch.num_rows() == 0 {
                continue;
            }
            let rows = record_batch.num_rows();
            attempted_rows += rows;
            let (batch_refs, replay_refs) = grouped
                .entry((storage_signal, batch.source_format))
                .or_default();
            replay_refs.insert(batch.replay_ref);
            batch_refs.push(record_batch);
        }
        for ((storage_signal, source_format), (batches, replay_refs)) in grouped {
            let rows = batches.iter().map(|batch| batch.num_rows()).sum();
            let coalesce_started = Instant::now();
            let batch = coalesce_storage_batches(storage_signal, &batches)?;
            prepare_timings.push(ArrowBatchBufferTiming {
                storage_signal,
                phase: TimingPhase::Coalesce,
                rows,
                seconds: coalesce_started.elapsed().as_secs_f64(),
            });
            let prepare_started = Instant::now();
            let prepared_batch = storage_duckdb_batch(storage_signal, &batch, source_format)?;
            let timestamp_days = batch_timestamp_days(&prepared_batch)?;
            let prepare_seconds = prepare_started.elapsed().as_secs_f64();
            prepared.push(PreparedArrowBatch {
                storage_signal,
                batch: prepared_batch,
                rows,
                timestamp_days,
                replay_refs,
            });
            prepare_timings.push(ArrowBatchBufferTiming {
                storage_signal,
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

    fn commit_internal_telemetry_records(
        &self,
        storage_signal: StorageSignal,
        batch: &RecordBatch,
        source_format: &str,
    ) -> Result<InternalTelemetryCommitResult> {
        if batch.num_rows() == 0 {
            return Ok(InternalTelemetryCommitResult {
                rows: 0,
                timings: Vec::new(),
            });
        }

        let rows = batch.num_rows();
        let prepare_started = Instant::now();
        let prepared = storage_duckdb_batch(storage_signal, batch, source_format)?;
        let timestamp_days = batch_timestamp_days(&prepared)?;
        let mut timings = vec![ArrowBatchBufferTiming {
            storage_signal,
            phase: TimingPhase::Prepare,
            rows,
            seconds: prepare_started.elapsed().as_secs_f64(),
        }];

        let writer_lock_wait_started = Instant::now();
        let conn = self.writer.lock_or_poisoned();
        timings.push(ArrowBatchBufferTiming {
            storage_signal,
            phase: TimingPhase::WriterLockWait,
            rows,
            seconds: writer_lock_wait_started.elapsed().as_secs_f64(),
        });
        configure_write_connection(&conn, &self.write_memory_limit)?;

        conn.execute_batch("BEGIN TRANSACTION;")?;
        let result = (|| -> Result<()> {
            let append_started = Instant::now();
            append_record_batch_to_ducklake(&conn, &self.catalog_name, storage_signal, prepared)?;
            timings.push(ArrowBatchBufferTiming {
                storage_signal,
                phase: TimingPhase::DuckdbArrowAppend,
                rows,
                seconds: append_started.elapsed().as_secs_f64(),
            });

            let commit_started = Instant::now();
            conn.execute_batch("COMMIT;")?;
            timings.push(ArrowBatchBufferTiming {
                storage_signal,
                phase: TimingPhase::DucklakeCommit,
                rows,
                seconds: commit_started.elapsed().as_secs_f64(),
            });
            Ok(())
        })();

        if let Err(err) = result {
            let _ = conn.execute_batch("ROLLBACK;");
            return Err(err);
        }

        *self.last_error.lock_or_poisoned() = None;
        self.mark_metadata_dirty(BTreeMap::from([(
            storage_signal,
            timestamp_days.into_iter().collect(),
        )]));
        Ok(InternalTelemetryCommitResult { rows, timings })
    }

    fn buffer_arrow_write_batches(
        &self,
        prepared: Vec<PreparedArrowBatch>,
        prepare_timings: Vec<ArrowBatchBufferTiming>,
        attempted_rows: usize,
    ) -> Result<ArrowBatchBufferResult> {
        let error_table = prepared
            .first()
            .map(|batch| batch.storage_signal)
            .unwrap_or(StorageSignal::Logs);
        let mut timings = prepare_timings;

        {
            let mut buffers = self.arrow_write_buffers.lock_or_poisoned();
            let started = Instant::now();
            for batch in prepared {
                buffers
                    .entry(batch.storage_signal)
                    .or_insert_with(|| ArrowWriteBuffer::new(started))
                    .push(batch);
            }
            timings.push(ArrowBatchBufferTiming {
                storage_signal: error_table,
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

    /// Commit all buffered rows to DuckLake. Every storage signal with a
    /// non-empty Arrow write buffer is detached, coalesced into one
    /// `RecordBatch`, appended through the DuckDB Arrow appender, and COMMITted in
    /// the DuckLake transaction. This is the durable-commit verb on the seal path:
    /// the COMMIT here is what makes raw-spool checkpointing legal, distinct from
    /// the in-memory `appender.flush()` drain that runs inside the transaction
    /// before it (see [`append_record_batch_to_ducklake`]). The returned
    /// [`ArrowFlushOutcome`] carries the committed snapshot's replay refs as a
    /// capability token so the caller ([`crate::seal`]) can checkpoint exactly
    /// those raw-spool records afterward. SealDriver owns when; this method
    /// commits what is buffered now.
    pub(crate) fn commit_arrow_write_buffer(&self) -> Result<ArrowFlushOutcome> {
        let to_commit = {
            let mut buffers = self.arrow_write_buffers.lock_or_poisoned();
            if buffers.is_empty() {
                return Ok(ArrowFlushOutcome {
                    flushed_rows: 0,
                    flushed_buffers: 0,
                    timings: Vec::new(),
                    active_write_buffers: arrow_write_buffer_snapshot(&buffers),
                    replay_backed_records: 0,
                    committed_replay_refs: CommittedReplayRefs::empty(),
                });
            }
            std::mem::take(&mut *buffers)
        };
        let commit_result = match self.commit_arrow_write_buffers(&to_commit) {
            Ok(result) => result,
            Err(err) => {
                self.restore_arrow_write_buffers(to_commit);
                return Err(err);
            }
        };
        let active_write_buffers =
            arrow_write_buffer_snapshot(&self.arrow_write_buffers.lock_or_poisoned());
        *self.last_error.lock_or_poisoned() = None;
        self.mark_metadata_dirty(commit_result.affected);

        Ok(ArrowFlushOutcome {
            flushed_rows: commit_result.rows,
            flushed_buffers: commit_result.buffers,
            timings: commit_result.timings,
            active_write_buffers,
            replay_backed_records: commit_result.committed_replay_refs.len(),
            committed_replay_refs: commit_result.committed_replay_refs,
        })
    }

    fn commit_arrow_write_buffers(
        &self,
        buffers: &BTreeMap<StorageSignal, ArrowWriteBuffer>,
    ) -> Result<ArrowFlushResult> {
        let rows = buffers.values().map(|buffer| buffer.rows).sum();
        let replay_refs = buffers
            .values()
            .flat_map(|buffer| buffer.replay_refs.iter().copied())
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        let affected = buffers
            .iter()
            .map(|(&storage_signal, buffer)| (storage_signal, buffer.timestamp_days.clone()))
            .collect::<BTreeMap<_, _>>();
        // Time the wait to acquire the single writer connection so writer
        // contention (seal vs. metadata-refresh on the same lock) is visible.
        let writer_lock_wait_started = Instant::now();
        let conn = self.writer.lock_or_poisoned();
        let writer_lock_wait_seconds = writer_lock_wait_started.elapsed().as_secs_f64();
        configure_write_connection(&conn, &self.write_memory_limit)?;

        let mut timings = distributed_buffer_timing(
            TimingPhase::WriterLockWait,
            buffers,
            writer_lock_wait_seconds,
        );
        append_buffers_to_ducklake(&conn, &self.catalog_name, buffers, &mut timings).with_context(
            || "DuckDB Arrow appender flush failed; no fallback write path is enabled",
        )?;

        Ok(ArrowFlushResult {
            rows,
            buffers: buffers.len(),
            timings,
            affected,
            committed_replay_refs: CommittedReplayRefs::new(replay_refs),
        })
    }

    fn restore_arrow_write_buffers(&self, detached: BTreeMap<StorageSignal, ArrowWriteBuffer>) {
        let mut buffers = self.arrow_write_buffers.lock_or_poisoned();
        for (storage_signal, mut detached_buffer) in detached {
            if let Some(current) = buffers.remove(&storage_signal) {
                detached_buffer.append_buffer(current);
            }
            buffers.insert(storage_signal, detached_buffer);
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
        for (&storage_signal, buffer) in buffers {
            let coalesce_started = Instant::now();
            let batch = buffer.record_batch(storage_signal)?;
            timings.push(ArrowBatchBufferTiming {
                storage_signal,
                phase: TimingPhase::ArrowWriteCoalesce,
                rows: buffer.rows,
                seconds: coalesce_started.elapsed().as_secs_f64(),
            });

            let append_started = Instant::now();
            append_record_batch_to_ducklake(conn, catalog_name, storage_signal, batch)?;
            timings.push(ArrowBatchBufferTiming {
                storage_signal,
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
    storage_signal: StorageSignal,
    batch: RecordBatch,
) -> Result<()> {
    let mut appender = conn
        .appender_to_catalog_and_db(storage_signal.as_str(), catalog_name, "main")
        .with_context(|| {
            format!("open DuckDB Arrow appender for {catalog_name}.main.{storage_signal}")
        })?;
    appender.append_record_batch(batch).with_context(|| {
        format!("append Arrow RecordBatch to {catalog_name}.main.{storage_signal}")
    })?;
    // In-memory appender drain into the OPEN transaction — NOT the durable commit.
    // The COMMIT in `append_buffers_to_ducklake` is what makes the rows durable and
    // raw-spool checkpointing legal; this only flushes the appender's buffer.
    appender.flush().with_context(|| {
        format!("drain DuckDB Arrow appender for {catalog_name}.main.{storage_signal}")
    })?;
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
        .map(|(&storage_signal, buffer)| {
            let buffer_seconds = if total_rows == 0 {
                seconds / buffers.len() as f64
            } else {
                seconds * buffer.rows as f64 / total_rows as f64
            };
            ArrowBatchBufferTiming {
                storage_signal,
                phase,
                rows: buffer.rows,
                seconds: buffer_seconds,
            }
        })
        .collect()
}

/// Per-signal Arrow write-buffer metrics: the detail view the scheduler/admin
/// paths consume. [`Storage::arrow_write_buffer_freshness`] folds the same buffer
/// state into just the scalars the ingest hot path needs;
/// [`freshness_from_buffers`] MUST stay equal to folding this vec, which the
/// `freshness_matches_folded_metrics` test pins so the two accessors cannot drift.
fn metrics_from_buffers(
    buffers: &BTreeMap<StorageSignal, ArrowWriteBuffer>,
) -> Vec<ArrowWriteBufferMetric> {
    buffers
        .iter()
        .map(|(storage_signal, buffer)| ArrowWriteBufferMetric {
            storage_signal: *storage_signal,
            rows: buffer.rows,
            bytes: buffer.bytes,
            age_seconds: buffer.opened_at.elapsed().as_secs_f64(),
        })
        .collect()
}

/// Folded Arrow write-buffer freshness scalars for the ingest admission
/// projection: sum of buffered bytes, count of active (non-empty) buffers, and
/// the oldest buffer age. Equivalent to folding [`metrics_from_buffers`]; the
/// equality is pinned by `freshness_matches_folded_metrics` so this hot-path fold
/// cannot silently diverge from the per-signal detail accessor.
fn freshness_from_buffers(
    buffers: &BTreeMap<StorageSignal, ArrowWriteBuffer>,
) -> ArrowWriteBufferFreshness {
    let mut freshness = ArrowWriteBufferFreshness::default();
    for buffer in buffers.values() {
        freshness.buffered_bytes = freshness.buffered_bytes.saturating_add(buffer.bytes);
        freshness.buffered_active_count += usize::from(buffer.bytes > 0);
        freshness.oldest_buffer_age_seconds = freshness
            .oldest_buffer_age_seconds
            .max(buffer.opened_at.elapsed().as_secs_f64());
    }
    freshness
}

fn coalesce_storage_batches(
    storage_signal: StorageSignal,
    batches: &[&RecordBatch],
) -> Result<RecordBatch> {
    match batches {
        [] => anyhow::bail!("cannot coalesce empty {storage_signal} storage batch group"),
        [batch] => Ok((*batch).clone()),
        [first, ..] => {
            let schema = first.schema();
            concat_batches(&schema, batches.iter().copied())
                .with_context(|| format!("coalesce {storage_signal} storage batches"))
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
    fn freshness_matches_folded_metrics() {
        use std::time::{Duration, Instant};

        let now = Instant::now();
        let mut buffers = BTreeMap::new();
        let mut logs = ArrowWriteBuffer::new(now - Duration::from_secs(3));
        logs.rows = 10;
        logs.bytes = 100;
        let mut spans = ArrowWriteBuffer::new(now - Duration::from_secs(1));
        spans.rows = 5;
        spans.bytes = 40;
        // An opened-but-empty buffer must not count toward the active total.
        let empty = ArrowWriteBuffer::new(now);
        buffers.insert(StorageSignal::Logs, logs);
        buffers.insert(StorageSignal::Spans, spans);
        buffers.insert(StorageSignal::MetricGauge, empty);

        let freshness = freshness_from_buffers(&buffers);
        let metrics = metrics_from_buffers(&buffers);

        let folded_bytes: usize = metrics.iter().map(|m| m.bytes).sum();
        let folded_active = metrics.iter().filter(|m| m.bytes > 0).count();
        let folded_oldest = metrics
            .iter()
            .map(|m| m.age_seconds)
            .fold(0.0_f64, f64::max);

        assert_eq!(freshness.buffered_bytes, folded_bytes);
        assert_eq!(freshness.buffered_active_count, folded_active);
        // Both folds call `opened_at.elapsed()` independently, so allow a small
        // wall-clock skew between the two reads of the oldest age.
        assert!(
            (freshness.oldest_buffer_age_seconds - folded_oldest).abs() < 0.25,
            "freshness oldest {} vs folded {}",
            freshness.oldest_buffer_age_seconds,
            folded_oldest
        );
    }

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
