use super::queue::{self, QueueKey, QueuedBatch};
use super::{Ingestor, Signal};
use crate::metrics::Metrics;
use crate::storage::{ArrowBatchInsert, Storage};
use crate::LockExt;
use std::collections::HashMap;
use std::time::Instant;

/// Flush failed mid-batch. `committed_rows` were accepted by the catalog
/// before the failure; remote object-storage durability is the lake's
/// responsibility, not this process's.
#[derive(Debug)]
pub struct PartialFlushError {
    pub committed_rows: usize,
    pub signal: Signal,
    pub source: anyhow::Error,
}

impl std::fmt::Display for PartialFlushError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} (signal={}, {} row(s) accepted by catalog before failure)",
            self.source, self.signal, self.committed_rows
        )
    }
}

impl std::error::Error for PartialFlushError {}

impl Ingestor {
    pub fn flush_all(&self, storage: &Storage) -> anyhow::Result<usize> {
        self.flush_all_observed(storage, None)
    }

    pub(crate) fn flush_all_with_metrics(
        &self,
        storage: &Storage,
        metrics: Option<&Metrics>,
    ) -> anyhow::Result<usize> {
        self.flush_all_observed(storage, metrics)
    }

    fn flush_all_observed(
        &self,
        storage: &Storage,
        metrics: Option<&Metrics>,
    ) -> anyhow::Result<usize> {
        let _guard = self.flush_lock.lock_or_poisoned();
        let mut total = 0;
        for signal in [Signal::Logs, Signal::Spans] {
            total += self.flush_signal_observed(signal, storage, metrics)?;
        }
        total += self
            .flush_metric_pair_observed(storage, metrics)?
            .into_values()
            .sum::<usize>();
        Ok(total)
    }

    pub fn flush_due(
        &self,
        storage: &Storage,
        metrics: Option<&Metrics>,
    ) -> anyhow::Result<HashMap<Signal, usize>> {
        let due_started = Instant::now();
        let due = {
            let queues = self.queues.lock_or_poisoned();
            queue::due_keys(&queues, &self.config)
        };
        if let Some(metrics) = metrics {
            metrics.observe_phase_seconds(
                "all",
                "flush_due_scan",
                None,
                due_started.elapsed().as_secs_f64(),
            );
        }
        observe_due_keys(metrics, &due);
        let lock_started = Instant::now();
        let _guard = self.flush_lock.lock_or_poisoned();
        if let Some(metrics) = metrics {
            metrics.observe_phase_seconds(
                "all",
                "flush_lock_wait",
                None,
                lock_started.elapsed().as_secs_f64(),
            );
        }
        let hold_started = Instant::now();
        let rows_by_signal = self.flush_due_keys_observed(due, storage, metrics);
        if let Some(metrics) = metrics {
            metrics.observe_phase_seconds(
                "all",
                "flush_lock_hold",
                None,
                hold_started.elapsed().as_secs_f64(),
            );
        }
        let mut flushed = HashMap::new();
        for (signal, rows) in rows_by_signal? {
            if rows > 0 {
                flushed.insert(signal, rows);
            }
        }
        Ok(flushed)
    }

    pub fn flush_aged(&self, storage: &Storage) -> anyhow::Result<HashMap<Signal, usize>> {
        self.flush_due(storage, None)
    }

    pub fn flush_signal(&self, signal: Signal, storage: &Storage) -> anyhow::Result<usize> {
        let _guard = self.flush_lock.lock_or_poisoned();
        self.flush_signal_observed(signal, storage, None)
    }

    fn flush_signal_observed(
        &self,
        signal: Signal,
        storage: &Storage,
        metrics: Option<&Metrics>,
    ) -> anyhow::Result<usize> {
        let sets = self.drain_flush_batches_for_signal(signal);
        self.insert_drained_batches_observed(sets, storage, metrics)
            .map(|rows| rows.get(&signal).copied().unwrap_or(0))
    }

    fn flush_metric_pair_observed(
        &self,
        storage: &Storage,
        metrics: Option<&Metrics>,
    ) -> anyhow::Result<HashMap<Signal, usize>> {
        let mut batches = self.drain_flush_batches_for_signal(Signal::MetricGauge);
        batches.extend(self.drain_flush_batches_for_signal(Signal::MetricSum));
        self.insert_drained_batches_observed(batches, storage, metrics)
    }

    fn flush_due_keys_observed(
        &self,
        due: Vec<QueueKey>,
        storage: &Storage,
        metrics: Option<&Metrics>,
    ) -> anyhow::Result<HashMap<Signal, usize>> {
        let drain_started = Instant::now();
        let mut sets = Vec::new();
        let mut metric_partitions = Vec::new();
        for key in due {
            if key.signal.is_metric() {
                if !metric_partitions.contains(&key.partition) {
                    metric_partitions.push(key.partition);
                }
                continue;
            }
            sets.push((key, self.drain_flush_batches(key)));
        }
        for partition in metric_partitions {
            for signal in [Signal::MetricGauge, Signal::MetricSum] {
                let key = QueueKey { signal, partition };
                sets.push((key, self.drain_flush_batches(key)));
            }
        }
        if let Some(metrics) = metrics {
            metrics.observe_phase_seconds(
                "all",
                "flush_drain",
                None,
                drain_started.elapsed().as_secs_f64(),
            );
        }
        self.insert_drained_batches_observed(sets, storage, metrics)
    }

    fn insert_drained_batches_observed(
        &self,
        sets: Vec<(QueueKey, Vec<QueuedBatch>)>,
        storage: &Storage,
        metrics: Option<&Metrics>,
    ) -> anyhow::Result<HashMap<Signal, usize>> {
        if sets.iter().all(|(_, batches)| batches.is_empty()) {
            return Ok(HashMap::new());
        }
        if let Some(metrics) = metrics {
            let metrics_started = Instant::now();
            for (key, batches) in &sets {
                let attempted_rows: usize = batches.iter().map(QueuedBatch::len).sum();
                let attempted_bytes: usize = batches.iter().map(|batch| batch.approx_bytes).sum();
                let attempted_batches = batches.len();
                if attempted_rows == 0 {
                    continue;
                }
                metrics.inc(
                    "canardstack_ingest_flush_attempts_total",
                    &[("signal", key.signal.as_str())],
                    1,
                );
                metrics.inc(
                    "canardstack_ingest_flush_attempted_rows_total",
                    &[("signal", key.signal.as_str())],
                    attempted_rows as u64,
                );
                metrics.inc(
                    "canardstack_ingest_flush_attempted_bytes_total",
                    &[("signal", key.signal.as_str())],
                    attempted_bytes as u64,
                );
                metrics.inc(
                    "canardstack_ingest_flush_drained_batches_total",
                    &[("signal", key.signal.as_str())],
                    attempted_batches as u64,
                );
                metrics.inc(
                    "canardstack_ingest_flush_drained_rows_total",
                    &[("signal", key.signal.as_str())],
                    attempted_rows as u64,
                );
                metrics.inc(
                    "canardstack_ingest_flush_drained_bytes_total",
                    &[("signal", key.signal.as_str())],
                    attempted_bytes as u64,
                );
            }
            metrics.observe_phase_seconds(
                "all",
                "flush_attempt_metrics",
                None,
                metrics_started.elapsed().as_secs_f64(),
            );
        }
        if let Some(metrics) = metrics {
            let metrics_started = Instant::now();
            let mut by_signal = HashMap::<Signal, (u64, u64, u64)>::new();
            for (key, batches) in &sets {
                for batch in batches {
                    if batch.len() == 0 {
                        continue;
                    }
                    let (batch_count, rows, bytes) = by_signal.entry(key.signal).or_default();
                    *batch_count = batch_count.saturating_add(1);
                    *rows = rows.saturating_add(batch.batch.num_rows() as u64);
                    *bytes = bytes.saturating_add(batch.batch.get_array_memory_size() as u64);
                }
            }
            for (signal, (batch_count, rows, bytes)) in by_signal {
                metrics.inc(
                    "canardstack_ingest_flush_coalesced_batches_total",
                    &[("signal", signal.as_str())],
                    batch_count,
                );
                metrics.inc(
                    "canardstack_ingest_flush_coalesced_rows_total",
                    &[("signal", signal.as_str())],
                    rows,
                );
                metrics.inc(
                    "canardstack_ingest_flush_coalesced_bytes_total",
                    &[("signal", signal.as_str())],
                    bytes,
                );
            }
            metrics.observe_phase_seconds(
                "all",
                "flush_batch_metrics",
                None,
                metrics_started.elapsed().as_secs_f64(),
            );
        }
        let insert_result = {
            let insert_prepare_started = Instant::now();
            let inserts =
                sets.iter()
                    .flat_map(|(key, batches)| {
                        batches.iter().filter(|batch| batch.len() > 0).map(|batch| {
                            ArrowBatchInsert {
                                table: key.signal,
                                batch: &batch.batch,
                                source_format: batch.source_format,
                            }
                        })
                    })
                    .collect::<Vec<_>>();
            if let Some(metrics) = metrics {
                metrics.observe_phase_seconds(
                    "all",
                    "flush_insert_prepare",
                    None,
                    insert_prepare_started.elapsed().as_secs_f64(),
                );
            }
            let insert_started = Instant::now();
            let result = storage.insert_arrow_batches(&inserts);
            if let Some(metrics) = metrics {
                metrics.observe_phase_seconds(
                    "all",
                    "flush_storage_insert",
                    None,
                    insert_started.elapsed().as_secs_f64(),
                );
            }
            result
        };
        match insert_result {
            Ok(result) => {
                let mut rows = HashMap::new();
                let mut success_metrics = metrics.map(|_| HashMap::<Signal, (u64, u64)>::new());
                for (key, batches) in &sets {
                    for batch in batches {
                        let batch_rows = batch.batch.num_rows();
                        if batch_rows == 0 {
                            continue;
                        }
                        *rows.entry(key.signal).or_default() += batch_rows;
                        if metrics.is_some() {
                            let batch_bytes = batch.batch.get_array_memory_size();
                            if let Some(success_metrics) = &mut success_metrics {
                                let (rows, bytes) = success_metrics.entry(key.signal).or_default();
                                *rows = rows.saturating_add(batch_rows as u64);
                                *bytes = bytes.saturating_add(batch_bytes as u64);
                            }
                        }
                    }
                }
                if let (Some(metrics), Some(success_metrics)) = (metrics, success_metrics) {
                    let metrics_started = Instant::now();
                    for (signal, (rows, bytes)) in success_metrics {
                        metrics.inc(
                            "canardstack_ingest_flush_rows_total",
                            &[("signal", signal.as_str())],
                            rows,
                        );
                        metrics.inc(
                            "canardstack_ingest_flush_buffered_rows_total",
                            &[("signal", signal.as_str())],
                            rows,
                        );
                        metrics.inc(
                            "canardstack_ingest_flush_buffered_bytes_total",
                            &[("signal", signal.as_str())],
                            bytes,
                        );
                    }
                    metrics.observe_phase_seconds(
                        "all",
                        "flush_success_metrics",
                        None,
                        metrics_started.elapsed().as_secs_f64(),
                    );
                }
                for timing in result.timings {
                    if let Some(metrics) = metrics {
                        metrics.observe_phase_seconds(
                            timing.table.as_str(),
                            timing.phase.as_str(),
                            None,
                            timing.seconds,
                        );
                    }
                }
                let commit_started = Instant::now();
                self.release_queue_credits_for_batches(&sets);
                self.mark_raw_spool_batches_storage_committed(&sets, metrics)?;
                if let Some(metrics) = metrics {
                    metrics.observe_phase_seconds(
                        "all",
                        "flush_commit_accounting",
                        None,
                        commit_started.elapsed().as_secs_f64(),
                    );
                }
                Ok(rows)
            }
            Err(err) => {
                let failed_signal = sets
                    .iter()
                    .find(|(_, batches)| !batches.is_empty())
                    .map(|(key, _)| key.signal)
                    .unwrap_or(Signal::Logs);
                for (key, batches) in sets {
                    let restored_rows: usize = batches.iter().map(QueuedBatch::len).sum();
                    let restored_bytes: usize =
                        batches.iter().map(|batch| batch.approx_bytes).sum();
                    if restored_rows == 0 {
                        continue;
                    }
                    if let Some(metrics) = metrics {
                        metrics.inc(
                            "canardstack_ingest_flush_failures_total",
                            &[
                                ("signal", key.signal.as_str()),
                                ("reason", flush_failure_reason(&err)),
                            ],
                            1,
                        );
                        metrics.inc(
                            "canardstack_ingest_flush_restored_rows_total",
                            &[("signal", key.signal.as_str())],
                            restored_rows as u64,
                        );
                    }
                    tracing::error!(
                        event = "ingest_flush_failed",
                        signal = key.signal.as_str(),
                        partition = key.partition.as_str(),
                        committed_rows = 0,
                        restored_rows,
                        restored_bytes,
                        reason = flush_failure_reason(&err),
                        error = %err
                    );
                    self.restore_batches(key, batches);
                }
                Err(anyhow::Error::new(PartialFlushError {
                    committed_rows: 0,
                    signal: failed_signal,
                    source: err,
                }))
            }
        }
    }

    fn drain_flush_batches_for_signal(&self, signal: Signal) -> Vec<(QueueKey, Vec<QueuedBatch>)> {
        let keys = {
            let queues = self.queues.lock_or_poisoned();
            queue::drain_keys_for_signal(&queues, signal)
        };
        keys.into_iter()
            .map(|key| (key, self.drain_flush_batches(key)))
            .collect()
    }

    fn drain_flush_batches(&self, key: QueueKey) -> Vec<QueuedBatch> {
        let mut queues = self.queues.lock_or_poisoned();
        queue::drain_flush_batches(&mut queues, key, &self.config)
    }

    fn restore_batches(&self, key: QueueKey, batches: Vec<QueuedBatch>) {
        let mut queues = self.queues.lock_or_poisoned();
        queue::restore_batches(&mut queues, key, batches);
    }
}

fn flush_failure_reason(err: &anyhow::Error) -> &'static str {
    let lower = err.to_string().to_ascii_lowercase();
    if lower.contains("failed to pin block")
        || lower.contains("memory limit")
        || lower.contains("out of memory")
    {
        "duckdb_memory_limit"
    } else if lower.contains("database is locked") || lower.contains("conflict") {
        "duckdb_lock_conflict"
    } else {
        "storage_error"
    }
}

fn observe_due_keys(metrics: Option<&Metrics>, due: &[QueueKey]) {
    let Some(metrics) = metrics else {
        return;
    };
    let mut by_signal: HashMap<Signal, u64> = HashMap::new();
    for key in due {
        *by_signal.entry(key.signal).or_default() += 1;
    }
    for signal in [
        Signal::Logs,
        Signal::Spans,
        Signal::MetricGauge,
        Signal::MetricSum,
    ] {
        let due_count = by_signal.get(&signal).copied().unwrap_or(0);
        metrics.gauge(
            "canardstack_ingest_flush_due_keys",
            &[("signal", signal.as_str())],
            due_count as f64,
        );
        if due_count > 0 {
            metrics.inc(
                "canardstack_ingest_flush_due_keys_total",
                &[("signal", signal.as_str())],
                due_count,
            );
        }
    }
}

/// Inspect an error returned by `flush_signal` / `flush_all` / `flush_aged` and
/// return (signal, committed_rows) when the failure was a partial commit. The
/// signal lets callers label metrics correctly even when the failure surfaces
/// far from the original ingest call (e.g. admin-triggered flush, scheduler).
pub fn partial_commit_info(err: &anyhow::Error) -> Option<(Signal, usize)> {
    err.downcast_ref::<PartialFlushError>()
        .map(|p| (p.signal, p.committed_rows))
}
