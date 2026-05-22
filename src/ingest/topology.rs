use super::{queue, Ingestor, Signal, SpooledIngestWork};
use crate::metrics::Metrics;
use crate::storage::{ArrowBatchInsert, ArrowBatchInsertTiming, ImmutableFlushOutcome, Storage};
use crate::LockExt;
use anyhow::{Context, Result};
use std::collections::HashMap;
use std::sync::mpsc::{self, RecvTimeoutError, SyncSender};
use std::sync::{Arc, Weak};
use std::thread::{self, JoinHandle};
use std::time::Instant;

pub(super) struct IngestTopologyDispatcher {
    pub(super) commands: Vec<SyncSender<SpooledIngestWork>>,
    pub(super) handles: Vec<JoinHandle<()>>,
    pub(super) next_worker: usize,
}

pub(super) struct StorageSinkDispatcher {
    pub(super) commands: Option<SyncSender<StorageSinkWork>>,
    pub(super) handle: Option<JoinHandle<()>>,
}

pub(super) struct StorageSinkWork {
    pub(super) request_signal: Signal,
    pub(super) sets: Vec<(queue::QueueKey, Vec<queue::QueuedBatch>)>,
}

impl Drop for IngestTopologyDispatcher {
    fn drop(&mut self) {
        self.commands.clear();
        for handle in self.handles.drain(..) {
            if handle.thread().id() != thread::current().id() {
                let _ = handle.join();
            }
        }
    }
}

impl Drop for StorageSinkDispatcher {
    fn drop(&mut self) {
        self.commands.take();
        if let Some(handle) = self.handle.take() {
            if handle.thread().id() != thread::current().id() {
                let _ = handle.join();
            }
        }
    }
}

impl Ingestor {
    pub fn start_topology(
        self: &Arc<Self>,
        storage: Arc<Storage>,
        metrics: Arc<Metrics>,
    ) -> Result<()> {
        self.start_storage_sink(storage, metrics)?;
        self.start_transform_workers()
    }

    fn start_storage_sink(
        self: &Arc<Self>,
        storage: Arc<Storage>,
        metrics: Arc<Metrics>,
    ) -> Result<()> {
        let mut storage_sink = self.storage_sink.lock_or_poisoned();
        if storage_sink.is_some() {
            return Ok(());
        }
        let (commands, receiver) = mpsc::sync_channel(self.config.storage_sink_buffer_capacity);
        let weak = Arc::downgrade(self);
        let handle = thread::Builder::new()
            .name("canardstack-storage-sink".to_string())
            .spawn(move || run_storage_sink_worker(receiver, weak, storage, metrics))
            .context("spawn storage sink thread")?;
        tracing::info!(
            event = "storage_sink_started",
            buffer_capacity = self.config.storage_sink_buffer_capacity
        );
        *storage_sink = Some(StorageSinkDispatcher {
            commands: Some(commands),
            handle: Some(handle),
        });
        Ok(())
    }

    pub fn start_transform_workers(self: &Arc<Self>) -> Result<()> {
        let worker_count = self.config.ingest_workers;
        let mut topology = self.topology.lock_or_poisoned();
        if topology.is_some() {
            return Ok(());
        }
        let weak = Arc::downgrade(self);
        let mut handles = Vec::with_capacity(worker_count);
        let per_worker_capacity = self
            .config
            .ingest_buffer_capacity
            .div_ceil(worker_count)
            .max(1);
        let mut commands = Vec::with_capacity(worker_count);
        for worker_idx in 0..worker_count {
            let weak = Weak::clone(&weak);
            let (command_tx, command_rx) = mpsc::sync_channel(per_worker_capacity);
            let handle = thread::Builder::new()
                .name(format!("canardstack-ingest-worker-{worker_idx}"))
                .spawn(move || run_transform_worker(command_rx, weak))
                .context("spawn ingest transform worker thread")?;
            commands.push(command_tx);
            handles.push(handle);
        }
        tracing::info!(
            event = "ingest_topology_started",
            workers = worker_count,
            buffer_capacity = self.config.ingest_buffer_capacity,
            per_worker_buffer_capacity = per_worker_capacity
        );
        *topology = Some(IngestTopologyDispatcher {
            commands,
            handles,
            next_worker: 0,
        });
        Ok(())
    }
}

fn run_transform_worker(receiver: mpsc::Receiver<SpooledIngestWork>, ingestor: Weak<Ingestor>) {
    loop {
        let work = receiver.recv();
        let Ok(work) = work else {
            return;
        };
        let Some(ingestor) = ingestor.upgrade() else {
            return;
        };
        let signal = work.signal;
        let metrics = Arc::clone(&work.metrics);
        let started = Instant::now();
        let result = ingestor.process_spooled_ingest(work, false);
        match result {
            Ok(_) => {
                metrics.inc(
                    "canardstack_ingest_transform_completed_total",
                    &[("signal", signal.as_str()), ("status", "ok")],
                    1,
                );
                metrics.observe_phase_seconds(
                    signal.as_str(),
                    "ingest_transform_worker",
                    Some("ok"),
                    started.elapsed().as_secs_f64(),
                );
            }
            Err(err) => {
                metrics.inc(
                    "canardstack_ingest_transform_completed_total",
                    &[("signal", signal.as_str()), ("status", err.reason)],
                    1,
                );
                metrics.observe_phase_seconds(
                    signal.as_str(),
                    "ingest_transform_worker",
                    Some("error"),
                    started.elapsed().as_secs_f64(),
                );
                tracing::warn!(
                    event = "ingest_transform_worker_failed",
                    signal = signal.as_str(),
                    status = err.status,
                    reason = err.reason,
                    message = %err.message
                );
            }
        }
    }
}

fn run_storage_sink_worker(
    receiver: mpsc::Receiver<StorageSinkWork>,
    ingestor: Weak<Ingestor>,
    storage: Arc<Storage>,
    metrics: Arc<Metrics>,
) {
    while let Ok(first) = receiver.recv() {
        let Some(ingestor) = ingestor.upgrade() else {
            return;
        };
        let mut works = vec![first];
        let mut rows = storage_sink_work_rows(&works[0]);
        let flush_by = Instant::now() + ingestor.config.storage_sink_flush_interval;
        while rows < ingestor.config.storage_sink_batch_rows {
            let timeout = flush_by.saturating_duration_since(Instant::now());
            if timeout.is_zero() {
                break;
            }
            match receiver.recv_timeout(timeout) {
                Ok(work) => {
                    rows = rows.saturating_add(storage_sink_work_rows(&work));
                    works.push(work);
                }
                Err(RecvTimeoutError::Timeout) => break,
                Err(RecvTimeoutError::Disconnected) => break,
            }
        }
        let started = Instant::now();
        let result = insert_storage_sink_work(&works, &storage, &metrics);
        match result {
            Ok(()) => {
                for work in works {
                    if let Err(err) = ingestor
                        .mark_raw_spool_batches_storage_committed(&work.sets, Some(&metrics))
                    {
                        tracing::error!(
                            event = "storage_sink_checkpoint_failed",
                            signal = work.request_signal.as_str(),
                            error = %err
                        );
                    }
                    metrics.inc(
                        "canardstack_storage_sink_completed_total",
                        &[("signal", work.request_signal.as_str()), ("status", "ok")],
                        1,
                    );
                }
                metrics.observe_phase_seconds(
                    "all",
                    "storage_sink_worker",
                    Some("ok"),
                    started.elapsed().as_secs_f64(),
                );
            }
            Err(err) => {
                for work in works {
                    ingestor.untrack_storage_sink_work(&work);
                    metrics.inc(
                        "canardstack_storage_sink_completed_total",
                        &[
                            ("signal", work.request_signal.as_str()),
                            ("status", "storage_error"),
                        ],
                        1,
                    );
                }
                metrics.observe_phase_seconds(
                    "all",
                    "storage_sink_worker",
                    Some("error"),
                    started.elapsed().as_secs_f64(),
                );
                tracing::error!(event = "storage_sink_failed", error = %err);
            }
        }
    }
}

fn storage_sink_work_rows(work: &StorageSinkWork) -> usize {
    work.sets
        .iter()
        .flat_map(|(_, batches)| batches)
        .map(queue::QueuedBatch::len)
        .sum()
}

fn insert_storage_sink_work(
    works: &[StorageSinkWork],
    storage: &Storage,
    metrics: &Metrics,
) -> Result<()> {
    let insert_prepare_started = Instant::now();
    let inserts = works
        .iter()
        .flat_map(|work| {
            work.sets.iter().flat_map(|(key, batches)| {
                batches
                    .iter()
                    .filter(|batch| batch.len() > 0)
                    .map(|batch| ArrowBatchInsert {
                        table: key.signal,
                        batch: &batch.batch,
                        source_format: batch.source_format,
                    })
            })
        })
        .collect::<Vec<_>>();
    metrics.observe_phase_seconds(
        "all",
        "storage_sink_insert_prepare",
        None,
        insert_prepare_started.elapsed().as_secs_f64(),
    );
    if inserts.is_empty() {
        return Ok(());
    }

    let insert_started = Instant::now();
    let result = storage.insert_arrow_batches(&inserts)?;
    metrics.observe_phase_seconds(
        "all",
        "storage_sink_insert",
        None,
        insert_started.elapsed().as_secs_f64(),
    );
    observe_storage_timings(metrics, &result.timings);
    record_storage_sink_success_metrics(works, metrics);

    let seal_started = Instant::now();
    let immutable = storage.flush_immutable_segments(true)?;
    metrics.observe_phase_seconds(
        "all",
        "storage_sink_seal",
        None,
        seal_started.elapsed().as_secs_f64(),
    );
    observe_storage_timings(metrics, &immutable.timings);
    observe_storage_sink_immutable_flush(metrics, &immutable);
    Ok(())
}

fn record_storage_sink_success_metrics(works: &[StorageSinkWork], metrics: &Metrics) {
    let mut by_signal = HashMap::<Signal, (u64, u64)>::new();
    for work in works {
        for (key, batches) in &work.sets {
            for batch in batches {
                if batch.len() == 0 {
                    continue;
                }
                let (rows, bytes) = by_signal.entry(key.signal).or_default();
                *rows = rows.saturating_add(batch.len() as u64);
                *bytes = bytes.saturating_add(batch.batch.get_array_memory_size() as u64);
            }
        }
    }
    for (signal, (rows, bytes)) in by_signal {
        metrics.inc(
            "canardstack_storage_sink_rows_total",
            &[("signal", signal.as_str())],
            rows,
        );
        metrics.inc(
            "canardstack_storage_sink_buffered_rows_total",
            &[("signal", signal.as_str())],
            rows,
        );
        metrics.inc(
            "canardstack_storage_sink_buffered_bytes_total",
            &[("signal", signal.as_str())],
            bytes,
        );
    }
}

fn observe_storage_timings(metrics: &Metrics, timings: &[ArrowBatchInsertTiming]) {
    for timing in timings {
        metrics.observe_phase_seconds(
            timing.table.as_str(),
            timing.phase.as_str(),
            None,
            timing.seconds,
        );
    }
}

fn observe_storage_sink_immutable_flush(metrics: &Metrics, outcome: &ImmutableFlushOutcome) {
    if outcome.sealed_rows == 0 && outcome.sealed_files == 0 {
        return;
    }
    metrics.inc(
        "canardstack_storage_sink_sealed_rows_total",
        &[("signal", "all")],
        outcome.sealed_rows as u64,
    );
    metrics.inc(
        "canardstack_storage_sink_sealed_files_total",
        &[("signal", "all")],
        outcome.sealed_files as u64,
    );
}
