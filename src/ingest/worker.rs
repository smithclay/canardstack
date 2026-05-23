use super::{Ingestor, SpooledIngestWork};
use crate::storage::Storage;
use crate::LockExt;
use anyhow::{Context, Result};
use std::sync::mpsc::{self, SyncSender};
use std::sync::{Arc, Weak};
use std::thread::{self, JoinHandle};
use std::time::Instant;

/// Parallel ingest across OS threads: a fixed pool of worker threads that turn
/// durably-spooled requests into buffered Arrow rows. Each worker appends into
/// the storage immutable buffer; the scheduler is the single seal driver (see
/// `Ingestor::seal_committed_to_storage`).
pub(super) struct IngestWorkerPool {
    pub(super) commands: Vec<SyncSender<SpooledIngestWork>>,
    pub(super) handles: Vec<JoinHandle<()>>,
    pub(super) next_worker: usize,
}

impl Drop for IngestWorkerPool {
    fn drop(&mut self) {
        self.commands.clear();
        for handle in self.handles.drain(..) {
            if handle.thread().id() != thread::current().id() {
                let _ = handle.join();
            }
        }
    }
}

impl Ingestor {
    pub fn start_ingest_workers(self: &Arc<Self>, storage: Arc<Storage>) -> Result<()> {
        let worker_count = self.config.ingest_workers;
        let mut pool = self.ingest_workers.lock_or_poisoned();
        if pool.is_some() {
            return Ok(());
        }
        let weak = Arc::downgrade(self);
        let per_worker_capacity = self
            .config
            .ingest_buffer_capacity
            .div_ceil(worker_count)
            .max(1);
        let mut commands = Vec::with_capacity(worker_count);
        let mut handles = Vec::with_capacity(worker_count);
        for worker_idx in 0..worker_count {
            let weak = Weak::clone(&weak);
            let storage = Arc::clone(&storage);
            let (command_tx, command_rx) = mpsc::sync_channel(per_worker_capacity);
            let handle = thread::Builder::new()
                .name(format!("canardstack-ingest-worker-{worker_idx}"))
                .spawn(move || run_ingest_worker(command_rx, weak, storage))
                .context("spawn ingest worker thread")?;
            commands.push(command_tx);
            handles.push(handle);
        }
        tracing::info!(
            event = "ingest_workers_started",
            workers = worker_count,
            buffer_capacity = self.config.ingest_buffer_capacity,
            per_worker_buffer_capacity = per_worker_capacity
        );
        *pool = Some(IngestWorkerPool {
            commands,
            handles,
            next_worker: 0,
        });
        Ok(())
    }
}

fn run_ingest_worker(
    receiver: mpsc::Receiver<SpooledIngestWork>,
    ingestor: Weak<Ingestor>,
    storage: Arc<Storage>,
) {
    while let Ok(work) = receiver.recv() {
        let Some(ingestor) = ingestor.upgrade() else {
            return;
        };
        let signal = work.signal;
        let metrics = Arc::clone(&work.metrics);
        let started = Instant::now();
        match ingestor.process_spooled_ingest(work, &storage) {
            Ok(()) => {
                metrics.inc(
                    "canardstack_ingest_worker_completed_total",
                    &[("signal", signal.as_str()), ("status", "ok")],
                    1,
                );
                metrics.observe_phase_seconds(
                    signal.as_str(),
                    "ingest_worker",
                    Some("ok"),
                    started.elapsed().as_secs_f64(),
                );
            }
            Err(err) => {
                metrics.inc(
                    "canardstack_ingest_worker_completed_total",
                    &[("signal", signal.as_str()), ("status", err.reason)],
                    1,
                );
                metrics.observe_phase_seconds(
                    signal.as_str(),
                    "ingest_worker",
                    Some("error"),
                    started.elapsed().as_secs_f64(),
                );
                tracing::warn!(
                    event = "ingest_worker_failed",
                    signal = signal.as_str(),
                    status = err.status,
                    reason = err.reason,
                    message = %err.message
                );
            }
        }
    }
}
