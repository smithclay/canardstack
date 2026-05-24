use super::{IngestPipeline, Ingestor, SpooledIngestWork};
use crate::metrics::MetricName;
use crate::storage::Storage;
use crate::LockExt;
use anyhow::{Context, Result};
use std::sync::mpsc::{self, SyncSender};
use std::sync::{Arc, Weak};
use std::thread::{self, JoinHandle};
use std::time::Instant;

/// Fixed in-flight worker-handoff capacity for the ingest worker pool. Internal
/// mechanic (not an operator policy knob); kept here next to the pool it sizes.
/// `Config::test_overrides.ingest_worker_channel_capacity` defaults from this
/// and exists only for deterministic test injection.
pub(crate) const INGEST_WORKER_CHANNEL_CAPACITY: usize = 1024;

/// Parallel ingest across OS threads: a fixed pool of worker threads that turn
/// durably-spooled requests into buffered Arrow rows. Each worker appends into
/// the storage Arrow write buffer; `crate::seal` is the single seal owner.
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
        self.pipeline().start_ingest_workers(storage)
    }
}

impl IngestPipeline {
    pub(crate) fn start_ingest_workers(self: &Arc<Self>, storage: Arc<Storage>) -> Result<()> {
        let worker_count = self.config.mechanics.ingest_workers;
        let mut pool = self.ingest_workers.lock_or_poisoned();
        if pool.is_some() {
            return Ok(());
        }
        let weak = Arc::downgrade(self);
        let per_worker_capacity = self
            .config
            .test_overrides
            .ingest_worker_channel_capacity
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
            worker_channel_capacity = self.config.test_overrides.ingest_worker_channel_capacity,
            per_worker_channel_capacity = per_worker_capacity
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
    ingestor: Weak<IngestPipeline>,
    storage: Arc<Storage>,
) {
    while let Ok(work) = receiver.recv() {
        let Some(ingestor) = ingestor.upgrade() else {
            return;
        };
        let route = work.request_kind;
        let metrics = Arc::clone(&work.metrics);
        // `process_spooled_ingest` emits the per-request boundary counters itself.
        let started = Instant::now();
        match ingestor.process_spooled_ingest(work, &storage) {
            Ok(disposition) => {
                metrics.inc(
                    MetricName::IngestWorkerCompletedTotal,
                    &[
                        ("request_kind", route.as_str()),
                        ("status", disposition.as_str()),
                    ],
                    1,
                );
                metrics.observe_seconds(
                    MetricName::PhaseDurationSeconds,
                    &[
                        ("request_kind", route.as_str()),
                        ("phase", "ingest_worker"),
                        ("status", "ok"),
                    ],
                    started.elapsed().as_secs_f64(),
                );
            }
            Err(err) => {
                metrics.inc(
                    MetricName::IngestWorkerCompletedTotal,
                    &[
                        ("request_kind", route.as_str()),
                        ("status", err.disposition.as_str()),
                    ],
                    1,
                );
                metrics.observe_seconds(
                    MetricName::PhaseDurationSeconds,
                    &[
                        ("request_kind", route.as_str()),
                        ("phase", "ingest_worker"),
                        ("status", "error"),
                    ],
                    started.elapsed().as_secs_f64(),
                );
                tracing::warn!(
                    event = "ingest_worker_failed",
                    request_kind = route.as_str(),
                    status = err.error.status,
                    reason = err.error.reason,
                    disposition = err.disposition.as_str(),
                    message = %err.error.message
                );
            }
        }
    }
}
