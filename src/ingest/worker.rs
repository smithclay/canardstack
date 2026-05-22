use super::{Ingestor, SpooledIngestWork};
use crate::storage::Storage;
use crate::validation::{ApiError, ApiResult};
use crate::LockExt;
use anyhow::{Context, Result};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{self, SyncSender};
use std::sync::{Arc, Weak};
use std::thread::{self, JoinHandle};
use std::time::Instant;

/// Parallel ingest across OS threads: a fixed pool of worker threads that turn
/// durably-spooled requests into buffered Arrow rows. Each worker inserts into
/// the storage immutable buffer; the scheduler is the single seal driver (see
/// `Ingestor::flush_committed_to_storage`).
pub(super) struct IngestWorkerPool {
    pub(super) commands: Vec<SyncSender<SpooledIngestWork>>,
    pub(super) handles: Vec<JoinHandle<()>>,
    pub(super) next_worker: usize,
}

pub(super) struct WorkerQueueSlots {
    used: AtomicUsize,
    capacity: usize,
}

impl WorkerQueueSlots {
    pub(super) fn new(capacity: usize) -> Self {
        Self {
            used: AtomicUsize::new(0),
            capacity: capacity.max(1),
        }
    }

    pub(super) fn reserve(self: &Arc<Self>) -> ApiResult<WorkerQueueReservation> {
        let mut current = self.used.load(Ordering::Acquire);
        loop {
            if current >= self.capacity {
                return Err(ApiError::new(
                    429,
                    "ingest_buffer_full",
                    "ingest worker buffer is full",
                )
                .with_retry_after(5));
            }
            match self.used.compare_exchange_weak(
                current,
                current + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    return Ok(WorkerQueueReservation {
                        slots: Arc::clone(self),
                        active: true,
                    });
                }
                Err(observed) => current = observed,
            }
        }
    }

    pub(super) fn used(&self) -> usize {
        self.used.load(Ordering::Acquire)
    }

    pub(super) fn capacity(&self) -> usize {
        self.capacity
    }
}

pub(super) struct WorkerQueueReservation {
    slots: Arc<WorkerQueueSlots>,
    active: bool,
}

impl WorkerQueueReservation {
    pub(super) fn release(&mut self) {
        if self.active {
            self.slots.used.fetch_sub(1, Ordering::AcqRel);
            self.active = false;
        }
    }
}

impl Drop for WorkerQueueReservation {
    fn drop(&mut self) {
        self.release();
    }
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
        let mut work = work;
        work.worker_queue_reservation.release();
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn worker_queue_slots_reject_when_capacity_is_reserved() {
        let slots = Arc::new(WorkerQueueSlots::new(1));
        let reservation = slots.reserve().unwrap();

        let err = match slots.reserve() {
            Ok(_) => panic!("second queue reservation must reject"),
            Err(err) => err,
        };
        assert_eq!(err.status, 429);
        assert_eq!(err.reason, "ingest_buffer_full");
        assert_eq!(slots.used(), 1);

        drop(reservation);
        assert_eq!(slots.used(), 0);
    }
}
