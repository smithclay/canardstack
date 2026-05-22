use super::codec::{prepare_append_records, PreparedRawSpoolRecord};
use super::{
    raw_spool_full_info, RawSpool, RawSpoolAppendAck, RawSpoolCheckpointBatchStats,
    RawSpoolOptions, RawSpoolRecord, RawSpoolRecordId, RawSpoolStats, RecoveredRawSpoolRecord,
};
use anyhow::{Context, Result};
use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, SyncSender};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

pub struct RawSpoolWriter {
    commands: Option<SyncSender<RawSpoolCommand>>,
    depths: Arc<RawSpoolWriterDepths>,
    handle: Option<JoinHandle<()>>,
    max_record_bytes: u64,
}

#[derive(Clone, Copy, Debug, Default)]
pub(super) struct RawSpoolCommandDepthSnapshot {
    pending_commands: usize,
    pending_append_commands: usize,
    pending_checkpoint_commands: usize,
}

impl RawSpoolCommandDepthSnapshot {
    fn max(self, other: Self) -> Self {
        Self {
            pending_commands: self.pending_commands.max(other.pending_commands),
            pending_append_commands: self
                .pending_append_commands
                .max(other.pending_append_commands),
            pending_checkpoint_commands: self
                .pending_checkpoint_commands
                .max(other.pending_checkpoint_commands),
        }
    }
}

#[derive(Default)]
pub(super) struct RawSpoolWriterDepths {
    pending_commands: AtomicUsize,
    pending_append_commands: AtomicUsize,
    pending_checkpoint_commands: AtomicUsize,
    pending_commands_max: AtomicUsize,
    pending_append_commands_max: AtomicUsize,
    pending_checkpoint_commands_max: AtomicUsize,
    append_commands_total: AtomicU64,
    checkpoint_commands_total: AtomicU64,
    recover_commands_total: AtomicU64,
    stats_commands_total: AtomicU64,
}

#[derive(Clone, Copy, Debug)]
pub(super) enum RawSpoolCommandKind {
    Append,
    Checkpoint,
    Recover,
    Stats,
}

impl RawSpoolWriterDepths {
    fn enqueue(&self, kind: RawSpoolCommandKind) -> RawSpoolCommandDepthSnapshot {
        let mut snapshot = RawSpoolCommandDepthSnapshot {
            pending_commands: self.pending_commands.load(Ordering::Acquire),
            pending_append_commands: self.pending_append_commands.load(Ordering::Acquire),
            pending_checkpoint_commands: self.pending_checkpoint_commands.load(Ordering::Acquire),
        };
        match kind {
            RawSpoolCommandKind::Append => {
                snapshot.pending_commands =
                    self.pending_commands.fetch_add(1, Ordering::AcqRel) + 1;
                self.pending_commands_max
                    .fetch_max(snapshot.pending_commands, Ordering::AcqRel);
                snapshot.pending_append_commands =
                    self.pending_append_commands.fetch_add(1, Ordering::AcqRel) + 1;
                self.pending_append_commands_max
                    .fetch_max(snapshot.pending_append_commands, Ordering::AcqRel);
            }
            RawSpoolCommandKind::Checkpoint => {
                snapshot.pending_commands =
                    self.pending_commands.fetch_add(1, Ordering::AcqRel) + 1;
                self.pending_commands_max
                    .fetch_max(snapshot.pending_commands, Ordering::AcqRel);
                snapshot.pending_checkpoint_commands = self
                    .pending_checkpoint_commands
                    .fetch_add(1, Ordering::AcqRel)
                    + 1;
                self.pending_checkpoint_commands_max
                    .fetch_max(snapshot.pending_checkpoint_commands, Ordering::AcqRel);
            }
            RawSpoolCommandKind::Recover | RawSpoolCommandKind::Stats => {}
        }
        snapshot
    }

    fn command_sent(&self, kind: RawSpoolCommandKind) {
        match kind {
            RawSpoolCommandKind::Append => {
                self.append_commands_total.fetch_add(1, Ordering::AcqRel);
            }
            RawSpoolCommandKind::Checkpoint => {
                self.checkpoint_commands_total
                    .fetch_add(1, Ordering::AcqRel);
            }
            RawSpoolCommandKind::Recover => {
                self.recover_commands_total.fetch_add(1, Ordering::AcqRel);
            }
            RawSpoolCommandKind::Stats => {
                self.stats_commands_total.fetch_add(1, Ordering::AcqRel);
            }
        }
    }

    fn command_send_failed(&self, kind: RawSpoolCommandKind) {
        self.command_started(kind);
    }

    fn command_started(&self, kind: RawSpoolCommandKind) {
        match kind {
            RawSpoolCommandKind::Append => {
                decrement_pending(&self.pending_commands);
                decrement_pending(&self.pending_append_commands);
            }
            RawSpoolCommandKind::Checkpoint => {
                decrement_pending(&self.pending_commands);
                decrement_pending(&self.pending_checkpoint_commands);
            }
            RawSpoolCommandKind::Recover | RawSpoolCommandKind::Stats => {}
        }
    }

    fn record_stats(&self, stats: &mut RawSpoolStats) {
        stats.writer_pending_commands = self.pending_commands.load(Ordering::Acquire);
        stats.writer_pending_append_commands = self.pending_append_commands.load(Ordering::Acquire);
        stats.writer_pending_checkpoint_commands =
            self.pending_checkpoint_commands.load(Ordering::Acquire);
        stats.writer_pending_commands_max = self.pending_commands_max.load(Ordering::Acquire);
        stats.writer_pending_append_commands_max =
            self.pending_append_commands_max.load(Ordering::Acquire);
        stats.writer_pending_checkpoint_commands_max =
            self.pending_checkpoint_commands_max.load(Ordering::Acquire);
        stats.writer_append_commands_total = self.append_commands_total.load(Ordering::Acquire);
        stats.writer_checkpoint_commands_total =
            self.checkpoint_commands_total.load(Ordering::Acquire);
        stats.writer_recover_commands_total = self.recover_commands_total.load(Ordering::Acquire);
        stats.writer_stats_commands_total = self.stats_commands_total.load(Ordering::Acquire);
    }
}

fn decrement_pending(pending: &AtomicUsize) {
    let _ = pending.fetch_update(Ordering::AcqRel, Ordering::Acquire, |value| {
        value.checked_sub(1)
    });
}

pub(super) struct AppendCommand {
    pub(super) record: PreparedRawSpoolRecord,
    pub(super) queued_at: Instant,
    pub(super) enqueue_depth: RawSpoolCommandDepthSnapshot,
    pub(super) reply: mpsc::Sender<Result<RawSpoolAppendAck>>,
}

pub(super) struct CheckpointCommand {
    pub(super) ids: Vec<RawSpoolRecordId>,
    pub(super) queued_at: Instant,
    pub(super) enqueue_depth: RawSpoolCommandDepthSnapshot,
    pub(super) reply: mpsc::Sender<Result<RawSpoolCheckpointBatchStats>>,
}

pub(super) enum RawSpoolCommand {
    Append(AppendCommand),
    Checkpoint(CheckpointCommand),
    RecoverPending {
        reply: mpsc::Sender<Result<Vec<RecoveredRawSpoolRecord>>>,
    },
    Stats {
        reply: mpsc::Sender<Result<RawSpoolStats>>,
    },
}

impl RawSpoolCommand {
    fn kind(&self) -> RawSpoolCommandKind {
        match self {
            RawSpoolCommand::Append(_) => RawSpoolCommandKind::Append,
            RawSpoolCommand::Checkpoint(_) => RawSpoolCommandKind::Checkpoint,
            RawSpoolCommand::RecoverPending { .. } => RawSpoolCommandKind::Recover,
            RawSpoolCommand::Stats { .. } => RawSpoolCommandKind::Stats,
        }
    }
}

impl RawSpoolWriter {
    pub fn spawn(
        options: RawSpoolOptions,
        queue_capacity: usize,
        max_batch_records: usize,
        max_batch_delay: Duration,
    ) -> Result<Self> {
        let spool = RawSpool::open(options)?;
        let max_record_bytes = spool.max_record_bytes;
        let (commands, receiver) = mpsc::sync_channel(queue_capacity.max(1));
        let depths = Arc::new(RawSpoolWriterDepths::default());
        let writer_depths = Arc::clone(&depths);
        let handle = thread::Builder::new()
            .name("canardstack-raw-spool-writer".to_string())
            .spawn(move || {
                run_raw_spool_writer(
                    spool,
                    receiver,
                    max_batch_records.max(1),
                    max_batch_delay,
                    writer_depths,
                )
            })
            .context("spawn raw spool writer thread")?;
        Ok(Self {
            commands: Some(commands),
            depths,
            handle: Some(handle),
            max_record_bytes,
        })
    }

    pub fn append(&self, record: RawSpoolRecord) -> Result<RawSpoolAppendAck> {
        let (reply, rx) = mpsc::channel();
        let record = prepare_append_records(vec![record], self.max_record_bytes)?
            .into_iter()
            .next()
            .expect("single prepared append record");
        let commands = self
            .commands
            .as_ref()
            .context("raw spool writer is stopped")?;
        let kind = RawSpoolCommandKind::Append;
        let enqueue_depth = self.depths.enqueue(kind);
        commands
            .send(RawSpoolCommand::Append(AppendCommand {
                record,
                queued_at: Instant::now(),
                enqueue_depth,
                reply,
            }))
            .inspect_err(|_| {
                self.depths.command_send_failed(kind);
            })
            .context("send raw spool append command")?;
        self.depths.command_sent(kind);
        rx.recv().context("receive raw spool append result")?
    }

    pub fn mark_committed(&self, id: RawSpoolRecordId) -> Result<RawSpoolCheckpointBatchStats> {
        self.mark_committed_batch(&[id])
    }

    pub fn mark_committed_batch(
        &self,
        ids: &[RawSpoolRecordId],
    ) -> Result<RawSpoolCheckpointBatchStats> {
        if ids.is_empty() {
            return Ok(RawSpoolCheckpointBatchStats::default());
        }
        let (reply, rx) = mpsc::channel();
        let commands = self
            .commands
            .as_ref()
            .context("raw spool writer is stopped")?;
        let kind = RawSpoolCommandKind::Checkpoint;
        let enqueue_depth = self.depths.enqueue(kind);
        commands
            .send(RawSpoolCommand::Checkpoint(CheckpointCommand {
                ids: ids.to_vec(),
                queued_at: Instant::now(),
                enqueue_depth,
                reply,
            }))
            .inspect_err(|_| {
                self.depths.command_send_failed(kind);
            })
            .context("send raw spool checkpoint command")?;
        self.depths.command_sent(kind);
        rx.recv().context("receive raw spool checkpoint result")?
    }

    pub fn recover_pending(&self) -> Result<Vec<RecoveredRawSpoolRecord>> {
        let (reply, rx) = mpsc::channel();
        let commands = self
            .commands
            .as_ref()
            .context("raw spool writer is stopped")?;
        let kind = RawSpoolCommandKind::Recover;
        self.depths.enqueue(kind);
        commands
            .send(RawSpoolCommand::RecoverPending { reply })
            .inspect_err(|_| {
                self.depths.command_send_failed(kind);
            })
            .context("send raw spool recovery command")?;
        self.depths.command_sent(kind);
        rx.recv().context("receive raw spool recovery result")?
    }

    pub fn stats(&self) -> Result<RawSpoolStats> {
        let (reply, rx) = mpsc::channel();
        let commands = self
            .commands
            .as_ref()
            .context("raw spool writer is stopped")?;
        let kind = RawSpoolCommandKind::Stats;
        self.depths.enqueue(kind);
        commands
            .send(RawSpoolCommand::Stats { reply })
            .inspect_err(|_| {
                self.depths.command_send_failed(kind);
            })
            .context("send raw spool stats command")?;
        self.depths.command_sent(kind);
        rx.recv().context("receive raw spool stats result")?
    }
}

impl Drop for RawSpoolWriter {
    fn drop(&mut self) {
        self.commands.take();
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

pub(super) fn run_raw_spool_writer(
    mut spool: RawSpool,
    receiver: Receiver<RawSpoolCommand>,
    max_batch_records: usize,
    max_batch_delay: Duration,
    depths: Arc<RawSpoolWriterDepths>,
) {
    let mut deferred = VecDeque::new();
    loop {
        let command = match deferred.pop_front() {
            Some(command) => command,
            None => match next_writer_timeout(&spool) {
                None => match receiver.recv() {
                    Ok(command) => command,
                    Err(_) => {
                        let _ = spool.sync_append_if_due(true);
                        let _ = spool.sync_checkpoint_if_due(true);
                        break;
                    }
                },
                Some(sync_due_in) => match receiver.recv_timeout(sync_due_in) {
                    Ok(command) => command,
                    Err(RecvTimeoutError::Timeout) => {
                        let _ = spool.sync_append_if_due(false);
                        let _ = spool.sync_checkpoint_if_due(false);
                        continue;
                    }
                    Err(RecvTimeoutError::Disconnected) => {
                        let _ = spool.sync_append_if_due(true);
                        let _ = spool.sync_checkpoint_if_due(true);
                        break;
                    }
                },
            },
        };
        depths.command_started(command.kind());
        match command {
            RawSpoolCommand::Append(first) => handle_append_batch(
                &mut spool,
                first,
                &receiver,
                &mut deferred,
                max_batch_records,
                max_batch_delay,
                &depths,
            ),
            RawSpoolCommand::Checkpoint(first) => handle_checkpoint_batch(
                &mut spool,
                first,
                &receiver,
                &mut deferred,
                max_batch_records,
                max_batch_delay,
                &depths,
            ),
            RawSpoolCommand::RecoverPending { reply } => {
                let _ = reply.send(spool.recover_pending());
            }
            RawSpoolCommand::Stats { reply } => {
                let result = spool.stats().map(|mut stats| {
                    depths.record_stats(&mut stats);
                    stats
                });
                let _ = reply.send(result);
            }
        }
        let _ = spool.sync_append_if_due(false);
    }
}

pub(super) fn next_writer_timeout(spool: &RawSpool) -> Option<Duration> {
    match (spool.append_sync_due_in(), spool.checkpoint_sync_due_in()) {
        (Some(append), Some(checkpoint)) => Some(append.min(checkpoint)),
        (Some(append), None) => Some(append),
        (None, Some(checkpoint)) => Some(checkpoint),
        (None, None) => None,
    }
}

pub(super) fn handle_append_batch(
    spool: &mut RawSpool,
    first: AppendCommand,
    receiver: &Receiver<RawSpoolCommand>,
    deferred: &mut VecDeque<RawSpoolCommand>,
    max_batch_records: usize,
    max_batch_delay: Duration,
    depths: &RawSpoolWriterDepths,
) {
    let mut batch = vec![first];
    let collect_started = Instant::now();
    let deferred_checkpoint_commands = collect_append_batch_with_depths(
        receiver,
        deferred,
        max_batch_records,
        max_batch_delay,
        &mut batch,
        depths,
    );
    let mut replies = Vec::with_capacity(batch.len());
    let max_enqueue_depth = batch
        .iter()
        .map(|command| command.enqueue_depth)
        .fold(RawSpoolCommandDepthSnapshot::default(), |max, depth| {
            max.max(depth)
        });
    let queue_seconds = batch
        .iter()
        .map(|command| {
            collect_started
                .saturating_duration_since(command.queued_at)
                .as_secs_f64()
        })
        .sum::<f64>();
    let records = batch
        .into_iter()
        .map(|command| {
            replies.push(command.reply);
            command.record
        })
        .collect::<Vec<_>>();
    let wait_seconds = collect_started.elapsed().as_secs_f64();
    match spool.append_prepared_batch(records) {
        Ok(mut appended) => {
            match spool.sync_append_if_due(true) {
                Ok(Some(sync)) => {
                    appended.stats.fsync_seconds = sync.seconds;
                    appended.stats.fsync_count = sync.file_count;
                }
                Ok(None) => {}
                Err(err) => {
                    let message = err.to_string();
                    for reply in replies {
                        let _ = reply.send(Err(anyhow::anyhow!(message.clone())));
                    }
                    return;
                }
            }
            appended.stats.queue_seconds = queue_seconds;
            appended.stats.wait_seconds = wait_seconds;
            appended.stats.max_pending_commands_at_enqueue = max_enqueue_depth.pending_commands;
            appended.stats.max_pending_append_commands_at_enqueue =
                max_enqueue_depth.pending_append_commands;
            appended.stats.max_pending_checkpoint_commands_at_enqueue =
                max_enqueue_depth.pending_checkpoint_commands;
            appended.stats.deferred_checkpoint_commands = deferred_checkpoint_commands;
            let mut stats = Some(appended.stats);
            for ((reply, id), compressed_body) in replies
                .into_iter()
                .zip(appended.ids)
                .zip(appended.compressed_bodies)
            {
                let _ = reply.send(Ok(RawSpoolAppendAck {
                    id,
                    compressed_body,
                    batch_stats: stats.take(),
                }));
            }
        }
        Err(err) => {
            let message = err.to_string();
            let is_full = raw_spool_full_info(&err).copied();
            for reply in replies {
                let result = match is_full {
                    Some(full) => Err(anyhow::Error::new(full)),
                    None => Err(anyhow::anyhow!(message.clone())),
                };
                let _ = reply.send(result);
            }
        }
    }
}

#[cfg(test)]
pub(super) fn collect_append_batch(
    receiver: &Receiver<RawSpoolCommand>,
    deferred: &mut VecDeque<RawSpoolCommand>,
    max_batch_records: usize,
    max_batch_delay: Duration,
    batch: &mut Vec<AppendCommand>,
) -> usize {
    let depths = RawSpoolWriterDepths::default();
    collect_append_batch_with_depths(
        receiver,
        deferred,
        max_batch_records,
        max_batch_delay,
        batch,
        &depths,
    )
}

pub(super) fn collect_append_batch_with_depths(
    receiver: &Receiver<RawSpoolCommand>,
    deferred: &mut VecDeque<RawSpoolCommand>,
    max_batch_records: usize,
    max_batch_delay: Duration,
    batch: &mut Vec<AppendCommand>,
    depths: &RawSpoolWriterDepths,
) -> usize {
    let deadline = Instant::now() + max_batch_delay;
    let mut deferred_checkpoint_commands = 0usize;
    while batch.len() < max_batch_records {
        let command = if max_batch_delay.is_zero() {
            match receiver.try_recv() {
                Ok(command) => command,
                Err(_) => break,
            }
        } else {
            let now = Instant::now();
            if now >= deadline {
                break;
            }
            match receiver.recv_timeout(deadline - now) {
                Ok(command) => command,
                Err(_) => break,
            }
        };
        match command {
            RawSpoolCommand::Append(append) => {
                depths.command_started(RawSpoolCommandKind::Append);
                batch.push(append);
            }
            other => {
                if matches!(other, RawSpoolCommand::Checkpoint(_)) {
                    deferred_checkpoint_commands += 1;
                }
                deferred.push_back(other);
            }
        }
    }
    deferred_checkpoint_commands
}

pub(super) fn handle_checkpoint_batch(
    spool: &mut RawSpool,
    first: CheckpointCommand,
    receiver: &Receiver<RawSpoolCommand>,
    deferred: &mut VecDeque<RawSpoolCommand>,
    max_batch_records: usize,
    max_batch_delay: Duration,
    depths: &RawSpoolWriterDepths,
) {
    let mut batch = vec![first];
    let collect_started = Instant::now();
    drain_deferred_checkpoints(deferred, max_batch_records, &mut batch, depths);
    let deferred_append_commands = collect_checkpoint_batch(
        receiver,
        deferred,
        max_batch_records,
        max_batch_delay,
        &mut batch,
        depths,
    );
    let mut ids = Vec::new();
    let mut queue_seconds = 0.0;
    let max_enqueue_depth = batch
        .iter()
        .map(|command| command.enqueue_depth)
        .fold(RawSpoolCommandDepthSnapshot::default(), |max, depth| {
            max.max(depth)
        });
    let records = batch.iter().map(|command| command.ids.len()).sum::<usize>();
    for command in &batch {
        queue_seconds += collect_started
            .saturating_duration_since(command.queued_at)
            .as_secs_f64()
            * command.ids.len() as f64;
        ids.extend(command.ids.iter().copied());
    }
    let stats = RawSpoolCheckpointBatchStats {
        records,
        commands: batch.len(),
        queue_seconds,
        wait_seconds: collect_started.elapsed().as_secs_f64(),
        max_pending_commands_at_enqueue: max_enqueue_depth.pending_commands,
        max_pending_append_commands_at_enqueue: max_enqueue_depth.pending_append_commands,
        max_pending_checkpoint_commands_at_enqueue: max_enqueue_depth.pending_checkpoint_commands,
        deferred_append_commands,
    };
    match spool.mark_committed_batch(ids) {
        Ok(()) => {
            let mut stats = Some(stats);
            for command in batch {
                let _ = command.reply.send(Ok(stats.take().unwrap_or_default()));
            }
        }
        Err(err) => {
            let message = err.to_string();
            for command in batch {
                let _ = command.reply.send(Err(anyhow::anyhow!(message.clone())));
            }
        }
    }
}

fn drain_deferred_checkpoints(
    deferred: &mut VecDeque<RawSpoolCommand>,
    max_batch_records: usize,
    batch: &mut Vec<CheckpointCommand>,
    depths: &RawSpoolWriterDepths,
) {
    while batch.len() < max_batch_records {
        let Some(RawSpoolCommand::Checkpoint(_)) = deferred.front() else {
            break;
        };
        let Some(RawSpoolCommand::Checkpoint(checkpoint)) = deferred.pop_front() else {
            unreachable!("front matched checkpoint");
        };
        depths.command_started(RawSpoolCommandKind::Checkpoint);
        batch.push(checkpoint);
    }
}

fn collect_checkpoint_batch(
    receiver: &Receiver<RawSpoolCommand>,
    deferred: &mut VecDeque<RawSpoolCommand>,
    max_batch_records: usize,
    max_batch_delay: Duration,
    batch: &mut Vec<CheckpointCommand>,
    depths: &RawSpoolWriterDepths,
) -> usize {
    let deadline = Instant::now() + max_batch_delay;
    let mut deferred_append_commands = 0usize;
    while batch.len() < max_batch_records {
        let command = if max_batch_delay.is_zero() {
            match receiver.try_recv() {
                Ok(command) => command,
                Err(_) => break,
            }
        } else {
            let now = Instant::now();
            if now >= deadline {
                break;
            }
            match receiver.recv_timeout(deadline - now) {
                Ok(command) => command,
                Err(_) => break,
            }
        };
        match command {
            RawSpoolCommand::Checkpoint(checkpoint) => {
                depths.command_started(RawSpoolCommandKind::Checkpoint);
                batch.push(checkpoint);
            }
            other => {
                if matches!(other, RawSpoolCommand::Append(_)) {
                    deferred_append_commands += 1;
                }
                deferred.push_back(other);
                break;
            }
        }
    }
    deferred_append_commands
}
