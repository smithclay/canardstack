use super::codec::{prepare_append_records, PreparedRawSpoolRecord};
use super::{
    raw_spool_full_info, RawSpool, RawSpoolAppendAck, RawSpoolCheckpointBatchStats,
    RawSpoolOptions, RawSpoolRecord, RawSpoolRecordId, RawSpoolStats, RecoveredRawSpoolRecord,
};
use anyhow::{Context, Result};
use std::collections::VecDeque;
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, SyncSender};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

pub struct RawSpoolWriter {
    commands: Option<SyncSender<RawSpoolCommand>>,
    handle: Option<JoinHandle<()>>,
    max_record_bytes: u64,
}

pub(super) struct AppendCommand {
    pub(super) record: PreparedRawSpoolRecord,
    pub(super) queued_at: Instant,
    pub(super) reply: mpsc::Sender<Result<RawSpoolAppendAck>>,
}

pub(super) struct CheckpointCommand {
    pub(super) ids: Vec<RawSpoolRecordId>,
    pub(super) queued_at: Instant,
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
        let handle = thread::Builder::new()
            .name("canardstack-raw-spool-writer".to_string())
            .spawn(move || {
                run_raw_spool_writer(spool, receiver, max_batch_records.max(1), max_batch_delay)
            })
            .context("spawn raw spool writer thread")?;
        Ok(Self {
            commands: Some(commands),
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
        self.commands
            .as_ref()
            .context("raw spool writer is stopped")?
            .send(RawSpoolCommand::Append(AppendCommand {
                record,
                queued_at: Instant::now(),
                reply,
            }))
            .context("send raw spool append command")?;
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
        self.commands
            .as_ref()
            .context("raw spool writer is stopped")?
            .send(RawSpoolCommand::Checkpoint(CheckpointCommand {
                ids: ids.to_vec(),
                queued_at: Instant::now(),
                reply,
            }))
            .context("send raw spool checkpoint command")?;
        rx.recv().context("receive raw spool checkpoint result")?
    }

    pub fn recover_pending(&self) -> Result<Vec<RecoveredRawSpoolRecord>> {
        let (reply, rx) = mpsc::channel();
        self.commands
            .as_ref()
            .context("raw spool writer is stopped")?
            .send(RawSpoolCommand::RecoverPending { reply })
            .context("send raw spool recovery command")?;
        rx.recv().context("receive raw spool recovery result")?
    }

    pub fn stats(&self) -> Result<RawSpoolStats> {
        let (reply, rx) = mpsc::channel();
        self.commands
            .as_ref()
            .context("raw spool writer is stopped")?
            .send(RawSpoolCommand::Stats { reply })
            .context("send raw spool stats command")?;
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
        match command {
            RawSpoolCommand::Append(first) => handle_append_batch(
                &mut spool,
                first,
                &receiver,
                &mut deferred,
                max_batch_records,
                max_batch_delay,
            ),
            RawSpoolCommand::Checkpoint(first) => handle_checkpoint_batch(
                &mut spool,
                first,
                &receiver,
                &mut deferred,
                max_batch_records,
                max_batch_delay,
            ),
            RawSpoolCommand::RecoverPending { reply } => {
                let _ = reply.send(spool.recover_pending());
            }
            RawSpoolCommand::Stats { reply } => {
                let _ = reply.send(spool.stats());
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
) {
    let mut batch = vec![first];
    let collect_started = Instant::now();
    collect_append_batch(
        receiver,
        deferred,
        max_batch_records,
        max_batch_delay,
        &mut batch,
    );
    let mut replies = Vec::with_capacity(batch.len());
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
            appended.stats.queue_seconds = queue_seconds;
            appended.stats.wait_seconds = wait_seconds;
            let mut stats = Some(appended.stats);
            for (reply, id) in replies.into_iter().zip(appended.ids) {
                let _ = reply.send(Ok(RawSpoolAppendAck {
                    id,
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

pub(super) fn collect_append_batch(
    receiver: &Receiver<RawSpoolCommand>,
    deferred: &mut VecDeque<RawSpoolCommand>,
    max_batch_records: usize,
    max_batch_delay: Duration,
    batch: &mut Vec<AppendCommand>,
) {
    let deadline = Instant::now() + max_batch_delay;
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
            RawSpoolCommand::Append(append) => batch.push(append),
            other => deferred.push_back(other),
        }
    }
}

pub(super) fn handle_checkpoint_batch(
    spool: &mut RawSpool,
    first: CheckpointCommand,
    receiver: &Receiver<RawSpoolCommand>,
    deferred: &mut VecDeque<RawSpoolCommand>,
    max_batch_records: usize,
    max_batch_delay: Duration,
) {
    let mut batch = vec![first];
    let collect_started = Instant::now();
    drain_deferred_checkpoints(deferred, max_batch_records, &mut batch);
    collect_batch(
        receiver,
        deferred,
        max_batch_records,
        max_batch_delay,
        &mut batch,
        |command| match command {
            RawSpoolCommand::Checkpoint(checkpoint) => Ok(checkpoint),
            other => Err(other),
        },
    );
    let mut ids = Vec::new();
    let mut queue_seconds = 0.0;
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
) {
    while batch.len() < max_batch_records {
        let Some(RawSpoolCommand::Checkpoint(_)) = deferred.front() else {
            break;
        };
        let Some(RawSpoolCommand::Checkpoint(checkpoint)) = deferred.pop_front() else {
            unreachable!("front matched checkpoint");
        };
        batch.push(checkpoint);
    }
}

pub(super) fn collect_batch<T>(
    receiver: &Receiver<RawSpoolCommand>,
    deferred: &mut VecDeque<RawSpoolCommand>,
    max_batch_records: usize,
    max_batch_delay: Duration,
    batch: &mut Vec<T>,
    mut extract: impl FnMut(RawSpoolCommand) -> std::result::Result<T, RawSpoolCommand>,
) {
    let deadline = Instant::now() + max_batch_delay;
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
        match extract(command) {
            Ok(item) => batch.push(item),
            Err(other) => {
                deferred.push_back(other);
                break;
            }
        }
    }
}
