use super::codec::{prepare_append_records, PreparedRecord};
use super::{
    full_info, AppendAck, CheckpointBatchStats, Options, Record, RecordId, RecoveredRecord, Spool,
    Stats,
};
use anyhow::{Context, Result};
use std::collections::VecDeque;
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, SyncSender, TrySendError};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

pub struct Writer {
    commands: Option<SyncSender<Command>>,
    handle: Option<JoinHandle<()>>,
    max_record_bytes: u64,
}

pub(super) struct AppendCommand {
    pub(super) record: PreparedRecord,
    pub(super) queued_at: Instant,
    pub(super) reply: mpsc::Sender<Result<AppendAck>>,
}

pub(super) struct CheckpointCommand {
    pub(super) ids: Vec<RecordId>,
    pub(super) queued_at: Instant,
    pub(super) reply: mpsc::Sender<Result<CheckpointBatchStats>>,
}

pub(super) enum Command {
    Append(AppendCommand),
    Checkpoint(CheckpointCommand),
    RecoverPending {
        reply: mpsc::Sender<Result<Vec<RecoveredRecord>>>,
    },
    Stats {
        reply: mpsc::Sender<Result<Stats>>,
    },
    InjectFatal {
        message: String,
        reply: mpsc::Sender<()>,
    },
}

impl Writer {
    pub fn spawn(
        options: Options,
        queue_capacity: usize,
        max_batch_records: usize,
        max_batch_delay: Duration,
    ) -> Result<Self> {
        let spool = Spool::open(options)?;
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

    pub fn append(&self, record: Record) -> Result<AppendAck> {
        let (reply, rx) = mpsc::channel();
        let record = prepare_append_records(vec![record], self.max_record_bytes)?
            .into_iter()
            .next()
            .expect("single prepared append record");
        let commands = self
            .commands
            .as_ref()
            .context("raw spool writer is stopped")?;
        commands
            .try_send(Command::Append(AppendCommand {
                record,
                queued_at: Instant::now(),
                reply,
            }))
            .map_err(|err| match err {
                TrySendError::Full(_) => anyhow::anyhow!("raw spool writer queue is full"),
                TrySendError::Disconnected(_) => anyhow::anyhow!("raw spool writer is stopped"),
            })?;
        rx.recv().context("receive raw spool append result")?
    }

    pub fn mark_committed(&self, id: RecordId) -> Result<CheckpointBatchStats> {
        self.mark_committed_batch(&[id])
    }

    pub fn mark_committed_batch(&self, ids: &[RecordId]) -> Result<CheckpointBatchStats> {
        if ids.is_empty() {
            return Ok(CheckpointBatchStats::default());
        }
        let (reply, rx) = mpsc::channel();
        let commands = self
            .commands
            .as_ref()
            .context("raw spool writer is stopped")?;
        commands
            .send(Command::Checkpoint(CheckpointCommand {
                ids: ids.to_vec(),
                queued_at: Instant::now(),
                reply,
            }))
            .context("send raw spool checkpoint command")?;
        rx.recv().context("receive raw spool checkpoint result")?
    }

    pub fn recover_pending(&self) -> Result<Vec<RecoveredRecord>> {
        let (reply, rx) = mpsc::channel();
        let commands = self
            .commands
            .as_ref()
            .context("raw spool writer is stopped")?;
        commands
            .send(Command::RecoverPending { reply })
            .context("send raw spool recovery command")?;
        rx.recv().context("receive raw spool recovery result")?
    }

    pub fn stats(&self) -> Result<Stats> {
        let (reply, rx) = mpsc::channel();
        let commands = self
            .commands
            .as_ref()
            .context("raw spool writer is stopped")?;
        commands
            .send(Command::Stats { reply })
            .context("send raw spool stats command")?;
        rx.recv().context("receive raw spool stats result")?
    }

    /// Drive this writer into the fatal/unhealthy latch for tests, mirroring a
    /// real append/fsync failure. Blocks until the writer thread applies it so
    /// a subsequent `stats()` observes `healthy=false`.
    #[doc(hidden)]
    pub fn inject_fatal(&self, message: impl Into<String>) -> Result<()> {
        let (reply, rx) = mpsc::channel();
        let commands = self
            .commands
            .as_ref()
            .context("raw spool writer is stopped")?;
        commands
            .send(Command::InjectFatal {
                message: message.into(),
                reply,
            })
            .context("send raw spool inject-fatal command")?;
        rx.recv().context("receive raw spool inject-fatal ack")
    }
}

impl Drop for Writer {
    fn drop(&mut self) {
        self.commands.take();
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

pub(super) fn run_raw_spool_writer(
    mut spool: Spool,
    receiver: Receiver<Command>,
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
            Command::Append(first) => handle_append_batch(
                &mut spool,
                first,
                &receiver,
                &mut deferred,
                max_batch_records,
                max_batch_delay,
            ),
            Command::Checkpoint(first) => handle_checkpoint_batch(
                &mut spool,
                first,
                &receiver,
                &mut deferred,
                max_batch_records,
                max_batch_delay,
            ),
            Command::RecoverPending { reply } => {
                let _ = reply.send(spool.recover_pending());
            }
            Command::Stats { reply } => {
                let _ = reply.send(spool.stats());
            }
            Command::InjectFatal { message, reply } => {
                spool.inject_fatal(message);
                let _ = reply.send(());
            }
        }
        let _ = spool.sync_append_if_due(false);
    }
}

pub(super) fn next_writer_timeout(spool: &Spool) -> Option<Duration> {
    match (spool.append_sync_due_in(), spool.checkpoint_sync_due_in()) {
        (Some(append), Some(checkpoint)) => Some(append.min(checkpoint)),
        (Some(append), None) => Some(append),
        (None, Some(checkpoint)) => Some(checkpoint),
        (None, None) => None,
    }
}

pub(super) fn handle_append_batch(
    spool: &mut Spool,
    first: AppendCommand,
    receiver: &Receiver<Command>,
    deferred: &mut VecDeque<Command>,
    max_batch_records: usize,
    max_batch_delay: Duration,
) {
    let mut batch = vec![first];
    let collect_started = Instant::now();
    let deferred_checkpoint_commands = collect_append_batch(
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
            appended.stats.deferred_checkpoint_commands = deferred_checkpoint_commands;
            let mut stats = Some(appended.stats);
            for ((reply, id), compressed_body) in replies
                .into_iter()
                .zip(appended.ids)
                .zip(appended.compressed_bodies)
            {
                let _ = reply.send(Ok(AppendAck {
                    id,
                    compressed_body,
                    batch_stats: stats.take(),
                }));
            }
        }
        Err(err) => {
            let message = err.to_string();
            let is_full = full_info(&err).copied();
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
    receiver: &Receiver<Command>,
    deferred: &mut VecDeque<Command>,
    max_batch_records: usize,
    max_batch_delay: Duration,
    batch: &mut Vec<AppendCommand>,
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
            Command::Append(append) => {
                batch.push(append);
            }
            other => {
                if matches!(other, Command::Checkpoint(_)) {
                    deferred_checkpoint_commands += 1;
                }
                deferred.push_back(other);
            }
        }
    }
    deferred_checkpoint_commands
}

pub(super) fn handle_checkpoint_batch(
    spool: &mut Spool,
    first: CheckpointCommand,
    receiver: &Receiver<Command>,
    deferred: &mut VecDeque<Command>,
    max_batch_records: usize,
    max_batch_delay: Duration,
) {
    let mut batch = vec![first];
    let collect_started = Instant::now();
    drain_deferred_checkpoints(deferred, max_batch_records, &mut batch);
    let deferred_append_commands = collect_checkpoint_batch(
        receiver,
        deferred,
        max_batch_records,
        max_batch_delay,
        &mut batch,
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
    let stats = CheckpointBatchStats {
        records,
        commands: batch.len(),
        queue_seconds,
        wait_seconds: collect_started.elapsed().as_secs_f64(),
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
    deferred: &mut VecDeque<Command>,
    max_batch_records: usize,
    batch: &mut Vec<CheckpointCommand>,
) {
    while batch.len() < max_batch_records {
        let Some(Command::Checkpoint(_)) = deferred.front() else {
            break;
        };
        let Some(Command::Checkpoint(checkpoint)) = deferred.pop_front() else {
            unreachable!("front matched checkpoint");
        };
        batch.push(checkpoint);
    }
}

fn collect_checkpoint_batch(
    receiver: &Receiver<Command>,
    deferred: &mut VecDeque<Command>,
    max_batch_records: usize,
    max_batch_delay: Duration,
    batch: &mut Vec<CheckpointCommand>,
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
            Command::Checkpoint(checkpoint) => {
                batch.push(checkpoint);
            }
            other => {
                if matches!(other, Command::Append(_)) {
                    deferred_append_commands += 1;
                }
                deferred.push_back(other);
                break;
            }
        }
    }
    deferred_append_commands
}
