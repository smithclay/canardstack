use super::codec::{prepare_append_records, PreparedRawSpoolRecord};
use super::{
    raw_spool_full_info, RawSpool, RawSpoolAppendAck, RawSpoolOptions, RawSpoolRecord,
    RawSpoolRecordId, RawSpoolStats, RecoveredRawSpoolRecord,
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
    pub(super) reply: mpsc::Sender<Result<RawSpoolAppendAck>>,
}

pub(super) struct CheckpointCommand {
    pub(super) id: RawSpoolRecordId,
    pub(super) reply: mpsc::Sender<Result<()>>,
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
            .send(RawSpoolCommand::Append(AppendCommand { record, reply }))
            .context("send raw spool append command")?;
        rx.recv().context("receive raw spool append result")?
    }

    pub fn mark_committed(&self, id: RawSpoolRecordId) -> Result<()> {
        self.mark_committed_batch(&[id])
    }

    pub fn mark_committed_batch(&self, ids: &[RawSpoolRecordId]) -> Result<()> {
        if ids.is_empty() {
            return Ok(());
        }
        let mut replies = Vec::with_capacity(ids.len());
        for id in ids {
            let (reply, rx) = mpsc::channel();
            self.commands
                .as_ref()
                .context("raw spool writer is stopped")?
                .send(RawSpoolCommand::Checkpoint(CheckpointCommand {
                    id: *id,
                    reply,
                }))
                .context("send raw spool checkpoint command")?;
            replies.push(rx);
        }
        for rx in replies {
            rx.recv().context("receive raw spool checkpoint result")??;
        }
        Ok(())
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
    let ids = batch.iter().map(|command| command.id).collect::<Vec<_>>();
    match spool.mark_committed_batch(ids) {
        Ok(()) => {
            for command in batch {
                let _ = command.reply.send(Ok(()));
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
