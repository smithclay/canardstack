use super::Signal;
use anyhow::{bail, Context, Result};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufRead, BufReader, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::mpsc::{self, Receiver, SyncSender};
use std::thread;
use std::time::{Duration, Instant};

const RECORD_MAGIC: &[u8; 8] = b"CSRAW01\n";
const RECORD_HEADER_BYTES: u64 = 8 + 4 + 8;
const DEFAULT_MAX_SEGMENT_BYTES: u64 = 64 * 1024 * 1024;
const DEFAULT_MAX_RECORD_BYTES: u64 = 8 * 1024 * 1024;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct RawSpoolRecordId {
    pub segment: u64,
    pub sequence: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RawSpoolRecord {
    pub signal: Signal,
    pub content_type: String,
    pub content_encoding: Option<String>,
    pub accepted_at_micros: i64,
    pub compressed_body: Vec<u8>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecoveredRawSpoolRecord {
    pub id: RawSpoolRecordId,
    pub record: RawSpoolRecord,
}

#[derive(Clone, Debug)]
pub struct RawSpoolOptions {
    pub dir: PathBuf,
    pub max_segment_bytes: u64,
    pub max_record_bytes: u64,
    pub max_total_bytes: u64,
}

impl RawSpoolOptions {
    pub fn new(dir: impl Into<PathBuf>) -> Self {
        Self {
            dir: dir.into(),
            max_segment_bytes: DEFAULT_MAX_SEGMENT_BYTES,
            max_record_bytes: DEFAULT_MAX_RECORD_BYTES,
            max_total_bytes: 1024 * 1024 * 1024,
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize)]
pub struct RawSpoolStats {
    pub segment_count: usize,
    pub segment_bytes: u64,
    pub pending_records: usize,
    pub pending_bytes: u64,
}

#[derive(Clone, Copy, Debug)]
pub struct RawSpoolFull {
    pub required_bytes: u64,
    pub max_bytes: u64,
}

impl std::fmt::Display for RawSpoolFull {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "raw spool needs {} bytes but max is {}",
            self.required_bytes, self.max_bytes
        )
    }
}

impl std::error::Error for RawSpoolFull {}

#[derive(Clone, Copy, Debug, Default)]
struct SegmentState {
    bytes: u64,
    record_count: u64,
    seq_lo: u64,
    seq_hi: u64,
}

pub struct RawSpool {
    dir: PathBuf,
    max_segment_bytes: u64,
    max_record_bytes: u64,
    max_total_bytes: u64,
    completed: BTreeSet<u64>,
    segments: BTreeMap<u64, SegmentState>,
    pending: BTreeMap<RawSpoolRecordId, u64>,
    total_segment_bytes: u64,
    total_pending_bytes: u64,
    active_segment: u64,
    active: File,
    checkpoint: File,
    next_sequence: u64,
}

impl RawSpool {
    pub fn open(options: RawSpoolOptions) -> Result<Self> {
        let max_segment_bytes = options.max_segment_bytes.max(RECORD_HEADER_BYTES + 1);
        let max_record_bytes = options.max_record_bytes.max(1);
        let max_total_bytes = options.max_total_bytes.max(1);
        fs::create_dir_all(&options.dir)
            .with_context(|| format!("create raw spool dir {}", options.dir.display()))?;
        sync_dir(&options.dir)?;

        let checkpoint_path = checkpoint_path(&options.dir);
        let completed = read_completed_sequences(&checkpoint_path)?;
        let mut existing_segments = segment_ids(&options.dir)?;
        let mut max_sequence = 0u64;
        let mut segments = BTreeMap::new();
        let mut pending = BTreeMap::new();
        let mut total_segment_bytes = 0u64;
        let mut total_pending_bytes = 0u64;
        for segment in &existing_segments {
            let path = segment_path(&options.dir, *segment);
            let mut recovered = Vec::new();
            let scan = scan_segment(
                &path,
                *segment,
                max_record_bytes,
                &completed,
                Some(&mut recovered),
            )?;
            OpenOptions::new()
                .write(true)
                .open(&path)
                .with_context(|| format!("open raw spool segment {}", path.display()))?
                .set_len(scan.valid_len)
                .with_context(|| format!("truncate raw spool segment {}", path.display()))?;
            max_sequence = max_sequence.max(scan.seq_hi);
            total_segment_bytes = total_segment_bytes.saturating_add(scan.valid_len);
            for record in recovered {
                let body_len = record.record.compressed_body.len() as u64;
                total_pending_bytes = total_pending_bytes.saturating_add(body_len);
                pending.insert(record.id, body_len);
            }
            segments.insert(
                *segment,
                SegmentState {
                    bytes: scan.valid_len,
                    record_count: scan.record_count,
                    seq_lo: scan.seq_lo,
                    seq_hi: scan.seq_hi,
                },
            );
        }

        if existing_segments.is_empty() {
            existing_segments.push(1);
            let path = segment_path(&options.dir, 1);
            OpenOptions::new()
                .create(true)
                .append(true)
                .open(&path)
                .with_context(|| format!("create raw spool segment {}", path.display()))?;
            sync_dir(&options.dir)?;
            segments.insert(1, SegmentState::default());
        }

        let mut active_segment = *existing_segments.last().unwrap_or(&1);
        let active_len = segments.get(&active_segment).map_or(0, |s| s.bytes);
        if active_len >= max_segment_bytes {
            active_segment += 1;
            let path = segment_path(&options.dir, active_segment);
            OpenOptions::new()
                .create_new(true)
                .append(true)
                .open(&path)
                .with_context(|| format!("create raw spool segment {}", path.display()))?;
            sync_dir(&options.dir)?;
            segments.insert(active_segment, SegmentState::default());
        }

        let active = open_segment_append(&options.dir, active_segment)?;
        let checkpoint = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(&checkpoint_path)
            .with_context(|| format!("open raw spool checkpoint {}", checkpoint_path.display()))?;

        Ok(Self {
            dir: options.dir,
            max_segment_bytes,
            max_record_bytes,
            max_total_bytes,
            completed,
            segments,
            pending,
            total_segment_bytes,
            total_pending_bytes,
            active_segment,
            active,
            checkpoint,
            next_sequence: max_sequence.saturating_add(1).max(1),
        })
    }

    pub fn append(&mut self, record: RawSpoolRecord) -> Result<RawSpoolRecordId> {
        self.append_batch(vec![record]).map(|mut ids| ids.remove(0))
    }

    pub fn append_batch(&mut self, records: Vec<RawSpoolRecord>) -> Result<Vec<RawSpoolRecordId>> {
        if records.is_empty() {
            return Ok(Vec::new());
        }
        let mut encoded = Vec::with_capacity(records.len());
        let mut encoded_bytes = 0u64;
        for mut record in records {
            if record.accepted_at_micros == 0 {
                record.accepted_at_micros = Utc::now().timestamp_micros();
            }
            if record.compressed_body.len() as u64 > self.max_record_bytes {
                bail!(
                    "raw spool record body has {} bytes; max is {}",
                    record.compressed_body.len(),
                    self.max_record_bytes
                );
            }
            let sequence = self.next_sequence + encoded.len() as u64;
            let body_len = record.compressed_body.len() as u64;
            let bytes = encode_record(sequence, &record)?;
            encoded_bytes = encoded_bytes.saturating_add(bytes.len() as u64);
            encoded.push((sequence, bytes, body_len));
        }

        let mut required_bytes = self.total_segment_bytes.saturating_add(encoded_bytes);
        if required_bytes > self.max_total_bytes {
            let _removed = self.reclaim_committed_segments()?;
            required_bytes = self.total_segment_bytes.saturating_add(encoded_bytes);
        }
        if required_bytes > self.max_total_bytes {
            return Err(RawSpoolFull {
                required_bytes,
                max_bytes: self.max_total_bytes,
            }
            .into());
        }

        let mut ids = Vec::with_capacity(encoded.len());
        let mut active_dirty = false;
        for (sequence, bytes, body_len) in encoded {
            let active_bytes = self
                .segments
                .get(&self.active_segment)
                .map_or(0, |s| s.bytes);
            if active_bytes > 0
                && active_bytes.saturating_add(bytes.len() as u64) > self.max_segment_bytes
            {
                if active_dirty {
                    self.active
                        .sync_data()
                        .context("fsync raw spool record batch before rotate")?;
                }
                self.rotate()?;
            }
            let id = RawSpoolRecordId {
                segment: self.active_segment,
                sequence,
            };
            self.active
                .write_all(&bytes)
                .context("append raw spool record")?;
            let written = bytes.len() as u64;
            let state = self
                .segments
                .get_mut(&self.active_segment)
                .expect("active segment is tracked");
            if state.record_count == 0 {
                state.seq_lo = sequence;
            }
            state.seq_hi = sequence;
            state.record_count += 1;
            state.bytes = state.bytes.saturating_add(written);
            self.total_segment_bytes = self.total_segment_bytes.saturating_add(written);
            self.pending.insert(id, body_len);
            self.total_pending_bytes = self.total_pending_bytes.saturating_add(body_len);
            self.next_sequence = self.next_sequence.saturating_add(1);
            active_dirty = true;
            ids.push(id);
        }
        if active_dirty {
            self.active
                .sync_data()
                .context("fsync raw spool record batch")?;
        }
        Ok(ids)
    }

    pub fn stats(&self) -> Result<RawSpoolStats> {
        Ok(RawSpoolStats {
            segment_count: self.segments.len(),
            segment_bytes: self.total_segment_bytes,
            pending_records: self.pending.len(),
            pending_bytes: self.total_pending_bytes,
        })
    }

    pub fn recover_pending(&self) -> Result<Vec<RecoveredRawSpoolRecord>> {
        let mut pending = Vec::new();
        for segment in self.segments.keys() {
            let path = segment_path(&self.dir, *segment);
            scan_segment(
                &path,
                *segment,
                self.max_record_bytes,
                &self.completed,
                Some(&mut pending),
            )?;
        }
        Ok(pending)
    }

    pub fn mark_committed(&mut self, id: RawSpoolRecordId) -> Result<()> {
        self.mark_committed_batch(vec![id])
    }

    pub fn mark_committed_batch(&mut self, ids: Vec<RawSpoolRecordId>) -> Result<()> {
        let mut wrote = false;
        for id in ids {
            if !self.completed.insert(id.sequence) {
                continue;
            }
            if let Some(body_len) = self.pending.remove(&id) {
                self.total_pending_bytes = self.total_pending_bytes.saturating_sub(body_len);
            }
            writeln!(self.checkpoint, "{}", id.sequence).context("append raw spool checkpoint")?;
            wrote = true;
        }
        if wrote {
            self.checkpoint
                .sync_data()
                .context("fsync raw spool checkpoint batch")?;
        }
        Ok(())
    }

    pub fn reclaim_committed_segments(&mut self) -> Result<usize> {
        let candidates: Vec<u64> = self
            .segments
            .keys()
            .copied()
            .filter(|segment| *segment != self.active_segment)
            .collect();
        let mut removed = 0usize;
        let mut pruned_completed = false;
        for segment in candidates {
            if self.segment_has_pending(segment) {
                continue;
            }
            let path = segment_path(&self.dir, segment);
            fs::remove_file(&path).with_context(|| {
                format!("remove committed raw spool segment {}", path.display())
            })?;
            if let Some(state) = self.segments.remove(&segment) {
                self.total_segment_bytes = self.total_segment_bytes.saturating_sub(state.bytes);
                if state.record_count > 0 {
                    let stale: Vec<u64> = self
                        .completed
                        .range(state.seq_lo..=state.seq_hi)
                        .copied()
                        .collect();
                    for sequence in stale {
                        self.completed.remove(&sequence);
                        pruned_completed = true;
                    }
                }
            }
            removed += 1;
        }
        if removed > 0 {
            sync_dir(&self.dir)?;
        }
        if pruned_completed {
            self.rewrite_checkpoint()?;
        }
        Ok(removed)
    }

    fn segment_has_pending(&self, segment: u64) -> bool {
        let lo = RawSpoolRecordId {
            segment,
            sequence: 0,
        };
        let hi = RawSpoolRecordId {
            segment,
            sequence: u64::MAX,
        };
        self.pending.range(lo..=hi).next().is_some()
    }

    fn rewrite_checkpoint(&mut self) -> Result<()> {
        let path = checkpoint_path(&self.dir);
        let tmp = self.dir.join("checkpoint.log.tmp");
        let mut contents = String::new();
        for sequence in &self.completed {
            contents.push_str(&sequence.to_string());
            contents.push('\n');
        }
        {
            let mut file = OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&tmp)
                .with_context(|| format!("create raw spool checkpoint temp {}", tmp.display()))?;
            file.write_all(contents.as_bytes())
                .context("write compacted raw spool checkpoint")?;
            file.sync_all()
                .context("fsync compacted raw spool checkpoint")?;
        }
        fs::rename(&tmp, &path)
            .with_context(|| format!("replace raw spool checkpoint {}", path.display()))?;
        sync_dir(&self.dir)?;
        self.checkpoint = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(&path)
            .with_context(|| format!("reopen raw spool checkpoint {}", path.display()))?;
        Ok(())
    }

    #[cfg(test)]
    fn segment_path(&self, segment: u64) -> PathBuf {
        segment_path(&self.dir, segment)
    }

    fn rotate(&mut self) -> Result<()> {
        self.active_segment = self.active_segment.saturating_add(1);
        let path = segment_path(&self.dir, self.active_segment);
        self.active = OpenOptions::new()
            .create_new(true)
            .append(true)
            .open(&path)
            .with_context(|| format!("create raw spool segment {}", path.display()))?;
        sync_dir(&self.dir)?;
        self.segments
            .insert(self.active_segment, SegmentState::default());
        Ok(())
    }
}

#[derive(Clone)]
pub struct RawSpoolWriter {
    commands: SyncSender<RawSpoolCommand>,
}

struct AppendCommand {
    record: RawSpoolRecord,
    reply: mpsc::Sender<Result<RawSpoolRecordId>>,
}

struct CheckpointCommand {
    id: RawSpoolRecordId,
    reply: mpsc::Sender<Result<()>>,
}

enum RawSpoolCommand {
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
        let (commands, receiver) = mpsc::sync_channel(queue_capacity.max(1));
        thread::Builder::new()
            .name("canardstack-raw-spool-writer".to_string())
            .spawn(move || {
                run_raw_spool_writer(spool, receiver, max_batch_records.max(1), max_batch_delay)
            })
            .context("spawn raw spool writer thread")?;
        Ok(Self { commands })
    }

    pub fn append(&self, record: RawSpoolRecord) -> Result<RawSpoolRecordId> {
        let (reply, rx) = mpsc::channel();
        self.commands
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
            .send(RawSpoolCommand::RecoverPending { reply })
            .context("send raw spool recovery command")?;
        rx.recv().context("receive raw spool recovery result")?
    }

    pub fn stats(&self) -> Result<RawSpoolStats> {
        let (reply, rx) = mpsc::channel();
        self.commands
            .send(RawSpoolCommand::Stats { reply })
            .context("send raw spool stats command")?;
        rx.recv().context("receive raw spool stats result")?
    }
}

fn run_raw_spool_writer(
    mut spool: RawSpool,
    receiver: Receiver<RawSpoolCommand>,
    max_batch_records: usize,
    max_batch_delay: Duration,
) {
    let mut deferred = VecDeque::new();
    loop {
        let command = match deferred.pop_front() {
            Some(command) => command,
            None => match receiver.recv() {
                Ok(command) => command,
                Err(_) => break,
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
    }
}

fn handle_append_batch(
    spool: &mut RawSpool,
    first: AppendCommand,
    receiver: &Receiver<RawSpoolCommand>,
    deferred: &mut VecDeque<RawSpoolCommand>,
    max_batch_records: usize,
    max_batch_delay: Duration,
) {
    let mut batch = vec![first];
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
    match spool.append_batch(records) {
        Ok(ids) => {
            for (reply, id) in replies.into_iter().zip(ids) {
                let _ = reply.send(Ok(id));
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

fn collect_append_batch(
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

fn handle_checkpoint_batch(
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

fn collect_batch<T>(
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

pub fn raw_spool_full_info(err: &anyhow::Error) -> Option<&RawSpoolFull> {
    err.downcast_ref::<RawSpoolFull>()
}

impl RawSpoolRecord {
    pub fn new(
        signal: Signal,
        content_type: impl Into<String>,
        content_encoding: Option<String>,
        compressed_body: impl Into<Vec<u8>>,
    ) -> Self {
        Self {
            signal,
            content_type: content_type.into(),
            content_encoding,
            accepted_at_micros: Utc::now().timestamp_micros(),
            compressed_body: compressed_body.into(),
        }
    }
}

#[derive(Serialize, Deserialize)]
struct RawSpoolHeader {
    sequence: u64,
    signal: String,
    content_type: String,
    content_encoding: Option<String>,
    accepted_at_micros: i64,
    body_checksum: u64,
}

fn encode_record(sequence: u64, record: &RawSpoolRecord) -> Result<Vec<u8>> {
    let header = RawSpoolHeader {
        sequence,
        signal: record.signal.as_str().to_string(),
        content_type: record.content_type.clone(),
        content_encoding: record.content_encoding.clone(),
        accepted_at_micros: record.accepted_at_micros,
        body_checksum: checksum(&record.compressed_body),
    };
    let header = serde_json::to_vec(&header).context("serialize raw spool header")?;
    let mut out = Vec::with_capacity(
        RECORD_HEADER_BYTES as usize + header.len() + record.compressed_body.len(),
    );
    out.extend_from_slice(RECORD_MAGIC);
    out.extend_from_slice(&(header.len() as u32).to_le_bytes());
    out.extend_from_slice(&(record.compressed_body.len() as u64).to_le_bytes());
    out.extend_from_slice(&header);
    out.extend_from_slice(&record.compressed_body);
    Ok(out)
}

#[derive(Clone, Copy, Debug, Default)]
struct SegmentScan {
    valid_len: u64,
    record_count: u64,
    seq_lo: u64,
    seq_hi: u64,
}

fn scan_segment(
    path: &Path,
    segment: u64,
    max_record_bytes: u64,
    completed: &BTreeSet<u64>,
    mut pending: Option<&mut Vec<RecoveredRawSpoolRecord>>,
) -> Result<SegmentScan> {
    let mut file =
        File::open(path).with_context(|| format!("open raw spool segment {}", path.display()))?;
    let mut scan = SegmentScan::default();
    loop {
        let offset = scan.valid_len;
        match read_record_at(&mut file, max_record_bytes) {
            Ok(Some((sequence, record, bytes_read))) => {
                scan.valid_len = scan.valid_len.saturating_add(bytes_read);
                if scan.record_count == 0 {
                    scan.seq_lo = sequence;
                }
                scan.seq_hi = scan.seq_hi.max(sequence);
                scan.record_count += 1;
                if !completed.contains(&sequence) {
                    if let Some(out) = pending.as_deref_mut() {
                        out.push(RecoveredRawSpoolRecord {
                            id: RawSpoolRecordId { segment, sequence },
                            record,
                        });
                    }
                }
            }
            Ok(None) => break,
            Err(err) if err.kind() == io::ErrorKind::UnexpectedEof => {
                file.seek(SeekFrom::Start(offset)).ok();
                break;
            }
            Err(err) => {
                return Err(err)
                    .with_context(|| format!("read raw spool segment {}", path.display()))
            }
        }
    }
    Ok(scan)
}

fn read_record_at(
    file: &mut File,
    max_record_bytes: u64,
) -> io::Result<Option<(u64, RawSpoolRecord, u64)>> {
    let mut magic = [0u8; 8];
    let mut read = 0usize;
    while read < magic.len() {
        match file.read(&mut magic[read..])? {
            0 if read == 0 => return Ok(None),
            0 => return Err(io::Error::from(io::ErrorKind::UnexpectedEof)),
            n => read += n,
        }
    }
    if &magic != RECORD_MAGIC {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid raw spool record magic",
        ));
    }

    let mut header_len = [0u8; 4];
    file.read_exact(&mut header_len)?;
    let header_len = u32::from_le_bytes(header_len) as u64;
    let mut body_len = [0u8; 8];
    file.read_exact(&mut body_len)?;
    let body_len = u64::from_le_bytes(body_len);
    if header_len == 0 || body_len > max_record_bytes {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid raw spool record length",
        ));
    }
    if header_len > usize::MAX as u64 || body_len > usize::MAX as u64 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "raw spool record is too large for this platform",
        ));
    }

    let mut header = vec![0; header_len as usize];
    file.read_exact(&mut header)?;
    let mut body = vec![0; body_len as usize];
    file.read_exact(&mut body)?;

    let header: RawSpoolHeader = serde_json::from_slice(&header)
        .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))?;
    if header.body_checksum != checksum(&body) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "raw spool record checksum mismatch",
        ));
    }
    let signal = signal_from_str(&header.signal)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "invalid raw spool signal"))?;
    Ok(Some((
        header.sequence,
        RawSpoolRecord {
            signal,
            content_type: header.content_type,
            content_encoding: header.content_encoding,
            accepted_at_micros: header.accepted_at_micros,
            compressed_body: body,
        },
        RECORD_HEADER_BYTES + header_len + body_len,
    )))
}

fn read_completed_sequences(path: &Path) -> Result<BTreeSet<u64>> {
    if !path.exists() {
        return Ok(BTreeSet::new());
    }
    let file = File::open(path)
        .with_context(|| format!("open raw spool checkpoint {}", path.display()))?;
    let mut completed = BTreeSet::new();
    for line in BufReader::new(file).lines() {
        let line = line.context("read raw spool checkpoint")?;
        if line.trim().is_empty() {
            continue;
        }
        let sequence = line
            .trim()
            .parse::<u64>()
            .with_context(|| format!("parse raw spool checkpoint sequence {line:?}"))?;
        completed.insert(sequence);
    }
    Ok(completed)
}

fn open_segment_append(dir: &Path, segment: u64) -> Result<File> {
    let path = segment_path(dir, segment);
    OpenOptions::new()
        .create(true)
        .append(true)
        .read(true)
        .open(&path)
        .with_context(|| format!("open raw spool segment {}", path.display()))
}

fn segment_ids(dir: &Path) -> Result<Vec<u64>> {
    let mut ids = Vec::new();
    if !dir.exists() {
        return Ok(ids);
    }
    for entry in
        fs::read_dir(dir).with_context(|| format!("read raw spool dir {}", dir.display()))?
    {
        let entry = entry?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        let Some(raw) = name
            .strip_prefix("segment-")
            .and_then(|name| name.strip_suffix(".spool"))
        else {
            continue;
        };
        if let Ok(id) = raw.parse::<u64>() {
            ids.push(id);
        }
    }
    ids.sort_unstable();
    Ok(ids)
}

fn segment_path(dir: &Path, segment: u64) -> PathBuf {
    dir.join(format!("segment-{segment:020}.spool"))
}

fn checkpoint_path(dir: &Path) -> PathBuf {
    dir.join("checkpoint.log")
}

fn signal_from_str(value: &str) -> Option<Signal> {
    match value {
        "logs" => Some(Signal::Logs),
        "spans" => Some(Signal::Spans),
        "metric_gauge" => Some(Signal::MetricGauge),
        "metric_sum" => Some(Signal::MetricSum),
        _ => None,
    }
}

fn checksum(bytes: &[u8]) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325u64;
    for byte in bytes {
        hash ^= *byte as u64;
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

fn sync_dir(path: &Path) -> Result<()> {
    File::open(path)
        .with_context(|| format!("open dir {} for fsync", path.display()))?
        .sync_all()
        .with_context(|| format!("fsync dir {}", path.display()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn options(dir: &Path) -> RawSpoolOptions {
        RawSpoolOptions {
            dir: dir.to_path_buf(),
            max_segment_bytes: 256,
            max_record_bytes: 1024,
            max_total_bytes: 1024 * 1024,
        }
    }

    fn record(body: &[u8]) -> RawSpoolRecord {
        RawSpoolRecord {
            signal: Signal::Logs,
            content_type: "application/x-protobuf".to_string(),
            content_encoding: Some("gzip".to_string()),
            accepted_at_micros: 1_234_567,
            compressed_body: body.to_vec(),
        }
    }

    #[test]
    fn raw_spool_recovers_fsynced_uncommitted_records() {
        let dir = tempdir().unwrap();
        let mut spool = RawSpool::open(options(dir.path())).unwrap();
        let id = spool.append(record(b"request-body")).unwrap();
        drop(spool);

        let spool = RawSpool::open(options(dir.path())).unwrap();
        let pending = spool.recover_pending().unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].id, id);
        assert_eq!(pending[0].record, record(b"request-body"));
    }

    #[test]
    fn raw_spool_checkpoint_skips_committed_records() {
        let dir = tempdir().unwrap();
        let mut spool = RawSpool::open(options(dir.path())).unwrap();
        let first = spool.append(record(b"first")).unwrap();
        let second = spool.append(record(b"second")).unwrap();
        spool.mark_committed(first).unwrap();
        drop(spool);

        let spool = RawSpool::open(options(dir.path())).unwrap();
        let pending = spool.recover_pending().unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].id, second);
        assert_eq!(pending[0].record.compressed_body, b"second");
    }

    #[test]
    fn raw_spool_ignores_and_truncates_torn_tail() {
        let dir = tempdir().unwrap();
        let mut spool = RawSpool::open(options(dir.path())).unwrap();
        let id = spool.append(record(b"complete")).unwrap();
        let path = spool.segment_path(id.segment);
        drop(spool);

        OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap()
            .write_all(&RECORD_MAGIC[..4])
            .unwrap();

        let spool = RawSpool::open(options(dir.path())).unwrap();
        let pending = spool.recover_pending().unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].id, id);
        assert_eq!(
            fs::metadata(path).unwrap().len(),
            encode_record(id.sequence, &record(b"complete"))
                .unwrap()
                .len() as u64
        );
    }

    #[test]
    fn raw_spool_reclaims_fully_committed_closed_segments() {
        let dir = tempdir().unwrap();
        let mut opts = options(dir.path());
        opts.max_segment_bytes = 128;
        let mut spool = RawSpool::open(opts.clone()).unwrap();
        let first = spool.append(record(b"first-payload")).unwrap();
        let second = spool.append(record(b"second-payload")).unwrap();
        assert_ne!(first.segment, second.segment);

        spool.mark_committed(first).unwrap();
        let removed = spool.reclaim_committed_segments().unwrap();
        assert_eq!(removed, 1);
        assert!(!spool.segment_path(first.segment).exists());
        assert!(spool.segment_path(second.segment).exists());

        let pending = spool.recover_pending().unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].id, second);
    }

    #[test]
    fn raw_spool_reclaims_committed_closed_segments_before_full() {
        let dir = tempdir().unwrap();
        let mut opts = options(dir.path());
        opts.max_segment_bytes = 128;
        opts.max_total_bytes = 500;
        let mut spool = RawSpool::open(opts).unwrap();

        for _ in 0..5 {
            let id = spool.append(record(b"committed-payload")).unwrap();
            spool.mark_committed(id).unwrap();
        }

        assert_eq!(spool.recover_pending().unwrap().len(), 0);
        assert!(
            spool.stats().unwrap().segment_bytes <= 500,
            "committed closed segments should be reclaimed before reporting full"
        );
    }

    #[test]
    fn raw_spool_compacts_checkpoint_on_reclaim() {
        let dir = tempdir().unwrap();
        let mut opts = options(dir.path());
        opts.max_segment_bytes = 128;
        let mut spool = RawSpool::open(opts.clone()).unwrap();

        let first = spool.append(record(b"first-payload")).unwrap();
        let second = spool.append(record(b"second-payload")).unwrap();
        assert_ne!(first.segment, second.segment);
        spool.mark_committed(first).unwrap();
        spool.mark_committed(second).unwrap();
        assert_eq!(spool.completed.len(), 2);

        let removed = spool.reclaim_committed_segments().unwrap();
        assert_eq!(removed, 1);

        // the reclaimed segment's committed sequence is dropped from the in-memory set
        assert!(!spool.completed.contains(&first.sequence));
        assert!(spool.completed.contains(&second.sequence));

        // the on-disk checkpoint log is rewritten to match
        let persisted = read_completed_sequences(&checkpoint_path(dir.path())).unwrap();
        assert_eq!(persisted, spool.completed);

        let stats = spool.stats().unwrap();
        assert_eq!(stats.segment_count, 1);
        assert_eq!(stats.pending_records, 0);

        // reopening reflects the compacted checkpoint without re-recovering pruned records
        let reopened = RawSpool::open(opts).unwrap();
        assert_eq!(reopened.completed, spool.completed);
        assert_eq!(reopened.recover_pending().unwrap().len(), 0);
    }

    #[test]
    fn raw_spool_stats_track_pending_incrementally() {
        let dir = tempdir().unwrap();
        let mut spool = RawSpool::open(options(dir.path())).unwrap();

        let first = spool.append(record(b"first")).unwrap();
        let _second = spool.append(record(b"second")).unwrap();
        let stats = spool.stats().unwrap();
        assert_eq!(stats.pending_records, 2);
        assert_eq!(
            stats.pending_bytes,
            b"first".len() as u64 + b"second".len() as u64
        );

        spool.mark_committed(first).unwrap();
        let stats = spool.stats().unwrap();
        assert_eq!(stats.pending_records, 1);
        assert_eq!(stats.pending_bytes, b"second".len() as u64);
    }

    #[test]
    fn raw_spool_rejects_records_over_total_byte_limit() {
        let dir = tempdir().unwrap();
        let mut opts = options(dir.path());
        opts.max_total_bytes = 32;
        let mut spool = RawSpool::open(opts).unwrap();
        let err = spool.append(record(b"too-large-for-limit")).unwrap_err();
        assert!(raw_spool_full_info(&err).is_some(), "{err:?}");
    }

    #[test]
    fn raw_spool_group_commit_collects_until_record_limit() {
        let (reply, _reply_rx) = mpsc::channel();
        let first = AppendCommand {
            record: record(b"first"),
            reply,
        };
        let (tx, rx) = mpsc::sync_channel(4);
        let (reply, _reply_rx) = mpsc::channel();
        tx.send(RawSpoolCommand::Append(AppendCommand {
            record: record(b"second"),
            reply,
        }))
        .unwrap();

        let mut deferred = VecDeque::new();
        let mut batch = vec![first];
        collect_batch(
            &rx,
            &mut deferred,
            2,
            Duration::from_secs(5),
            &mut batch,
            |command| match command {
                RawSpoolCommand::Append(append) => Ok(append),
                other => Err(other),
            },
        );

        assert_eq!(batch.len(), 2);
        assert!(deferred.is_empty());
    }

    #[test]
    fn raw_spool_append_batch_defers_checkpoint_and_keeps_collecting_appends() {
        let (reply, _reply_rx) = mpsc::channel();
        let first = AppendCommand {
            record: record(b"first"),
            reply,
        };
        let (tx, rx) = mpsc::sync_channel(4);
        let (checkpoint_reply, _checkpoint_rx) = mpsc::channel();
        tx.send(RawSpoolCommand::Checkpoint(CheckpointCommand {
            id: RawSpoolRecordId {
                segment: 1,
                sequence: 1,
            },
            reply: checkpoint_reply,
        }))
        .unwrap();
        let (reply, _reply_rx) = mpsc::channel();
        tx.send(RawSpoolCommand::Append(AppendCommand {
            record: record(b"second"),
            reply,
        }))
        .unwrap();

        let mut deferred = VecDeque::new();
        let mut batch = vec![first];
        collect_append_batch(&rx, &mut deferred, 2, Duration::from_secs(5), &mut batch);

        assert_eq!(batch.len(), 2);
        assert_eq!(deferred.len(), 1);
        assert!(matches!(
            deferred.front(),
            Some(RawSpoolCommand::Checkpoint(_))
        ));
    }

    #[test]
    fn raw_spool_group_commit_delay_flushes_partial_batch() {
        let (_tx, rx) = mpsc::sync_channel(4);
        let (reply, _reply_rx) = mpsc::channel();
        let first = AppendCommand {
            record: record(b"first"),
            reply,
        };
        let mut deferred = VecDeque::new();
        let mut batch = vec![first];
        let started = Instant::now();

        collect_batch(
            &rx,
            &mut deferred,
            64,
            Duration::from_millis(10),
            &mut batch,
            |command| match command {
                RawSpoolCommand::Append(append) => Ok(append),
                other => Err(other),
            },
        );

        assert_eq!(batch.len(), 1);
        assert!(deferred.is_empty());
        assert!(started.elapsed() >= Duration::from_millis(5));
    }
}
