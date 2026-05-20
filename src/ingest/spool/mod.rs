use super::Signal;
use anyhow::{bail, Context, Result};
use chrono::Utc;
use serde::Serialize;
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::PathBuf;
use std::time::{Duration, Instant};

mod codec;
#[cfg(test)]
mod tests;
mod writer;

use codec::{
    checkpoint_path, encode_prepared_records, open_segment_append, prepare_append_records,
    read_completed_sequences, scan_segment, segment_ids, segment_path, sync_dir,
    EncodedRawSpoolRecord, PreparedRawSpoolRecord,
};
pub use writer::RawSpoolWriter;

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

#[derive(Clone, Copy, Debug)]
pub struct RawSpoolAppendBatchStats {
    pub records: usize,
    pub encoded_bytes: u64,
    pub queue_seconds: f64,
    pub wait_seconds: f64,
    pub encode_seconds: f64,
    pub write_seconds: f64,
    pub fsync_seconds: f64,
    pub fsync_count: u64,
}

#[derive(Clone, Copy, Debug)]
pub struct RawSpoolAppendAck {
    pub id: RawSpoolRecordId,
    pub batch_stats: Option<RawSpoolAppendBatchStats>,
}

pub struct RawSpoolAppendBatch {
    pub ids: Vec<RawSpoolRecordId>,
    pub stats: RawSpoolAppendBatchStats,
}

#[derive(Clone, Copy, Debug)]
struct AppendedRawSpoolRecord {
    id: RawSpoolRecordId,
    body_len: u64,
    written: u64,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct RawSpoolAppendSyncStats {
    pub seconds: f64,
    pub file_count: u64,
}

#[derive(Clone, Debug)]
pub struct RawSpoolOptions {
    pub dir: PathBuf,
    pub max_segment_bytes: u64,
    pub max_record_bytes: u64,
    pub max_total_bytes: u64,
    pub append_sync_interval: Duration,
    pub append_sync_bytes: u64,
    pub checkpoint_fsync_records: usize,
    pub checkpoint_fsync_delay: Duration,
}

impl RawSpoolOptions {
    pub fn new(dir: impl Into<PathBuf>) -> Self {
        Self {
            dir: dir.into(),
            max_segment_bytes: DEFAULT_MAX_SEGMENT_BYTES,
            max_record_bytes: DEFAULT_MAX_RECORD_BYTES,
            max_total_bytes: 1024 * 1024 * 1024,
            append_sync_interval: Duration::from_millis(500),
            append_sync_bytes: 16 * 1024 * 1024,
            checkpoint_fsync_records: 1024,
            checkpoint_fsync_delay: Duration::from_millis(1000),
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Serialize)]
pub struct RawSpoolStats {
    pub segment_count: usize,
    pub segment_bytes: u64,
    pub pending_records: usize,
    pub pending_bytes: u64,
    pub unsynced_records: usize,
    pub unsynced_bytes: u64,
    pub unsynced_age_seconds: f64,
    pub append_syncs_total: u64,
    pub append_sync_failures_total: u64,
    pub append_sync_seconds_total: f64,
    pub append_sync_file_fsyncs_total: u64,
    pub healthy: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
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
    append_sync_interval: Duration,
    append_sync_bytes: u64,
    append_dirty_records: usize,
    append_dirty_bytes: u64,
    append_dirty_since: Option<Instant>,
    append_dirty_segments: BTreeSet<u64>,
    append_syncs_total: u64,
    append_sync_failures_total: u64,
    append_sync_seconds_total: f64,
    append_sync_file_fsyncs_total: u64,
    fatal_error: Option<String>,
    checkpoint_fsync_records: usize,
    checkpoint_fsync_delay: Duration,
    checkpoint_dirty_records: usize,
    checkpoint_last_sync: Instant,
    next_sequence: u64,
    #[cfg(test)]
    fail_next_append_sync: bool,
}

impl RawSpool {
    pub fn open(options: RawSpoolOptions) -> Result<Self> {
        let max_segment_bytes = options.max_segment_bytes.max(RECORD_HEADER_BYTES + 1);
        let max_record_bytes = options.max_record_bytes.max(1);
        let max_total_bytes = options.max_total_bytes.max(1);
        let append_sync_interval = options.append_sync_interval.max(Duration::from_millis(1));
        let append_sync_bytes = options.append_sync_bytes.max(1);
        let checkpoint_fsync_records = options.checkpoint_fsync_records.max(1);
        let checkpoint_fsync_delay = options.checkpoint_fsync_delay.max(Duration::from_millis(1));
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
            append_sync_interval,
            append_sync_bytes,
            append_dirty_records: 0,
            append_dirty_bytes: 0,
            append_dirty_since: None,
            append_dirty_segments: BTreeSet::new(),
            append_syncs_total: 0,
            append_sync_failures_total: 0,
            append_sync_seconds_total: 0.0,
            append_sync_file_fsyncs_total: 0,
            fatal_error: None,
            checkpoint_fsync_records,
            checkpoint_fsync_delay,
            checkpoint_dirty_records: 0,
            checkpoint_last_sync: Instant::now(),
            next_sequence: max_sequence.saturating_add(1).max(1),
            #[cfg(test)]
            fail_next_append_sync: false,
        })
    }

    pub fn append(&mut self, record: RawSpoolRecord) -> Result<RawSpoolRecordId> {
        self.append_batch(vec![record]).map(|batch| {
            batch
                .ids
                .into_iter()
                .next()
                .expect("single append returns id")
        })
    }

    pub fn append_batch(&mut self, records: Vec<RawSpoolRecord>) -> Result<RawSpoolAppendBatch> {
        let prepared = prepare_append_records(records, self.max_record_bytes)?;
        self.append_prepared_batch(prepared)
    }

    fn append_prepared_batch(
        &mut self,
        records: Vec<PreparedRawSpoolRecord>,
    ) -> Result<RawSpoolAppendBatch> {
        let base_sequence = self.next_sequence;
        let encode_started = Instant::now();
        let encoded = encode_prepared_records(records, base_sequence)?;
        let encode_seconds = encode_started.elapsed().as_secs_f64();
        let mut appended = self.append_encoded_batch(encoded)?;
        appended.stats.encode_seconds = encode_seconds;
        self.next_sequence = base_sequence.saturating_add(appended.ids.len() as u64);
        Ok(appended)
    }

    fn append_encoded_batch(
        &mut self,
        encoded: Vec<EncodedRawSpoolRecord>,
    ) -> Result<RawSpoolAppendBatch> {
        self.ensure_healthy()?;
        if encoded.is_empty() {
            return Ok(RawSpoolAppendBatch {
                ids: Vec::new(),
                stats: RawSpoolAppendBatchStats {
                    records: 0,
                    encoded_bytes: 0,
                    queue_seconds: 0.0,
                    wait_seconds: 0.0,
                    encode_seconds: 0.0,
                    write_seconds: 0.0,
                    fsync_seconds: 0.0,
                    fsync_count: 0,
                },
            });
        }
        let encoded_bytes = encoded
            .iter()
            .map(|record| record.bytes.len() as u64)
            .sum::<u64>();

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
        let mut write_seconds = 0.0;
        let mut group = Vec::new();
        let mut group_records = Vec::new();
        for record in encoded {
            let active_bytes = self
                .segments
                .get(&self.active_segment)
                .map_or(0, |s| s.bytes);
            if active_bytes > 0
                && active_bytes.saturating_add(record.bytes.len() as u64) > self.max_segment_bytes
            {
                self.flush_append_group(&mut group, &mut group_records, &mut write_seconds)?;
                self.rotate()?;
            }
            let id = RawSpoolRecordId {
                segment: self.active_segment,
                sequence: record.sequence,
            };
            let written = record.bytes.len() as u64;
            group.extend_from_slice(&record.bytes);
            group_records.push(AppendedRawSpoolRecord {
                id,
                body_len: record.body_len,
                written,
            });
            ids.push(id);
        }
        self.flush_append_group(&mut group, &mut group_records, &mut write_seconds)?;
        Ok(RawSpoolAppendBatch {
            stats: RawSpoolAppendBatchStats {
                records: ids.len(),
                encoded_bytes,
                queue_seconds: 0.0,
                wait_seconds: 0.0,
                encode_seconds: 0.0,
                write_seconds,
                fsync_seconds: 0.0,
                fsync_count: 0,
            },
            ids,
        })
    }

    fn flush_append_group(
        &mut self,
        group: &mut Vec<u8>,
        records: &mut Vec<AppendedRawSpoolRecord>,
        write_seconds: &mut f64,
    ) -> Result<()> {
        if records.is_empty() {
            return Ok(());
        }
        let started = Instant::now();
        self.active
            .write_all(group)
            .context("append raw spool record batch")?;
        *write_seconds += started.elapsed().as_secs_f64();
        for record in records.drain(..) {
            self.record_appended(record);
        }
        group.clear();
        Ok(())
    }

    fn record_appended(&mut self, record: AppendedRawSpoolRecord) {
        let state = self
            .segments
            .get_mut(&record.id.segment)
            .expect("active segment is tracked");
        if state.record_count == 0 {
            state.seq_lo = record.id.sequence;
        }
        state.seq_hi = record.id.sequence;
        state.record_count += 1;
        state.bytes = state.bytes.saturating_add(record.written);
        self.total_segment_bytes = self.total_segment_bytes.saturating_add(record.written);
        self.pending.insert(record.id, record.body_len);
        self.total_pending_bytes = self.total_pending_bytes.saturating_add(record.body_len);
        self.mark_append_dirty(record.id.segment, record.written);
    }

    pub fn stats(&self) -> Result<RawSpoolStats> {
        Ok(RawSpoolStats {
            segment_count: self.segments.len(),
            segment_bytes: self.total_segment_bytes,
            pending_records: self.pending.len(),
            pending_bytes: self.total_pending_bytes,
            unsynced_records: self.append_dirty_records,
            unsynced_bytes: self.append_dirty_bytes,
            unsynced_age_seconds: self
                .append_dirty_since
                .map(|started| started.elapsed().as_secs_f64())
                .unwrap_or(0.0),
            append_syncs_total: self.append_syncs_total,
            append_sync_failures_total: self.append_sync_failures_total,
            append_sync_seconds_total: self.append_sync_seconds_total,
            append_sync_file_fsyncs_total: self.append_sync_file_fsyncs_total,
            healthy: self.fatal_error.is_none(),
            error: self.fatal_error.clone(),
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
        self.ensure_healthy()?;
        for id in ids {
            if !self.completed.insert(id.sequence) {
                continue;
            }
            if let Some(body_len) = self.pending.remove(&id) {
                self.total_pending_bytes = self.total_pending_bytes.saturating_sub(body_len);
            }
            writeln!(self.checkpoint, "{}", id.sequence).context("append raw spool checkpoint")?;
            self.checkpoint_dirty_records += 1;
        }
        self.sync_checkpoint_if_due(false)?;
        Ok(())
    }

    fn sync_checkpoint_if_due(&mut self, force: bool) -> Result<()> {
        if self.checkpoint_dirty_records == 0 {
            return Ok(());
        }
        if force
            || self.checkpoint_dirty_records >= self.checkpoint_fsync_records
            || self.checkpoint_last_sync.elapsed() >= self.checkpoint_fsync_delay
        {
            self.checkpoint
                .sync_data()
                .context("fsync raw spool checkpoint batch")?;
            self.checkpoint_dirty_records = 0;
            self.checkpoint_last_sync = Instant::now();
        }
        Ok(())
    }

    fn checkpoint_sync_due_in(&self) -> Option<Duration> {
        (self.checkpoint_dirty_records > 0).then(|| {
            self.checkpoint_fsync_delay
                .saturating_sub(self.checkpoint_last_sync.elapsed())
        })
    }

    fn append_sync_due_in(&self) -> Option<Duration> {
        if self.append_dirty_records == 0 {
            return None;
        }
        if self.append_dirty_bytes >= self.append_sync_bytes {
            return Some(Duration::ZERO);
        }
        self.append_dirty_since
            .map(|started| self.append_sync_interval.saturating_sub(started.elapsed()))
    }

    fn sync_append_if_due(&mut self, force: bool) -> Result<Option<RawSpoolAppendSyncStats>> {
        if self.append_dirty_records == 0 {
            return Ok(None);
        }
        if force
            || self.append_dirty_bytes >= self.append_sync_bytes
            || self
                .append_dirty_since
                .map(|started| started.elapsed() >= self.append_sync_interval)
                .unwrap_or(false)
        {
            return self.sync_append().map(Some);
        }
        Ok(None)
    }

    fn sync_append(&mut self) -> Result<RawSpoolAppendSyncStats> {
        self.ensure_healthy()?;
        if self.append_dirty_records == 0 {
            return Ok(RawSpoolAppendSyncStats::default());
        }
        let started = Instant::now();
        let dirty_segments = self
            .append_dirty_segments
            .iter()
            .copied()
            .collect::<Vec<_>>();
        let mut file_count = 0u64;
        let result = (|| -> Result<()> {
            #[cfg(test)]
            if self.fail_next_append_sync {
                self.fail_next_append_sync = false;
                bail!("injected raw spool append sync failure");
            }
            for segment in dirty_segments {
                if segment == self.active_segment {
                    self.active
                        .sync_data()
                        .context("fsync active raw spool append segment")?;
                } else {
                    OpenOptions::new()
                        .write(true)
                        .read(true)
                        .open(segment_path(&self.dir, segment))
                        .with_context(|| format!("open raw spool segment {segment} for fsync"))?
                        .sync_data()
                        .with_context(|| format!("fsync raw spool segment {segment}"))?;
                }
                file_count += 1;
            }
            Ok(())
        })();
        match result {
            Ok(()) => {
                self.append_dirty_records = 0;
                self.append_dirty_bytes = 0;
                self.append_dirty_since = None;
                self.append_dirty_segments.clear();
                self.append_syncs_total = self.append_syncs_total.saturating_add(1);
                self.append_sync_seconds_total += started.elapsed().as_secs_f64();
                self.append_sync_file_fsyncs_total = self
                    .append_sync_file_fsyncs_total
                    .saturating_add(file_count);
                Ok(RawSpoolAppendSyncStats {
                    seconds: started.elapsed().as_secs_f64(),
                    file_count,
                })
            }
            Err(err) => {
                self.append_sync_failures_total = self.append_sync_failures_total.saturating_add(1);
                let message = err.to_string();
                self.fatal_error = Some(message.clone());
                Err(anyhow::anyhow!(message))
            }
        }
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
        self.checkpoint_dirty_records = 0;
        self.checkpoint_last_sync = Instant::now();
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

    fn mark_append_dirty(&mut self, segment: u64, bytes: u64) {
        self.append_dirty_records += 1;
        self.append_dirty_bytes = self.append_dirty_bytes.saturating_add(bytes);
        self.append_dirty_since.get_or_insert_with(Instant::now);
        self.append_dirty_segments.insert(segment);
    }

    fn ensure_healthy(&self) -> Result<()> {
        if let Some(error) = &self.fatal_error {
            bail!("raw spool append sync failed: {error}");
        }
        Ok(())
    }

    #[cfg(test)]
    fn fail_next_append_sync(&mut self) {
        self.fail_next_append_sync = true;
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
