use super::super::Signal;
use super::{Record, RecordId, RecoveredRecord, RECORD_HEADER_BYTES, RECORD_MAGIC};
use anyhow::{bail, Context, Result};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufRead, BufReader, Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

#[derive(Debug)]
pub(super) struct EncodedRecord {
    pub(super) sequence: u64,
    pub(super) body_len: u64,
    pub(super) bytes: Vec<u8>,
}

#[derive(Debug)]
pub(super) struct PreparedRecord {
    pub(super) record: Record,
    pub(super) body_len: u64,
    body_checksum: u64,
}

#[derive(Serialize, Deserialize)]
struct Header {
    sequence: u64,
    signal: String,
    content_type: String,
    content_encoding: Option<String>,
    accepted_at_micros: i64,
    body_checksum: u64,
}

pub(super) fn prepare_append_records(
    records: Vec<Record>,
    max_record_bytes: u64,
) -> Result<Vec<PreparedRecord>> {
    let mut prepared = Vec::with_capacity(records.len());
    for mut record in records {
        if record.accepted_at_micros == 0 {
            record.accepted_at_micros = Utc::now().timestamp_micros();
        }
        if record.compressed_body.len() as u64 > max_record_bytes {
            bail!(
                "raw spool record body has {} bytes; max is {}",
                record.compressed_body.len(),
                max_record_bytes
            );
        }
        let body_len = record.compressed_body.len() as u64;
        let body_checksum = checksum(&record.compressed_body);
        prepared.push(PreparedRecord {
            record,
            body_len,
            body_checksum,
        });
    }
    Ok(prepared)
}

pub(super) fn encode_prepared_records(
    records: Vec<PreparedRecord>,
    base_sequence: u64,
) -> Result<(Vec<EncodedRecord>, Vec<Vec<u8>>)> {
    let mut encoded = Vec::with_capacity(records.len());
    let mut compressed_bodies = Vec::with_capacity(records.len());
    for record in records {
        let sequence = base_sequence.saturating_add(encoded.len() as u64);
        let bytes = encode_prepared_record(sequence, &record)?;
        let compressed_body = record.record.compressed_body;
        encoded.push(EncodedRecord {
            sequence,
            body_len: record.body_len,
            bytes,
        });
        compressed_bodies.push(compressed_body);
    }
    Ok((encoded, compressed_bodies))
}

#[cfg(test)]
pub(super) fn encode_record(sequence: u64, record: &Record) -> Result<Vec<u8>> {
    let prepared = PreparedRecord {
        body_len: record.compressed_body.len() as u64,
        body_checksum: checksum(&record.compressed_body),
        record: record.clone(),
    };
    encode_prepared_record(sequence, &prepared)
}

fn encode_prepared_record(sequence: u64, prepared: &PreparedRecord) -> Result<Vec<u8>> {
    let header = Header {
        sequence,
        signal: prepared.record.signal.as_str().to_string(),
        content_type: prepared.record.content_type.clone(),
        content_encoding: prepared.record.content_encoding.clone(),
        accepted_at_micros: prepared.record.accepted_at_micros,
        body_checksum: prepared.body_checksum,
    };
    let header = serde_json::to_vec(&header).context("serialize raw spool header")?;
    let mut out = Vec::with_capacity(
        RECORD_HEADER_BYTES as usize + header.len() + prepared.record.compressed_body.len(),
    );
    out.extend_from_slice(RECORD_MAGIC);
    out.extend_from_slice(&(header.len() as u32).to_le_bytes());
    out.extend_from_slice(&prepared.body_len.to_le_bytes());
    out.extend_from_slice(&header);
    out.extend_from_slice(&prepared.record.compressed_body);
    Ok(out)
}

#[derive(Clone, Copy, Debug, Default)]
pub(super) struct SegmentScan {
    pub(super) valid_len: u64,
    pub(super) record_count: u64,
    pub(super) seq_lo: u64,
    pub(super) seq_hi: u64,
}

pub(super) fn scan_segment(
    path: &Path,
    segment: u64,
    max_record_bytes: u64,
    completed: &BTreeSet<u64>,
    mut pending: Option<&mut Vec<RecoveredRecord>>,
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
                        out.push(RecoveredRecord {
                            id: RecordId { segment, sequence },
                            record,
                        });
                    }
                }
            }
            Ok(None) => break,
            Err(err)
                if err.kind() == io::ErrorKind::UnexpectedEof
                    || err.kind() == io::ErrorKind::InvalidData =>
            {
                // A torn tail (clean EOF) is the common steady-state case and is
                // expected after a partial write, so it stays quiet. A corrupt
                // record (InvalidData) was never acknowledged to a client, so we
                // drop it and everything after it (framing past a corrupt frame
                // in an append-only log cannot be trusted) and surface it loudly
                // instead of failing the whole boot.
                if err.kind() == io::ErrorKind::InvalidData {
                    tracing::warn!(
                        segment = %path.display(),
                        offset,
                        error = %err,
                        "raw spool record corrupt; truncating segment at offset and recovering preceding records"
                    );
                }
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
) -> io::Result<Option<(u64, Record, u64)>> {
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

    let header: Header = serde_json::from_slice(&header)
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
        Record {
            signal,
            content_type: header.content_type,
            content_encoding: header.content_encoding,
            accepted_at_micros: header.accepted_at_micros,
            compressed_body: body,
        },
        RECORD_HEADER_BYTES + header_len + body_len,
    )))
}

pub(super) fn read_completed_sequences(path: &Path) -> Result<BTreeSet<u64>> {
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

pub(super) fn open_segment_append(dir: &Path, segment: u64) -> Result<File> {
    let path = segment_path(dir, segment);
    OpenOptions::new()
        .create(true)
        .append(true)
        .read(true)
        .open(&path)
        .with_context(|| format!("open raw spool segment {}", path.display()))
}

pub(super) fn segment_ids(dir: &Path) -> Result<Vec<u64>> {
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

pub(super) fn segment_path(dir: &Path, segment: u64) -> PathBuf {
    dir.join(format!("segment-{segment:020}.spool"))
}

pub(super) fn checkpoint_path(dir: &Path) -> PathBuf {
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

pub(super) fn sync_dir(path: &Path) -> Result<()> {
    File::open(path)
        .with_context(|| format!("open dir {} for fsync", path.display()))?
        .sync_all()
        .with_context(|| format!("fsync dir {}", path.display()))
}
