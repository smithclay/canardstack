use super::codec::{
    checkpoint_path, encode_record, prepare_append_records, read_completed_sequences,
};
use super::writer::{
    collect_append_batch, collect_batch, handle_append_batch, handle_checkpoint_batch,
    AppendCommand, CheckpointCommand, RawSpoolCommand,
};
use super::*;
use std::collections::VecDeque;
use std::path::Path;
use std::sync::mpsc;
use tempfile::tempdir;

fn options(dir: &Path) -> RawSpoolOptions {
    RawSpoolOptions {
        dir: dir.to_path_buf(),
        max_segment_bytes: 256,
        max_record_bytes: 1024,
        max_total_bytes: 1024 * 1024,
        append_sync_interval: Duration::from_millis(500),
        append_sync_bytes: 16 * 1024 * 1024,
        checkpoint_fsync_records: 1,
        checkpoint_fsync_delay: Duration::from_millis(1),
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

fn append_command(
    body: &[u8],
    _sequence: u64,
) -> (AppendCommand, mpsc::Receiver<Result<RawSpoolAppendAck>>) {
    let (reply, rx) = mpsc::channel();
    let record = prepare_append_records(vec![record(body)], 1024)
        .unwrap()
        .into_iter()
        .next()
        .unwrap();
    (
        AppendCommand {
            record,
            queued_at: Instant::now(),
            reply,
        },
        rx,
    )
}

fn checkpoint_command(
    ids: Vec<RawSpoolRecordId>,
) -> (
    CheckpointCommand,
    mpsc::Receiver<Result<RawSpoolCheckpointBatchStats>>,
) {
    let (reply, rx) = mpsc::channel();
    (
        CheckpointCommand {
            ids,
            queued_at: Instant::now(),
            reply,
        },
        rx,
    )
}

#[test]
fn raw_spool_recovers_written_uncommitted_records() {
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
fn raw_spool_append_batch_reports_write_stats_without_fsyncing() {
    let dir = tempdir().unwrap();
    let mut spool = RawSpool::open(options(dir.path())).unwrap();
    let appended = spool
        .append_batch(vec![record(b"first"), record(b"second")])
        .unwrap();

    assert_eq!(appended.ids.len(), 2);
    assert_eq!(appended.stats.records, 2);
    assert!(appended.stats.encoded_bytes > 0);
    assert!(appended.stats.write_seconds >= 0.0);
    assert_eq!(appended.stats.fsync_seconds, 0.0);
    assert_eq!(appended.stats.fsync_count, 0);
    let stats = spool.stats().unwrap();
    assert_eq!(stats.unsynced_records, 2);
    assert!(stats.unsynced_bytes > 0);
    assert_eq!(stats.append_syncs_total, 0);
}

#[test]
fn raw_spool_writer_reports_batch_wait_stats_once() {
    let dir = tempdir().unwrap();
    let mut spool = RawSpool::open(options(dir.path())).unwrap();
    let (first, first_rx) = append_command(b"first", 1);
    let (tx, rx) = mpsc::sync_channel(4);
    let (second, second_rx) = append_command(b"second", 2);
    tx.send(RawSpoolCommand::Append(second)).unwrap();
    let mut deferred = VecDeque::new();

    handle_append_batch(
        &mut spool,
        first,
        &rx,
        &mut deferred,
        2,
        Duration::from_secs(5),
    );

    let first_ack = first_rx.recv().unwrap().unwrap();
    let second_ack = second_rx.recv().unwrap().unwrap();
    let stats = first_ack.batch_stats.unwrap();
    assert_eq!(stats.records, 2);
    assert_eq!(stats.fsync_count, 0);
    assert!(stats.wait_seconds >= 0.0);
    assert!(second_ack.batch_stats.is_none());
    assert_eq!(spool.stats().unwrap().append_syncs_total, 0);
}

#[test]
fn raw_spool_writer_batches_checkpoint_commands_and_reports_stats_once() {
    let dir = tempdir().unwrap();
    let mut spool = RawSpool::open(options(dir.path())).unwrap();
    let first = spool.append(record(b"first")).unwrap();
    let second = spool.append(record(b"second")).unwrap();
    let (first_command, first_rx) = checkpoint_command(vec![first]);
    let (second_command, second_rx) = checkpoint_command(vec![second]);
    let (tx, rx) = mpsc::sync_channel(4);
    tx.send(RawSpoolCommand::Checkpoint(second_command))
        .unwrap();
    let mut deferred = VecDeque::new();

    handle_checkpoint_batch(
        &mut spool,
        first_command,
        &rx,
        &mut deferred,
        2,
        Duration::from_secs(5),
    );

    let first_stats = first_rx.recv().unwrap().unwrap();
    let second_stats = second_rx.recv().unwrap().unwrap();
    assert_eq!(first_stats.records, 2);
    assert_eq!(first_stats.commands, 2);
    assert!(first_stats.wait_seconds >= 0.0);
    assert_eq!(second_stats.records, 0);
    assert_eq!(spool.recover_pending().unwrap().len(), 0);
}

#[test]
fn raw_spool_writer_batches_deferred_checkpoint_commands() {
    let dir = tempdir().unwrap();
    let mut spool = RawSpool::open(options(dir.path())).unwrap();
    let first = spool.append(record(b"first")).unwrap();
    let second = spool.append(record(b"second")).unwrap();
    let third = spool.append(record(b"third")).unwrap();
    let (first_command, first_rx) = checkpoint_command(vec![first]);
    let (second_command, second_rx) = checkpoint_command(vec![second]);
    let (third_command, third_rx) = checkpoint_command(vec![third]);
    let mut deferred = VecDeque::from([
        RawSpoolCommand::Checkpoint(second_command),
        RawSpoolCommand::Checkpoint(third_command),
    ]);
    let (_tx, rx) = mpsc::sync_channel(4);

    handle_checkpoint_batch(
        &mut spool,
        first_command,
        &rx,
        &mut deferred,
        64,
        Duration::from_millis(1),
    );

    let first_stats = first_rx.recv().unwrap().unwrap();
    let second_stats = second_rx.recv().unwrap().unwrap();
    let third_stats = third_rx.recv().unwrap().unwrap();
    assert_eq!(first_stats.records, 3);
    assert_eq!(first_stats.commands, 3);
    assert_eq!(second_stats.records, 0);
    assert_eq!(third_stats.records, 0);
    assert!(deferred.is_empty());
    assert_eq!(spool.recover_pending().unwrap().len(), 0);
}

#[test]
fn raw_spool_periodic_append_sync_clears_unsynced_accounting() {
    let dir = tempdir().unwrap();
    let mut opts = options(dir.path());
    opts.append_sync_interval = Duration::from_millis(500);
    opts.append_sync_bytes = 1024 * 1024;
    let mut spool = RawSpool::open(opts).unwrap();

    spool.append(record(b"first")).unwrap();
    spool.append_dirty_since = Some(Instant::now() - Duration::from_secs(1));
    let sync = spool.sync_append_if_due(false).unwrap().unwrap();

    assert!(sync.file_count >= 1);
    assert!(sync.seconds >= 0.0);
    let stats = spool.stats().unwrap();
    assert_eq!(stats.unsynced_records, 0);
    assert_eq!(stats.unsynced_bytes, 0);
    assert_eq!(stats.append_syncs_total, 1);
    assert_eq!(stats.append_sync_failures_total, 0);
    assert!(stats.healthy);
}

#[test]
fn raw_spool_append_sync_byte_threshold_clears_unsynced_accounting() {
    let dir = tempdir().unwrap();
    let mut opts = options(dir.path());
    opts.append_sync_interval = Duration::from_secs(60);
    opts.append_sync_bytes = 1;
    let mut spool = RawSpool::open(opts).unwrap();

    spool.append(record(b"first")).unwrap();
    let sync = spool.sync_append_if_due(false).unwrap().unwrap();

    assert!(sync.file_count >= 1);
    let stats = spool.stats().unwrap();
    assert_eq!(stats.unsynced_records, 0);
    assert_eq!(stats.unsynced_bytes, 0);
    assert_eq!(stats.append_syncs_total, 1);
}

#[test]
fn raw_spool_forced_append_sync_on_shutdown_clears_unsynced_accounting() {
    let dir = tempdir().unwrap();
    let mut opts = options(dir.path());
    opts.append_sync_interval = Duration::from_secs(60);
    opts.append_sync_bytes = 1024 * 1024;
    let mut spool = RawSpool::open(opts).unwrap();

    spool.append(record(b"first")).unwrap();
    assert_eq!(spool.stats().unwrap().unsynced_records, 1);
    spool.sync_append_if_due(true).unwrap();

    let stats = spool.stats().unwrap();
    assert_eq!(stats.unsynced_records, 0);
    assert_eq!(stats.unsynced_bytes, 0);
    assert_eq!(stats.append_syncs_total, 1);
}

#[test]
fn raw_spool_append_sync_failure_marks_unhealthy_and_blocks_appends() {
    let dir = tempdir().unwrap();
    let mut spool = RawSpool::open(options(dir.path())).unwrap();

    spool.append(record(b"first")).unwrap();
    spool.fail_next_append_sync();
    let err = spool.sync_append_if_due(true).unwrap_err();
    assert!(err
        .to_string()
        .contains("injected raw spool append sync failure"));

    let stats = spool.stats().unwrap();
    assert_eq!(stats.unsynced_records, 1);
    assert_eq!(stats.append_sync_failures_total, 1);
    assert!(!stats.healthy);
    assert!(stats
        .error
        .unwrap()
        .contains("injected raw spool append sync failure"));

    let err = spool.append(record(b"second")).unwrap_err();
    assert!(err.to_string().contains("raw spool append sync failed"));
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
fn raw_spool_checkpoint_fsync_can_be_delayed_until_record_threshold() {
    let dir = tempdir().unwrap();
    let mut opts = options(dir.path());
    opts.checkpoint_fsync_records = 2;
    opts.checkpoint_fsync_delay = Duration::from_secs(60);
    let mut spool = RawSpool::open(opts).unwrap();
    let first = spool.append(record(b"first")).unwrap();
    let second = spool.append(record(b"second")).unwrap();

    spool.mark_committed(first).unwrap();
    assert_eq!(spool.checkpoint_dirty_records, 1);

    spool.mark_committed(second).unwrap();
    assert_eq!(spool.checkpoint_dirty_records, 0);
}

#[test]
fn raw_spool_checkpoint_fsync_can_be_forced_on_shutdown() {
    let dir = tempdir().unwrap();
    let mut opts = options(dir.path());
    opts.checkpoint_fsync_records = 1024;
    opts.checkpoint_fsync_delay = Duration::from_secs(60);
    let mut spool = RawSpool::open(opts).unwrap();
    let first = spool.append(record(b"first")).unwrap();

    spool.mark_committed(first).unwrap();
    assert_eq!(spool.checkpoint_dirty_records, 1);

    spool.sync_checkpoint_if_due(true).unwrap();
    assert_eq!(spool.checkpoint_dirty_records, 0);
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
    let (first, _first_rx) = append_command(b"first", 1);
    let (tx, rx) = mpsc::sync_channel(4);
    let (second, _second_rx) = append_command(b"second", 2);
    tx.send(RawSpoolCommand::Append(second)).unwrap();

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
    let (first, _first_rx) = append_command(b"first", 1);
    let (tx, rx) = mpsc::sync_channel(4);
    let (checkpoint_reply, _checkpoint_rx) = mpsc::channel();
    tx.send(RawSpoolCommand::Checkpoint(CheckpointCommand {
        ids: vec![RawSpoolRecordId {
            segment: 1,
            sequence: 1,
        }],
        queued_at: Instant::now(),
        reply: checkpoint_reply,
    }))
    .unwrap();
    let (second, _second_rx) = append_command(b"second", 2);
    tx.send(RawSpoolCommand::Append(second)).unwrap();

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
    let (first, _first_rx) = append_command(b"first", 1);
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
