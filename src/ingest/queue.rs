use super::Signal;
use crate::config::Config;
use crate::ingest::spool::RawSpoolRecordId;
use crate::otlp::Transformed;
use arrow58::record_batch::RecordBatch;
use serde::Serialize;
use std::collections::{HashMap, VecDeque};
use std::time::{Duration, Instant};

pub(super) type QueueMap = HashMap<QueueKey, SignalQueue>;

#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash)]
pub(super) struct QueueKey {
    pub(super) signal: Signal,
    pub(super) partition: BatchPartition,
}

impl QueueKey {
    pub(super) fn new(signal: Signal, source_format: &'static str) -> Self {
        Self {
            signal,
            partition: BatchPartition::from_source_format(source_format),
        }
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash)]
pub(super) enum BatchPartition {
    Json,
    Protobuf,
}

impl BatchPartition {
    fn from_source_format(source_format: &'static str) -> Self {
        match source_format {
            "json" | "otlp_json" => Self::Json,
            _ => Self::Protobuf,
        }
    }

    pub(super) fn as_str(self) -> &'static str {
        match self {
            Self::Json => "json",
            Self::Protobuf => "protobuf",
        }
    }
}

#[derive(Clone)]
pub(super) struct QueuedBatch {
    pub(super) batch: RecordBatch,
    pub(super) source_format: &'static str,
    pub(super) raw_spool_id: Option<RawSpoolRecordId>,
    accepted_at: Instant,
    pub(super) approx_bytes: usize,
}

impl QueuedBatch {
    fn split_at(mut self, take_rows: usize) -> (Self, Self) {
        debug_assert!(take_rows > 0);
        debug_assert!(take_rows < self.len());
        let original_rows = self.len();
        let rest_batch = self.batch.slice(take_rows, original_rows - take_rows);
        self.batch = self.batch.slice(0, take_rows);
        let taken_bytes = proportional_bytes(self.approx_bytes, take_rows, original_rows);
        let rest_bytes = self.approx_bytes.saturating_sub(taken_bytes);
        let accepted_at = self.accepted_at;
        let source_format = self.source_format;
        let raw_spool_id = self.raw_spool_id;
        let taken = Self {
            batch: self.batch,
            source_format,
            raw_spool_id,
            accepted_at,
            approx_bytes: taken_bytes,
        };
        let rest = Self {
            batch: rest_batch,
            source_format,
            raw_spool_id,
            accepted_at,
            approx_bytes: rest_bytes,
        };
        (taken, rest)
    }

    pub(super) fn len(&self) -> usize {
        self.batch.num_rows()
    }
}

pub(super) struct PendingBatch {
    pub(super) key: QueueKey,
    pub(super) batch: RecordBatch,
    pub(super) source_format: &'static str,
    pub(super) raw_spool_id: Option<RawSpoolRecordId>,
    pub(super) approx_bytes: usize,
}

#[derive(Default, Clone)]
pub(super) struct SignalQueue {
    batches: VecDeque<QueuedBatch>,
    rows: usize,
    bytes: usize,
}

#[derive(Debug, Serialize)]
pub struct IngestSnapshot {
    pub signal: &'static str,
    pub queued_rows: usize,
    pub queued_bytes: usize,
    pub oldest_age_seconds: f64,
    pub pressure: f64,
}

pub(super) fn pending_batches(transformed: Transformed) -> Vec<PendingBatch> {
    let source_format = transformed.source_format;
    let mut batches = Vec::new();
    push_pending_arrow(&mut batches, Signal::Logs, transformed.logs, source_format);
    push_pending_arrow(
        &mut batches,
        Signal::Spans,
        transformed.spans,
        source_format,
    );
    push_pending_arrow(
        &mut batches,
        Signal::MetricGauge,
        transformed.gauge,
        source_format,
    );
    push_pending_arrow(
        &mut batches,
        Signal::MetricSum,
        transformed.sum,
        source_format,
    );
    batches
}

pub(super) fn queued_bytes_for_signal(queues: &QueueMap, signal: Signal) -> usize {
    queues
        .iter()
        .filter(|(key, _)| key.signal == signal)
        .map(|(_, queue)| queue.bytes)
        .sum()
}

pub(super) fn process_bytes(queues: &QueueMap) -> usize {
    queues.values().map(|q| q.bytes).sum()
}

pub(super) fn added_process_bytes(batches: &[PendingBatch]) -> usize {
    batches.iter().map(|b| b.approx_bytes).sum()
}

pub(super) fn added_bytes_by_signal(batches: &[PendingBatch]) -> HashMap<Signal, usize> {
    let mut added_by_signal = HashMap::new();
    for batch in batches {
        *added_by_signal.entry(batch.key.signal).or_default() += batch.approx_bytes;
    }
    added_by_signal
}

pub(super) fn enqueue_batches(queues: &mut QueueMap, batches: Vec<PendingBatch>) -> usize {
    let accepted = batches.iter().map(|b| b.batch.num_rows()).sum();
    for batch in batches {
        let queue = queues.entry(batch.key).or_default();
        queue.rows += batch.batch.num_rows();
        queue.bytes += batch.approx_bytes;
        queue.batches.push_back(QueuedBatch {
            batch: batch.batch,
            source_format: batch.source_format,
            raw_spool_id: batch.raw_spool_id,
            accepted_at: Instant::now(),
            approx_bytes: batch.approx_bytes,
        });
    }
    accepted
}

pub(super) fn has_threshold_due_queue(queues: &QueueMap, config: &Config) -> bool {
    queues.values().any(|queue| {
        queue.rows >= config.max_rows_per_flush || queue.bytes >= config.max_bytes_per_flush
    })
}

pub(super) fn due_keys(queues: &QueueMap, config: &Config) -> Vec<QueueKey> {
    queues
        .iter()
        .filter_map(|(key, q)| {
            let oldest = q.batches.front()?;
            let age = oldest.accepted_at.elapsed();
            (q.rows >= config.max_rows_per_flush
                || q.bytes >= config.max_bytes_per_flush
                || age >= flush_age(config, q.bytes))
            .then_some(*key)
        })
        .collect()
}

pub(super) fn drain_keys_for_signal(queues: &QueueMap, signal: Signal) -> Vec<QueueKey> {
    queues
        .keys()
        .copied()
        .filter(|key| key.signal == signal)
        .collect()
}

pub(super) fn drain_flush_batches(
    queues: &mut QueueMap,
    key: QueueKey,
    config: &Config,
) -> Vec<QueuedBatch> {
    let Some(queue) = queues.get_mut(&key) else {
        return Vec::new();
    };
    let mut drained = Vec::new();
    let mut remaining_rows = config.max_rows_per_flush;
    let mut remaining_bytes = config.max_bytes_per_flush;

    while remaining_rows > 0 && remaining_bytes > 0 {
        let Some(batch) = queue.batches.pop_front() else {
            break;
        };
        let original_rows = batch.len();
        if original_rows == 0 {
            continue;
        }
        let row_bytes = batch.approx_bytes.div_ceil(original_rows).max(1);
        let rows_by_bytes = (remaining_bytes / row_bytes).max(1);
        let take_rows = original_rows.min(remaining_rows).min(rows_by_bytes);
        if take_rows == original_rows || batch.raw_spool_id.is_some() {
            queue.rows = queue.rows.saturating_sub(original_rows);
            queue.bytes = queue.bytes.saturating_sub(batch.approx_bytes);
            remaining_rows = remaining_rows.saturating_sub(original_rows);
            remaining_bytes = remaining_bytes.saturating_sub(batch.approx_bytes);
            drained.push(batch);
        } else {
            let (taken, rest) = batch.split_at(take_rows);
            queue.rows = queue.rows.saturating_sub(taken.len());
            queue.bytes = queue.bytes.saturating_sub(taken.approx_bytes);
            queue.batches.push_front(rest);
            drained.push(taken);
            break;
        }
    }

    if queue.batches.is_empty() {
        queues.remove(&key);
    }
    drained
}

pub(super) fn restore_batches(queues: &mut QueueMap, key: QueueKey, batches: Vec<QueuedBatch>) {
    let queue = queues.entry(key).or_default();
    for batch in batches.into_iter().rev() {
        queue.rows += batch.len();
        queue.bytes += batch.approx_bytes;
        queue.batches.push_front(batch);
    }
}

pub(super) fn snapshots(queues: &QueueMap, config: &Config) -> Vec<IngestSnapshot> {
    [
        Signal::Logs,
        Signal::Spans,
        Signal::MetricGauge,
        Signal::MetricSum,
    ]
    .into_iter()
    .map(|signal| {
        let mut rows = 0;
        let mut bytes = 0;
        let mut oldest_age_seconds = 0.0;
        for (_, q) in queues.iter().filter(|(key, _)| key.signal == signal) {
            rows += q.rows;
            bytes += q.bytes;
            if let Some(oldest) = q.batches.front() {
                let age = oldest.accepted_at.elapsed().as_secs_f64();
                if age > oldest_age_seconds {
                    oldest_age_seconds = age;
                }
            }
        }
        IngestSnapshot {
            signal: signal.as_str(),
            queued_rows: rows,
            queued_bytes: bytes,
            oldest_age_seconds,
            pressure: bytes as f64 / config.per_signal_queue_bytes as f64,
        }
    })
    .collect()
}

pub(super) fn flush_age(config: &Config, queue_bytes: usize) -> Duration {
    let pressure = queue_bytes as f64 / config.per_signal_queue_bytes as f64;
    if pressure >= 0.70 {
        config.high_pressure_max_age
    } else {
        config.max_age
    }
}

fn proportional_bytes(total_bytes: usize, rows: usize, total_rows: usize) -> usize {
    if rows == 0 || total_rows == 0 {
        return 0;
    }
    total_bytes.saturating_mul(rows).div_ceil(total_rows).max(1)
}

fn push_pending_arrow(
    batches: &mut Vec<PendingBatch>,
    signal: Signal,
    batch: Option<RecordBatch>,
    source_format: &'static str,
) {
    let Some(batch) = batch else {
        return;
    };
    if batch.num_rows() == 0 {
        return;
    }
    let approx_bytes = batch.get_array_memory_size().max(batch.num_rows());
    batches.push(PendingBatch {
        key: QueueKey::new(signal, source_format),
        batch,
        source_format,
        raw_spool_id: None,
        approx_bytes,
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow58::array::Int64Array;
    use arrow58::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;
    use tempfile::tempdir;

    #[test]
    fn drain_split_restore_preserves_exact_rows_and_bytes() {
        let dir = tempdir().unwrap();
        let mut config = Config::test(dir.path().join("canardstack.duckdb"));
        config.max_rows_per_flush = 2;
        config.max_bytes_per_flush = 10_000;
        let key = QueueKey::new(Signal::Logs, "json");
        let batch = batch_with_rows(5);
        let mut queues = QueueMap::new();

        enqueue_batches(
            &mut queues,
            vec![PendingBatch {
                key,
                batch,
                source_format: "json",
                raw_spool_id: None,
                approx_bytes: 50,
            }],
        );
        assert_queue_totals(&queues, &config, 5, 50);

        let drained = drain_flush_batches(&mut queues, key, &config);
        assert_eq!(drained.iter().map(QueuedBatch::len).sum::<usize>(), 2);
        assert_eq!(
            drained
                .iter()
                .map(|batch| batch.approx_bytes)
                .sum::<usize>(),
            20
        );
        assert_queue_totals(&queues, &config, 3, 30);

        restore_batches(&mut queues, key, drained);
        assert_queue_totals(&queues, &config, 5, 50);
    }

    fn batch_with_rows(rows: usize) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )]));
        let values = (0..rows as i64).collect::<Vec<_>>();
        RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(values))]).unwrap()
    }

    fn assert_queue_totals(queues: &QueueMap, config: &Config, rows: usize, bytes: usize) {
        let snapshot = snapshots(queues, config)
            .into_iter()
            .find(|snapshot| snapshot.signal == Signal::Logs.as_str())
            .unwrap();
        assert_eq!(snapshot.queued_rows, rows);
        assert_eq!(snapshot.queued_bytes, bytes);
    }
}
