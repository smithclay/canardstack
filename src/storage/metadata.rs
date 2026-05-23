use super::ducklake::configure_write_connection;
use super::metadata_refresh::{merge_dirty_metadata, refresh_metadata_summaries_on};
use super::{Storage, StorageSignal};
use crate::LockExt;
use anyhow::Result;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::Ordering;
use std::time::Instant;

/// Result of a bounded `metadata_summary` re-aggregation pass, carrying the
/// number of re-aggregated buckets plus the time spent waiting on the single
/// writer connection so the scheduler can emit the `writer_lock_wait` phase.
#[derive(Clone, Copy, Debug, Default)]
pub struct MetadataRefreshOutcome {
    pub buckets: usize,
    pub writer_lock_wait_seconds: f64,
}

impl Storage {
    pub fn metadata_generation(&self) -> u64 {
        self.metadata_generation.load(Ordering::SeqCst)
    }

    pub(super) fn mark_metadata_dirty(&self, affected: BTreeMap<StorageSignal, BTreeSet<String>>) {
        merge_dirty_metadata(&mut self.dirty_metadata.lock_or_poisoned(), affected);
    }

    /// Re-aggregate at most `max_buckets` dirtied signal/date buckets.
    ///
    /// Each `metadata_summary` re-aggregation is driven by this bounded form so
    /// metadata discovery work cannot monopolize the writer connection while
    /// ingest is under load. The `metadata_refresh` scheduler job passes a small
    /// limit; pass `usize::MAX` to drain every pending bucket in one pass. On
    /// failure the drained buckets are re-queued so the next call retries them —
    /// committed telemetry must not stay invisible to the discovery APIs.
    pub fn refresh_metadata_limited(&self, max_buckets: usize) -> Result<MetadataRefreshOutcome> {
        let affected = {
            let mut dirty = self.dirty_metadata.lock_or_poisoned();
            take_dirty_metadata_batch(&mut dirty, max_buckets)
        };
        if affected.is_empty() {
            return Ok(MetadataRefreshOutcome::default());
        }
        // Time the wait to acquire the single writer connection so contention
        // against the seal flush path is observable via `writer_lock_wait`.
        let writer_lock_wait_started = Instant::now();
        let conn = self.writer.lock_or_poisoned();
        let writer_lock_wait_seconds = writer_lock_wait_started.elapsed().as_secs_f64();
        configure_write_connection(&conn, &self.write_memory_limit)?;
        match refresh_metadata_summaries_on(&conn, &self.target_prefix, &affected) {
            Ok(buckets) => {
                self.metadata_generation.fetch_add(1, Ordering::SeqCst);
                Ok(MetadataRefreshOutcome {
                    buckets,
                    writer_lock_wait_seconds,
                })
            }
            Err(err) => {
                self.mark_metadata_dirty(affected);
                Err(err)
            }
        }
    }
}

fn take_dirty_metadata_batch(
    dirty: &mut BTreeMap<StorageSignal, BTreeSet<String>>,
    max_buckets: usize,
) -> BTreeMap<StorageSignal, BTreeSet<String>> {
    let mut selected = BTreeMap::<StorageSignal, BTreeSet<String>>::new();
    let mut remaining = max_buckets;
    if remaining == 0 {
        return selected;
    }

    let signals = dirty.keys().copied().collect::<Vec<_>>();
    for signal in signals {
        while remaining > 0 {
            let Some(dates) = dirty.get_mut(&signal) else {
                break;
            };
            let Some(date) = dates.iter().next().cloned() else {
                dirty.remove(&signal);
                break;
            };
            dates.remove(&date);
            selected.entry(signal).or_default().insert(date);
            remaining -= 1;
            if dates.is_empty() {
                dirty.remove(&signal);
                break;
            }
        }
        if remaining == 0 {
            break;
        }
    }
    selected
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dirty(signals: &[(StorageSignal, &[&str])]) -> BTreeMap<StorageSignal, BTreeSet<String>> {
        signals
            .iter()
            .map(|(signal, dates)| (*signal, dates.iter().map(|date| date.to_string()).collect()))
            .collect()
    }

    #[test]
    fn metadata_batch_limit_preserves_unselected_dirty_buckets() {
        let mut pending = dirty(&[
            (StorageSignal::Logs, &["2026-05-18", "2026-05-19"]),
            (StorageSignal::Spans, &["2026-05-19"]),
            (StorageSignal::MetricGauge, &["2026-05-19"]),
        ]);

        let selected = take_dirty_metadata_batch(&mut pending, 2);

        assert_eq!(
            selected,
            dirty(&[(StorageSignal::Logs, &["2026-05-18", "2026-05-19"])])
        );
        assert_eq!(
            pending,
            dirty(&[
                (StorageSignal::Spans, &["2026-05-19"]),
                (StorageSignal::MetricGauge, &["2026-05-19"]),
            ])
        );
    }

    #[test]
    fn metadata_batch_limit_zero_does_not_drain_dirty_buckets() {
        let mut pending = dirty(&[(StorageSignal::Logs, &["2026-05-19"])]);

        let selected = take_dirty_metadata_batch(&mut pending, 0);

        assert!(selected.is_empty());
        assert_eq!(pending, dirty(&[(StorageSignal::Logs, &["2026-05-19"])]));
    }
}
