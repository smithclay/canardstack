use super::ducklake::configure_write_connection;
use super::metadata_refresh::{merge_dirty_metadata, refresh_metadata_summaries_on};
use super::{Signal, Storage};
use crate::LockExt;
use anyhow::Result;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::Ordering;

impl Storage {
    pub fn metadata_generation(&self) -> u64 {
        self.metadata_generation.load(Ordering::SeqCst)
    }

    pub(super) fn mark_metadata_dirty(&self, affected: BTreeMap<Signal, BTreeSet<String>>) {
        merge_dirty_metadata(&mut self.dirty_metadata.lock_or_poisoned(), affected);
    }

    /// Re-aggregate `metadata_summary` for every signal/date bucket dirtied by
    /// a committed insert. Runs on the `metadata_refresh` scheduler job so the
    /// full day-partition scan stays off the ingest commit path. On failure the
    /// drained buckets are re-queued so the next tick retries them — committed
    /// telemetry must not stay invisible to the discovery APIs.
    pub fn refresh_metadata(&self) -> Result<usize> {
        let affected = std::mem::take(&mut *self.dirty_metadata.lock_or_poisoned());
        if affected.is_empty() {
            return Ok(0);
        }
        let conn = self.writer.lock_or_poisoned();
        configure_write_connection(&conn, &self.write_memory_limit)?;
        match refresh_metadata_summaries_on(&conn, &self.target_prefix, &affected) {
            Ok(buckets) => {
                self.metadata_generation.fetch_add(1, Ordering::SeqCst);
                Ok(buckets)
            }
            Err(err) => {
                self.mark_metadata_dirty(affected);
                Err(err)
            }
        }
    }
}
