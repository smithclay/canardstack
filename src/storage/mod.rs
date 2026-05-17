use crate::config::Config;
use crate::ingest::Signal;
use anyhow::{Context, Result};
use arrow58::record_batch::RecordBatch;
use duckdb::Connection;
use serde::Serialize;
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::AtomicU64;
use std::sync::Mutex;
use std::time::Duration;

mod arrow;
mod ducklake;
mod health;
mod immutable;
mod immutable_write;
mod maintenance;
mod metadata;
mod metadata_refresh;
mod query_conn;
mod schema;

pub use ducklake::install_ducklake_extension;
use ducklake::{
    attach_ducklake_connection, configure_base_connection, configure_write_connection,
    ducklake_attach_plan,
};
pub use immutable::ImmutableFlushOutcome;
use immutable::ImmutableSegmentBuffer;
use schema::create_tables_on;

#[derive(Clone, Debug, Serialize)]
pub struct StorageHealth {
    pub healthy: bool,
    pub mode: String,
    pub ducklake_catalog: String,
    pub ducklake_available: bool,
    pub ducklake_required: bool,
    pub postgres_catalog_configured: bool,
    pub last_error: Option<String>,
    pub capabilities: StorageCapabilities,
    pub freshness_watermarks: Value,
    pub logical_rows: Value,
    pub ducklake_storage_layout: Value,
    pub physical_bytes: u64,
}

#[derive(Clone, Debug, Serialize)]
pub struct StorageProbe {
    pub healthy: bool,
    pub mode: String,
    pub ducklake_available: bool,
    pub ducklake_required: bool,
    pub last_error: Option<String>,
}

impl StorageProbe {
    pub fn is_ready(&self) -> bool {
        self.healthy && (!self.ducklake_required || self.ducklake_available)
    }
}

impl StorageHealth {
    pub fn is_ready(&self) -> bool {
        self.healthy && (!self.ducklake_required || self.ducklake_available)
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct StorageCapabilities {
    pub insert: bool,
    pub query: bool,
    pub inlined_flush: bool,
    pub snapshot_expiration: bool,
    pub cleanup_old_files: bool,
    pub merge_adjacent_files: bool,
    pub whole_day_retention: bool,
}

pub struct Storage {
    /// Write-side connection. Held for inserts, DDL, and DuckLake maintenance.
    /// Reader path never touches this mutex — that decoupling keeps /healthz,
    /// /metrics, and queries responsive while a flush is in flight.
    writer: Mutex<Connection>,
    /// Cloned from `writer` at startup after ATTACH + DDL; shares the
    /// underlying Database (attached schemas survive `try_clone`). Source of
    /// per-query clones in `with_query_conn`.
    reader: Mutex<Connection>,
    target_prefix: String,
    mode: String,
    catalog_name: String,
    ducklake_available: bool,
    postgres_catalog_configured: bool,
    local_storage_dir: PathBuf,
    ducklake_required: bool,
    ducklake_managed_maintenance: bool,
    immutable_segment_target_bytes: usize,
    immutable_segment_max_age: Duration,
    immutable_buffers: Mutex<BTreeMap<Signal, ImmutableSegmentBuffer>>,
    write_memory_limit: String,
    last_error: Mutex<Option<String>>,
    /// Cache-invalidation token for discovery metadata. Bumped only after a
    /// committed `metadata_summary` change (refresh or retention); discovery
    /// caches in `Metadata` key entries on this value and drop them on a bump.
    metadata_generation: AtomicU64,
    /// Signal/event-date buckets whose `metadata_summary` rows are stale after
    /// a committed insert. Drained by the `metadata_refresh` scheduler job so
    /// the day-partition re-aggregation stays off the ingest commit path.
    dirty_metadata: Mutex<BTreeMap<Signal, BTreeSet<String>>>,
}

pub struct RetentionPolicy {
    pub logs_days: i64,
    pub spans_days: i64,
    pub metrics_days: i64,
}

/// Typed timeout from `with_query_conn`. Downcastable so callers can classify
/// the 503 as `query_timeout` without substring-matching the error message.
#[derive(Debug)]
pub struct QueryTimeoutError {
    pub timeout: Duration,
}

impl std::fmt::Display for QueryTimeoutError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "query timeout after {}ms", self.timeout.as_millis())
    }
}

impl std::error::Error for QueryTimeoutError {}

/// Storage insert failed after one or more smaller transactions may already
/// have committed. Callers use `committed_rows` to avoid retrying rows that
/// DuckDB has already accepted.
#[derive(Debug)]
pub struct InsertRecordsError {
    pub table: Signal,
    pub committed_rows: usize,
    pub attempted_rows: usize,
    pub source: anyhow::Error,
}

impl std::fmt::Display for InsertRecordsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "insert failed for {} after committing {}/{} row(s): {}",
            self.table, self.committed_rows, self.attempted_rows, self.source
        )
    }
}

impl std::error::Error for InsertRecordsError {}

pub struct ArrowBatchInsert<'a> {
    pub table: Signal,
    pub batch: &'a RecordBatch,
    pub source_format: &'a str,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TimingPhase {
    Prepare,
    Buffer,
    PartitionSplit,
    ParquetEncode,
    ParquetWrite,
    FileWrite,
    FileFsync,
    FileRename,
    DucklakeRegister,
    DucklakeCommit,
    Insert,
}

impl TimingPhase {
    pub fn as_str(self) -> &'static str {
        match self {
            TimingPhase::Prepare => "storage_prepare",
            TimingPhase::Buffer => "storage_buffer",
            TimingPhase::PartitionSplit => "storage_partition_split",
            TimingPhase::ParquetEncode => "storage_parquet_encode",
            TimingPhase::ParquetWrite => "storage_parquet_write",
            TimingPhase::FileWrite => "storage_file_write",
            TimingPhase::FileFsync => "storage_file_fsync",
            TimingPhase::FileRename => "storage_file_rename",
            TimingPhase::DucklakeRegister => "storage_ducklake_register",
            TimingPhase::DucklakeCommit => "storage_ducklake_commit",
            TimingPhase::Insert => "storage_insert",
        }
    }
}

impl std::fmt::Display for TimingPhase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Clone, Debug)]
pub struct ArrowBatchInsertTiming {
    pub table: Signal,
    pub phase: TimingPhase,
    pub rows: usize,
    pub seconds: f64,
}

#[derive(Clone, Debug)]
pub struct ArrowBatchInsertResult {
    pub rows: usize,
    pub timings: Vec<ArrowBatchInsertTiming>,
}

struct PreparedArrowBatch {
    pub(super) table: Signal,
    pub(super) batch: RecordBatch,
    pub(super) rows: usize,
    pub(super) timestamp_days: Vec<String>,
}

impl Storage {
    pub fn open(config: &Config) -> Result<Self> {
        if let Some(parent) = config.duckdb_path.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::create_dir_all(&config.local_storage_dir)?;

        let writer = Connection::open(&config.duckdb_path)
            .with_context(|| format!("open DuckDB file {}", config.duckdb_path.display()))?;
        configure_base_connection(&writer)?;
        configure_write_connection(&writer, &config.duckdb_write_memory_limit)?;

        attach_ducklake_connection(
            &writer,
            config.postgres_dsn.as_deref(),
            config.ducklake_attach_uri.as_deref(),
            &config.duckdb_path,
            &config.local_storage_dir,
            config.duckdb_extension_dir.as_deref(),
            config.ducklake_data_inlining_row_limit,
        )
        .context(
            "DuckLake attach failed. Fix the catalog config (URI, token, network, or extension path) and restart.",
        )?;
        let target_prefix = "canardlake.".to_string();
        let plan = ducklake_attach_plan(config)?;
        let mode = format!("{}_immutable_segments", plan.mode);
        let ducklake_managed_maintenance = plan.managed_maintenance;

        create_tables_on(&writer, &target_prefix)?;

        // Reader must be cloned AFTER attach + create_tables so the new
        // session inherits the catalog.
        let reader = writer
            .try_clone()
            .context("clone writer connection for reader pool")?;
        configure_base_connection(&reader)?;

        Ok(Self {
            writer: Mutex::new(writer),
            reader: Mutex::new(reader),
            target_prefix,
            mode,
            catalog_name: "canardlake".to_string(),
            ducklake_available: true,
            postgres_catalog_configured: config.postgres_dsn.is_some(),
            local_storage_dir: config.local_storage_dir.clone(),
            ducklake_required: true,
            ducklake_managed_maintenance,
            immutable_segment_target_bytes: config.immutable_segment_target_bytes,
            immutable_segment_max_age: config.immutable_segment_max_age,
            immutable_buffers: Mutex::new(BTreeMap::new()),
            write_memory_limit: config.duckdb_write_memory_limit.clone(),
            last_error: Mutex::new(None),
            metadata_generation: AtomicU64::new(0),
            dirty_metadata: Mutex::new(BTreeMap::new()),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::ducklake::build_ducklake_attach_plan;
    use super::immutable::split_batch_by_immutable_partition;
    use super::metadata_refresh::metadata_refresh_sql;
    use super::*;
    use arrow58::array as arrow58_array;
    use arrow58::datatypes as arrow58_types;
    use arrow58::record_batch::RecordBatch;
    use chrono::{DateTime, NaiveDate, Utc};
    use std::sync::Arc;
    use tempfile::tempdir;

    #[test]
    fn custom_ducklake_attach_uri_uses_ducklake_extension_and_canardlake_alias() {
        let dir = tempdir().unwrap();
        let plan = build_ducklake_attach_plan(
            None,
            Some("ducklake:md:test-ducklake"),
            &dir.path().join("canardstack.duckdb"),
            &dir.path().join("storage"),
            1_000,
        )
        .unwrap();

        assert_eq!(
            plan.sql,
            "ATTACH 'ducklake:md:test-ducklake' AS canardlake (DATA_INLINING_ROW_LIMIT 1000); USE canardlake;"
        );
        assert_eq!(plan.mode, "ducklake_custom_uri");
        assert!(plan.needs_ducklake);
        assert!(!plan.needs_postgres);
        assert!(plan.managed_maintenance);
    }

    #[test]
    fn motherduck_attach_uri_uses_md_extension_and_canardlake_alias() {
        let dir = tempdir().unwrap();
        let plan = build_ducklake_attach_plan(
            None,
            Some("md:test-ducklake"),
            &dir.path().join("canardstack.duckdb"),
            &dir.path().join("storage"),
            1_000,
        )
        .unwrap();

        assert_eq!(
            plan.sql,
            "ATTACH 'md:test-ducklake' AS canardlake; USE canardlake;"
        );
        assert_eq!(plan.mode, "ducklake_motherduck_remote");
        assert!(!plan.needs_ducklake);
        assert!(plan.needs_motherduck);
        assert!(!plan.needs_postgres);
        assert!(!plan.managed_maintenance);
    }

    #[test]
    fn custom_attach_uri_and_postgres_catalog_are_mutually_exclusive() {
        let dir = tempdir().unwrap();
        let err = build_ducklake_attach_plan(
            Some("dbname=ducklake_catalog host=localhost"),
            Some("ducklake:md:test-ducklake"),
            &dir.path().join("canardstack.duckdb"),
            &dir.path().join("storage"),
            1_000,
        )
        .unwrap_err();

        assert!(err.to_string().contains(
            "set only one of CANARDSTACK_POSTGRES_DSN or CANARDSTACK_DUCKLAKE_ATTACH_URI"
        ));
    }

    #[test]
    fn ducklake_attach_uses_configured_data_inlining_limit() {
        let dir = tempdir().unwrap();
        let plan = build_ducklake_attach_plan(
            None,
            None,
            &dir.path().join("canardstack.duckdb"),
            &dir.path().join("storage"),
            0,
        )
        .unwrap();

        assert!(plan.sql.contains("DATA_INLINING_ROW_LIMIT 0"));
    }

    #[test]
    fn custom_attach_uri_must_be_uri_not_attach_statement() {
        let dir = tempdir().unwrap();
        let err = build_ducklake_attach_plan(
            None,
            Some("ATTACH 'md:test-ducklake';"),
            &dir.path().join("canardstack.duckdb"),
            &dir.path().join("storage"),
            1_000,
        )
        .unwrap_err();

        assert!(err
            .to_string()
            .contains("must be the URI only, not an ATTACH statement"));
    }

    #[test]
    fn custom_attach_uri_must_be_md_or_ducklake_uri() {
        let dir = tempdir().unwrap();
        let err = build_ducklake_attach_plan(
            None,
            Some("sqlite:/tmp/not-ducklake.db"),
            &dir.path().join("canardstack.duckdb"),
            &dir.path().join("storage"),
            1_000,
        )
        .unwrap_err();

        assert!(err.to_string().contains("must be an md: or ducklake: URI"));
    }

    #[test]
    fn metadata_refresh_uses_one_insert_per_signal_bucket() {
        for (signal, select_count) in [
            (Signal::Logs, 8),
            (Signal::Spans, 6),
            (Signal::MetricGauge, 5),
            (Signal::MetricSum, 5),
        ] {
            let sql = metadata_refresh_sql("canardlake.", signal, "2026-05-16").unwrap();
            assert_eq!(
                sql.matches("INSERT INTO canardlake.metadata_summary")
                    .count(),
                1
            );
            assert_eq!(sql.matches("UNION ALL").count(), select_count - 1);
        }
    }

    #[test]
    fn immutable_segment_split_preserves_timestamp_day_hour_partitions() {
        fn timestamp_micros(year: i32, month: u32, day: u32, hour: u32) -> i64 {
            DateTime::<Utc>::from_naive_utc_and_offset(
                NaiveDate::from_ymd_opt(year, month, day)
                    .unwrap()
                    .and_hms_opt(hour, 0, 0)
                    .unwrap(),
                Utc,
            )
            .timestamp_micros()
        }

        let schema = Arc::new(arrow58_types::Schema::new(vec![
            arrow58_types::Field::new(
                "timestamp",
                arrow58_types::DataType::Timestamp(arrow58_types::TimeUnit::Microsecond, None),
                true,
            ),
            arrow58_types::Field::new("value", arrow58_types::DataType::Int64, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(arrow58_array::TimestampMicrosecondArray::from(vec![
                    Some(timestamp_micros(2026, 5, 16, 0)),
                    Some(timestamp_micros(2026, 5, 16, 1)),
                    Some(timestamp_micros(2026, 5, 17, 0)),
                ])),
                Arc::new(arrow58_array::Int64Array::from(vec![1, 2, 3])),
            ],
        )
        .unwrap();

        let splits = split_batch_by_immutable_partition(&batch).unwrap();

        assert_eq!(splits.len(), 3);
        assert_eq!(splits[0].0.timestamp_day, "2026-05-16");
        assert_eq!(splits[0].0.hour, 0);
        assert_eq!(splits[0].1.num_rows(), 1);
        assert_eq!(splits[1].0.timestamp_day, "2026-05-16");
        assert_eq!(splits[1].0.hour, 1);
        assert_eq!(splits[1].1.num_rows(), 1);
        assert_eq!(splits[2].0.timestamp_day, "2026-05-17");
        assert_eq!(splits[2].0.hour, 0);
        assert_eq!(splits[2].1.num_rows(), 1);
    }
}
