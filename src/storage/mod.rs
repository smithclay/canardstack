use crate::config::Config;
use crate::ingest::ReplayBackedRecordRef;
use crate::signal::StorageSignal;
use anyhow::{Context, Result};
use arrow58::record_batch::RecordBatch;
use duckdb::Connection;
use serde::Serialize;
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64};
use std::sync::Mutex;
use std::time::Duration;

mod arrow;
mod arrow_write;
mod arrow_write_buffer;
mod ducklake;
mod health;
mod maintenance;
mod metadata;
mod metadata_refresh;
mod query_conn;
mod schema;

pub use arrow_write::ArrowFlushOutcome;
use arrow_write::ArrowWriteBuffer;
use ducklake::{
    attach_ducklake_connection, configure_base_connection, configure_ducklake_maintenance_options,
    configure_write_connection, ducklake_attach_plan, DUCKLAKE_CATALOG_NAME,
    DUCKLAKE_TARGET_PREFIX,
};
pub use ducklake::{
    configure_object_store_for_data_path, install_ducklake_extension, open_quack_catalog_connection,
};
pub use metadata::MetadataRefreshOutcome;
use schema::{create_tables_on, enforce_schema_version_on};

#[derive(Clone, Debug, Serialize)]
pub struct StorageHealth {
    pub healthy: bool,
    pub mode: String,
    pub ducklake_catalog: String,
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
    pub last_error: Option<String>,
}

pub(crate) struct CommittedReplayRefs(Vec<ReplayBackedRecordRef>);

impl CommittedReplayRefs {
    pub(crate) fn len(&self) -> usize {
        self.0.len()
    }

    pub(crate) fn as_slice(&self) -> &[ReplayBackedRecordRef] {
        &self.0
    }

    // Module-private on purpose: a committed-replay-refs token may be minted ONLY
    // by the `storage` commit path, so holding one is proof the rows were
    // DuckLake-committed. Do NOT widen to `pub(super)`/`pub(crate)` — `storage`
    // is a depth-1 module, so either would make the token forgeable from any
    // in-crate module (e.g. `seal`/`ingest`) and break the checkpoint-after-commit
    // guarantee. The mint sites live in `storage`'s descendant modules
    // (`arrow_write`, `arrow_write_buffer`), which can reach a private constructor.
    fn new(replay_refs: Vec<ReplayBackedRecordRef>) -> Self {
        Self(replay_refs)
    }

    fn empty() -> Self {
        Self(Vec::new())
    }
}

impl StorageProbe {
    pub fn is_ready(&self) -> bool {
        self.healthy
    }
}

impl StorageHealth {
    pub fn is_ready(&self) -> bool {
        self.healthy
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct StorageCapabilities {
    pub insert: bool,
    pub query: bool,
    pub ducklake_maintenance_enabled: bool,
    pub ducklake_checkpoint_maintenance: bool,
    pub ducklake_maintenance_options: bool,
    pub whole_day_retention: bool,
}

#[derive(Clone, Debug, Serialize)]
pub struct ArrowWriteBufferMetric {
    pub storage_signal: StorageSignal,
    pub rows: usize,
    pub bytes: usize,
    pub age_seconds: f64,
}

/// Folded Arrow write-buffer aggregates the ingest admission freshness
/// projection consumes (the three [`crate::admission_control::FreshnessBudgetInputs`]
/// buffer scalars), produced by [`Storage::arrow_write_buffer_freshness`] under a
/// single lock without the per-signal [`ArrowWriteBufferMetric`] vec.
#[derive(Clone, Copy, Debug, Default)]
pub struct ArrowWriteBufferFreshness {
    pub buffered_bytes: usize,
    pub buffered_active_count: usize,
    pub oldest_buffer_age_seconds: f64,
}

pub struct Storage {
    /// Write-side connection. Held for inserts, DDL, and DuckLake maintenance.
    /// Reader path never touches this mutex — that decoupling keeps /healthz,
    /// /metrics, and queries responsive while a seal is in flight.
    writer: Mutex<Connection>,
    /// Cloned from `writer` at startup after ATTACH + DDL; shares the
    /// underlying Database (attached schemas survive `try_clone`). Source of
    /// per-query clones in `with_query_conn`.
    reader: Mutex<Connection>,
    target_prefix: String,
    mode: String,
    catalog_name: String,
    #[cfg(debug_assertions)]
    force_dependency_unhealthy: AtomicBool,
    postgres_catalog_configured: bool,
    local_storage_dir: PathBuf,
    ducklake_maintenance_enabled: bool,
    ducklake_maintenance_options_supported: bool,
    ducklake_checkpoint_supported: AtomicBool,
    ducklake_maintenance_capability_reason: Mutex<Option<String>>,
    arrow_write_buffers: Mutex<BTreeMap<StorageSignal, ArrowWriteBuffer>>,
    write_memory_limit: String,
    last_error: Mutex<Option<String>>,
    /// Cache-invalidation token for discovery metadata. Bumped only after a
    /// committed `metadata_summary` change (refresh or retention); discovery
    /// caches in `Metadata` key entries on this value and drop them on a bump.
    metadata_generation: AtomicU64,
    /// StorageSignal/event-date buckets whose `metadata_summary` rows are stale after
    /// a committed insert. Drained by the `metadata_refresh` scheduler job so
    /// the day-partition re-aggregation stays off the ingest commit path.
    dirty_metadata: Mutex<BTreeMap<StorageSignal, BTreeSet<String>>>,
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

pub(crate) struct ReplayBackedArrowBatch<'a> {
    pub(crate) storage_signal: StorageSignal,
    pub(crate) batch: &'a RecordBatch,
    pub(crate) source_format: &'a str,
    pub(crate) replay_ref: ReplayBackedRecordRef,
}

#[derive(Clone, Debug)]
pub struct InternalTelemetryCommitResult {
    pub rows: usize,
    pub timings: Vec<ArrowBatchBufferTiming>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TimingPhase {
    Coalesce,
    Prepare,
    ArrowWriteCoalesce,
    ArrowWriteBuffer,
    DuckdbArrowAppend,
    DucklakeCommit,
    WriterLockWait,
}

impl TimingPhase {
    pub fn as_str(self) -> &'static str {
        match self {
            TimingPhase::Coalesce => "storage_coalesce",
            TimingPhase::Prepare => "storage_prepare",
            TimingPhase::ArrowWriteCoalesce => "storage_arrow_write_coalesce",
            TimingPhase::ArrowWriteBuffer => "storage_arrow_write_buffer",
            TimingPhase::DuckdbArrowAppend => "storage_duckdb_arrow_append",
            TimingPhase::DucklakeCommit => "storage_ducklake_commit",
            TimingPhase::WriterLockWait => "writer_lock_wait",
        }
    }
}

impl std::fmt::Display for TimingPhase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Clone, Debug)]
pub struct ArrowBatchBufferTiming {
    pub storage_signal: StorageSignal,
    pub phase: TimingPhase,
    pub rows: usize,
    pub seconds: f64,
}

#[derive(Clone, Debug)]
pub struct ArrowBatchBufferResult {
    pub rows: usize,
    pub timings: Vec<ArrowBatchBufferTiming>,
}

struct PreparedArrowBatch {
    pub(super) storage_signal: StorageSignal,
    pub(super) batch: RecordBatch,
    pub(super) rows: usize,
    pub(super) timestamp_days: Vec<String>,
    pub(super) replay_refs: BTreeSet<ReplayBackedRecordRef>,
}

impl Storage {
    pub fn open(config: &Config) -> Result<Self> {
        if let Some(parent) = config.operator.duckdb_path.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::create_dir_all(&config.operator.local_storage_dir)?;

        let writer = Connection::open(&config.operator.duckdb_path).with_context(|| {
            format!("open DuckDB file {}", config.operator.duckdb_path.display())
        })?;
        configure_base_connection(&writer)?;
        configure_write_connection(&writer, &config.operator.duckdb_write_memory_limit)?;

        attach_ducklake_connection(&writer, config).context(
            "DuckLake attach failed. Fix the catalog config (URI, token, network, or extension path) and restart.",
        )?;
        let target_prefix = DUCKLAKE_TARGET_PREFIX.to_string();
        let plan = ducklake_attach_plan(config)?;
        let mode = format!("{}_arrow_append", plan.mode);
        let ducklake_maintenance_capability =
            configure_ducklake_maintenance_options(&writer, &config.operator.ducklake_maintenance)
                .context("configure DuckLake maintenance options")?;

        create_tables_on(&writer, &target_prefix)?;
        // Fail-closed if this binary cannot safely operate on the catalog's
        // schema generation; stamp it on a fresh/legacy catalog.
        enforce_schema_version_on(&writer, &target_prefix)?;

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
            catalog_name: DUCKLAKE_CATALOG_NAME.to_string(),
            #[cfg(debug_assertions)]
            force_dependency_unhealthy: AtomicBool::new(false),
            postgres_catalog_configured: config.operator.postgres_dsn.is_some(),
            local_storage_dir: config.operator.local_storage_dir.clone(),
            ducklake_maintenance_enabled: config.operator.ducklake_maintenance.enabled,
            ducklake_maintenance_options_supported: ducklake_maintenance_capability
                .options_supported,
            ducklake_checkpoint_supported: AtomicBool::new(
                ducklake_maintenance_capability.checkpoint_supported,
            ),
            ducklake_maintenance_capability_reason: Mutex::new(
                ducklake_maintenance_capability.reason,
            ),
            arrow_write_buffers: Mutex::new(BTreeMap::new()),
            write_memory_limit: config.operator.duckdb_write_memory_limit.clone(),
            last_error: Mutex::new(None),
            metadata_generation: AtomicU64::new(0),
            dirty_metadata: Mutex::new(BTreeMap::new()),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::arrow::batch_timestamp_days;
    use super::ducklake::{
        build_ducklake_attach_plan, ducklake_maintenance_options_sql, object_store_kind,
        object_store_secret_sql, ObjectStore,
    };
    use super::metadata_refresh::metadata_refresh_sql;
    use super::*;
    use crate::config::DuckLakeMaintenanceConfig;
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
            None,
            None,
            None,
            false,
            &dir.path().join("canardstack.duckdb"),
            &dir.path().join("storage"),
        )
        .unwrap();

        assert_eq!(
            plan.sql,
            "ATTACH 'ducklake:md:test-ducklake' AS canardlake; USE canardlake;"
        );
        assert_eq!(plan.mode, "ducklake_custom_uri");
        assert!(plan.needs_ducklake);
        assert!(!plan.needs_postgres);
    }

    #[test]
    fn motherduck_attach_uri_uses_md_extension_and_canardlake_alias() {
        let dir = tempdir().unwrap();
        let plan = build_ducklake_attach_plan(
            None,
            Some("md:test-ducklake"),
            None,
            None,
            None,
            false,
            &dir.path().join("canardstack.duckdb"),
            &dir.path().join("storage"),
        )
        .unwrap();

        assert_eq!(
            plan.sql,
            "ATTACH 'md:test-ducklake' AS canardlake; USE canardlake;"
        );
        assert_eq!(plan.mode, "ducklake_motherduck_remote");
        assert!(!plan.needs_ducklake);
        assert!(plan.needs_motherduck);
        assert!(!plan.needs_quack);
        assert!(!plan.needs_postgres);
    }

    #[test]
    fn quack_attach_uri_loads_quack_and_creates_secret() {
        let dir = tempdir().unwrap();
        let plan = build_ducklake_attach_plan(
            None,
            Some("ducklake:quack:catalog.internal:443"),
            None,
            Some("s3://canardstack-data/prod/"),
            Some("catalog'token"),
            false,
            &dir.path().join("canardstack.duckdb"),
            &dir.path().join("storage"),
        )
        .unwrap();

        assert_eq!(
            plan.sql,
            "CREATE OR REPLACE SECRET canardstack_ducklake_quack (TYPE quack, SCOPE 'quack:catalog.internal:443', TOKEN 'catalog''token'); ATTACH 'ducklake:quack:catalog.internal:443' AS canardlake (DATA_PATH 's3://canardstack-data/prod/'); USE canardlake;"
        );
        assert!(plan.needs_ducklake);
        assert!(plan.needs_quack);
    }

    #[test]
    fn quack_attach_uri_insecure_tls_adds_scoped_verify_ssl_secret() {
        let dir = tempdir().unwrap();
        let plan = build_ducklake_attach_plan(
            None,
            Some("ducklake:quack:catalog.internal:9494"),
            None,
            Some("s3://canardstack-data/prod/"),
            Some("catalog-token"),
            true,
            &dir.path().join("canardstack.duckdb"),
            &dir.path().join("storage"),
        )
        .unwrap();

        // Insecure TLS = a scoped HTTP VERIFY_SSL 0 secret for the catalog URL
        // (the quack secret has no SSL param).
        assert!(plan.sql.contains(
            "CREATE OR REPLACE SECRET canardstack_quack_tls (TYPE HTTP, SCOPE 'https://catalog.internal:9494', VERIFY_SSL 0);"
        ));
        assert!(plan.sql.contains(
            "CREATE OR REPLACE SECRET canardstack_ducklake_quack (TYPE quack, SCOPE 'quack:catalog.internal:9494', TOKEN 'catalog-token');"
        ));
        assert!(!plan.sql.contains("DISABLE_SSL"));
        assert!(plan.needs_quack);
    }

    #[test]
    fn local_quack_insecure_tls_does_not_emit_ducklake_ssl_option() {
        let dir = tempdir().unwrap();
        let plan = build_ducklake_attach_plan(
            None,
            Some("ducklake:quack:127.0.0.1:9494"),
            None,
            Some("/data"),
            Some("catalog-token"),
            true,
            &dir.path().join("canardstack.duckdb"),
            &dir.path().join("storage"),
        )
        .unwrap();

        assert!(plan.sql.contains(
            "CREATE OR REPLACE SECRET canardstack_quack_tls (TYPE HTTP, SCOPE 'https://127.0.0.1:9494', VERIFY_SSL 0);"
        ));
        assert!(plan
            .sql
            .contains("ATTACH 'ducklake:quack:127.0.0.1:9494' AS canardlake (DATA_PATH '/data');"));
        assert!(!plan.sql.contains("DISABLE_SSL"));
    }

    #[test]
    fn quack_attach_uri_requires_token() {
        let dir = tempdir().unwrap();
        let err = build_ducklake_attach_plan(
            None,
            Some("ducklake:quack:catalog.internal:443"),
            None,
            Some("s3://canardstack-data/prod/"),
            None,
            false,
            &dir.path().join("canardstack.duckdb"),
            &dir.path().join("storage"),
        )
        .unwrap_err();

        assert!(err
            .to_string()
            .contains("CANARDSTACK_DUCKLAKE_QUACK_TOKEN must be set"));
    }

    #[test]
    fn custom_attach_uri_and_postgres_catalog_are_mutually_exclusive() {
        let dir = tempdir().unwrap();
        let err = build_ducklake_attach_plan(
            Some("dbname=ducklake_catalog host=localhost"),
            Some("ducklake:md:test-ducklake"),
            None,
            None,
            None,
            false,
            &dir.path().join("canardstack.duckdb"),
            &dir.path().join("storage"),
        )
        .unwrap_err();

        assert!(err.to_string().contains(
            "set only one of CANARDSTACK_POSTGRES_DSN or CANARDSTACK_DUCKLAKE_ATTACH_URI"
        ));
    }

    #[test]
    fn catalog_path_requires_local_duckdb_catalog() {
        let dir = tempdir().unwrap();
        let err = build_ducklake_attach_plan(
            None,
            Some("ducklake:/catalog/canardstack.ducklake"),
            Some(&dir.path().join("catalog.ducklake")),
            None,
            None,
            false,
            &dir.path().join("canardstack.duckdb"),
            &dir.path().join("storage"),
        )
        .unwrap_err();

        assert!(err
            .to_string()
            .contains("CANARDSTACK_DUCKLAKE_CATALOG_PATH can only be set"));
    }

    #[test]
    fn local_ducklake_attach_uses_data_path() {
        let dir = tempdir().unwrap();
        let plan = build_ducklake_attach_plan(
            None,
            None,
            None,
            None,
            None,
            false,
            &dir.path().join("canardstack.duckdb"),
            &dir.path().join("storage"),
        )
        .unwrap();

        assert!(plan.sql.contains("DATA_PATH"));
    }

    #[test]
    fn local_ducklake_attach_accepts_object_store_data_path() {
        let dir = tempdir().unwrap();
        let plan = build_ducklake_attach_plan(
            None,
            None,
            None,
            Some("s3://canardstack-data/prod/"),
            None,
            false,
            &dir.path().join("canardstack.duckdb"),
            &dir.path().join("storage"),
        )
        .unwrap();

        assert!(plan
            .sql
            .contains("(DATA_PATH 's3://canardstack-data/prod/')"));
    }

    #[test]
    fn local_ducklake_attach_accepts_catalog_path_override() {
        let dir = tempdir().unwrap();
        let catalog_path = dir.path().join("catalog/canardstack.ducklake");
        let plan = build_ducklake_attach_plan(
            None,
            None,
            Some(&catalog_path),
            Some("s3://canardstack-data/prod/"),
            None,
            false,
            &dir.path().join("canardstack.duckdb"),
            &dir.path().join("storage"),
        )
        .unwrap();

        assert!(plan.sql.contains(&format!(
            "ATTACH 'ducklake:{}' AS canardlake",
            catalog_path.to_string_lossy()
        )));
        assert!(plan
            .sql
            .contains("(DATA_PATH 's3://canardstack-data/prod/')"));
    }

    #[test]
    fn object_store_kind_detects_cloud_schemes() {
        assert_eq!(
            object_store_kind("s3://canardstack-data/prod/"),
            Some(ObjectStore::S3)
        );
        assert_eq!(object_store_kind("s3a://bucket/x"), Some(ObjectStore::S3));
        assert_eq!(object_store_kind("gcs://bucket/p/"), Some(ObjectStore::Gcs));
        assert_eq!(object_store_kind("gs://bucket/p/"), Some(ObjectStore::Gcs));
        assert_eq!(object_store_kind("/var/lib/canardstack/storage"), None);
    }

    #[test]
    fn configure_object_store_for_data_path_skips_local() {
        // A local data path needs no object-store secret, so the helper is a
        // no-op (returns None) without loading httpfs/aws -- this keeps the
        // serve-catalog server offline-safe when DATA_PATH is a local directory.
        let conn = duckdb::Connection::open_in_memory().unwrap();
        assert_eq!(
            super::configure_object_store_for_data_path(&conn, "/var/lib/canardstack/storage")
                .unwrap(),
            None
        );
    }

    #[test]
    fn s3_secret_uses_credential_chain_and_optional_region() {
        assert_eq!(
            object_store_secret_sql(ObjectStore::S3, Some("us-west-2")),
            "CREATE OR REPLACE SECRET canardstack_object_store (TYPE s3, PROVIDER credential_chain, REGION 'us-west-2');"
        );
        assert_eq!(
            object_store_secret_sql(ObjectStore::S3, None),
            "CREATE OR REPLACE SECRET canardstack_object_store (TYPE s3, PROVIDER credential_chain);"
        );
    }

    #[test]
    fn gcs_secret_uses_credential_chain() {
        assert_eq!(
            object_store_secret_sql(ObjectStore::Gcs, None),
            "CREATE OR REPLACE SECRET canardstack_object_store (TYPE gcs, PROVIDER credential_chain);"
        );
    }

    #[test]
    fn s3_data_path_sets_object_store_on_plan() {
        let dir = tempdir().unwrap();
        let plan = build_ducklake_attach_plan(
            None,
            None,
            None,
            Some("s3://canardstack-data/prod/"),
            None,
            false,
            &dir.path().join("canardstack.duckdb"),
            &dir.path().join("storage"),
        )
        .unwrap();
        assert_eq!(plan.object_store, Some(ObjectStore::S3));
    }

    #[test]
    fn local_data_path_has_no_object_store() {
        let dir = tempdir().unwrap();
        let plan = build_ducklake_attach_plan(
            None,
            None,
            None,
            None,
            None,
            false,
            &dir.path().join("canardstack.duckdb"),
            &dir.path().join("storage"),
        )
        .unwrap();
        assert_eq!(plan.object_store, None);
    }

    // Live check that DuckDB 1.5.3 accepts the credential_chain secrets we emit.
    // credential_chain secrets validate at CREATE time, so fake env creds are set
    // to resolve the chain. Downloads httpfs/aws, so it is offline-excluded.
    #[test]
    #[ignore = "downloads httpfs/aws extensions; run with --ignored"]
    fn object_store_secrets_create_on_duckdb() {
        unsafe {
            std::env::set_var("AWS_ACCESS_KEY_ID", "AKIAFAKEFAKEFAKEFAKE");
            std::env::set_var(
                "AWS_SECRET_ACCESS_KEY",
                "fakefakefakefakefakefakefakefakefakefake",
            );
            std::env::set_var("AWS_REGION", "us-west-2");
        }
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch("INSTALL httpfs; LOAD httpfs; INSTALL aws; LOAD aws;")
            .unwrap();
        conn.execute_batch(&object_store_secret_sql(ObjectStore::S3, Some("us-west-2")))
            .expect("s3 credential_chain secret should create with resolvable creds");
        // Probe GCS support without failing the test on missing ADC: a credential
        // error proves the provider parsed; only an unknown-provider error matters.
        match conn.execute_batch(&object_store_secret_sql(ObjectStore::Gcs, None)) {
            Ok(()) => eprintln!("gcs credential_chain: created"),
            Err(err) => eprintln!("gcs credential_chain: {err}"),
        }
    }

    #[test]
    fn custom_duckdb_ducklake_attach_accepts_object_store_data_path() {
        let dir = tempdir().unwrap();
        let plan = build_ducklake_attach_plan(
            None,
            Some("ducklake:/catalog/canardstack.ducklake"),
            None,
            Some("gcs://canardstack-data/prod/"),
            None,
            false,
            &dir.path().join("canardstack.duckdb"),
            &dir.path().join("storage"),
        )
        .unwrap();

        assert_eq!(
            plan.sql,
            "ATTACH 'ducklake:/catalog/canardstack.ducklake' AS canardlake (DATA_PATH 'gcs://canardstack-data/prod/'); USE canardlake;"
        );
    }

    #[test]
    fn ducklake_maintenance_options_enable_checkpoint_owned_defaults() {
        let sql = ducklake_maintenance_options_sql(&DuckLakeMaintenanceConfig {
            enabled: true,
            data_inlining_row_limit: 10,
            expire_older_than_days: 14,
            delete_older_than_secs: 86_400,
        });

        assert!(sql.contains("set_option('data_inlining_row_limit', 10)"));
        assert!(sql.contains("set_option('auto_compact', true)"));
        assert!(sql.contains("set_option('expire_older_than', '14 days')"));
        assert!(sql.contains("set_option('delete_older_than', '86400 seconds')"));
    }

    #[test]
    fn ducklake_maintenance_options_disable_compaction_and_inlining() {
        let sql = ducklake_maintenance_options_sql(&DuckLakeMaintenanceConfig {
            enabled: false,
            data_inlining_row_limit: 0,
            expire_older_than_days: 14,
            delete_older_than_secs: 86_400,
        });

        assert!(sql.contains("set_option('data_inlining_row_limit', 0)"));
        assert!(sql.contains("set_option('auto_compact', false)"));
    }

    #[test]
    fn custom_attach_uri_must_be_uri_not_attach_statement() {
        let dir = tempdir().unwrap();
        let err = build_ducklake_attach_plan(
            None,
            Some("ATTACH 'md:test-ducklake';"),
            None,
            None,
            None,
            false,
            &dir.path().join("canardstack.duckdb"),
            &dir.path().join("storage"),
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
            None,
            None,
            None,
            false,
            &dir.path().join("canardstack.duckdb"),
            &dir.path().join("storage"),
        )
        .unwrap_err();

        assert!(err.to_string().contains("must be an md: or ducklake: URI"));
    }

    #[test]
    fn motherduck_attach_uri_rejects_custom_data_path() {
        let dir = tempdir().unwrap();
        let err = build_ducklake_attach_plan(
            None,
            Some("md:test-ducklake"),
            None,
            Some("s3://canardstack-data/prod/"),
            None,
            false,
            &dir.path().join("canardstack.duckdb"),
            &dir.path().join("storage"),
        )
        .unwrap_err();

        assert!(err
            .to_string()
            .contains("CANARDSTACK_DUCKLAKE_DATA_PATH cannot be set"));
    }

    #[test]
    fn metadata_refresh_uses_one_insert_per_signal_bucket() {
        // Counts exclude high-cardinality id labels (trace_id, span_id,
        // service_instance_id), which stay filterable but are not materialized.
        for (signal, select_count) in [
            (StorageSignal::Logs, 9),
            (StorageSignal::Spans, 16),
            (StorageSignal::MetricGauge, 5),
            (StorageSignal::MetricSum, 5),
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
    fn arrow_write_buffer_tracks_distinct_timestamp_days() {
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

        let days = batch_timestamp_days(&batch).unwrap();

        assert_eq!(days, vec!["2026-05-16", "2026-05-17"]);
    }
}
