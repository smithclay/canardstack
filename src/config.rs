use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, Result};
use toml_edit::{DocumentMut, Item};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ServeRole {
    All,
    Ingest,
    Query,
}

impl ServeRole {
    pub fn parse(value: &str) -> Result<Self> {
        match value {
            "all" => Ok(Self::All),
            "ingest" => Ok(Self::Ingest),
            "query" => Ok(Self::Query),
            _ => anyhow::bail!("--role must be all, ingest, or query"),
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::All => "all",
            Self::Ingest => "ingest",
            Self::Query => "query",
        }
    }

    pub fn accepts_ingest(self) -> bool {
        matches!(self, Self::All | Self::Ingest)
    }

    pub fn serves_queries(self) -> bool {
        matches!(self, Self::All | Self::Query)
    }

    pub fn runs_scheduler(self) -> bool {
        matches!(self, Self::All | Self::Ingest)
    }

    pub fn allows_maintenance_mutation(self) -> bool {
        matches!(self, Self::All | Self::Ingest)
    }
}

#[derive(Clone, Debug)]
pub struct QueryLane {
    pub concurrency: usize,
    pub timeout_secs: u64,
    pub memory_limit: String,
}

#[derive(Clone, Debug)]
pub struct Config {
    pub serve_role: ServeRole,
    pub bind: String,
    pub api_key: String,
    pub admin_api_key: String,
    pub duckdb_path: PathBuf,
    pub local_storage_dir: PathBuf,
    pub duckdb_extension_dir: Option<PathBuf>,
    pub postgres_dsn: Option<String>,
    pub ducklake_attach_uri: Option<String>,
    pub max_body_bytes: usize,
    pub per_signal_queue_bytes: usize,
    pub process_ingest_bytes: usize,
    pub runtime_memory_limit_bytes: Option<usize>,
    pub max_rows_per_flush: usize,
    pub max_bytes_per_flush: usize,
    pub max_age: Duration,
    pub high_pressure_max_age: Duration,
    pub duckdb_write_memory_limit: String,
    pub late_accept_secs: i64,
    pub future_accept_secs: i64,
    pub immutable_segment_target_bytes: usize,
    pub immutable_segment_max_age: Duration,
    pub query_interactive: QueryLane,
    pub lane_flush_capacity: usize,
    pub lane_cheap_query_capacity: usize,
    pub lane_heavy_query_degraded_capacity: usize,
    pub lane_freshness_sla: Duration,
    pub logs_retention_days: i64,
    pub spans_retention_days: i64,
    pub metrics_retention_days: i64,
    pub scheduler_enabled: bool,
    pub scheduler_flush_interval: Duration,
    pub scheduler_metadata_interval: Duration,
    pub scheduler_metrics_interval: Duration,
    pub scheduler_retention_interval: Duration,
    pub max_concurrent_connections: usize,
    pub socket_read_timeout: Duration,
    pub socket_write_timeout: Duration,
    pub bench_http_keepalive: bool,
    pub raw_spool_dir: PathBuf,
    pub raw_spool_max_segment_bytes: usize,
    pub raw_spool_max_record_bytes: usize,
    pub raw_spool_max_total_bytes: usize,
    pub raw_spool_writer_queue_capacity: usize,
    pub raw_spool_group_commit_records: usize,
    pub raw_spool_group_commit_delay: Duration,
    pub raw_spool_append_sync_interval: Duration,
    pub raw_spool_append_sync_bytes: usize,
    pub raw_spool_checkpoint_fsync_records: usize,
    pub raw_spool_checkpoint_fsync_delay: Duration,
    pub ingest_workers: usize,
    pub ingest_buffer_capacity: usize,
}

const DEFAULT_CONFIG_PATH: &str = "config.toml";
const CONFIG_PATH_ENV: &str = "CANARDSTACK_CONFIG";

impl Config {
    pub fn from_env() -> Result<Self> {
        let file = FileConfig::load()?;
        let data_dir = env_path("CANARDSTACK_DATA_DIR")?
            .or(file.path(&["paths", "data_dir"])?)
            .unwrap_or_else(|| PathBuf::from(".canardstack"));
        let max_body_bytes = env_usize("CANARDSTACK_MAX_BODY_BYTES")?
            .or(file.usize(&["ingest", "max_body_bytes"])?)
            .unwrap_or(8 * 1024 * 1024);
        let ingest_memory_bytes = env_usize("CANARDSTACK_INGEST_MEMORY_BYTES")?
            .or(file.usize(&["ingest", "memory_bytes"])?)
            .unwrap_or(2 * 1024 * 1024 * 1024);
        let flush_target_bytes = env_usize("CANARDSTACK_FLUSH_TARGET_BYTES")?
            .or(file.usize(&["ingest", "flush_target_bytes"])?)
            .unwrap_or(4 * 1024 * 1024);
        let flush_max_age = duration_ms_or_secs(
            &file,
            &["ingest", "flush_max_age_ms"],
            &["ingest", "flush_max_age_secs"],
            "CANARDSTACK_FLUSH_MAX_AGE_MS",
            "CANARDSTACK_FLUSH_MAX_AGE_SECS",
            10,
        )?;
        let query_concurrency = env_usize("CANARDSTACK_QUERY_CONCURRENCY")?
            .or(file.usize(&["query", "concurrency"])?)
            .unwrap_or(4);
        let query_timeout_secs = env_usize("CANARDSTACK_QUERY_TIMEOUT_SECS")?
            .or(file.usize(&["query", "timeout_secs"])?)
            .unwrap_or(15) as u64;
        let query_memory_limit = env_string("CANARDSTACK_QUERY_MEMORY_LIMIT")?
            .or(file.string(&["query", "memory_limit"])?)
            .unwrap_or_else(|| "1GiB".to_string());
        let retention_days = env_i64("CANARDSTACK_RETENTION_DAYS")?
            .or(file.i64(&["retention", "days"])?)
            .unwrap_or(14);
        let maintenance_interval = duration_ms_or_secs(
            &file,
            &["scheduler", "maintenance_interval_ms"],
            &["scheduler", "maintenance_interval_secs"],
            "CANARDSTACK_MAINTENANCE_INTERVAL_MS",
            "CANARDSTACK_MAINTENANCE_INTERVAL_SECS",
            30,
        )?;
        let raw_spool_capacity_bytes = env_usize("CANARDSTACK_RAW_SPOOL_CAPACITY_BYTES")?
            .or(file.usize(&["raw_spool", "capacity_bytes"])?)
            .unwrap_or(1024 * 1024 * 1024);

        Ok(Self {
            serve_role: ServeRole::All,
            bind: env_string("CANARDSTACK_BIND")?
                .or(file.string(&["server", "bind"])?)
                .unwrap_or_else(|| "127.0.0.1:4318".to_string()),
            api_key: env_string("CANARDSTACK_API_KEY")?
                .or(file.string(&["auth", "api_key"])?)
                .unwrap_or_else(|| "dev-canardstack-key".to_string()),
            admin_api_key: env_string("CANARDSTACK_ADMIN_API_KEY")?
                .or(file.string(&["auth", "admin_api_key"])?)
                .unwrap_or_else(|| "dev-canardstack-admin-key".to_string()),
            duckdb_path: data_dir.join("canardstack.duckdb"),
            local_storage_dir: data_dir.join("storage"),
            duckdb_extension_dir: match env_optional_path("CANARDSTACK_DUCKDB_EXTENSION_DIR")? {
                Some(value) => value,
                None => file.optional_path(&["paths", "duckdb_extension_dir"])?,
            },
            postgres_dsn: match env_optional_string("CANARDSTACK_POSTGRES_DSN")? {
                Some(value) => value,
                None => file.optional_string(&["ducklake", "postgres_dsn"])?,
            },
            ducklake_attach_uri: match env_optional_string("CANARDSTACK_DUCKLAKE_ATTACH_URI")? {
                Some(value) => value,
                None => file.optional_string(&["ducklake", "attach_uri"])?,
            },
            max_body_bytes,
            per_signal_queue_bytes: (ingest_memory_bytes / 4).max(max_body_bytes),
            process_ingest_bytes: ingest_memory_bytes,
            runtime_memory_limit_bytes: match env_optional_usize(
                "CANARDSTACK_PROCESS_MEMORY_LIMIT_BYTES",
            )? {
                Some(value) => value,
                None => file.usize(&["ingest", "process_memory_limit_bytes"])?,
            },
            max_rows_per_flush: 5_000,
            max_bytes_per_flush: flush_target_bytes,
            max_age: flush_max_age,
            high_pressure_max_age: (flush_max_age / 5).max(Duration::from_millis(500)),
            duckdb_write_memory_limit: env_string("CANARDSTACK_DUCKDB_MEMORY_LIMIT")?
                .or(file.string(&["duckdb", "memory_limit"])?)
                .unwrap_or_else(|| "1GiB".to_string()),
            late_accept_secs: env_i64("CANARDSTACK_ACCEPT_LATE_SECS")?
                .or(file.i64(&["validation", "accept_late_secs"])?)
                .unwrap_or(24 * 60 * 60),
            future_accept_secs: env_i64("CANARDSTACK_ACCEPT_FUTURE_SECS")?
                .or(file.i64(&["validation", "accept_future_secs"])?)
                .unwrap_or(10 * 60),
            immutable_segment_target_bytes: env_usize("CANARDSTACK_SEGMENT_TARGET_BYTES")?
                .or(file.usize(&["storage", "segment_target_bytes"])?)
                .unwrap_or(64 * 1024 * 1024),
            immutable_segment_max_age: duration_ms_or_secs(
                &file,
                &["storage", "segment_max_age_ms"],
                &["storage", "segment_max_age_secs"],
                "CANARDSTACK_SEGMENT_MAX_AGE_MS",
                "CANARDSTACK_SEGMENT_MAX_AGE_SECS",
                10,
            )?,
            query_interactive: QueryLane {
                concurrency: query_concurrency,
                timeout_secs: query_timeout_secs,
                memory_limit: query_memory_limit,
            },
            lane_flush_capacity: env_usize("CANARDSTACK_FLUSH_LANE_CAPACITY")?
                .or(file.usize(&["lanes", "flush_capacity"])?)
                .unwrap_or(1),
            lane_cheap_query_capacity: env_usize("CANARDSTACK_CHEAP_QUERY_LANE_CAPACITY")?
                .or(file.usize(&["lanes", "cheap_query_capacity"])?)
                .unwrap_or(1),
            lane_heavy_query_degraded_capacity: env_usize(
                "CANARDSTACK_HEAVY_QUERY_DEGRADED_CAPACITY",
            )?
            .or(file.usize(&["lanes", "heavy_query_degraded_capacity"])?)
            .unwrap_or(1),
            lane_freshness_sla: duration_ms_or_secs(
                &file,
                &["lanes", "freshness_sla_ms"],
                &["lanes", "freshness_sla_secs"],
                "CANARDSTACK_FRESHNESS_SLA_MS",
                "CANARDSTACK_FRESHNESS_SLA_SECS",
                15,
            )?,
            logs_retention_days: retention_days,
            spans_retention_days: retention_days,
            metrics_retention_days: retention_days,
            scheduler_enabled: env_bool("CANARDSTACK_SCHEDULER_ENABLED")?
                .or(file.bool(&["scheduler", "enabled"])?)
                .unwrap_or(true),
            // Seal cadence for the single flush driver. Decoupled from the coarse
            // maintenance interval: it must stay well under the freshness SLA so
            // immutable-buffer age never approaches the lane reject threshold.
            scheduler_flush_interval: duration_ms_or_secs(
                &file,
                &["scheduler", "flush_interval_ms"],
                &["scheduler", "flush_interval_secs"],
                "CANARDSTACK_FLUSH_INTERVAL_MS",
                "CANARDSTACK_FLUSH_INTERVAL_SECS",
                1,
            )?,
            scheduler_metadata_interval: maintenance_interval,
            scheduler_metrics_interval: maintenance_interval.saturating_mul(2),
            scheduler_retention_interval: maintenance_interval.saturating_mul(120),
            max_concurrent_connections: env_usize("CANARDSTACK_MAX_CONNECTIONS")?
                .or(file.usize(&["server", "max_connections"])?)
                .unwrap_or(1024),
            socket_read_timeout: Duration::from_secs(
                env_usize("CANARDSTACK_SOCKET_READ_TIMEOUT_SECS")?
                    .or(file.usize(&["server", "socket_read_timeout_secs"])?)
                    .unwrap_or(30) as u64,
            ),
            socket_write_timeout: Duration::from_secs(
                env_usize("CANARDSTACK_SOCKET_WRITE_TIMEOUT_SECS")?
                    .or(file.usize(&["server", "socket_write_timeout_secs"])?)
                    .unwrap_or(30) as u64,
            ),
            bench_http_keepalive: env_bool("CANARDSTACK_BENCH_HTTP_KEEPALIVE")?
                .or(file.bool(&["bench", "http_keepalive"])?)
                .unwrap_or(true),
            raw_spool_dir: data_dir.join("raw-spool"),
            raw_spool_max_segment_bytes: (64 * 1024 * 1024).min(raw_spool_capacity_bytes),
            raw_spool_max_record_bytes: max_body_bytes,
            raw_spool_max_total_bytes: raw_spool_capacity_bytes,
            raw_spool_writer_queue_capacity: 1024,
            raw_spool_group_commit_records: 64,
            raw_spool_group_commit_delay: Duration::from_millis(
                env_usize("CANARDSTACK_RAW_SPOOL_GROUP_COMMIT_MS")?
                    .or(file.usize(&["raw_spool", "group_commit_ms"])?)
                    .unwrap_or(1) as u64,
            ),
            raw_spool_append_sync_interval: Duration::from_millis(
                env_usize("CANARDSTACK_RAW_SPOOL_APPEND_SYNC_MS")?
                    .or(file.usize(&["raw_spool", "append_sync_ms"])?)
                    .unwrap_or(500) as u64,
            ),
            raw_spool_append_sync_bytes: env_usize("CANARDSTACK_RAW_SPOOL_APPEND_SYNC_BYTES")?
                .or(file.usize(&["raw_spool", "append_sync_bytes"])?)
                .unwrap_or(16 * 1024 * 1024),
            raw_spool_checkpoint_fsync_records: 1024,
            raw_spool_checkpoint_fsync_delay: Duration::from_millis(1000),
            ingest_workers: env_usize("CANARDSTACK_INGEST_WORKERS")?
                .or(file.usize(&["ingest", "workers"])?)
                .unwrap_or(4),
            ingest_buffer_capacity: env_usize("CANARDSTACK_INGEST_BUFFER_CAPACITY")?
                .or(file.usize(&["ingest", "buffer_capacity"])?)
                .unwrap_or(1024),
        })
    }

    pub fn test(duckdb_path: PathBuf) -> Self {
        let local_storage_dir = duckdb_path
            .parent()
            .unwrap_or_else(|| std::path::Path::new("."))
            .join("storage");
        Self {
            serve_role: ServeRole::All,
            bind: "127.0.0.1:0".to_string(),
            api_key: "test-key".to_string(),
            admin_api_key: "test-admin-key".to_string(),
            duckdb_path,
            local_storage_dir: local_storage_dir.clone(),
            duckdb_extension_dir: None,
            postgres_dsn: None,
            ducklake_attach_uri: None,
            max_body_bytes: 8 * 1024 * 1024,
            per_signal_queue_bytes: 1024 * 1024,
            process_ingest_bytes: 4 * 1024 * 1024,
            runtime_memory_limit_bytes: None,
            max_rows_per_flush: 100,
            max_bytes_per_flush: 256 * 1024,
            max_age: Duration::from_millis(50),
            high_pressure_max_age: Duration::from_millis(10),
            duckdb_write_memory_limit: "512MiB".to_string(),
            late_accept_secs: 24 * 60 * 60,
            future_accept_secs: 10 * 60,
            immutable_segment_target_bytes: 64 * 1024 * 1024,
            immutable_segment_max_age: Duration::from_secs(10),
            query_interactive: QueryLane {
                concurrency: 4,
                timeout_secs: 15,
                memory_limit: "512MiB".to_string(),
            },
            lane_flush_capacity: 1,
            lane_cheap_query_capacity: 1,
            lane_heavy_query_degraded_capacity: 1,
            lane_freshness_sla: Duration::from_secs(15),
            logs_retention_days: 14,
            spans_retention_days: 14,
            metrics_retention_days: 30,
            scheduler_enabled: false,
            scheduler_flush_interval: Duration::from_millis(200),
            scheduler_metadata_interval: Duration::from_millis(200),
            scheduler_metrics_interval: Duration::from_millis(200),
            scheduler_retention_interval: Duration::from_secs(3_600),
            max_concurrent_connections: 64,
            socket_read_timeout: Duration::from_secs(5),
            socket_write_timeout: Duration::from_secs(5),
            bench_http_keepalive: false,
            raw_spool_dir: local_storage_dir.join("raw-spool"),
            raw_spool_max_segment_bytes: 64 * 1024 * 1024,
            raw_spool_max_record_bytes: 8 * 1024 * 1024,
            raw_spool_max_total_bytes: 1024 * 1024 * 1024,
            raw_spool_writer_queue_capacity: 1024,
            raw_spool_group_commit_records: 64,
            raw_spool_group_commit_delay: Duration::from_millis(1),
            raw_spool_append_sync_interval: Duration::from_millis(500),
            raw_spool_append_sync_bytes: 16 * 1024 * 1024,
            raw_spool_checkpoint_fsync_records: 1024,
            raw_spool_checkpoint_fsync_delay: Duration::from_millis(1000),
            ingest_workers: 4,
            ingest_buffer_capacity: 1024,
        }
    }

    /// Fail boot rather than start with a misconfiguration that's invisible at runtime.
    pub fn validate(&self) -> anyhow::Result<()> {
        if self.api_key.trim().is_empty() {
            anyhow::bail!("CANARDSTACK_API_KEY must not be empty");
        }
        if self.admin_api_key.trim().is_empty() {
            anyhow::bail!("CANARDSTACK_ADMIN_API_KEY must not be empty");
        }
        if self.api_key == self.admin_api_key {
            anyhow::bail!(
                "CANARDSTACK_API_KEY and CANARDSTACK_ADMIN_API_KEY must differ; reusing a single key collapses the admin authorization gate"
            );
        }
        if self.per_signal_queue_bytes > self.process_ingest_bytes {
            anyhow::bail!(
                "derived per-signal ingest queue bytes ({}) must be <= CANARDSTACK_INGEST_MEMORY_BYTES ({})",
                self.per_signal_queue_bytes,
                self.process_ingest_bytes
            );
        }
        if self.max_body_bytes == 0 {
            anyhow::bail!("CANARDSTACK_MAX_BODY_BYTES must be > 0");
        }
        if self.process_ingest_bytes == 0 || self.per_signal_queue_bytes == 0 {
            anyhow::bail!("ingest memory caps must be > 0");
        }
        if matches!(self.runtime_memory_limit_bytes, Some(0)) {
            anyhow::bail!("CANARDSTACK_PROCESS_MEMORY_LIMIT_BYTES must be > 0 when set");
        }
        if self.max_rows_per_flush == 0 || self.max_bytes_per_flush == 0 {
            anyhow::bail!("flush thresholds must be > 0");
        }
        if self.max_age.is_zero() || self.high_pressure_max_age.is_zero() {
            anyhow::bail!("flush age controls must be > 0");
        }
        if self.duckdb_write_memory_limit.trim().is_empty() {
            anyhow::bail!("CANARDSTACK_DUCKDB_MEMORY_LIMIT must not be empty");
        }
        if self.immutable_segment_target_bytes == 0 {
            anyhow::bail!("CANARDSTACK_SEGMENT_TARGET_BYTES must be > 0");
        }
        if self.immutable_segment_max_age.is_zero() {
            anyhow::bail!("CANARDSTACK_SEGMENT_MAX_AGE_MS/SECS must be > 0");
        }
        if self.high_pressure_max_age > self.max_age {
            anyhow::bail!(
                "derived high-pressure flush age must be <= CANARDSTACK_FLUSH_MAX_AGE_MS/SECS"
            );
        }
        if self.logs_retention_days <= 0
            || self.spans_retention_days <= 0
            || self.metrics_retention_days <= 0
        {
            anyhow::bail!("retention day counts must be > 0");
        }
        if self.max_concurrent_connections == 0 {
            anyhow::bail!("CANARDSTACK_MAX_CONNECTIONS must be > 0");
        }
        if self.query_interactive.concurrency == 0 {
            anyhow::bail!("query concurrency limits must be > 0");
        }
        if self.lane_flush_capacity == 0
            || self.lane_cheap_query_capacity == 0
            || self.lane_heavy_query_degraded_capacity == 0
        {
            anyhow::bail!("lane capacities must be > 0");
        }
        if self.lane_freshness_sla.is_zero() {
            anyhow::bail!("CANARDSTACK_FRESHNESS_SLA_MS/SECS must be > 0");
        }
        if self.query_interactive.concurrency
            <= self
                .lane_flush_capacity
                .saturating_add(self.lane_cheap_query_capacity)
        {
            anyhow::bail!(
                "CANARDSTACK_QUERY_CONCURRENCY must leave at least one heavy query slot after flush and cheap-query lane reservations"
            );
        }
        if self.query_interactive.timeout_secs == 0 {
            anyhow::bail!("query timeouts must be > 0");
        }
        if self.socket_read_timeout.is_zero() || self.socket_write_timeout.is_zero() {
            anyhow::bail!("socket timeouts must be > 0");
        }
        if self.raw_spool_max_segment_bytes == 0
            || self.raw_spool_max_record_bytes == 0
            || self.raw_spool_max_total_bytes == 0
            || self.raw_spool_writer_queue_capacity == 0
            || self.raw_spool_group_commit_records == 0
            || self.raw_spool_append_sync_bytes == 0
            || self.raw_spool_checkpoint_fsync_records == 0
        {
            anyhow::bail!("raw spool limits must be > 0");
        }
        if self.raw_spool_group_commit_delay.is_zero() {
            anyhow::bail!("CANARDSTACK_RAW_SPOOL_GROUP_COMMIT_MS must be > 0");
        }
        if self.raw_spool_append_sync_interval.is_zero() {
            anyhow::bail!("CANARDSTACK_RAW_SPOOL_APPEND_SYNC_MS must be > 0");
        }
        if self.raw_spool_checkpoint_fsync_delay.is_zero() {
            anyhow::bail!("raw spool checkpoint fsync delay must be > 0");
        }
        if self.ingest_workers == 0 {
            anyhow::bail!("CANARDSTACK_INGEST_WORKERS must be > 0");
        }
        if self.ingest_buffer_capacity == 0 {
            anyhow::bail!("CANARDSTACK_INGEST_BUFFER_CAPACITY must be > 0");
        }
        if self.raw_spool_max_record_bytes > self.raw_spool_max_total_bytes {
            anyhow::bail!(
                "CANARDSTACK_MAX_BODY_BYTES must be <= CANARDSTACK_RAW_SPOOL_CAPACITY_BYTES"
            );
        }
        if self.scheduler_flush_interval.is_zero()
            || self.scheduler_metadata_interval.is_zero()
            || self.scheduler_metrics_interval.is_zero()
            || self.scheduler_retention_interval.is_zero()
        {
            anyhow::bail!("scheduler intervals must be > 0");
        }
        Ok(())
    }
}

struct FileConfig {
    doc: Option<DocumentMut>,
}

impl FileConfig {
    fn load() -> Result<Self> {
        let path = match env_optional_string(CONFIG_PATH_ENV)? {
            Some(Some(path)) => Some(PathBuf::from(path)),
            Some(None) => None,
            None if Path::new(DEFAULT_CONFIG_PATH).exists() => {
                Some(PathBuf::from(DEFAULT_CONFIG_PATH))
            }
            None => None,
        };

        let Some(path) = path else {
            return Ok(Self { doc: None });
        };
        let content = fs::read_to_string(&path)
            .with_context(|| format!("read config file {}", path.display()))?;
        let doc = content
            .parse::<DocumentMut>()
            .with_context(|| format!("parse config file {}", path.display()))?;
        Ok(Self { doc: Some(doc) })
    }

    fn item(&self, path: &[&str]) -> Option<&Item> {
        let (first, rest) = path.split_first()?;
        let mut item = self.doc.as_ref()?.get(first)?;
        for key in rest {
            item = item.as_table_like()?.get(key)?;
        }
        Some(item)
    }

    fn string(&self, path: &[&str]) -> Result<Option<String>> {
        self.item(path)
            .map(|item| {
                item.as_str()
                    .map(str::to_string)
                    .with_context(|| format!("{} must be a string", path.join(".")))
            })
            .transpose()
    }

    fn optional_string(&self, path: &[&str]) -> Result<Option<String>> {
        Ok(self.string(path)?.and_then(trimmed_non_empty))
    }

    fn path(&self, path: &[&str]) -> Result<Option<PathBuf>> {
        Ok(self.string(path)?.map(PathBuf::from))
    }

    fn optional_path(&self, path: &[&str]) -> Result<Option<PathBuf>> {
        Ok(self.optional_string(path)?.map(PathBuf::from))
    }

    fn usize(&self, path: &[&str]) -> Result<Option<usize>> {
        self.item(path)
            .map(|item| {
                let value = item
                    .as_integer()
                    .with_context(|| format!("{} must be an unsigned integer", path.join(".")))?;
                usize::try_from(value)
                    .with_context(|| format!("{} must be an unsigned integer", path.join(".")))
            })
            .transpose()
    }

    fn i64(&self, path: &[&str]) -> Result<Option<i64>> {
        self.item(path)
            .map(|item| {
                item.as_integer()
                    .with_context(|| format!("{} must be an integer", path.join(".")))
            })
            .transpose()
    }

    fn bool(&self, path: &[&str]) -> Result<Option<bool>> {
        self.item(path)
            .map(|item| {
                item.as_bool()
                    .with_context(|| format!("{} must be a boolean", path.join(".")))
            })
            .transpose()
    }
}

fn duration_ms_or_secs(
    file: &FileConfig,
    file_ms_path: &[&str],
    file_secs_path: &[&str],
    env_ms_name: &str,
    env_secs_name: &str,
    default_secs: usize,
) -> Result<Duration> {
    if let Some(millis) = env_usize(env_ms_name)? {
        return Ok(Duration::from_millis(millis as u64));
    }
    if let Some(seconds) = env_usize(env_secs_name)? {
        return Ok(Duration::from_secs(seconds as u64));
    }
    if let Some(millis) = file.usize(file_ms_path)? {
        return Ok(Duration::from_millis(millis as u64));
    }
    if let Some(seconds) = file.usize(file_secs_path)? {
        return Ok(Duration::from_secs(seconds as u64));
    }
    Ok(Duration::from_secs(default_secs as u64))
}

fn env_string(name: &str) -> Result<Option<String>> {
    match env::var(name) {
        Ok(value) => Ok(Some(value)),
        Err(env::VarError::NotPresent) => Ok(None),
        Err(err) => Err(err).with_context(|| format!("read {name}")),
    }
}

fn env_optional_string(name: &str) -> Result<Option<Option<String>>> {
    match env::var(name) {
        Ok(value) => Ok(Some(trimmed_non_empty(value))),
        Err(env::VarError::NotPresent) => Ok(None),
        Err(err) => Err(err).with_context(|| format!("read {name}")),
    }
}

fn env_path(name: &str) -> Result<Option<PathBuf>> {
    Ok(env_string(name)?.map(PathBuf::from))
}

fn env_optional_path(name: &str) -> Result<Option<Option<PathBuf>>> {
    Ok(env_optional_string(name)?.map(|value| value.map(PathBuf::from)))
}

fn env_usize(name: &str) -> Result<Option<usize>> {
    match env::var(name) {
        Ok(value) => value
            .parse()
            .with_context(|| format!("{name} must be an unsigned integer"))
            .map(Some),
        Err(env::VarError::NotPresent) => Ok(None),
        Err(err) => Err(err).with_context(|| format!("read {name}")),
    }
}

fn env_i64(name: &str) -> Result<Option<i64>> {
    match env::var(name) {
        Ok(value) => value
            .parse()
            .with_context(|| format!("{name} must be an integer"))
            .map(Some),
        Err(env::VarError::NotPresent) => Ok(None),
        Err(err) => Err(err).with_context(|| format!("read {name}")),
    }
}

fn env_bool(name: &str) -> Result<Option<bool>> {
    match env::var(name) {
        Ok(value) => match value.trim().to_ascii_lowercase().as_str() {
            "1" | "true" | "yes" => Ok(Some(true)),
            "0" | "false" | "no" => Ok(Some(false)),
            _ => anyhow::bail!("{name} must be a boolean: true/false, yes/no, or 1/0"),
        },
        Err(env::VarError::NotPresent) => Ok(None),
        Err(err) => Err(err).with_context(|| format!("read {name}")),
    }
}

fn env_optional_usize(name: &str) -> Result<Option<Option<usize>>> {
    match env::var(name) {
        Ok(value) if !value.trim().is_empty() => value
            .parse::<usize>()
            .with_context(|| format!("invalid {name}"))
            .map(|value| Some(Some(value))),
        Ok(_) => Ok(Some(None)),
        Err(env::VarError::NotPresent) => Ok(None),
        Err(err) => Err(err).with_context(|| format!("invalid {name}")),
    }
}

fn trimmed_non_empty(value: String) -> Option<String> {
    let trimmed = value.trim();
    (!trimmed.is_empty()).then(|| trimmed.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::LockExt;
    use std::sync::{Mutex, OnceLock};

    const CONFIG_ENV_VARS: &[&str] = &[
        CONFIG_PATH_ENV,
        "CANARDSTACK_BIND",
        "CANARDSTACK_API_KEY",
        "CANARDSTACK_ADMIN_API_KEY",
        "CANARDSTACK_DATA_DIR",
        "CANARDSTACK_DUCKDB_EXTENSION_DIR",
        "CANARDSTACK_POSTGRES_DSN",
        "CANARDSTACK_DUCKLAKE_ATTACH_URI",
        "CANARDSTACK_MAX_BODY_BYTES",
        "CANARDSTACK_INGEST_MEMORY_BYTES",
        "CANARDSTACK_PROCESS_MEMORY_LIMIT_BYTES",
        "CANARDSTACK_FLUSH_TARGET_BYTES",
        "CANARDSTACK_FLUSH_MAX_AGE_MS",
        "CANARDSTACK_FLUSH_MAX_AGE_SECS",
        "CANARDSTACK_DUCKDB_MEMORY_LIMIT",
        "CANARDSTACK_ACCEPT_LATE_SECS",
        "CANARDSTACK_ACCEPT_FUTURE_SECS",
        "CANARDSTACK_SEGMENT_TARGET_BYTES",
        "CANARDSTACK_SEGMENT_MAX_AGE_MS",
        "CANARDSTACK_SEGMENT_MAX_AGE_SECS",
        "CANARDSTACK_QUERY_CONCURRENCY",
        "CANARDSTACK_QUERY_TIMEOUT_SECS",
        "CANARDSTACK_QUERY_MEMORY_LIMIT",
        "CANARDSTACK_FLUSH_LANE_CAPACITY",
        "CANARDSTACK_CHEAP_QUERY_LANE_CAPACITY",
        "CANARDSTACK_HEAVY_QUERY_DEGRADED_CAPACITY",
        "CANARDSTACK_FRESHNESS_SLA_MS",
        "CANARDSTACK_FRESHNESS_SLA_SECS",
        "CANARDSTACK_RETENTION_DAYS",
        "CANARDSTACK_SCHEDULER_ENABLED",
        "CANARDSTACK_MAINTENANCE_INTERVAL_MS",
        "CANARDSTACK_MAINTENANCE_INTERVAL_SECS",
        "CANARDSTACK_MAX_CONNECTIONS",
        "CANARDSTACK_SOCKET_READ_TIMEOUT_SECS",
        "CANARDSTACK_SOCKET_WRITE_TIMEOUT_SECS",
        "CANARDSTACK_BENCH_HTTP_KEEPALIVE",
        "CANARDSTACK_RAW_SPOOL_CAPACITY_BYTES",
        "CANARDSTACK_RAW_SPOOL_GROUP_COMMIT_MS",
        "CANARDSTACK_INGEST_WORKERS",
        "CANARDSTACK_INGEST_BUFFER_CAPACITY",
    ];

    fn env_lock() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
    }

    struct EnvSnapshot(Vec<(&'static str, Option<String>)>);

    impl EnvSnapshot {
        fn capture_and_clear() -> Self {
            let snapshot = Self(
                CONFIG_ENV_VARS
                    .iter()
                    .map(|name| (*name, env::var(name).ok()))
                    .collect(),
            );
            for name in CONFIG_ENV_VARS {
                unsafe {
                    env::remove_var(name);
                }
            }
            snapshot
        }
    }

    impl Drop for EnvSnapshot {
        fn drop(&mut self) {
            for (name, value) in &self.0 {
                unsafe {
                    match value {
                        Some(value) => env::set_var(name, value),
                        None => env::remove_var(name),
                    }
                }
            }
        }
    }

    #[test]
    fn config_file_values_load_before_env_overrides() {
        let _guard = env_lock().lock_or_poisoned();
        let _snapshot = EnvSnapshot::capture_and_clear();
        let dir = tempfile::tempdir().unwrap();
        let config_path = dir.path().join("config.toml");
        fs::write(
            &config_path,
            r#"
[server]
bind = "0.0.0.0:9999"
max_connections = 42

[auth]
api_key = "file-api-key"
admin_api_key = "file-admin-api-key"

[paths]
data_dir = "file-data"
duckdb_extension_dir = "/opt/duckdb/extensions"

[duckdb]
memory_limit = "2GiB"

[ducklake]
attach_uri = "md:file"

[ingest]
max_body_bytes = 12345
memory_bytes = 3000000
process_memory_limit_bytes = 4000000
flush_target_bytes = 654321
flush_max_age_secs = 11
workers = 3
buffer_capacity = 33

[validation]
accept_late_secs = 100
accept_future_secs = 20

[storage]
segment_target_bytes = 777
segment_max_age_secs = 12

[query]
concurrency = 6
timeout_secs = 7
memory_limit = "384MiB"

[lanes]
flush_capacity = 1
cheap_query_capacity = 2
heavy_query_degraded_capacity = 1
freshness_sla_secs = 9

[retention]
days = 5

[scheduler]
enabled = false
maintenance_interval_secs = 40
flush_interval_ms = 250

[raw_spool]
capacity_bytes = 16384
group_commit_ms = 3
append_sync_ms = 250
append_sync_bytes = 8192

[bench]
http_keepalive = true
"#,
        )
        .unwrap();

        unsafe {
            env::set_var(CONFIG_PATH_ENV, &config_path);
            env::set_var("CANARDSTACK_BIND", "127.0.0.1:4319");
            env::set_var("CANARDSTACK_FLUSH_MAX_AGE_MS", "125");
            env::set_var("CANARDSTACK_DUCKLAKE_ATTACH_URI", "");
        }

        let config = Config::from_env().unwrap();
        assert_eq!(config.bind, "127.0.0.1:4319");
        assert_eq!(config.api_key, "file-api-key");
        assert_eq!(config.admin_api_key, "file-admin-api-key");
        assert_eq!(
            config.duckdb_path,
            PathBuf::from("file-data/canardstack.duckdb")
        );
        assert_eq!(config.local_storage_dir, PathBuf::from("file-data/storage"));
        assert_eq!(
            config.duckdb_extension_dir,
            Some(PathBuf::from("/opt/duckdb/extensions"))
        );
        assert_eq!(config.ducklake_attach_uri, None);
        assert_eq!(config.max_body_bytes, 12345);
        assert_eq!(config.process_ingest_bytes, 3_000_000);
        assert_eq!(config.per_signal_queue_bytes, 750_000);
        assert_eq!(config.runtime_memory_limit_bytes, Some(4_000_000));
        assert_eq!(config.max_bytes_per_flush, 654_321);
        assert_eq!(config.max_age, Duration::from_millis(125));
        assert_eq!(config.high_pressure_max_age, Duration::from_millis(500));
        assert_eq!(config.duckdb_write_memory_limit, "2GiB");
        assert_eq!(config.immutable_segment_target_bytes, 777);
        assert_eq!(config.immutable_segment_max_age, Duration::from_secs(12));
        assert_eq!(config.query_interactive.concurrency, 6);
        assert_eq!(config.query_interactive.timeout_secs, 7);
        assert_eq!(config.query_interactive.memory_limit, "384MiB");
        assert_eq!(config.lane_flush_capacity, 1);
        assert_eq!(config.lane_cheap_query_capacity, 2);
        assert_eq!(config.lane_heavy_query_degraded_capacity, 1);
        assert_eq!(config.lane_freshness_sla, Duration::from_secs(9));
        assert_eq!(config.logs_retention_days, 5);
        assert_eq!(config.metrics_retention_days, 5);
        assert!(!config.scheduler_enabled);
        assert_eq!(config.scheduler_flush_interval, Duration::from_millis(250));
        assert_eq!(config.scheduler_metadata_interval, Duration::from_secs(40));
        assert_eq!(config.scheduler_metrics_interval, Duration::from_secs(80));
        assert_eq!(
            config.scheduler_retention_interval,
            Duration::from_secs(4800)
        );
        assert_eq!(config.raw_spool_max_total_bytes, 16_384);
        assert_eq!(config.raw_spool_max_segment_bytes, 16_384);
        assert_eq!(config.raw_spool_max_record_bytes, 12_345);
        assert_eq!(
            config.raw_spool_group_commit_delay,
            Duration::from_millis(3)
        );
        assert_eq!(
            config.raw_spool_append_sync_interval,
            Duration::from_millis(250)
        );
        assert_eq!(config.raw_spool_append_sync_bytes, 8192);
        assert_eq!(config.raw_spool_checkpoint_fsync_records, 1024);
        assert_eq!(
            config.raw_spool_checkpoint_fsync_delay,
            Duration::from_millis(1000)
        );
        assert_eq!(config.ingest_workers, 3);
        assert_eq!(config.ingest_buffer_capacity, 33);
        assert!(config.bench_http_keepalive);
    }

    #[test]
    fn http_keepalive_defaults_on_for_http11_clients() {
        let _guard = env_lock().lock_or_poisoned();
        let _snapshot = EnvSnapshot::capture_and_clear();

        let config = Config::from_env().unwrap();

        assert!(config.bench_http_keepalive);
    }

    #[test]
    fn config_file_type_errors_name_the_toml_path() {
        let _guard = env_lock().lock_or_poisoned();
        let _snapshot = EnvSnapshot::capture_and_clear();
        let dir = tempfile::tempdir().unwrap();
        let config_path = dir.path().join("config.toml");
        fs::write(
            &config_path,
            r#"
[ingest]
max_body_bytes = "large"
"#,
        )
        .unwrap();

        unsafe {
            env::set_var(CONFIG_PATH_ENV, &config_path);
        }

        let err = Config::from_env().unwrap_err().to_string();
        assert!(
            err.contains("ingest.max_body_bytes must be an unsigned integer"),
            "{err}"
        );
    }

    #[test]
    fn example_toml_loads_and_validates() {
        let _guard = env_lock().lock_or_poisoned();
        let _snapshot = EnvSnapshot::capture_and_clear();
        let config_path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("config/example.toml");

        unsafe {
            env::set_var(CONFIG_PATH_ENV, config_path);
        }

        Config::from_env().unwrap().validate().unwrap();
    }
}
