use std::env;
use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result};

#[derive(Clone, Debug)]
pub struct QueryLane {
    pub concurrency: usize,
    pub timeout_secs: u64,
    pub memory_limit: String,
}

#[derive(Clone, Debug)]
pub struct Config {
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
    pub force_dependency_unhealthy: bool,
    pub immutable_segment_target_bytes: usize,
    pub immutable_segment_max_age: Duration,
    pub ducklake_data_inlining_row_limit: usize,
    pub ducklake_compaction_min_files: usize,
    pub query_interactive: QueryLane,
    pub query_background: QueryLane,
    pub logs_retention_days: i64,
    pub spans_retention_days: i64,
    pub metrics_retention_days: i64,
    pub scheduler_enabled: bool,
    pub scheduler_watchdog_interval: Duration,
    pub scheduler_flush_interval: Duration,
    pub scheduler_metadata_interval: Duration,
    pub scheduler_metrics_interval: Duration,
    pub scheduler_compaction_interval: Duration,
    pub scheduler_retention_interval: Duration,
    pub max_concurrent_connections: usize,
    pub socket_read_timeout: Duration,
    pub socket_write_timeout: Duration,
}

impl Config {
    pub fn from_env() -> Result<Self> {
        let data_dir = env::var("CANARDSTACK_DATA_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|_| PathBuf::from(".canardstack"));

        Ok(Self {
            bind: env::var("CANARDSTACK_BIND").unwrap_or_else(|_| "127.0.0.1:4318".to_string()),
            api_key: env::var("CANARDSTACK_API_KEY")
                .unwrap_or_else(|_| "dev-canardstack-key".to_string()),
            admin_api_key: env::var("CANARDSTACK_ADMIN_API_KEY")
                .unwrap_or_else(|_| "dev-canardstack-admin-key".to_string()),
            duckdb_path: env::var("CANARDSTACK_DUCKDB_PATH")
                .map(PathBuf::from)
                .unwrap_or_else(|_| data_dir.join("canardstack.duckdb")),
            local_storage_dir: env::var("CANARDSTACK_STORAGE_DIR")
                .map(PathBuf::from)
                .unwrap_or_else(|_| data_dir.join("storage")),
            duckdb_extension_dir: env::var("CANARDSTACK_DUCKDB_EXTENSION_DIR")
                .ok()
                .map(PathBuf::from),
            postgres_dsn: env_non_empty("CANARDSTACK_POSTGRES_DSN"),
            ducklake_attach_uri: env_non_empty("CANARDSTACK_DUCKLAKE_ATTACH_URI"),
            max_body_bytes: env_usize("CANARDSTACK_MAX_BODY_BYTES", 8 * 1024 * 1024)?,
            per_signal_queue_bytes: env_usize(
                "CANARDSTACK_PER_SIGNAL_QUEUE_BYTES",
                512 * 1024 * 1024,
            )?,
            process_ingest_bytes: env_usize(
                "CANARDSTACK_PROCESS_INGEST_BYTES",
                2 * 1024 * 1024 * 1024,
            )?,
            runtime_memory_limit_bytes: env_optional_usize(
                "CANARDSTACK_RUNTIME_MEMORY_LIMIT_BYTES",
            )?,
            max_rows_per_flush: env_usize("CANARDSTACK_MAX_ROWS_PER_FLUSH", 5_000)?,
            max_bytes_per_flush: env_usize("CANARDSTACK_MAX_BYTES_PER_FLUSH", 4 * 1024 * 1024)?,
            max_age: Duration::from_secs(env_usize("CANARDSTACK_MAX_FLUSH_AGE_SECS", 10)? as u64),
            high_pressure_max_age: Duration::from_secs(env_usize(
                "CANARDSTACK_HIGH_PRESSURE_FLUSH_AGE_SECS",
                2,
            )? as u64),
            duckdb_write_memory_limit: env::var("CANARDSTACK_DUCKDB_WRITE_MEMORY_LIMIT")
                .unwrap_or_else(|_| "1GiB".to_string()),
            late_accept_secs: env_i64("CANARDSTACK_ACCEPT_LATE_SECS", 24 * 60 * 60)?,
            future_accept_secs: env_i64("CANARDSTACK_ACCEPT_FUTURE_SECS", 10 * 60)?,
            force_dependency_unhealthy: false,
            immutable_segment_target_bytes: env_usize(
                "CANARDSTACK_IMMUTABLE_SEGMENT_TARGET_BYTES",
                64 * 1024 * 1024,
            )?,
            immutable_segment_max_age: Duration::from_secs(env_usize(
                "CANARDSTACK_IMMUTABLE_SEGMENT_MAX_AGE_SECS",
                10,
            )? as u64),
            ducklake_data_inlining_row_limit: env_usize(
                "CANARDSTACK_DUCKLAKE_DATA_INLINING_ROW_LIMIT",
                0,
            )?,
            ducklake_compaction_min_files: env_usize(
                "CANARDSTACK_DUCKLAKE_COMPACTION_MIN_FILES",
                8,
            )?,
            query_interactive: QueryLane {
                concurrency: env_usize("CANARDSTACK_QUERY_INTERACTIVE_CONCURRENCY", 4)?,
                timeout_secs: env_usize("CANARDSTACK_QUERY_INTERACTIVE_TIMEOUT_SECS", 15)? as u64,
                memory_limit: env::var("CANARDSTACK_QUERY_INTERACTIVE_MEMORY_LIMIT")
                    .unwrap_or_else(|_| "1GiB".to_string()),
            },
            query_background: QueryLane {
                concurrency: env_usize("CANARDSTACK_QUERY_BACKGROUND_CONCURRENCY", 2)?,
                timeout_secs: env_usize("CANARDSTACK_QUERY_BACKGROUND_TIMEOUT_SECS", 60)? as u64,
                memory_limit: env::var("CANARDSTACK_QUERY_BACKGROUND_MEMORY_LIMIT")
                    .unwrap_or_else(|_| "1GiB".to_string()),
            },
            logs_retention_days: env_i64("CANARDSTACK_LOGS_RETENTION_DAYS", 14)?,
            spans_retention_days: env_i64("CANARDSTACK_SPANS_RETENTION_DAYS", 14)?,
            metrics_retention_days: env_i64("CANARDSTACK_METRICS_RETENTION_DAYS", 30)?,
            scheduler_enabled: env_bool("CANARDSTACK_SCHEDULER_ENABLED", true)?,
            scheduler_watchdog_interval: Duration::from_millis(env_usize(
                "CANARDSTACK_SCHEDULER_WATCHDOG_MS",
                1_000,
            )? as u64),
            scheduler_flush_interval: Duration::from_secs(env_usize(
                "CANARDSTACK_SCHEDULER_FLUSH_SECS",
                30,
            )? as u64),
            scheduler_metadata_interval: Duration::from_secs(env_usize(
                "CANARDSTACK_SCHEDULER_METADATA_SECS",
                30,
            )? as u64),
            scheduler_metrics_interval: Duration::from_secs(env_usize(
                "CANARDSTACK_SCHEDULER_METRICS_SECS",
                60,
            )? as u64),
            scheduler_compaction_interval: Duration::from_secs(env_usize(
                "CANARDSTACK_SCHEDULER_COMPACTION_SECS",
                300,
            )? as u64),
            scheduler_retention_interval: Duration::from_secs(env_usize(
                "CANARDSTACK_SCHEDULER_RETENTION_SECS",
                3_600,
            )? as u64),
            max_concurrent_connections: env_usize("CANARDSTACK_MAX_CONNECTIONS", 1024)?,
            socket_read_timeout: Duration::from_secs(env_usize(
                "CANARDSTACK_SOCKET_READ_TIMEOUT_SECS",
                30,
            )? as u64),
            socket_write_timeout: Duration::from_secs(env_usize(
                "CANARDSTACK_SOCKET_WRITE_TIMEOUT_SECS",
                30,
            )? as u64),
        })
    }

    pub fn test(duckdb_path: PathBuf) -> Self {
        let local_storage_dir = duckdb_path
            .parent()
            .unwrap_or_else(|| std::path::Path::new("."))
            .join("storage");
        Self {
            bind: "127.0.0.1:0".to_string(),
            api_key: "test-key".to_string(),
            admin_api_key: "test-admin-key".to_string(),
            duckdb_path,
            local_storage_dir,
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
            force_dependency_unhealthy: false,
            immutable_segment_target_bytes: 64 * 1024 * 1024,
            immutable_segment_max_age: Duration::from_secs(10),
            ducklake_data_inlining_row_limit: 0,
            ducklake_compaction_min_files: 8,
            query_interactive: QueryLane {
                concurrency: 4,
                timeout_secs: 15,
                memory_limit: "512MiB".to_string(),
            },
            query_background: QueryLane {
                concurrency: 2,
                timeout_secs: 60,
                memory_limit: "1GiB".to_string(),
            },
            logs_retention_days: 14,
            spans_retention_days: 14,
            metrics_retention_days: 30,
            scheduler_enabled: false,
            scheduler_watchdog_interval: Duration::from_millis(50),
            scheduler_flush_interval: Duration::from_millis(200),
            scheduler_metadata_interval: Duration::from_millis(200),
            scheduler_metrics_interval: Duration::from_millis(200),
            scheduler_compaction_interval: Duration::from_secs(300),
            scheduler_retention_interval: Duration::from_secs(3_600),
            max_concurrent_connections: 64,
            socket_read_timeout: Duration::from_secs(5),
            socket_write_timeout: Duration::from_secs(5),
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
                "CANARDSTACK_PER_SIGNAL_QUEUE_BYTES ({}) must be <= CANARDSTACK_PROCESS_INGEST_BYTES ({})",
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
            anyhow::bail!("CANARDSTACK_RUNTIME_MEMORY_LIMIT_BYTES must be > 0 when set");
        }
        if self.max_rows_per_flush == 0 || self.max_bytes_per_flush == 0 {
            anyhow::bail!("flush thresholds must be > 0");
        }
        if self.duckdb_write_memory_limit.trim().is_empty() {
            anyhow::bail!("CANARDSTACK_DUCKDB_WRITE_MEMORY_LIMIT must not be empty");
        }
        if self.immutable_segment_target_bytes == 0 {
            anyhow::bail!("CANARDSTACK_IMMUTABLE_SEGMENT_TARGET_BYTES must be > 0");
        }
        if self.immutable_segment_max_age.is_zero() {
            anyhow::bail!("CANARDSTACK_IMMUTABLE_SEGMENT_MAX_AGE_SECS must be > 0");
        }
        if self.ducklake_compaction_min_files == 0 {
            anyhow::bail!("CANARDSTACK_DUCKLAKE_COMPACTION_MIN_FILES must be > 0");
        }
        if self.high_pressure_max_age > self.max_age {
            anyhow::bail!(
                "CANARDSTACK_HIGH_PRESSURE_FLUSH_AGE_SECS must be <= CANARDSTACK_MAX_FLUSH_AGE_SECS"
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
        if self.query_interactive.concurrency == 0 || self.query_background.concurrency == 0 {
            anyhow::bail!("query concurrency limits must be > 0");
        }
        if self.query_interactive.timeout_secs == 0 || self.query_background.timeout_secs == 0 {
            anyhow::bail!("query timeouts must be > 0");
        }
        if self.socket_read_timeout.is_zero() || self.socket_write_timeout.is_zero() {
            anyhow::bail!("socket timeouts must be > 0");
        }
        if self.scheduler_watchdog_interval.is_zero()
            || self.scheduler_flush_interval.is_zero()
            || self.scheduler_metadata_interval.is_zero()
            || self.scheduler_metrics_interval.is_zero()
            || self.scheduler_compaction_interval.is_zero()
            || self.scheduler_retention_interval.is_zero()
        {
            anyhow::bail!("scheduler intervals must be > 0");
        }
        Ok(())
    }
}

fn env_usize(name: &str, default: usize) -> Result<usize> {
    match env::var(name) {
        Ok(value) => value
            .parse()
            .with_context(|| format!("{name} must be an unsigned integer")),
        Err(env::VarError::NotPresent) => Ok(default),
        Err(err) => Err(err).with_context(|| format!("read {name}")),
    }
}

fn env_i64(name: &str, default: i64) -> Result<i64> {
    match env::var(name) {
        Ok(value) => value
            .parse()
            .with_context(|| format!("{name} must be an integer")),
        Err(env::VarError::NotPresent) => Ok(default),
        Err(err) => Err(err).with_context(|| format!("read {name}")),
    }
}

fn env_non_empty(name: &str) -> Option<String> {
    env::var(name).ok().and_then(|value| {
        let trimmed = value.trim();
        (!trimmed.is_empty()).then(|| trimmed.to_string())
    })
}

fn env_bool(name: &str, default: bool) -> Result<bool> {
    match env::var(name) {
        Ok(value) => match value.to_ascii_lowercase().as_str() {
            "1" | "true" | "yes" => Ok(true),
            "0" | "false" | "no" => Ok(false),
            _ => anyhow::bail!("{name} must be a boolean: true/false, yes/no, or 1/0"),
        },
        Err(env::VarError::NotPresent) => Ok(default),
        Err(err) => Err(err).with_context(|| format!("read {name}")),
    }
}

fn env_optional_usize(name: &str) -> Result<Option<usize>> {
    match env::var(name) {
        Ok(value) if !value.trim().is_empty() => value
            .parse::<usize>()
            .with_context(|| format!("invalid {name}"))
            .map(Some),
        Ok(_) => Ok(None),
        Err(env::VarError::NotPresent) => Ok(None),
        Err(err) => Err(err).with_context(|| format!("invalid {name}")),
    }
}
