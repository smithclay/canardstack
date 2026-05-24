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
pub struct QueryLimits {
    pub concurrency: usize,
    pub timeout_secs: u64,
    pub memory_limit: String,
}

/// Process configuration, split by responsibility:
///
/// - [`OperatorConfig`] — the public, supported deployment surface operators
///   set to run a deployment (endpoints, auth, catalog, retention, query
///   limits, freshness SLA, admission capacities, memory limits,
///   body/connection caps, socket timeouts, scheduler on/off).
/// - [`Mechanics`] — advanced mechanics that are either env/file-tunable or
///   derived from an operator setting (Arrow write-buffer target/age, raw-spool
///   max sizes + append-sync + group-commit cadence, ingest worker count,
///   scheduler intervals).
/// - [`TestOverrides`] — fixed production defaults exposed only so tests can
///   exercise edge cases deterministically. Operators cannot configure these.
///
/// Purely internal raw-spool batching/durability mechanics with no operator
/// meaning are NOT fields here; they live as consts in `ingest::spool`.
#[derive(Clone, Debug)]
pub struct Config {
    pub operator: OperatorConfig,
    pub mechanics: Mechanics,
    pub test_overrides: TestOverrides,
}

/// The public, supported deployment surface: settings operators set to run a
/// deployment. Validation errors for these settings name their env vars.
#[derive(Clone, Debug)]
pub struct OperatorConfig {
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
    pub runtime_memory_limit_bytes: Option<usize>,
    pub duckdb_write_memory_limit: String,
    pub late_accept_secs: i64,
    pub future_accept_secs: i64,
    pub query_interactive: QueryLimits,
    pub seal_admission_capacity: usize,
    pub cheap_query_admission_capacity: usize,
    pub heavy_query_degraded_capacity: usize,
    pub freshness_budget_sla: Duration,
    pub logs_retention_days: i64,
    pub spans_retention_days: i64,
    pub metrics_retention_days: i64,
    pub max_concurrent_connections: usize,
    pub socket_read_timeout: Duration,
    pub socket_write_timeout: Duration,
    pub scheduler_enabled: bool,
}

/// Advanced mechanics operators may tune, plus derived mechanics from operator
/// settings. Fixed production defaults used only for tests live in
/// [`TestOverrides`], not here.
#[derive(Clone, Debug)]
pub struct Mechanics {
    pub arrow_write_buffer_target_bytes: usize,
    pub arrow_write_buffer_max_age: Duration,
    pub scheduler_seal_interval: Duration,
    pub scheduler_metadata_interval: Duration,
    pub scheduler_metrics_interval: Duration,
    pub scheduler_retention_interval: Duration,
    pub raw_spool_dir: PathBuf,
    pub raw_spool_max_segment_bytes: usize,
    pub raw_spool_max_record_bytes: usize,
    pub raw_spool_max_total_bytes: usize,
    pub raw_spool_group_commit_delay: Duration,
    pub raw_spool_append_sync_interval: Duration,
    pub raw_spool_append_sync_bytes: usize,
    pub ingest_workers: usize,
    /// When true, the scheduler's metrics-snapshot job writes a snapshot of the
    /// current operator metrics into the `metric_gauge` / `metric_sum` storage
    /// tables (queryable via the Prometheus-compatible path). Off by default to
    /// avoid the extra write load and the `canardstack_operator_metrics` rows;
    /// the operator gauges still refresh and `/metrics` still serves them.
    pub operator_metrics_to_storage: bool,
}

/// Fixed-default hooks that production code reads but only tests mutate.
#[derive(Clone, Debug)]
pub struct TestOverrides {
    pub seal_rate_seed_bytes: usize,
    pub seal_rate_seed_window: Duration,
    pub bench_http_keepalive: bool,
    pub ingest_worker_channel_capacity: usize,
}

const DEFAULT_CONFIG_PATH: &str = "config.toml";
const CONFIG_PATH_ENV: &str = "CANARDSTACK_CONFIG";

/// Internal seal-rate EWMA warm-up mechanics: the seed rate
/// (`seal_rate_seed_bytes` / `seal_rate_seed_window`) only primes the estimator
/// until measured throughput converges, so it is not an operator policy knob.
/// Kept as `Config` fields purely for deterministic test injection (see the
/// admission-control characterization tests).
const DEFAULT_SEAL_RATE_SEED_BYTES: usize = 4 * 1024 * 1024;
const DEFAULT_SEAL_RATE_SEED_WINDOW: Duration = Duration::from_secs(10);

impl Config {
    pub fn from_env() -> Result<Self> {
        let file = FileConfig::load()?;
        // Shared inputs that feed both sub-structs: the data_dir drives derived
        // paths in both, max_body_bytes feeds mechanics.raw_spool_max_record_bytes,
        // and the raw-spool capacity / maintenance interval feed mechanics.
        let data_dir = env_path("CANARDSTACK_DATA_DIR")?
            .or(file.path(&["paths", "data_dir"])?)
            .unwrap_or_else(|| PathBuf::from(".canardstack"));
        let max_body_bytes = env_usize("CANARDSTACK_MAX_BODY_BYTES")?
            .or(file.usize(&["ingest", "max_body_bytes"])?)
            .unwrap_or(8 * 1024 * 1024);
        let raw_spool_capacity_bytes = env_usize("CANARDSTACK_RAW_SPOOL_CAPACITY_BYTES")?
            .or(file.usize(&["raw_spool", "capacity_bytes"])?)
            .unwrap_or(1024 * 1024 * 1024);
        let maintenance_interval = duration_ms_or_secs(
            &file,
            &["scheduler", "maintenance_interval_ms"],
            &["scheduler", "maintenance_interval_secs"],
            "CANARDSTACK_MAINTENANCE_INTERVAL_MS",
            "CANARDSTACK_MAINTENANCE_INTERVAL_SECS",
            30,
        )?;

        let operator = OperatorConfig::from_env(&file, &data_dir, max_body_bytes)?;
        let mechanics = Mechanics::from_env(
            &file,
            &data_dir,
            max_body_bytes,
            raw_spool_capacity_bytes,
            maintenance_interval,
        )?;

        Ok(Self {
            operator,
            mechanics,
            test_overrides: TestOverrides::production(),
        })
    }

    pub fn test(duckdb_path: PathBuf) -> Self {
        let local_storage_dir = duckdb_path
            .parent()
            .unwrap_or_else(|| std::path::Path::new("."))
            .join("storage");
        Self {
            operator: OperatorConfig::test(duckdb_path, local_storage_dir.clone()),
            mechanics: Mechanics::test(local_storage_dir),
            test_overrides: TestOverrides::test(),
        }
    }

    /// Fail boot rather than start with a misconfiguration that's invisible at runtime.
    pub fn validate(&self) -> anyhow::Result<()> {
        self.operator.validate()?;
        self.mechanics.validate()?;
        self.test_overrides.validate()?;
        // Cross-field check spanning both sub-structs: the raw-spool max record
        // size derives from operator.max_body_bytes and must fit the mechanics
        // raw-spool total capacity.
        if self.mechanics.raw_spool_max_record_bytes > self.mechanics.raw_spool_max_total_bytes {
            anyhow::bail!(
                "CANARDSTACK_MAX_BODY_BYTES must be <= CANARDSTACK_RAW_SPOOL_CAPACITY_BYTES"
            );
        }
        Ok(())
    }
}

impl OperatorConfig {
    fn from_env(file: &FileConfig, data_dir: &Path, max_body_bytes: usize) -> Result<Self> {
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
            runtime_memory_limit_bytes: match env_optional_usize(
                "CANARDSTACK_PROCESS_MEMORY_LIMIT_BYTES",
            )? {
                Some(value) => value,
                None => file.usize(&["ingest", "process_memory_limit_bytes"])?,
            },
            duckdb_write_memory_limit: env_string("CANARDSTACK_DUCKDB_MEMORY_LIMIT")?
                .or(file.string(&["duckdb", "memory_limit"])?)
                .unwrap_or_else(|| "1GiB".to_string()),
            late_accept_secs: env_i64("CANARDSTACK_ACCEPT_LATE_SECS")?
                .or(file.i64(&["validation", "accept_late_secs"])?)
                .unwrap_or(24 * 60 * 60),
            future_accept_secs: env_i64("CANARDSTACK_ACCEPT_FUTURE_SECS")?
                .or(file.i64(&["validation", "accept_future_secs"])?)
                .unwrap_or(10 * 60),
            query_interactive: QueryLimits {
                concurrency: query_concurrency,
                timeout_secs: query_timeout_secs,
                memory_limit: query_memory_limit,
            },
            seal_admission_capacity: env_usize("CANARDSTACK_SEAL_ADMISSION_CAPACITY")?
                .or(file.usize(&["admission", "seal_capacity"])?)
                .unwrap_or(1),
            cheap_query_admission_capacity: env_usize(
                "CANARDSTACK_CHEAP_QUERY_ADMISSION_CAPACITY",
            )?
            .or(file.usize(&["admission", "cheap_query_capacity"])?)
            .unwrap_or(1),
            heavy_query_degraded_capacity: env_usize("CANARDSTACK_HEAVY_QUERY_DEGRADED_CAPACITY")?
                .or(file.usize(&["admission", "heavy_query_degraded_capacity"])?)
                .unwrap_or(1),
            freshness_budget_sla: duration_ms_or_secs(
                file,
                &["admission", "freshness_budget_sla_ms"],
                &["admission", "freshness_budget_sla_secs"],
                "CANARDSTACK_FRESHNESS_BUDGET_SLA_MS",
                "CANARDSTACK_FRESHNESS_BUDGET_SLA_SECS",
                15,
            )?,
            logs_retention_days: retention_days,
            spans_retention_days: retention_days,
            metrics_retention_days: retention_days,
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
            scheduler_enabled: env_bool("CANARDSTACK_SCHEDULER_ENABLED")?
                .or(file.bool(&["scheduler", "enabled"])?)
                .unwrap_or(true),
        })
    }

    fn test(duckdb_path: PathBuf, local_storage_dir: PathBuf) -> Self {
        Self {
            serve_role: ServeRole::All,
            bind: "127.0.0.1:0".to_string(),
            api_key: "test-key".to_string(),
            admin_api_key: "test-admin-key".to_string(),
            duckdb_path,
            local_storage_dir,
            duckdb_extension_dir: None,
            postgres_dsn: None,
            ducklake_attach_uri: None,
            max_body_bytes: 8 * 1024 * 1024,
            runtime_memory_limit_bytes: None,
            duckdb_write_memory_limit: "512MiB".to_string(),
            late_accept_secs: 24 * 60 * 60,
            future_accept_secs: 10 * 60,
            query_interactive: QueryLimits {
                concurrency: 4,
                timeout_secs: 15,
                memory_limit: "512MiB".to_string(),
            },
            seal_admission_capacity: 1,
            cheap_query_admission_capacity: 1,
            heavy_query_degraded_capacity: 1,
            freshness_budget_sla: Duration::from_secs(15),
            logs_retention_days: 14,
            spans_retention_days: 14,
            metrics_retention_days: 30,
            max_concurrent_connections: 64,
            socket_read_timeout: Duration::from_secs(5),
            socket_write_timeout: Duration::from_secs(5),
            scheduler_enabled: false,
        }
    }

    /// Validate operator-facing settings; messages name their env vars.
    fn validate(&self) -> anyhow::Result<()> {
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
        if self.max_body_bytes == 0 {
            anyhow::bail!("CANARDSTACK_MAX_BODY_BYTES must be > 0");
        }
        if matches!(self.runtime_memory_limit_bytes, Some(0)) {
            anyhow::bail!("CANARDSTACK_PROCESS_MEMORY_LIMIT_BYTES must be > 0 when set");
        }
        if self.duckdb_write_memory_limit.trim().is_empty() {
            anyhow::bail!("CANARDSTACK_DUCKDB_MEMORY_LIMIT must not be empty");
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
        if self.seal_admission_capacity == 0
            || self.cheap_query_admission_capacity == 0
            || self.heavy_query_degraded_capacity == 0
        {
            anyhow::bail!("admission capacities must be > 0");
        }
        if self.freshness_budget_sla.is_zero() {
            anyhow::bail!("CANARDSTACK_FRESHNESS_BUDGET_SLA_MS/SECS must be > 0");
        }
        if self.query_interactive.concurrency
            <= self
                .seal_admission_capacity
                .saturating_add(self.cheap_query_admission_capacity)
        {
            anyhow::bail!(
                "CANARDSTACK_QUERY_CONCURRENCY must leave at least one heavy query slot after seal and cheap-query admission reservations"
            );
        }
        if self.query_interactive.timeout_secs == 0 {
            anyhow::bail!("query timeouts must be > 0");
        }
        if self.socket_read_timeout.is_zero() || self.socket_write_timeout.is_zero() {
            anyhow::bail!("socket timeouts must be > 0");
        }
        Ok(())
    }
}

impl Mechanics {
    fn from_env(
        file: &FileConfig,
        data_dir: &Path,
        max_body_bytes: usize,
        raw_spool_capacity_bytes: usize,
        maintenance_interval: Duration,
    ) -> Result<Self> {
        Ok(Self {
            arrow_write_buffer_target_bytes: env_usize(
                "CANARDSTACK_ARROW_WRITE_BUFFER_TARGET_BYTES",
            )?
            .or(file.usize(&["storage", "arrow_write_buffer_target_bytes"])?)
            .unwrap_or(64 * 1024 * 1024),
            arrow_write_buffer_max_age: duration_ms_or_secs(
                file,
                &["storage", "arrow_write_buffer_max_age_ms"],
                &["storage", "arrow_write_buffer_max_age_secs"],
                "CANARDSTACK_ARROW_WRITE_BUFFER_MAX_AGE_MS",
                "CANARDSTACK_ARROW_WRITE_BUFFER_MAX_AGE_SECS",
                10,
            )?,
            // Seal cadence for the single seal driver. Decoupled from the coarse
            // maintenance interval: it must stay well under the freshness-budget SLA so
            // Arrow write-buffer age never approaches the freshness-budget reject threshold.
            scheduler_seal_interval: duration_ms_or_secs(
                file,
                &["scheduler", "seal_interval_ms"],
                &["scheduler", "seal_interval_secs"],
                "CANARDSTACK_SEAL_INTERVAL_MS",
                "CANARDSTACK_SEAL_INTERVAL_SECS",
                1,
            )?,
            scheduler_metadata_interval: maintenance_interval,
            scheduler_metrics_interval: maintenance_interval.saturating_mul(2),
            scheduler_retention_interval: maintenance_interval.saturating_mul(120),
            raw_spool_dir: data_dir.join("raw-spool"),
            raw_spool_max_segment_bytes: (64 * 1024 * 1024).min(raw_spool_capacity_bytes),
            raw_spool_max_record_bytes: max_body_bytes,
            raw_spool_max_total_bytes: raw_spool_capacity_bytes,
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
            ingest_workers: env_usize("CANARDSTACK_INGEST_WORKERS")?
                .or(file.usize(&["ingest", "workers"])?)
                .unwrap_or(4),
            operator_metrics_to_storage: env_bool("CANARDSTACK_OPERATOR_METRICS_TO_STORAGE")?
                .or(file.bool(&["metrics", "operator_metrics_to_storage"])?)
                .unwrap_or(false),
        })
    }

    fn test(local_storage_dir: PathBuf) -> Self {
        Self {
            arrow_write_buffer_target_bytes: 64 * 1024 * 1024,
            arrow_write_buffer_max_age: Duration::from_secs(10),
            scheduler_seal_interval: Duration::from_millis(200),
            scheduler_metadata_interval: Duration::from_millis(200),
            scheduler_metrics_interval: Duration::from_millis(200),
            scheduler_retention_interval: Duration::from_secs(3_600),
            raw_spool_dir: local_storage_dir.join("raw-spool"),
            raw_spool_max_segment_bytes: 64 * 1024 * 1024,
            raw_spool_max_record_bytes: 8 * 1024 * 1024,
            raw_spool_max_total_bytes: 1024 * 1024 * 1024,
            raw_spool_group_commit_delay: Duration::from_millis(1),
            raw_spool_append_sync_interval: Duration::from_millis(500),
            raw_spool_append_sync_bytes: 16 * 1024 * 1024,
            ingest_workers: 4,
            operator_metrics_to_storage: false,
        }
    }

    /// Validate advanced/internal mechanics knobs.
    fn validate(&self) -> anyhow::Result<()> {
        if self.arrow_write_buffer_target_bytes == 0 {
            anyhow::bail!("CANARDSTACK_ARROW_WRITE_BUFFER_TARGET_BYTES must be > 0");
        }
        if self.arrow_write_buffer_max_age.is_zero() {
            anyhow::bail!("CANARDSTACK_ARROW_WRITE_BUFFER_MAX_AGE_MS/SECS must be > 0");
        }
        if self.raw_spool_max_segment_bytes == 0
            || self.raw_spool_max_record_bytes == 0
            || self.raw_spool_max_total_bytes == 0
            || self.raw_spool_append_sync_bytes == 0
        {
            anyhow::bail!("raw spool limits must be > 0");
        }
        if self.raw_spool_group_commit_delay.is_zero() {
            anyhow::bail!("CANARDSTACK_RAW_SPOOL_GROUP_COMMIT_MS must be > 0");
        }
        if self.raw_spool_append_sync_interval.is_zero() {
            anyhow::bail!("CANARDSTACK_RAW_SPOOL_APPEND_SYNC_MS must be > 0");
        }
        if self.ingest_workers == 0 {
            anyhow::bail!("CANARDSTACK_INGEST_WORKERS must be > 0");
        }
        if self.scheduler_seal_interval.is_zero()
            || self.scheduler_metadata_interval.is_zero()
            || self.scheduler_metrics_interval.is_zero()
            || self.scheduler_retention_interval.is_zero()
        {
            anyhow::bail!("scheduler intervals must be > 0");
        }
        Ok(())
    }
}

impl TestOverrides {
    fn production() -> Self {
        Self {
            seal_rate_seed_bytes: DEFAULT_SEAL_RATE_SEED_BYTES,
            seal_rate_seed_window: DEFAULT_SEAL_RATE_SEED_WINDOW,
            bench_http_keepalive: true,
            ingest_worker_channel_capacity: crate::ingest::INGEST_WORKER_CHANNEL_CAPACITY,
        }
    }

    fn test() -> Self {
        Self {
            seal_rate_seed_bytes: 256 * 1024,
            seal_rate_seed_window: Duration::from_millis(50),
            bench_http_keepalive: false,
            ingest_worker_channel_capacity: 1024,
        }
    }

    fn validate(&self) -> anyhow::Result<()> {
        if self.seal_rate_seed_bytes == 0 {
            anyhow::bail!("seal-rate seed bytes must be > 0");
        }
        if self.seal_rate_seed_window.is_zero() {
            anyhow::bail!("seal-rate seed window must be > 0");
        }
        if self.ingest_worker_channel_capacity == 0 {
            anyhow::bail!("ingest worker channel capacity must be > 0");
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
        "CANARDSTACK_PROCESS_MEMORY_LIMIT_BYTES",
        "CANARDSTACK_DUCKDB_MEMORY_LIMIT",
        "CANARDSTACK_ACCEPT_LATE_SECS",
        "CANARDSTACK_ACCEPT_FUTURE_SECS",
        "CANARDSTACK_ARROW_WRITE_BUFFER_TARGET_BYTES",
        "CANARDSTACK_ARROW_WRITE_BUFFER_MAX_AGE_MS",
        "CANARDSTACK_ARROW_WRITE_BUFFER_MAX_AGE_SECS",
        "CANARDSTACK_QUERY_CONCURRENCY",
        "CANARDSTACK_QUERY_TIMEOUT_SECS",
        "CANARDSTACK_QUERY_MEMORY_LIMIT",
        "CANARDSTACK_SEAL_ADMISSION_CAPACITY",
        "CANARDSTACK_CHEAP_QUERY_ADMISSION_CAPACITY",
        "CANARDSTACK_HEAVY_QUERY_DEGRADED_CAPACITY",
        "CANARDSTACK_FRESHNESS_BUDGET_SLA_MS",
        "CANARDSTACK_FRESHNESS_BUDGET_SLA_SECS",
        "CANARDSTACK_RETENTION_DAYS",
        "CANARDSTACK_SCHEDULER_ENABLED",
        "CANARDSTACK_MAINTENANCE_INTERVAL_MS",
        "CANARDSTACK_MAINTENANCE_INTERVAL_SECS",
        "CANARDSTACK_MAX_CONNECTIONS",
        "CANARDSTACK_SOCKET_READ_TIMEOUT_SECS",
        "CANARDSTACK_SOCKET_WRITE_TIMEOUT_SECS",
        "CANARDSTACK_RAW_SPOOL_CAPACITY_BYTES",
        "CANARDSTACK_RAW_SPOOL_GROUP_COMMIT_MS",
        "CANARDSTACK_INGEST_WORKERS",
        "CANARDSTACK_OPERATOR_METRICS_TO_STORAGE",
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
process_memory_limit_bytes = 4000000
workers = 3

[validation]
accept_late_secs = 100
accept_future_secs = 20

[storage]
arrow_write_buffer_target_bytes = 777
arrow_write_buffer_max_age_secs = 12

[query]
concurrency = 6
timeout_secs = 7
memory_limit = "384MiB"

[admission]
seal_capacity = 1
cheap_query_capacity = 2
heavy_query_degraded_capacity = 1
freshness_budget_sla_secs = 9

[retention]
days = 5

[scheduler]
enabled = false
maintenance_interval_secs = 40
seal_interval_ms = 250

[raw_spool]
capacity_bytes = 16384
group_commit_ms = 3
append_sync_ms = 250
append_sync_bytes = 8192
"#,
        )
        .unwrap();

        unsafe {
            env::set_var(CONFIG_PATH_ENV, &config_path);
            env::set_var("CANARDSTACK_BIND", "127.0.0.1:4319");
            env::set_var("CANARDSTACK_DUCKLAKE_ATTACH_URI", "");
        }

        let config = Config::from_env().unwrap();
        assert_eq!(config.operator.bind, "127.0.0.1:4319");
        assert_eq!(config.operator.api_key, "file-api-key");
        assert_eq!(config.operator.admin_api_key, "file-admin-api-key");
        assert_eq!(
            config.operator.duckdb_path,
            PathBuf::from("file-data/canardstack.duckdb")
        );
        assert_eq!(
            config.operator.local_storage_dir,
            PathBuf::from("file-data/storage")
        );
        assert_eq!(
            config.operator.duckdb_extension_dir,
            Some(PathBuf::from("/opt/duckdb/extensions"))
        );
        assert_eq!(config.operator.ducklake_attach_uri, None);
        assert_eq!(config.operator.max_body_bytes, 12345);
        assert_eq!(config.operator.runtime_memory_limit_bytes, Some(4_000_000));
        assert_eq!(config.operator.duckdb_write_memory_limit, "2GiB");
        assert_eq!(config.mechanics.arrow_write_buffer_target_bytes, 777);
        assert_eq!(
            config.mechanics.arrow_write_buffer_max_age,
            Duration::from_secs(12)
        );
        assert_eq!(config.operator.query_interactive.concurrency, 6);
        assert_eq!(config.operator.query_interactive.timeout_secs, 7);
        assert_eq!(config.operator.query_interactive.memory_limit, "384MiB");
        assert_eq!(config.operator.seal_admission_capacity, 1);
        assert_eq!(config.operator.cheap_query_admission_capacity, 2);
        assert_eq!(config.operator.heavy_query_degraded_capacity, 1);
        assert_eq!(config.operator.freshness_budget_sla, Duration::from_secs(9));
        assert_eq!(config.operator.logs_retention_days, 5);
        assert_eq!(config.operator.metrics_retention_days, 5);
        assert!(!config.operator.scheduler_enabled);
        assert_eq!(
            config.mechanics.scheduler_seal_interval,
            Duration::from_millis(250)
        );
        assert_eq!(
            config.mechanics.scheduler_metadata_interval,
            Duration::from_secs(40)
        );
        assert_eq!(
            config.mechanics.scheduler_metrics_interval,
            Duration::from_secs(80)
        );
        assert_eq!(
            config.mechanics.scheduler_retention_interval,
            Duration::from_secs(4800)
        );
        assert_eq!(config.mechanics.raw_spool_max_total_bytes, 16_384);
        assert_eq!(config.mechanics.raw_spool_max_segment_bytes, 16_384);
        assert_eq!(config.mechanics.raw_spool_max_record_bytes, 12_345);
        assert_eq!(
            config.mechanics.raw_spool_group_commit_delay,
            Duration::from_millis(3)
        );
        assert_eq!(
            config.mechanics.raw_spool_append_sync_interval,
            Duration::from_millis(250)
        );
        assert_eq!(config.mechanics.raw_spool_append_sync_bytes, 8192);
        assert_eq!(config.mechanics.ingest_workers, 3);
        // Internal mechanics are no longer file/env driven; they stay at their
        // fixed defaults regardless of any (now-ignored) file keys above.
        assert_eq!(
            config.test_overrides.ingest_worker_channel_capacity,
            crate::ingest::INGEST_WORKER_CHANNEL_CAPACITY
        );
        assert_eq!(
            config.test_overrides.seal_rate_seed_bytes,
            DEFAULT_SEAL_RATE_SEED_BYTES
        );
        assert_eq!(
            config.test_overrides.seal_rate_seed_window,
            DEFAULT_SEAL_RATE_SEED_WINDOW
        );
        assert!(config.test_overrides.bench_http_keepalive);
    }

    #[test]
    fn http_keepalive_defaults_on_for_http11_clients() {
        let _guard = env_lock().lock_or_poisoned();
        let _snapshot = EnvSnapshot::capture_and_clear();

        let config = Config::from_env().unwrap();

        assert!(config.test_overrides.bench_http_keepalive);
    }

    #[test]
    fn arrow_write_buffer_defaults_to_current_flush_policy() {
        let _guard = env_lock().lock_or_poisoned();
        let _snapshot = EnvSnapshot::capture_and_clear();

        let config = Config::from_env().unwrap();

        assert_eq!(
            config.mechanics.arrow_write_buffer_target_bytes,
            64 * 1024 * 1024
        );
        assert_eq!(
            config.mechanics.arrow_write_buffer_max_age,
            Duration::from_secs(10)
        );
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

    #[test]
    fn operator_metrics_to_storage_defaults_off() {
        let _guard = env_lock().lock_or_poisoned();
        let _snapshot = EnvSnapshot::capture_and_clear();

        let config = Config::from_env().unwrap();

        assert!(!config.mechanics.operator_metrics_to_storage);
    }
}
