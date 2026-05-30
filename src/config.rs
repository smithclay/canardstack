use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, Result};
use toml_edit::{DocumentMut, Item};

#[derive(Clone, Debug)]
pub struct QueryLimits {
    pub concurrency: usize,
    pub timeout_secs: u64,
    pub memory_limit: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TlsMode {
    File,
    EphemeralSelfSigned,
}

impl TlsMode {
    pub fn parse(value: &str) -> Result<Self> {
        match value.trim() {
            "file" => Ok(Self::File),
            "ephemeral_self_signed" => Ok(Self::EphemeralSelfSigned),
            _ => anyhow::bail!("TLS mode must be file or ephemeral_self_signed"),
        }
    }
}

#[derive(Clone, Debug)]
pub struct TlsServerConfig {
    pub enabled: bool,
    pub mode: TlsMode,
    pub cert_file: Option<PathBuf>,
    pub key_file: Option<PathBuf>,
    pub backend_bind: String,
}

#[derive(Clone, Debug)]
pub struct Config {
    pub operator: OperatorConfig,
}

#[derive(Clone, Debug)]
pub struct OperatorConfig {
    pub bind: String,
    pub tls: TlsServerConfig,
    pub api_key: String,
    pub admin_api_key: String,
    pub duckdb_path: PathBuf,
    pub local_storage_dir: PathBuf,
    pub duckdb_extension_dir: Option<PathBuf>,
    pub postgres_dsn: Option<String>,
    pub ducklake_attach_uri: Option<String>,
    pub ducklake_catalog_path: Option<PathBuf>,
    pub ducklake_data_path: Option<String>,
    pub ducklake_quack_token: Option<String>,
    pub ducklake_quack_insecure_tls: bool,
    pub max_body_bytes: usize,
    pub duckdb_write_memory_limit: String,
    pub late_accept_secs: i64,
    pub future_accept_secs: i64,
    pub query_interactive: QueryLimits,
    pub query_admission_wait: Duration,
    pub cheap_query_admission_capacity: usize,
    pub max_concurrent_connections: usize,
    pub socket_read_timeout: Duration,
    pub socket_write_timeout: Duration,
}

const DEFAULT_CONFIG_PATH: &str = "config.toml";
const CONFIG_PATH_ENV: &str = "CANARDSTACK_CONFIG";

impl Config {
    pub fn from_env() -> Result<Self> {
        let file = FileConfig::load()?;
        let data_dir = env_path("CANARDSTACK_DATA_DIR")?
            .or(file.path(&["paths", "data_dir"])?)
            .unwrap_or_else(|| PathBuf::from(".canardstack"));
        Ok(Self {
            operator: OperatorConfig::from_env(&file, &data_dir)?,
        })
    }

    pub fn test(duckdb_path: PathBuf) -> Self {
        let local_storage_dir = duckdb_path
            .parent()
            .unwrap_or_else(|| Path::new("."))
            .join("storage");
        Self {
            operator: OperatorConfig::test(duckdb_path, local_storage_dir),
        }
    }

    pub fn validate(&self) -> anyhow::Result<()> {
        self.operator.validate()
    }
}

impl OperatorConfig {
    fn from_env(file: &FileConfig, data_dir: &Path) -> Result<Self> {
        reject_removed_bool(
            "CANARDSTACK_GRPC_ENABLED",
            file,
            &["grpc", "enabled"],
            "CANARDSTACK_GRPC_ENABLED is no longer supported; canardstack is query-only",
        )?;
        reject_removed_bool(
            "CANARDSTACK_LOCAL_CATALOG_ENABLED",
            file,
            &["ducklake", "local_catalog_enabled"],
            "CANARDSTACK_LOCAL_CATALOG_ENABLED is no longer supported; use an external DuckDB catalog/writer",
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
        let tls_cert_file = env_path("CANARDSTACK_TLS_CERT_FILE")?.or(file.path(&[
            "server",
            "tls",
            "cert_file",
        ])?);
        let tls_key_file =
            env_path("CANARDSTACK_TLS_KEY_FILE")?.or(file.path(&["server", "tls", "key_file"])?);
        let tls_mode = env_string("CANARDSTACK_TLS_MODE")?
            .or(file.string(&["server", "tls", "mode"])?)
            .map(|value| TlsMode::parse(&value))
            .transpose()?
            .unwrap_or(TlsMode::File);

        Ok(Self {
            bind: env_string("CANARDSTACK_BIND")?
                .or(file.string(&["server", "bind"])?)
                .unwrap_or_else(|| "127.0.0.1:4318".to_string()),
            tls: TlsServerConfig {
                enabled: env_bool("CANARDSTACK_TLS_ENABLED")?
                    .or(file.bool(&["server", "tls", "enabled"])?)
                    .unwrap_or(false),
                mode: tls_mode,
                cert_file: tls_cert_file,
                key_file: tls_key_file,
                backend_bind: env_string("CANARDSTACK_TLS_BACKEND_BIND")?
                    .or(file.string(&["server", "tls", "backend_bind"])?)
                    .unwrap_or_else(|| "127.0.0.1:4319".to_string()),
            },
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
            ducklake_catalog_path: match env_optional_path("CANARDSTACK_DUCKLAKE_CATALOG_PATH")? {
                Some(value) => value,
                None => file.optional_path(&["ducklake", "catalog_path"])?,
            },
            ducklake_data_path: match env_optional_string("CANARDSTACK_DUCKLAKE_DATA_PATH")? {
                Some(value) => value,
                None => file.optional_string(&["ducklake", "data_path"])?,
            },
            ducklake_quack_token: match env_optional_string("CANARDSTACK_DUCKLAKE_QUACK_TOKEN")? {
                Some(value) => value,
                None => file.optional_string(&["ducklake", "quack_token"])?,
            },
            ducklake_quack_insecure_tls: env_bool("CANARDSTACK_DUCKLAKE_QUACK_INSECURE_TLS")?
                .or(file.bool(&["ducklake", "quack_insecure_tls"])?)
                .unwrap_or(false),
            max_body_bytes: env_usize("CANARDSTACK_MAX_BODY_BYTES")?
                .or(file.usize(&["server", "max_body_bytes"])?)
                .unwrap_or(8 * 1024 * 1024),
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
            query_admission_wait: duration_ms_or_secs(
                file,
                &["query", "admission_wait_ms"],
                &["query", "admission_wait_secs"],
                "CANARDSTACK_QUERY_ADMISSION_WAIT_MS",
                "CANARDSTACK_QUERY_ADMISSION_WAIT_SECS",
                1,
            )?,
            cheap_query_admission_capacity: env_usize(
                "CANARDSTACK_CHEAP_QUERY_ADMISSION_CAPACITY",
            )?
            .or(file.usize(&["admission", "cheap_query_capacity"])?)
            .unwrap_or(1),
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
        })
    }

    fn test(duckdb_path: PathBuf, local_storage_dir: PathBuf) -> Self {
        Self {
            bind: "127.0.0.1:0".to_string(),
            tls: TlsServerConfig {
                enabled: false,
                mode: TlsMode::File,
                cert_file: None,
                key_file: None,
                backend_bind: "127.0.0.1:0".to_string(),
            },
            api_key: "test-key".to_string(),
            admin_api_key: "test-admin-key".to_string(),
            duckdb_path,
            local_storage_dir,
            duckdb_extension_dir: None,
            postgres_dsn: None,
            ducklake_attach_uri: None,
            ducklake_catalog_path: None,
            ducklake_data_path: None,
            ducklake_quack_token: None,
            ducklake_quack_insecure_tls: false,
            max_body_bytes: 8 * 1024 * 1024,
            duckdb_write_memory_limit: "512MiB".to_string(),
            late_accept_secs: 24 * 60 * 60,
            future_accept_secs: 10 * 60,
            query_interactive: QueryLimits {
                concurrency: 4,
                timeout_secs: 15,
                memory_limit: "512MiB".to_string(),
            },
            query_admission_wait: Duration::from_secs(1),
            cheap_query_admission_capacity: 1,
            max_concurrent_connections: 64,
            socket_read_timeout: Duration::from_secs(5),
            socket_write_timeout: Duration::from_secs(5),
        }
    }

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
        if self.tls.enabled {
            if self.tls.backend_bind.trim().is_empty() {
                anyhow::bail!("CANARDSTACK_TLS_BACKEND_BIND must not be empty when TLS is enabled");
            }
            if self.tls.backend_bind == self.bind {
                anyhow::bail!(
                    "CANARDSTACK_TLS_BACKEND_BIND must differ from CANARDSTACK_BIND when TLS is enabled"
                );
            }
            if self.tls.mode == TlsMode::File {
                if self.tls.cert_file.is_none() {
                    anyhow::bail!(
                        "CANARDSTACK_TLS_CERT_FILE must be set when CANARDSTACK_TLS_ENABLED=true and CANARDSTACK_TLS_MODE=file"
                    );
                }
                if self.tls.key_file.is_none() {
                    anyhow::bail!(
                        "CANARDSTACK_TLS_KEY_FILE must be set when CANARDSTACK_TLS_ENABLED=true and CANARDSTACK_TLS_MODE=file"
                    );
                }
            }
        }
        if self.max_body_bytes == 0 {
            anyhow::bail!("CANARDSTACK_MAX_BODY_BYTES must be > 0");
        }
        if self.duckdb_write_memory_limit.trim().is_empty() {
            anyhow::bail!("CANARDSTACK_DUCKDB_MEMORY_LIMIT must not be empty");
        }
        if self
            .ducklake_attach_uri
            .as_deref()
            .is_some_and(|uri| uri.trim().starts_with("ducklake:quack:"))
            && self
                .ducklake_quack_token
                .as_deref()
                .is_none_or(|token| token.trim().is_empty())
        {
            anyhow::bail!(
                "CANARDSTACK_DUCKLAKE_QUACK_TOKEN must be set when CANARDSTACK_DUCKLAKE_ATTACH_URI uses ducklake:quack:"
            );
        }
        if self.ducklake_quack_insecure_tls {
            let uri_uses_quack = self
                .ducklake_attach_uri
                .as_deref()
                .is_some_and(|uri| uri.trim().starts_with("ducklake:quack:"));
            if !uri_uses_quack {
                anyhow::bail!(
                    "CANARDSTACK_DUCKLAKE_QUACK_INSECURE_TLS=true has no effect unless CANARDSTACK_DUCKLAKE_ATTACH_URI uses ducklake:quack:; unset one of them"
                );
            }
        }
        if self.max_concurrent_connections == 0 {
            anyhow::bail!("CANARDSTACK_MAX_CONNECTIONS must be > 0");
        }
        if self.query_interactive.concurrency == 0 {
            anyhow::bail!("CANARDSTACK_QUERY_CONCURRENCY must be > 0");
        }
        if self.cheap_query_admission_capacity == 0 {
            anyhow::bail!("CANARDSTACK_CHEAP_QUERY_ADMISSION_CAPACITY must be > 0");
        }
        if self.query_interactive.concurrency <= self.cheap_query_admission_capacity {
            anyhow::bail!(
                "CANARDSTACK_QUERY_CONCURRENCY must leave at least one heavy query slot after cheap-query admission reservations"
            );
        }
        if self.query_interactive.timeout_secs == 0 {
            anyhow::bail!("CANARDSTACK_QUERY_TIMEOUT_SECS must be > 0");
        }
        if self.socket_read_timeout.is_zero() || self.socket_write_timeout.is_zero() {
            anyhow::bail!("socket timeouts must be > 0");
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

fn reject_removed_bool(
    env_name: &str,
    file: &FileConfig,
    file_path: &[&str],
    message: &str,
) -> Result<()> {
    if env_bool(env_name)?
        .or(file.bool(file_path)?)
        .unwrap_or(false)
    {
        anyhow::bail!("{message}");
    }
    Ok(())
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
        "CANARDSTACK_TLS_ENABLED",
        "CANARDSTACK_TLS_MODE",
        "CANARDSTACK_TLS_CERT_FILE",
        "CANARDSTACK_TLS_KEY_FILE",
        "CANARDSTACK_TLS_BACKEND_BIND",
        "CANARDSTACK_GRPC_ENABLED",
        "CANARDSTACK_API_KEY",
        "CANARDSTACK_ADMIN_API_KEY",
        "CANARDSTACK_DATA_DIR",
        "CANARDSTACK_DUCKDB_EXTENSION_DIR",
        "CANARDSTACK_POSTGRES_DSN",
        "CANARDSTACK_DUCKLAKE_ATTACH_URI",
        "CANARDSTACK_DUCKLAKE_CATALOG_PATH",
        "CANARDSTACK_DUCKLAKE_DATA_PATH",
        "CANARDSTACK_DUCKLAKE_QUACK_TOKEN",
        "CANARDSTACK_DUCKLAKE_QUACK_INSECURE_TLS",
        "CANARDSTACK_LOCAL_CATALOG_ENABLED",
        "CANARDSTACK_MAX_BODY_BYTES",
        "CANARDSTACK_DUCKDB_MEMORY_LIMIT",
        "CANARDSTACK_ACCEPT_LATE_SECS",
        "CANARDSTACK_ACCEPT_FUTURE_SECS",
        "CANARDSTACK_QUERY_CONCURRENCY",
        "CANARDSTACK_QUERY_TIMEOUT_SECS",
        "CANARDSTACK_QUERY_MEMORY_LIMIT",
        "CANARDSTACK_QUERY_ADMISSION_WAIT_MS",
        "CANARDSTACK_QUERY_ADMISSION_WAIT_SECS",
        "CANARDSTACK_CHEAP_QUERY_ADMISSION_CAPACITY",
        "CANARDSTACK_MAX_CONNECTIONS",
        "CANARDSTACK_SOCKET_READ_TIMEOUT_SECS",
        "CANARDSTACK_SOCKET_WRITE_TIMEOUT_SECS",
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
max_body_bytes = 12345
max_connections = 42

[server.tls]
enabled = true
mode = "ephemeral_self_signed"
backend_bind = "127.0.0.1:9443"

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
catalog_path = "/catalog/canardstack.ducklake"
data_path = "s3://file-bucket/canardstack/"
quack_token = "file-quack-token"

[validation]
accept_late_secs = 100
accept_future_secs = 20

[query]
concurrency = 6
timeout_secs = 7
memory_limit = "384MiB"
admission_wait_ms = 250

[admission]
cheap_query_capacity = 2
"#,
        )
        .unwrap();

        unsafe {
            env::set_var(CONFIG_PATH_ENV, &config_path);
            env::set_var("CANARDSTACK_BIND", "127.0.0.1:4319");
            env::set_var("CANARDSTACK_TLS_BACKEND_BIND", "127.0.0.1:9444");
            env::set_var("CANARDSTACK_DUCKLAKE_ATTACH_URI", "");
            env::set_var("CANARDSTACK_DUCKLAKE_CATALOG_PATH", "/env/catalog.ducklake");
            env::set_var(
                "CANARDSTACK_DUCKLAKE_DATA_PATH",
                "gcs://env-bucket/canardstack/",
            );
            env::set_var("CANARDSTACK_DUCKLAKE_QUACK_TOKEN", "env-quack-token");
        }

        let config = Config::from_env().unwrap();
        assert_eq!(config.operator.bind, "127.0.0.1:4319");
        assert!(config.operator.tls.enabled);
        assert_eq!(config.operator.tls.mode, TlsMode::EphemeralSelfSigned);
        assert_eq!(config.operator.tls.backend_bind, "127.0.0.1:9444");
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
        assert_eq!(
            config.operator.ducklake_catalog_path,
            Some(PathBuf::from("/env/catalog.ducklake"))
        );
        assert_eq!(
            config.operator.ducklake_data_path,
            Some("gcs://env-bucket/canardstack/".to_string())
        );
        assert_eq!(
            config.operator.ducklake_quack_token,
            Some("env-quack-token".to_string())
        );
        assert_eq!(config.operator.max_body_bytes, 12345);
        assert_eq!(config.operator.duckdb_write_memory_limit, "2GiB");
        assert_eq!(config.operator.query_interactive.concurrency, 6);
        assert_eq!(config.operator.query_interactive.timeout_secs, 7);
        assert_eq!(config.operator.query_interactive.memory_limit, "384MiB");
        assert_eq!(
            config.operator.query_admission_wait,
            Duration::from_millis(250)
        );
        assert_eq!(config.operator.cheap_query_admission_capacity, 2);
        assert_eq!(config.operator.max_concurrent_connections, 42);
    }

    #[test]
    fn tls_file_mode_requires_cert_and_key() {
        let _guard = env_lock().lock_or_poisoned();
        let _snapshot = EnvSnapshot::capture_and_clear();

        unsafe {
            env::set_var("CANARDSTACK_TLS_ENABLED", "true");
            env::set_var("CANARDSTACK_TLS_MODE", "file");
            env::set_var("CANARDSTACK_TLS_CERT_FILE", "/tmp/cert.pem");
        }

        let err = Config::from_env().unwrap().validate().unwrap_err();
        assert!(err.to_string().contains("CANARDSTACK_TLS_KEY_FILE"));
    }

    #[test]
    fn removed_grpc_config_is_rejected() {
        let _guard = env_lock().lock_or_poisoned();
        let _snapshot = EnvSnapshot::capture_and_clear();

        unsafe {
            env::set_var("CANARDSTACK_GRPC_ENABLED", "true");
        }

        let err = Config::from_env().unwrap_err();
        assert!(err.to_string().contains("CANARDSTACK_GRPC_ENABLED"));
    }

    #[test]
    fn removed_local_catalog_config_is_rejected() {
        let _guard = env_lock().lock_or_poisoned();
        let _snapshot = EnvSnapshot::capture_and_clear();

        unsafe {
            env::set_var("CANARDSTACK_LOCAL_CATALOG_ENABLED", "true");
        }

        let err = Config::from_env().unwrap_err();
        assert!(err
            .to_string()
            .contains("CANARDSTACK_LOCAL_CATALOG_ENABLED"));
    }

    #[test]
    fn quack_insecure_tls_requires_quack_attach_uri() {
        let mut config = Config::test(PathBuf::from("test.duckdb"));
        config.operator.ducklake_quack_insecure_tls = true;
        config.operator.ducklake_attach_uri = Some("md:test-ducklake".to_string());

        let err = config.validate().unwrap_err();
        assert!(err
            .to_string()
            .contains("CANARDSTACK_DUCKLAKE_QUACK_INSECURE_TLS=true has no effect"));
    }

    #[test]
    fn quack_attach_uri_requires_token() {
        let mut config = Config::test(PathBuf::from("test.duckdb"));
        config.operator.ducklake_attach_uri =
            Some("ducklake:quack:catalog.internal:443".to_string());

        let err = config.validate().unwrap_err();
        assert!(err.to_string().contains("CANARDSTACK_DUCKLAKE_QUACK_TOKEN"));
    }

    #[test]
    fn query_concurrency_must_leave_heavy_slot() {
        let mut config = Config::test(PathBuf::from("test.duckdb"));
        config.operator.query_interactive.concurrency = 1;
        config.operator.cheap_query_admission_capacity = 1;

        let err = config.validate().unwrap_err();
        assert!(err
            .to_string()
            .contains("must leave at least one heavy query slot"));
    }
}
