//! `serve-catalog` role: run the canardstack binary as a DuckDB/Quack catalog
//! server. It opens the local DuckLake catalog DuckDB file and serves it over
//! the Quack remote protocol so query/ingest nodes can `ATTACH` DuckLake against
//! it. This is plain DuckDB plus Quack: it does not run the ingest/query
//! pipeline and performs no DuckLake `ATTACH` of its own.

use crate::config::TlsMode;
use crate::storage;
use crate::Config;
use anyhow::{Context, Result};
use std::env;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::Duration;

const DEFAULT_LISTEN: &str = "0.0.0.0:9494";
const DEFAULT_QUACK_BACKEND: &str = "127.0.0.1:9495";
const DEFAULT_HEALTH_BIND: &str = "0.0.0.0:8080";
const ACCEPT_POLL_INTERVAL: Duration = Duration::from_millis(50);
const HEALTH_SOCKET_TIMEOUT: Duration = Duration::from_secs(2);

pub fn run(args: impl Iterator<Item = String>, shutdown: &'static AtomicBool) -> Result<()> {
    let Some(options) = parse_options(args)? else {
        return Ok(());
    };

    let config = Config::from_env()?;
    let catalog_path = catalog_db_path(&config);
    let token = config
        .operator
        .ducklake_quack_token
        .as_deref()
        .map(str::trim)
        .filter(|token| !token.is_empty())
        .ok_or_else(|| {
            anyhow::anyhow!(
                "CANARDSTACK_DUCKLAKE_QUACK_TOKEN must be set to serve the Quack catalog"
            )
        })?
        .to_string();
    let public_listen = options
        .listen
        .unwrap_or_else(|| env_or("CANARDSTACK_CATALOG_LISTEN", DEFAULT_LISTEN));
    let health_bind = options
        .health_listen
        .unwrap_or_else(|| env_or("CANARDSTACK_CATALOG_HEALTH_BIND", DEFAULT_HEALTH_BIND));
    let tls = CatalogTlsConfig::from_env()?;

    // In TLS mode Quack binds a loopback backend and an in-binary TLS terminator
    // fronts it on the public address; requests then arrive with a non-local Host,
    // so allow_other_hostname must be on. In plaintext mode Quack binds the public
    // address directly and only opts in when that address is non-loopback.
    let (quack_listen, allow_other_hostname) = if tls.enabled {
        (
            options.backend_listen.unwrap_or_else(|| {
                env_or("CANARDSTACK_CATALOG_QUACK_BACKEND", DEFAULT_QUACK_BACKEND)
            }),
            true,
        )
    } else {
        (public_listen.clone(), !listens_on_loopback(&public_listen))
    };

    validate_catalog_listens(&public_listen, &quack_listen, &health_bind, tls.enabled)?;

    run_catalog_server(
        config,
        catalog_path,
        token,
        public_listen,
        health_bind,
        quack_listen,
        allow_other_hostname,
        tls,
        shutdown,
    )
}

#[allow(clippy::too_many_arguments)]
fn run_catalog_server(
    config: Config,
    catalog_path: PathBuf,
    token: String,
    public_listen: String,
    health_bind: String,
    quack_listen: String,
    allow_other_hostname: bool,
    tls: CatalogTlsConfig,
    shutdown: &'static AtomicBool,
) -> Result<()> {
    let conn = storage::open_quack_catalog_connection(
        &catalog_path,
        config.operator.duckdb_extension_dir.as_deref(),
    )?;
    // DuckLake CHECKPOINT over a Quack catalog runs its file-deletion scan as
    // catalog-side SQL (read_blob over the data path) on this server, so the
    // Quack connection needs object-store credentials when DATA_PATH is a cloud
    // store; otherwise that scan reaches S3 unsigned and CHECKPOINT fails.
    if let Some(data_path) = config
        .operator
        .ducklake_data_path
        .as_deref()
        .map(str::trim)
        .filter(|path| !path.is_empty())
    {
        match storage::configure_object_store_for_data_path(&conn, data_path)? {
            Some(scheme) => tracing::info!(
                event = "catalog_object_store_configured",
                scheme,
                "configured object-store credentials for catalog-side DuckLake maintenance"
            ),
            None => tracing::debug!(
                event = "catalog_object_store_skipped",
                "DATA_PATH is local; no object-store credentials needed"
            ),
        }
    }
    conn.execute_batch(&quack_serve_sql(
        &quack_listen,
        &token,
        allow_other_hostname,
    ))
    .context("start the Quack catalog server")?;
    tracing::info!(
        event = "catalog_listening",
        listen = %quack_listen,
        tls = tls.enabled,
        catalog = %catalog_path.display(),
        "serving the DuckLake catalog over Quack"
    );

    // Silence the public listener when TLS code that uses it is not compiled in.
    #[cfg(not(feature = "tls"))]
    let _ = &public_listen;
    if tls.enabled {
        #[cfg(feature = "tls")]
        {
            let public = public_listen.clone();
            let backend = quack_listen.clone();
            let identity = tls.identity()?;
            thread::spawn(move || {
                if let Err(err) = crate::tls::run_tls_terminator(
                    &public,
                    backend,
                    identity,
                    "serve-catalog",
                    shutdown,
                ) {
                    tracing::error!(event = "catalog_tls_terminator_failed", error = %err);
                }
            });
            tracing::info!(
                event = "catalog_tls_enabled",
                public = %public_listen,
                mode = tls.mode.as_str()
            );
        }
        #[cfg(not(feature = "tls"))]
        {
            anyhow::bail!(
                "CANARDSTACK_CATALOG_TLS_ENABLED=true requires a build with --features tls"
            );
        }
    }

    // Block on the health endpoint until shutdown; the Quack server (and the TLS
    // terminator, if any) run on their own threads while `conn` stays alive.
    let result = serve_health_until(&health_bind, shutdown);

    // Best-effort graceful stop so the listen socket and catalog file are
    // released before the process exits.
    if let Err(err) = conn.execute_batch(&quack_stop_sql(&quack_listen)) {
        tracing::info!(event = "quack_stop_skipped", error = %err);
    }
    tracing::info!(event = "catalog_shutdown_complete");
    result
}

/// RAII guard around an in-process Quack catalog endpoint. Dropping it stops
/// the embedded Quack server and closes the catalog `Connection`, mirroring the
/// shutdown path that `run_catalog_server` runs at the end of `serve-catalog`.
pub struct QuackCatalogEndpoint {
    conn: duckdb::Connection,
    listen: String,
}

impl QuackCatalogEndpoint {
    pub fn listen(&self) -> &str {
        &self.listen
    }
}

impl Drop for QuackCatalogEndpoint {
    fn drop(&mut self) {
        if let Err(err) = self.conn.execute_batch(&quack_stop_sql(&self.listen)) {
            tracing::info!(event = "quack_stop_skipped", error = %err);
        }
    }
}

/// Start a loopback Quack catalog endpoint inside the current process.
///
/// The returned guard owns the catalog DuckDB `Connection`; keeping it alive
/// keeps the Quack server bound to `listen`. The app's writer connection is
/// expected to `ATTACH 'ducklake:quack:<listen>'` rather than opening the
/// catalog file directly, so there is exactly one `Database` instance on the
/// catalog file even though everything runs in one process.
pub fn start_local_catalog_endpoint(config: &Config, listen: &str) -> Result<QuackCatalogEndpoint> {
    let catalog_path = catalog_db_path(config);
    let token = config
        .operator
        .ducklake_quack_token
        .as_deref()
        .map(str::trim)
        .filter(|token| !token.is_empty())
        .ok_or_else(|| {
            anyhow::anyhow!(
                "CANARDSTACK_DUCKLAKE_QUACK_TOKEN must be set to serve the local Quack catalog endpoint"
            )
        })?;
    let conn = storage::open_quack_catalog_connection(
        &catalog_path,
        config.operator.duckdb_extension_dir.as_deref(),
    )?;
    // CHECKPOINT on the app's writer fans out catalog-side `read_blob` calls
    // through Quack to this connection. When DATA_PATH is in object storage,
    // those reads need credentials installed on the catalog side too.
    if let Some(data_path) = config
        .operator
        .ducklake_data_path
        .as_deref()
        .map(str::trim)
        .filter(|path| !path.is_empty())
    {
        match storage::configure_object_store_for_data_path(&conn, data_path)? {
            Some(scheme) => tracing::info!(
                event = "local_catalog_object_store_configured",
                scheme,
                "configured object-store credentials for local Quack catalog endpoint"
            ),
            None => tracing::debug!(
                event = "local_catalog_object_store_skipped",
                "DATA_PATH is local; no object-store credentials needed"
            ),
        }
    }
    conn.execute_batch(&quack_serve_sql(listen, token, false))
        .with_context(|| format!("start local Quack catalog endpoint on {listen}"))?;
    tracing::info!(
        event = "local_catalog_listening",
        listen,
        catalog = %catalog_path.display(),
        "serving the local DuckLake catalog over Quack"
    );
    Ok(QuackCatalogEndpoint {
        conn,
        listen: listen.to_string(),
    })
}

#[derive(Default)]
struct CatalogOptions {
    listen: Option<String>,
    health_listen: Option<String>,
    backend_listen: Option<String>,
}

fn parse_options(mut args: impl Iterator<Item = String>) -> Result<Option<CatalogOptions>> {
    let mut options = CatalogOptions::default();
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--help" | "-h" => {
                println!("usage: canardstack serve-catalog [--listen addr] [--health-listen addr] [--backend-listen addr]");
                println!(
                    "serve the local DuckLake catalog DuckDB file over the Quack protocol.\n\
                     env: CANARDSTACK_DUCKLAKE_QUACK_TOKEN (required), CANARDSTACK_DUCKLAKE_CATALOG_PATH,\n\
                     CANARDSTACK_CATALOG_LISTEN (default {DEFAULT_LISTEN}), CANARDSTACK_CATALOG_HEALTH_BIND (default {DEFAULT_HEALTH_BIND}).\n\
                     TLS (requires --features tls): CANARDSTACK_CATALOG_TLS_ENABLED=true terminates TLS on\n\
                     CANARDSTACK_CATALOG_LISTEN and forwards to CANARDSTACK_CATALOG_QUACK_BACKEND (default {DEFAULT_QUACK_BACKEND});\n\
                     set CANARDSTACK_CATALOG_TLS_MODE=file with CANARDSTACK_CATALOG_TLS_CERT_FILE and\n\
                     CANARDSTACK_CATALOG_TLS_KEY_FILE to use a persistent certificate"
                );
                return Ok(None);
            }
            "--listen" => {
                options.listen = Some(
                    args.next()
                        .ok_or_else(|| anyhow::anyhow!("--listen requires an address"))?,
                );
            }
            "--health-listen" => {
                options.health_listen = Some(
                    args.next()
                        .ok_or_else(|| anyhow::anyhow!("--health-listen requires an address"))?,
                );
            }
            "--backend-listen" => {
                options.backend_listen = Some(
                    args.next()
                        .ok_or_else(|| anyhow::anyhow!("--backend-listen requires an address"))?,
                );
            }
            other => {
                if let Some(value) = other.strip_prefix("--listen=") {
                    options.listen = Some(value.to_string());
                } else if let Some(value) = other.strip_prefix("--health-listen=") {
                    options.health_listen = Some(value.to_string());
                } else if let Some(value) = other.strip_prefix("--backend-listen=") {
                    options.backend_listen = Some(value.to_string());
                } else {
                    anyhow::bail!(
                        "unknown serve-catalog option {other}; usage: canardstack serve-catalog [--listen addr] [--health-listen addr] [--backend-listen addr]"
                    );
                }
            }
        }
    }
    Ok(Some(options))
}

fn env_bool(name: &str) -> bool {
    matches!(
        std::env::var(name).ok().as_deref().map(str::trim),
        Some("1") | Some("true") | Some("yes")
    )
}

#[derive(Clone, Debug)]
struct CatalogTlsConfig {
    enabled: bool,
    mode: TlsMode,
    cert_file: Option<PathBuf>,
    key_file: Option<PathBuf>,
}

impl CatalogTlsConfig {
    fn from_env() -> Result<Self> {
        // Matches the main app's CANARDSTACK_TLS_ENABLED naming.
        let enabled = env_bool("CANARDSTACK_CATALOG_TLS_ENABLED");
        let cert_file = env_optional_path("CANARDSTACK_CATALOG_TLS_CERT_FILE");
        let key_file = env_optional_path("CANARDSTACK_CATALOG_TLS_KEY_FILE");
        let mode = env_optional_string("CANARDSTACK_CATALOG_TLS_MODE")
            .map(|value| TlsMode::parse(&value))
            .transpose()?
            .unwrap_or_else(|| {
                if cert_file.is_some() || key_file.is_some() {
                    TlsMode::File
                } else {
                    TlsMode::EphemeralSelfSigned
                }
            });
        let config = Self {
            enabled,
            mode,
            cert_file,
            key_file,
        };
        config.validate()?;
        Ok(config)
    }

    fn validate(&self) -> Result<()> {
        if self.enabled && self.mode == TlsMode::File {
            if self.cert_file.is_none() {
                anyhow::bail!(
                    "CANARDSTACK_CATALOG_TLS_CERT_FILE must be set when CANARDSTACK_CATALOG_TLS_ENABLED=true and CANARDSTACK_CATALOG_TLS_MODE=file"
                );
            }
            if self.key_file.is_none() {
                anyhow::bail!(
                    "CANARDSTACK_CATALOG_TLS_KEY_FILE must be set when CANARDSTACK_CATALOG_TLS_ENABLED=true and CANARDSTACK_CATALOG_TLS_MODE=file"
                );
            }
        }
        Ok(())
    }

    #[cfg(feature = "tls")]
    fn identity(&self) -> Result<crate::tls::TlsIdentity> {
        match self.mode {
            TlsMode::File => Ok(crate::tls::TlsIdentity::File {
                cert_file: self
                    .cert_file
                    .clone()
                    .expect("validated catalog TLS cert_file"),
                key_file: self
                    .key_file
                    .clone()
                    .expect("validated catalog TLS key_file"),
            }),
            TlsMode::EphemeralSelfSigned => Ok(crate::tls::TlsIdentity::EphemeralSelfSigned {
                subject_alt_names: vec!["canardstack-catalog".to_string()],
            }),
        }
    }
}

fn env_optional_string(name: &str) -> Option<String> {
    env::var(name)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn env_optional_path(name: &str) -> Option<PathBuf> {
    env_optional_string(name).map(PathBuf::from)
}

/// The catalog DuckDB file to serve: the explicit catalog path when set,
/// otherwise the same default location the local DuckLake catalog uses.
pub fn catalog_db_path(config: &Config) -> PathBuf {
    config
        .operator
        .ducklake_catalog_path
        .clone()
        .unwrap_or_else(|| {
            config
                .operator
                .duckdb_path
                .parent()
                .unwrap_or_else(|| Path::new("."))
                .join("canardstack.ducklake")
        })
}

fn env_or(name: &str, default: &str) -> String {
    match std::env::var(name) {
        Ok(value) if !value.trim().is_empty() => value.trim().to_string(),
        _ => default.to_string(),
    }
}

fn quack_serve_sql(listen: &str, token: &str, allow_other_hostname: bool) -> String {
    format!(
        "CALL quack_serve('quack:{}', token => '{}', allow_other_hostname => {});",
        sql_escape(listen),
        sql_escape(token),
        if allow_other_hostname {
            "true"
        } else {
            "false"
        },
    )
}

fn quack_stop_sql(listen: &str) -> String {
    format!("CALL quack_stop('quack:{}');", sql_escape(listen))
}

fn sql_escape(value: &str) -> String {
    value.replace('\'', "''")
}

/// Quack rejects binding to a non-local hostname unless `allow_other_hostname`
/// is set, so loopback binds keep it off and everything else opts in.
fn listens_on_loopback(listen: &str) -> bool {
    let host = listen.rsplit_once(':').map_or(listen, |(host, _)| host);
    let host = host.trim_start_matches('[').trim_end_matches(']');
    matches!(host, "127.0.0.1" | "localhost" | "::1")
}

/// Refuse to start when two catalog listeners would collide at bind time.
/// In TLS mode the public listener fronts a loopback Quack backend, so the
/// three addresses must be distinct; in plaintext mode `public` and `quack`
/// are the same socket by design, so only the health bind has to differ.
fn validate_catalog_listens(
    public: &str,
    quack: &str,
    health: &str,
    tls_enabled: bool,
) -> Result<()> {
    if tls_enabled && public == quack {
        anyhow::bail!(
            "CANARDSTACK_CATALOG_QUACK_BACKEND must differ from CANARDSTACK_CATALOG_LISTEN when CANARDSTACK_CATALOG_TLS_ENABLED=true (the TLS terminator cannot forward to itself)"
        );
    }
    if public == health {
        anyhow::bail!(
            "CANARDSTACK_CATALOG_HEALTH_BIND must differ from CANARDSTACK_CATALOG_LISTEN"
        );
    }
    if tls_enabled && quack == health {
        anyhow::bail!(
            "CANARDSTACK_CATALOG_HEALTH_BIND must differ from CANARDSTACK_CATALOG_QUACK_BACKEND when CANARDSTACK_CATALOG_TLS_ENABLED=true"
        );
    }
    Ok(())
}

/// Minimal single-threaded health endpoint: replies `{"status":"ok"}` to any
/// request so `canardstack healthcheck` and ECS/Cloud Map can probe liveness.
/// Health checks are infrequent, so one-at-a-time accept is sufficient.
fn serve_health_until(bind: &str, shutdown: &AtomicBool) -> Result<()> {
    let listener =
        TcpListener::bind(bind).with_context(|| format!("bind catalog health endpoint {bind}"))?;
    listener.set_nonblocking(true)?;
    tracing::info!(event = "catalog_health_listening", bind = %bind);
    while !shutdown.load(Ordering::SeqCst) {
        match listener.accept() {
            Ok((stream, _)) => {
                if let Err(err) = respond_health(stream) {
                    tracing::debug!(event = "catalog_health_response_failed", error = %err);
                }
            }
            Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => {
                thread::sleep(ACCEPT_POLL_INTERVAL);
            }
            Err(err) if err.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(err) => return Err(err.into()),
        }
    }
    tracing::info!(
        event = "shutdown_requested",
        "catalog stopped accepting health checks"
    );
    Ok(())
}

fn respond_health(mut stream: TcpStream) -> std::io::Result<()> {
    stream.set_read_timeout(Some(HEALTH_SOCKET_TIMEOUT))?;
    stream.set_write_timeout(Some(HEALTH_SOCKET_TIMEOUT))?;
    // Best-effort drain of the request line/headers so the client reads the
    // response cleanly; the body is irrelevant for a liveness probe.
    let mut buf = [0u8; 1024];
    let _ = stream.read(&mut buf);
    const BODY: &str = "{\"status\":\"ok\"}";
    write!(
        stream,
        "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
        BODY.len(),
        BODY
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quack_serve_sql_renders_allow_other_hostname() {
        assert_eq!(
            quack_serve_sql("0.0.0.0:9494", "tok", true),
            "CALL quack_serve('quack:0.0.0.0:9494', token => 'tok', allow_other_hostname => true);"
        );
        assert!(quack_serve_sql("127.0.0.1:9494", "tok", false)
            .contains("allow_other_hostname => false"));
    }

    #[test]
    fn quack_serve_sql_escapes_token_quotes() {
        assert!(quack_serve_sql("0.0.0.0:9494", "a'b", true).contains("token => 'a''b'"));
    }

    #[test]
    fn quack_stop_sql_targets_listen_uri() {
        assert_eq!(
            quack_stop_sql("0.0.0.0:9494"),
            "CALL quack_stop('quack:0.0.0.0:9494');"
        );
    }

    #[test]
    fn validate_listens_catches_tls_terminator_loop() {
        let err = validate_catalog_listens("0.0.0.0:9494", "0.0.0.0:9494", "0.0.0.0:8080", true)
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("CANARDSTACK_CATALOG_QUACK_BACKEND must differ"),
            "{err}"
        );
    }

    #[test]
    fn validate_listens_catches_public_health_collision() {
        let err = validate_catalog_listens("0.0.0.0:8080", "0.0.0.0:8080", "0.0.0.0:8080", false)
            .unwrap_err()
            .to_string();
        assert!(
            err.contains(
                "CANARDSTACK_CATALOG_HEALTH_BIND must differ from CANARDSTACK_CATALOG_LISTEN"
            ),
            "{err}"
        );
    }

    #[test]
    fn validate_listens_catches_backend_health_collision() {
        let err =
            validate_catalog_listens("0.0.0.0:9494", "127.0.0.1:8080", "127.0.0.1:8080", true)
                .unwrap_err()
                .to_string();
        assert!(
            err.contains("CANARDSTACK_CATALOG_HEALTH_BIND must differ from CANARDSTACK_CATALOG_QUACK_BACKEND"),
            "{err}"
        );
    }

    #[test]
    fn validate_listens_accepts_distinct_addresses() {
        // TLS mode: three distinct sockets.
        assert!(
            validate_catalog_listens("0.0.0.0:9494", "127.0.0.1:9495", "0.0.0.0:8080", true)
                .is_ok()
        );
        // Plaintext mode: public and quack are the same socket by design, only
        // health has to differ.
        assert!(
            validate_catalog_listens("0.0.0.0:9494", "0.0.0.0:9494", "0.0.0.0:8080", false).is_ok()
        );
    }

    #[test]
    fn loopback_detection_covers_common_forms() {
        assert!(listens_on_loopback("127.0.0.1:9494"));
        assert!(listens_on_loopback("localhost:8080"));
        assert!(listens_on_loopback("[::1]:9494"));
        assert!(!listens_on_loopback("0.0.0.0:9494"));
        assert!(!listens_on_loopback("10.0.0.5:9494"));
    }

    #[test]
    fn parse_options_accepts_listen_flags() {
        let options = parse_options(
            [
                "--listen",
                "0.0.0.0:9494",
                "--health-listen=127.0.0.1:8080",
                "--backend-listen",
                "127.0.0.1:9495",
            ]
            .into_iter()
            .map(str::to_string),
        )
        .unwrap()
        .unwrap();

        assert_eq!(options.listen.as_deref(), Some("0.0.0.0:9494"));
        assert_eq!(options.health_listen.as_deref(), Some("127.0.0.1:8080"));
        assert_eq!(options.backend_listen.as_deref(), Some("127.0.0.1:9495"));
    }

    #[test]
    fn catalog_db_path_defaults_next_to_duckdb_file() {
        let mut config = Config::test(std::path::PathBuf::from("/data/canardstack.duckdb"));
        config.operator.ducklake_catalog_path = None;
        assert_eq!(
            catalog_db_path(&config),
            std::path::PathBuf::from("/data/canardstack.ducklake")
        );
    }

    #[test]
    fn catalog_db_path_honors_explicit_override() {
        let mut config = Config::test(std::path::PathBuf::from("/data/canardstack.duckdb"));
        config.operator.ducklake_catalog_path =
            Some(std::path::PathBuf::from("/mnt/catalog/cat.ducklake"));
        assert_eq!(
            catalog_db_path(&config),
            std::path::PathBuf::from("/mnt/catalog/cat.ducklake")
        );
    }
}
