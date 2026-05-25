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

pub fn run(mut args: impl Iterator<Item = String>, shutdown: &'static AtomicBool) -> Result<()> {
    if let Some(arg) = args.next() {
        match arg.as_str() {
            "--help" | "-h" => {
                println!("usage: canardstack serve-catalog");
                println!(
                    "serve the local DuckLake catalog DuckDB file over the Quack protocol.\n\
                     env: CANARDSTACK_DUCKLAKE_QUACK_TOKEN (required), CANARDSTACK_DUCKLAKE_CATALOG_PATH,\n\
                     CANARDSTACK_CATALOG_LISTEN (default {DEFAULT_LISTEN}), CANARDSTACK_CATALOG_HEALTH_BIND (default {DEFAULT_HEALTH_BIND}).\n\
                     TLS (requires --features tls): CANARDSTACK_CATALOG_TLS=true terminates TLS on\n\
                     CANARDSTACK_CATALOG_LISTEN and forwards to CANARDSTACK_CATALOG_QUACK_BACKEND (default {DEFAULT_QUACK_BACKEND});\n\
                     set CANARDSTACK_CATALOG_TLS_MODE=file with CANARDSTACK_CATALOG_TLS_CERT_FILE and\n\
                     CANARDSTACK_CATALOG_TLS_KEY_FILE to use a persistent certificate"
                );
                return Ok(());
            }
            other => anyhow::bail!(
                "unknown serve-catalog option {other}; usage: canardstack serve-catalog"
            ),
        }
    }

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
    let public_listen = env_or("CANARDSTACK_CATALOG_LISTEN", DEFAULT_LISTEN);
    let health_bind = env_or("CANARDSTACK_CATALOG_HEALTH_BIND", DEFAULT_HEALTH_BIND);
    let tls = CatalogTlsConfig::from_env()?;

    // In TLS mode Quack binds a loopback backend and an in-binary TLS terminator
    // fronts it on the public address; requests then arrive with a non-local Host,
    // so allow_other_hostname must be on. In plaintext mode Quack binds the public
    // address directly and only opts in when that address is non-loopback.
    let (quack_listen, allow_other_hostname) = if tls.enabled {
        (
            env_or("CANARDSTACK_CATALOG_QUACK_BACKEND", DEFAULT_QUACK_BACKEND),
            true,
        )
    } else {
        (public_listen.clone(), !listens_on_loopback(&public_listen))
    };

    let conn = storage::open_quack_catalog_connection(
        &catalog_path,
        config.operator.duckdb_extension_dir.as_deref(),
    )?;
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
            anyhow::bail!("CANARDSTACK_CATALOG_TLS=true requires a build with --features tls");
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
        let enabled =
            env_bool("CANARDSTACK_CATALOG_TLS") || env_bool("CANARDSTACK_CATALOG_TLS_ENABLED");
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
                    "CANARDSTACK_CATALOG_TLS_CERT_FILE must be set when CANARDSTACK_CATALOG_TLS_MODE=file"
                );
            }
            if self.key_file.is_none() {
                anyhow::bail!(
                    "CANARDSTACK_CATALOG_TLS_KEY_FILE must be set when CANARDSTACK_CATALOG_TLS_MODE=file"
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
fn catalog_db_path(config: &Config) -> PathBuf {
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
    fn loopback_detection_covers_common_forms() {
        assert!(listens_on_loopback("127.0.0.1:9494"));
        assert!(listens_on_loopback("localhost:8080"));
        assert!(listens_on_loopback("[::1]:9494"));
        assert!(!listens_on_loopback("0.0.0.0:9494"));
        assert!(!listens_on_loopback("10.0.0.5:9494"));
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
