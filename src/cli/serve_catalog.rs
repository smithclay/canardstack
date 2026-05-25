//! `serve-catalog` role: run the canardstack binary as a DuckDB/Quack catalog
//! server. It opens the local DuckLake catalog DuckDB file and serves it over
//! the Quack remote protocol so query/ingest nodes can `ATTACH` DuckLake against
//! it. This is plain DuckDB plus Quack: it does not run the ingest/query
//! pipeline and performs no DuckLake `ATTACH` of its own.

use crate::storage;
use crate::Config;
use anyhow::{Context, Result};
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::Duration;

const DEFAULT_LISTEN: &str = "0.0.0.0:9494";
const DEFAULT_HEALTH_BIND: &str = "0.0.0.0:8080";
const ACCEPT_POLL_INTERVAL: Duration = Duration::from_millis(50);
const HEALTH_SOCKET_TIMEOUT: Duration = Duration::from_secs(2);

pub fn run(mut args: impl Iterator<Item = String>, shutdown: &AtomicBool) -> Result<()> {
    if let Some(arg) = args.next() {
        match arg.as_str() {
            "--help" | "-h" => {
                println!("usage: canardstack serve-catalog");
                println!(
                    "serve the local DuckLake catalog DuckDB file over the Quack protocol.\n\
                     env: CANARDSTACK_DUCKLAKE_QUACK_TOKEN (required), CANARDSTACK_DUCKLAKE_CATALOG_PATH,\n\
                     CANARDSTACK_CATALOG_LISTEN (default {DEFAULT_LISTEN}), CANARDSTACK_CATALOG_HEALTH_BIND (default {DEFAULT_HEALTH_BIND})"
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
    let listen = env_or("CANARDSTACK_CATALOG_LISTEN", DEFAULT_LISTEN);
    let health_bind = env_or("CANARDSTACK_CATALOG_HEALTH_BIND", DEFAULT_HEALTH_BIND);

    let conn = storage::open_quack_catalog_connection(
        &catalog_path,
        config.operator.duckdb_extension_dir.as_deref(),
    )?;
    conn.execute_batch(&quack_serve_sql(&listen, &token))
        .context("start the Quack catalog server")?;
    tracing::info!(
        event = "catalog_listening",
        listen = %listen,
        catalog = %catalog_path.display(),
        "serving the DuckLake catalog over Quack"
    );

    // Block on the health endpoint until shutdown; the Quack server runs on its
    // own DuckDB-managed threads while `conn` stays alive.
    let result = serve_health_until(&health_bind, shutdown);

    // Best-effort graceful stop so the listen socket and catalog file are
    // released before the process exits.
    if let Err(err) = conn.execute_batch(&quack_stop_sql(&listen)) {
        tracing::info!(event = "quack_stop_skipped", error = %err);
    }
    tracing::info!(event = "catalog_shutdown_complete");
    result
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

fn quack_serve_sql(listen: &str, token: &str) -> String {
    format!(
        "CALL quack_serve('quack:{}', token => '{}', allow_other_hostname => {});",
        sql_escape(listen),
        sql_escape(token),
        if listens_on_loopback(listen) {
            "false"
        } else {
            "true"
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
    fn quack_serve_sql_binds_all_interfaces_with_remote_access() {
        assert_eq!(
            quack_serve_sql("0.0.0.0:9494", "tok"),
            "CALL quack_serve('quack:0.0.0.0:9494', token => 'tok', allow_other_hostname => true);"
        );
    }

    #[test]
    fn quack_serve_sql_loopback_disallows_other_hostname() {
        assert!(quack_serve_sql("127.0.0.1:9494", "tok").contains("allow_other_hostname => false"));
    }

    #[test]
    fn quack_serve_sql_escapes_token_quotes() {
        assert!(quack_serve_sql("0.0.0.0:9494", "a'b").contains("token => 'a''b'"));
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
