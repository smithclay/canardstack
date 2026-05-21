use crate::validation::ApiError;
use crate::AppState;
use serde_json::json;
use std::collections::HashMap;
use std::io::{BufReader, ErrorKind, Read};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use super::parser::{
    read_bounded_line, split_target, LineRead, MAX_HEADER_BYTES, MAX_HEADER_COUNT,
    MAX_HEADER_LINE_BYTES, MAX_REQUEST_LINE_BYTES,
};
use super::response::{write_response, write_response_with_connection, HttpResponse};
use super::router::route_owned;

const ACCEPT_POLL_INTERVAL: Duration = Duration::from_millis(10);
const SHUTDOWN_DRAIN_TIMEOUT: Duration = Duration::from_secs(30);
static NEVER_SHUTDOWN: AtomicBool = AtomicBool::new(false);

fn log_startup_storage_mode(probe: &crate::storage::StorageProbe) {
    tracing::info!(
        event = "storage_mode",
        mode = probe.mode,
        "telemetry lands in immutable DuckLake data files"
    );
}

pub fn serve(state: Arc<AppState>) -> anyhow::Result<()> {
    serve_until(state, &NEVER_SHUTDOWN)
}

pub fn serve_until(state: Arc<AppState>, shutdown: &AtomicBool) -> anyhow::Result<()> {
    let listener = TcpListener::bind(&state.config.bind)?;
    listener.set_nonblocking(true)?;
    let addr = listener.local_addr()?;
    let probe = state.storage.probe();
    tracing::info!(event = "server_listening", addr = %addr);
    log_startup_storage_mode(&probe);
    tracing::info!(
        event = "ingest_acknowledgement",
        "2xx means written to the local raw spool and pending periodic append sync"
    );
    let active = Arc::new(AtomicUsize::new(0));
    let max_conns = state.config.max_concurrent_connections;
    let read_timeout = state.config.socket_read_timeout;
    let write_timeout = state.config.socket_write_timeout;
    while !shutdown.load(Ordering::SeqCst) {
        let stream = match listener.accept() {
            Ok((stream, _addr)) => stream,
            Err(err) if err.kind() == ErrorKind::WouldBlock => {
                thread::sleep(ACCEPT_POLL_INTERVAL);
                continue;
            }
            Err(err) if err.kind() == ErrorKind::Interrupted => continue,
            Err(err) => return Err(err.into()),
        };
        let _ = stream.set_nonblocking(false);
        let _ = stream.set_read_timeout(Some(read_timeout));
        let _ = stream.set_write_timeout(Some(write_timeout));

        // Bound concurrent connections so a slow-loris attacker can't pin one OS
        // thread per socket indefinitely. Reply 503 + Retry-After (not RST) so
        // polite exporters back off instead of hot-looping.
        let prev = active.fetch_add(1, Ordering::SeqCst);
        if prev >= max_conns {
            active.fetch_sub(1, Ordering::SeqCst);
            state.metrics.inc(
                "canardstack_http_connection_errors_total",
                &[("reason", "max_connections_exceeded")],
                1,
            );
            tracing::warn!(
                event = "http_connection_overflow",
                reason = "max_connections_exceeded",
                max_concurrent_connections = max_conns
            );
            let err = ApiError::new(
                503,
                "server_overloaded",
                "max concurrent connections exceeded",
            )
            .with_retry_after(5);
            let mut stream = stream;
            let _ = write_response(&mut stream, HttpResponse::from_api_error(&err));
            continue;
        }
        let active_for_thread = active.clone();
        let state = state.clone();
        thread::spawn(move || {
            let _guard = ConnectionGuard {
                active: active_for_thread,
            };
            if let Err(err) = handle_stream(stream, state.clone()) {
                let reason = classify_io_error(&err);
                state.metrics.inc(
                    "canardstack_http_connection_errors_total",
                    &[("reason", reason)],
                    1,
                );
                tracing::warn!(
                    event = "http_request_failed",
                    reason,
                    error = %err
                );
            }
        });
    }
    tracing::info!(
        event = "shutdown_requested",
        "stopped accepting new connections"
    );
    drain_active_connections(&active);
    Ok(())
}

fn drain_active_connections(active: &AtomicUsize) {
    let deadline = Instant::now() + SHUTDOWN_DRAIN_TIMEOUT;
    while active.load(Ordering::SeqCst) > 0 && Instant::now() < deadline {
        thread::sleep(Duration::from_millis(50));
    }
    let remaining = active.load(Ordering::SeqCst);
    if remaining > 0 {
        tracing::warn!(
            event = "shutdown_drain_timeout",
            active_connections = remaining
        );
    } else {
        tracing::info!(event = "shutdown_complete", "active connections drained");
    }
}

struct ConnectionGuard {
    active: Arc<AtomicUsize>,
}

impl Drop for ConnectionGuard {
    fn drop(&mut self) {
        self.active.fetch_sub(1, Ordering::SeqCst);
    }
}

fn classify_io_error(err: &anyhow::Error) -> &'static str {
    if is_socket_timeout(err) {
        return "socket_timeout";
    }
    let msg = err.to_string();
    if msg.contains("timed out") || msg.contains("timeout") {
        "socket_timeout"
    } else if msg.contains("connection reset") || msg.contains("broken pipe") {
        "connection_reset"
    } else {
        "io_error"
    }
}

fn is_socket_timeout(err: &anyhow::Error) -> bool {
    err.downcast_ref::<std::io::Error>()
        .is_some_and(|err| matches!(err.kind(), ErrorKind::TimedOut | ErrorKind::WouldBlock))
}

fn handle_stream(mut stream: TcpStream, state: Arc<AppState>) -> anyhow::Result<()> {
    let mut reader = BufReader::new(stream.try_clone()?);
    let keepalive_enabled = state.config.bench_http_keepalive;
    let mut requests = 0usize;
    loop {
        let mut first = String::new();
        let first_read = match read_bounded_line(&mut reader, &mut first, MAX_REQUEST_LINE_BYTES) {
            Ok(LineRead::Read(read)) => read,
            Ok(LineRead::TooLong) => {
                return write_response(&mut stream, header_limit_response());
            }
            Err(err) if requests > 0 && is_socket_timeout(&err) => {
                state.metrics.inc(
                    "canardstack_http_connection_closes_total",
                    &[("reason", "keepalive_idle_timeout")],
                    1,
                );
                return Ok(());
            }
            Err(err) => return Err(err),
        };
        if first_read == 0 {
            return Ok(());
        }
        let mut parts = first.split_whitespace();
        let method = parts.next().unwrap_or("").to_string();
        let target = parts.next().unwrap_or("/").to_string();

        let mut headers = HashMap::new();
        let mut header_bytes = 0usize;
        loop {
            let mut line = String::new();
            let read = match read_bounded_line(&mut reader, &mut line, MAX_HEADER_LINE_BYTES)? {
                LineRead::Read(read) => read,
                LineRead::TooLong => {
                    return write_response(&mut stream, header_limit_response());
                }
            };
            header_bytes += read;
            if header_bytes > MAX_HEADER_BYTES || headers.len() >= MAX_HEADER_COUNT {
                return write_response(&mut stream, header_limit_response());
            }
            let trimmed = line.trim_end_matches(['\r', '\n']);
            if trimmed.is_empty() {
                break;
            }
            if let Some((k, v)) = trimmed.split_once(':') {
                headers.insert(k.trim().to_ascii_lowercase(), v.trim().to_string());
            }
        }

        let content_length = match headers.get("content-length") {
            Some(raw) => match raw.parse() {
                Ok(value) => value,
                Err(_) => {
                    return write_response(
                        &mut stream,
                        HttpResponse::json(
                            400,
                            json!({
                                "error": "invalid_content_length",
                                "message": "content-length must be a non-negative integer"
                            }),
                        ),
                    );
                }
            },
            None => 0,
        };
        if content_length > state.config.max_body_bytes {
            let response = HttpResponse::json(
                400,
                json!({
                    "error": "payload_too_large",
                    "message": format!(
                        "payload has {content_length} bytes; max is {}",
                        state.config.max_body_bytes
                    )
                }),
            );
            return write_response(&mut stream, response);
        }
        let mut body = vec![0; content_length];
        if content_length > 0 {
            reader.read_exact(&mut body)?;
        }

        let (path, query) = split_target(&target);
        let response = route_owned(&method, &path, &query, &headers, body, &state);
        requests += 1;
        let client_requested_close = headers
            .get("connection")
            .is_some_and(|value| value.eq_ignore_ascii_case("close"));
        let keep_alive = keepalive_enabled && !client_requested_close;
        state.metrics.inc(
            "canardstack_http_connection_requests_total",
            &[(
                "mode",
                if keep_alive {
                    "keep_alive"
                } else {
                    "connection_close"
                },
            )],
            1,
        );
        write_response_with_connection(&mut stream, response, keep_alive)?;
        if !keep_alive {
            return Ok(());
        }
        if requests >= 10_000 {
            state.metrics.inc(
                "canardstack_http_connection_closes_total",
                &[("reason", "keepalive_request_limit")],
                1,
            );
            return Ok(());
        }
    }
}

fn header_limit_response() -> HttpResponse {
    HttpResponse::json(
        400,
        json!({
            "error": "headers_too_large",
            "message": "request headers exceed configured parser limits"
        }),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{AppState, Config};
    use std::io::{Read, Write};

    #[test]
    fn benchmark_keepalive_allows_multiple_requests_on_one_connection() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = Config::test(dir.path().join("canardstack.duckdb"));
        config.local_storage_dir = dir.path().join("storage");
        config.bench_http_keepalive = true;
        let state = Arc::new(AppState::new(config).unwrap());
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let state_for_thread = state.clone();
        let handle = thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            handle_stream(stream, state_for_thread).unwrap();
        });

        let mut client = TcpStream::connect(addr).unwrap();
        client
            .write_all(
                b"GET /healthz HTTP/1.1\r\nhost: localhost\r\ncontent-length: 0\r\nconnection: keep-alive\r\n\r\n",
            )
            .unwrap();
        let first = read_test_response(&mut client);
        assert!(first.starts_with("HTTP/1.1 200 OK"));
        assert!(first.contains("\r\nconnection: keep-alive\r\n"));

        client
            .write_all(
                b"GET /metrics HTTP/1.1\r\nhost: localhost\r\ncontent-length: 0\r\nconnection: close\r\n\r\n",
            )
            .unwrap();
        let second = read_test_response(&mut client);
        assert!(second.starts_with("HTTP/1.1 200 OK"));
        assert!(second.contains("\r\nconnection: close\r\n"));
        handle.join().unwrap();

        let metrics = state.metrics.render_prometheus();
        assert!(
            metrics.contains("canardstack_http_connection_requests_total{mode=\"keep_alive\"} 1")
        );
        assert!(metrics
            .contains("canardstack_http_connection_requests_total{mode=\"connection_close\"} 1"));
    }

    fn read_test_response(stream: &mut TcpStream) -> String {
        let mut bytes = Vec::new();
        let mut buf = [0u8; 4096];
        loop {
            let read = stream.read(&mut buf).unwrap();
            assert!(read > 0, "unexpected eof while reading test response");
            bytes.extend_from_slice(&buf[..read]);
            if test_response_complete(&bytes) {
                break;
            }
        }
        String::from_utf8(bytes).unwrap()
    }

    fn test_response_complete(bytes: &[u8]) -> bool {
        let Some(header_end) = bytes
            .windows(b"\r\n\r\n".len())
            .position(|window| window == b"\r\n\r\n")
        else {
            return false;
        };
        let head = String::from_utf8_lossy(&bytes[..header_end]);
        let content_length = head
            .lines()
            .skip(1)
            .filter_map(|line| line.split_once(':'))
            .find_map(|(name, value)| {
                name.eq_ignore_ascii_case("content-length")
                    .then(|| value.trim().parse::<usize>().ok())
                    .flatten()
            })
            .unwrap_or(0);
        bytes.len().saturating_sub(header_end + b"\r\n\r\n".len()) >= content_length
    }
}
