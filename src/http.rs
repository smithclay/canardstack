use crate::compat;
use crate::ingest::Signal;
use crate::validation::{self, ApiError};
use crate::AppState;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::io::{BufRead, BufReader, ErrorKind, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

const MAX_REQUEST_LINE_BYTES: usize = 8 * 1024;
const MAX_HEADER_LINE_BYTES: usize = 8 * 1024;
const MAX_HEADER_BYTES: usize = 64 * 1024;
const MAX_HEADER_COUNT: usize = 100;
const ACCEPT_POLL_INTERVAL: Duration = Duration::from_millis(100);
const SHUTDOWN_DRAIN_TIMEOUT: Duration = Duration::from_secs(30);
static NEVER_SHUTDOWN: AtomicBool = AtomicBool::new(false);

fn log_startup_storage_mode(probe: &crate::storage::StorageProbe) {
    eprintln!(
        "canardstack storage mode=ducklake ({}); telemetry lands in immutable DuckLake data files.",
        probe.mode
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
    eprintln!("canardstack listening on http://{addr}/");
    log_startup_storage_mode(&probe);
    eprintln!(
        "canardstack ingest acknowledgement is best-effort: 2xx means accepted into process memory, not durably committed"
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
            let max_conns_str = max_conns.to_string();
            crate::log_event(
                "warn",
                "http_connection_overflow",
                &[
                    ("reason", "max_connections_exceeded"),
                    ("max_concurrent_connections", &max_conns_str),
                ],
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
                let err_str = err.to_string();
                crate::log_event(
                    "warn",
                    "http_request_failed",
                    &[("reason", reason), ("error", &err_str)],
                );
            }
        });
    }
    eprintln!("canardstack shutdown requested; stopped accepting new connections");
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
        crate::log_event(
            "warn",
            "shutdown_drain_timeout",
            &[("active_connections", &remaining.to_string())],
        );
    } else {
        eprintln!("canardstack shutdown complete; active connections drained");
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
    let msg = err.to_string();
    if msg.contains("timed out") || msg.contains("timeout") {
        "socket_timeout"
    } else if msg.contains("connection reset") || msg.contains("broken pipe") {
        "connection_reset"
    } else {
        "io_error"
    }
}

fn handle_stream(mut stream: TcpStream, state: Arc<AppState>) -> anyhow::Result<()> {
    let mut reader = BufReader::new(stream.try_clone()?);
    let keepalive_enabled = state.config.bench_http_keepalive;
    let mut requests = 0usize;
    loop {
        let mut first = String::new();
        let first_read = match read_bounded_line(&mut reader, &mut first, MAX_REQUEST_LINE_BYTES)? {
            LineRead::Read(read) => read,
            LineRead::TooLong => {
                return write_response(&mut stream, header_limit_response());
            }
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
                    )
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
        let response = route(&method, &path, &query, &headers, &body, &state);
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
        if reader.buffer().is_empty() {
            if let Ok(()) = reader.get_ref().set_nonblocking(true) {
                let mut probe = [0u8; 1];
                match reader.get_ref().peek(&mut probe) {
                    Ok(0) => return Ok(()),
                    Ok(_) => {}
                    Err(err) if err.kind() == ErrorKind::WouldBlock => {}
                    Err(err) => return Err(err.into()),
                }
                let _ = reader.get_ref().set_nonblocking(false);
            }
            let _ = reader.get_ref().set_nonblocking(false);
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

enum LineRead {
    Read(usize),
    TooLong,
}

fn read_bounded_line(
    reader: &mut BufReader<TcpStream>,
    line: &mut String,
    max_bytes: usize,
) -> anyhow::Result<LineRead> {
    let mut out = Vec::new();
    let mut total = 0usize;
    loop {
        let available = reader.fill_buf()?;
        if available.is_empty() {
            break;
        }
        let take = available
            .iter()
            .position(|b| *b == b'\n')
            .map(|idx| idx + 1)
            .unwrap_or(available.len());
        total += take;
        if total > max_bytes {
            return Ok(LineRead::TooLong);
        }
        out.extend_from_slice(&available[..take]);
        reader.consume(take);
        if out.ends_with(b"\n") {
            break;
        }
    }
    *line = String::from_utf8_lossy(&out).to_string();
    Ok(LineRead::Read(total))
}

pub fn route(
    method: &str,
    path: &str,
    query: &HashMap<String, String>,
    headers: &HashMap<String, String>,
    body: &[u8],
    state: &AppState,
) -> HttpResponse {
    if let Some(response) = route_compat(method, path, query, headers, body, state) {
        return response;
    }

    let result = match (method, path) {
        ("GET", "/healthz") => {
            let probe = state.storage.probe();
            let ok = probe.is_ready();
            return HttpResponse::json(
                if ok { 200 } else { 503 },
                json!({
                    "status": if ok { "ok" } else { "error" },
                    "storage": probe
                }),
            );
        }
        ("GET", "/metrics") => {
            record_operator_gauges(state);
            return HttpResponse::text(
                200,
                "text/plain; charset=utf-8",
                state.metrics.render_prometheus(),
            );
        }
        ("POST", "/v1/logs") => {
            return ingest_response(ingest(Signal::Logs, headers, body, state));
        }
        ("POST", "/v1/traces") => {
            return ingest_response(ingest(Signal::Spans, headers, body, state));
        }
        ("POST", "/v1/metrics") => {
            return ingest_response(ingest(Signal::MetricGauge, headers, body, state));
        }
        ("GET", "/api/admin/health/storage") => {
            return admin_health_response(headers, state, || {
                let health = state.storage.health();
                (health.is_ready(), json!(health))
            });
        }
        ("GET", "/api/admin/health/ingest") => admin(headers, state, || {
            Ok(json!({"queues": state.ingestor.snapshots()}))
        }),
        ("GET", "/api/admin/health/maintenance") => {
            return admin_health_response(headers, state, || {
                (state.maintenance.is_ready(), state.maintenance.health())
            });
        }
        ("GET", "/api/admin/health/queries") => {
            return admin_health_response(headers, state, || {
                // Queries are only as healthy as DuckDB; 200 here while
                // storage is wedged would mislead the runbook step.
                (state.storage.probe().is_ready(), state.queries.health())
            });
        }
        ("POST", "/api/admin/maintenance/pause") => admin(headers, state, || {
            state.maintenance.pause();
            Ok(json!({"paused": true}))
        }),
        ("POST", "/api/admin/maintenance/resume") => admin(headers, state, || {
            state.maintenance.resume();
            Ok(json!({"paused": false}))
        }),
        ("POST", "/api/admin/maintenance/flush") => admin(headers, state, || {
            let started = Instant::now();
            let result = state
                .maintenance
                .run_flush(
                    &state.ingestor,
                    &state.storage,
                    &state.metrics,
                    crate::maintenance::FlushOptions {
                        table: query.get("table").map(String::as_str),
                        force_immutable_segments: true,
                    },
                )
                .map_err(|err| {
                    if let Some((partial_signal, committed)) =
                        crate::ingest::partial_commit_info(&err)
                    {
                        if committed > 0 {
                            state.metrics.inc(
                                "canardstack_ingest_partial_commit_rows_total",
                                &[
                                    ("signal", partial_signal.as_str()),
                                    ("triggered_by", "admin_flush"),
                                ],
                                committed as u64,
                            );
                        }
                    }
                    storage_error(err)
                });
            record_maintenance_metrics(state, "flush", &result, started);
            result
        }),
        ("POST", "/api/admin/maintenance/compaction/run") => admin(headers, state, || {
            let started = Instant::now();
            let result = state
                .maintenance
                .run_compaction(
                    &state.storage,
                    query.get("table").map(String::as_str),
                    &state.metrics,
                )
                .map_err(storage_error);
            record_maintenance_metrics(state, "compaction", &result, started);
            result
        }),
        ("POST", "/api/admin/maintenance/retention/dry-run") => admin(headers, state, || {
            let started = Instant::now();
            let result = state
                .maintenance
                .retention(&state.storage, true)
                .map_err(storage_error);
            record_maintenance_metrics(state, "retention", &result, started);
            result
        }),
        ("POST", "/api/admin/maintenance/retention/run") => admin(headers, state, || {
            let started = Instant::now();
            let result = state
                .maintenance
                .retention(&state.storage, false)
                .map_err(storage_error);
            record_maintenance_metrics(state, "retention", &result, started);
            result
        }),
        _ => Err(ApiError::new(404, "not_found", "route not found")),
    };

    match result {
        Ok(value) => HttpResponse::json(200, value),
        Err(err) => HttpResponse::from_api_error(&err),
    }
}

fn route_compat(
    method: &str,
    path: &str,
    query: &HashMap<String, String>,
    headers: &HashMap<String, String>,
    body: &[u8],
    state: &AppState,
) -> Option<HttpResponse> {
    let started = Instant::now();
    let params = request_params(query, headers, body);
    // `query_class` must be a static route template — never the raw `path` —
    // or the `canardstack_query_*` series cardinality explodes on every
    // distinct trace_id / label / tag in the URL.
    let (query_class, result) = match (method, path) {
        ("GET", "/api/v1/query") | ("POST", "/api/v1/query") => (
            "/api/v1/query",
            api_auth(headers, state, || compat::prometheus_query(state, &params)),
        ),
        ("GET", "/api/v1/query_range") | ("POST", "/api/v1/query_range") => (
            "/api/v1/query_range",
            api_auth(headers, state, || {
                compat::prometheus_query_range(state, &params)
            }),
        ),
        ("GET", "/api/v1/labels") => (
            "/api/v1/labels",
            api_auth(headers, state, || compat::prometheus_labels(state, &params)),
        ),
        ("GET", "/api/v1/series") => (
            "/api/v1/series",
            api_auth(headers, state, || compat::prometheus_series(state, &params)),
        ),
        ("GET", "/api/v1/metadata") => (
            "/api/v1/metadata",
            api_auth(headers, state, || compat::prometheus_metadata(state)),
        ),
        ("GET", "/loki/api/v1/query") => (
            "/loki/api/v1/query",
            api_auth(headers, state, || compat::loki_query(state, &params)),
        ),
        ("GET", "/loki/api/v1/query_range") => (
            "/loki/api/v1/query_range",
            api_auth(headers, state, || compat::loki_query_range(state, &params)),
        ),
        ("GET", "/loki/api/v1/labels") => (
            "/loki/api/v1/labels",
            api_auth(headers, state, || compat::loki_labels(state, &params)),
        ),
        ("GET", "/loki/api/v1/series") => (
            "/loki/api/v1/series",
            api_auth(headers, state, || compat::loki_series(state, &params)),
        ),
        ("GET", "/api/search") => (
            "/api/search",
            api_auth(headers, state, || compat::tempo_search(state, &params)),
        ),
        ("GET", "/api/search/tags") => (
            "/api/search/tags",
            api_auth(headers, state, || Ok(compat::tempo_tags())),
        ),
        ("GET", "/api/v2/search/tags") => (
            "/api/v2/search/tags",
            api_auth(headers, state, || Ok(compat::tempo_tags())),
        ),
        ("GET", "/api/status/buildinfo") => (
            "/api/status/buildinfo",
            api_auth(headers, state, || {
                Ok(json!({
                    "version": env!("CARGO_PKG_VERSION"),
                    "revision": "canardstack",
                    "branch": "local",
                    "buildUser": "canardstack",
                    "buildDate": ""
                }))
            }),
        ),
        _ => {
            if method == "GET" {
                if let Some(name) = path
                    .strip_prefix("/api/v1/label/")
                    .and_then(|s| s.strip_suffix("/values"))
                {
                    return Some(compat_http(
                        state,
                        "/api/v1/label/:name/values",
                        api_auth(headers, state, || {
                            compat::prometheus_label_values(state, name, &params)
                        }),
                        started,
                    ));
                }
                if let Some(name) = path
                    .strip_prefix("/loki/api/v1/label/")
                    .and_then(|s| s.strip_suffix("/values"))
                {
                    return Some(compat_http(
                        state,
                        "/loki/api/v1/label/:name/values",
                        api_auth(headers, state, || {
                            compat::loki_label_values(state, name, &params)
                        }),
                        started,
                    ));
                }
                if let Some(trace_id) = path.strip_prefix("/api/v2/traces/") {
                    return Some(tempo_trace_http(
                        state,
                        "/api/v2/traces/:trace_id",
                        trace_id,
                        headers,
                        started,
                    ));
                }
                if let Some(trace_id) = path.strip_prefix("/api/traces/") {
                    return Some(tempo_trace_http(
                        state,
                        "/api/traces/:trace_id",
                        trace_id,
                        headers,
                        started,
                    ));
                }
                if let Some(tag) = path
                    .strip_prefix("/api/search/tag/")
                    .and_then(|s| s.strip_suffix("/values"))
                {
                    return Some(compat_http(
                        state,
                        "/api/search/tag/:tag/values",
                        api_auth(headers, state, || {
                            compat::tempo_tag_values(state, tag, &params)
                        }),
                        started,
                    ));
                }
                if let Some(tag) = path
                    .strip_prefix("/api/v2/search/tag/")
                    .and_then(|s| s.strip_suffix("/values"))
                {
                    return Some(compat_http(
                        state,
                        "/api/v2/search/tag/:tag/values",
                        api_auth(headers, state, || {
                            compat::tempo_tag_values(state, tag, &params)
                        }),
                        started,
                    ));
                }
            }
            return None;
        }
    };
    Some(compat_http(state, query_class, result, started))
}

fn tempo_trace_http(
    state: &AppState,
    query_class: &'static str,
    trace_id: &str,
    headers: &HashMap<String, String>,
    started: Instant,
) -> HttpResponse {
    let wants_json = headers
        .get("accept")
        .is_some_and(|value| value.contains("application/json"));
    if wants_json {
        return compat_http(
            state,
            query_class,
            api_auth(headers, state, || compat::tempo_trace(state, trace_id)),
            started,
        );
    }

    let result = api_auth(headers, state, || {
        compat::tempo_trace_proto(state, trace_id)
    });
    let (status, reason) = match &result {
        Ok(_) => (200, "ok"),
        Err(err) => (err.status, err.reason),
    };
    state
        .metrics
        .query_request(query_class, status, reason, started.elapsed().as_secs_f64());
    state.metrics.observe_phase_seconds(
        "query",
        "query_execute",
        Some(query_class),
        started.elapsed().as_secs_f64(),
    );
    match result {
        Ok(bytes) => HttpResponse::bytes(200, "application/protobuf", bytes),
        Err(err) => compat_error_response(err),
    }
}

fn compat_error_response(err: ApiError) -> HttpResponse {
    let retry_after = err.retry_after_seconds;
    let status = err.status;
    let body = compat::compat_error(err);
    let mut response = HttpResponse::json(status, body);
    if let Some(seconds) = retry_after {
        response = response.with_retry_after(seconds);
    }
    response
}

fn compat_http(
    state: &AppState,
    query_class: &'static str,
    result: Result<Value, ApiError>,
    started: Instant,
) -> HttpResponse {
    let (status, reason) = match &result {
        Ok(_) => (200, "ok"),
        Err(err) => (err.status, err.reason),
    };
    state
        .metrics
        .query_request(query_class, status, reason, started.elapsed().as_secs_f64());
    state.metrics.observe_phase_seconds(
        "query",
        "query_execute",
        Some(query_class),
        started.elapsed().as_secs_f64(),
    );
    match result {
        Ok(value) => HttpResponse::json(200, value),
        Err(err) => compat_error_response(err),
    }
}

fn request_params(
    query: &HashMap<String, String>,
    headers: &HashMap<String, String>,
    body: &[u8],
) -> HashMap<String, String> {
    let mut params = query.clone();
    if body.is_empty() {
        return params;
    }
    let content_type = headers
        .get("content-type")
        .map(|v| v.split(';').next().unwrap_or(v).trim().to_ascii_lowercase())
        .unwrap_or_default();
    if content_type == "application/json" {
        if let Ok(Value::Object(map)) = serde_json::from_slice::<Value>(body) {
            for (key, value) in map {
                if let Some(value) = value.as_str() {
                    params.insert(key, value.to_string());
                } else if !value.is_null() {
                    params.insert(key, value.to_string());
                }
            }
        }
    } else {
        for pair in String::from_utf8_lossy(body)
            .split('&')
            .filter(|s| !s.is_empty())
        {
            let (k, v) = pair.split_once('=').unwrap_or((pair, ""));
            params.insert(percent_decode(k), percent_decode(v));
        }
    }
    params
}

fn ingest(
    signal: Signal,
    headers: &HashMap<String, String>,
    body: &[u8],
    state: &AppState,
) -> Result<Value, ApiError> {
    validation::validate_api_key(headers, &state.config, false)?;
    state
        .ingestor
        .ingest(signal, headers, body, &state.storage, &state.metrics)
}

fn ingest_response(result: Result<Value, ApiError>) -> HttpResponse {
    // 202 matches the best-effort acknowledgement the body already advertises
    // ("accepted_into_process_memory_not_durably_committed") and the metric
    // label canardstack_ingest_requests_total{status="202"}.
    match result {
        Ok(value) => HttpResponse::json(202, value),
        Err(err) => HttpResponse::from_api_error(&err),
    }
}

fn api_auth<T>(
    headers: &HashMap<String, String>,
    state: &AppState,
    f: impl FnOnce() -> Result<T, ApiError>,
) -> Result<T, ApiError> {
    validation::validate_api_key(headers, &state.config, false)?;
    f()
}

fn admin(
    headers: &HashMap<String, String>,
    state: &AppState,
    f: impl FnOnce() -> Result<Value, ApiError>,
) -> Result<Value, ApiError> {
    validation::validate_api_key(headers, &state.config, true)?;
    f()
}

/// Admin auth, then 200 if `ready` else 503 with the same JSON body.
fn admin_health_response(
    headers: &HashMap<String, String>,
    state: &AppState,
    compute: impl FnOnce() -> (bool, Value),
) -> HttpResponse {
    if let Err(err) = validation::validate_api_key(headers, &state.config, true) {
        return HttpResponse::from_api_error(&err);
    }
    let (ready, body) = compute();
    HttpResponse::json(if ready { 200 } else { 503 }, body)
}

fn storage_error(err: anyhow::Error) -> ApiError {
    ApiError::new(503, "storage_operation_failed", err.to_string())
}

fn record_maintenance_metrics(
    state: &AppState,
    job: &str,
    result: &Result<Value, ApiError>,
    started: Instant,
) {
    let (status, reason) = match result {
        Ok(_) => ("ok", "ok"),
        Err(err) => ("error", err.reason),
    };
    state
        .metrics
        .maintenance_run(job, status, reason, started.elapsed().as_secs_f64());
}

pub(crate) fn record_operator_gauges(state: &AppState) {
    state.ingestor.record_queue_metrics(&state.metrics);
    let storage = state.storage.health();
    state.metrics.gauge(
        "canardstack_storage_physical_bytes",
        &[("table", "all")],
        storage.physical_bytes as f64,
    );
    let immutable_buffers = state
        .storage
        .immutable_buffer_metrics()
        .into_iter()
        .map(|buffer| (buffer.table, buffer))
        .collect::<HashMap<_, _>>();
    for table in [
        Signal::Logs,
        Signal::Spans,
        Signal::MetricGauge,
        Signal::MetricSum,
    ] {
        let rows = immutable_buffers
            .get(&table)
            .map(|buffer| buffer.rows)
            .unwrap_or(0);
        let bytes = immutable_buffers
            .get(&table)
            .map(|buffer| buffer.bytes)
            .unwrap_or(0);
        let age_seconds = immutable_buffers
            .get(&table)
            .map(|buffer| buffer.age_seconds)
            .unwrap_or(0.0);
        state.metrics.gauge(
            "canardstack_immutable_buffer_rows",
            &[("table", table.as_str())],
            rows as f64,
        );
        state.metrics.gauge(
            "canardstack_immutable_buffer_bytes",
            &[("table", table.as_str())],
            bytes as f64,
        );
        state.metrics.gauge(
            "canardstack_immutable_buffer_age_seconds",
            &[("table", table.as_str())],
            age_seconds,
        );
    }
    if let Some(rows) = storage.logical_rows.as_object() {
        for (table, value) in rows {
            if let Some(count) = value.as_i64() {
                state.metrics.gauge(
                    "canardstack_storage_logical_rows",
                    &[("table", table.as_str())],
                    count as f64,
                );
            }
        }
    }
    if let Some(tables) = storage
        .ducklake_storage_layout
        .get("tables")
        .and_then(Value::as_object)
    {
        for (table, value) in tables {
            for (metric, field) in [
                ("canardstack_ducklake_parquet_files", "parquet_files"),
                ("canardstack_ducklake_parquet_rows", "parquet_rows"),
                ("canardstack_ducklake_inlined_rows", "inlined_rows"),
            ] {
                if let Some(count) = value.get(field).and_then(Value::as_i64) {
                    state
                        .metrics
                        .gauge(metric, &[("table", table.as_str())], count as f64);
                }
            }
        }
    }
    if let Some(watermarks) = storage.freshness_watermarks.as_object() {
        for (table, value) in watermarks {
            if let Some(epoch) = value.get("epoch_seconds").and_then(Value::as_f64) {
                state.metrics.gauge(
                    "canardstack_freshness_watermark_timestamp",
                    &[("table", table.as_str())],
                    epoch,
                );
            }
            if let Some(lag) = value.get("lag_seconds").and_then(Value::as_f64) {
                state.metrics.gauge(
                    "canardstack_ingest_to_query_lag_seconds",
                    &[("table", table.as_str())],
                    lag.max(0.0),
                );
            }
        }
    }
    let maintenance = state.maintenance.health();
    let paused = maintenance
        .get("paused")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    state.metrics.gauge(
        "canardstack_maintenance_paused",
        &[],
        if paused { 1.0 } else { 0.0 },
    );
}

fn split_target(target: &str) -> (String, HashMap<String, String>) {
    let (path, raw_query) = target.split_once('?').unwrap_or((target, ""));
    let mut query = HashMap::new();
    for pair in raw_query.split('&').filter(|s| !s.is_empty()) {
        let (k, v) = pair.split_once('=').unwrap_or((pair, ""));
        query.insert(percent_decode(k), percent_decode(v));
    }
    (path.to_string(), query)
}

fn percent_decode(input: &str) -> String {
    let bytes = input.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            if let Some(hex) = hex_byte(bytes[i + 1], bytes[i + 2]) {
                out.push(hex);
                i += 3;
                continue;
            }
        }
        out.push(if bytes[i] == b'+' { b' ' } else { bytes[i] });
        i += 1;
    }
    String::from_utf8_lossy(&out).to_string()
}

fn hex_byte(high: u8, low: u8) -> Option<u8> {
    Some(hex_nibble(high)? << 4 | hex_nibble(low)?)
}

fn hex_nibble(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

pub struct HttpResponse {
    status: u16,
    content_type: String,
    body: Vec<u8>,
    retry_after_seconds: Option<u32>,
}

impl HttpResponse {
    pub fn status(&self) -> u16 {
        self.status
    }

    pub fn json_body(&self) -> Value {
        serde_json::from_slice(&self.body)
            .unwrap_or_else(|_| json!({"raw": String::from_utf8_lossy(&self.body)}))
    }

    pub fn text_body(&self) -> String {
        String::from_utf8_lossy(&self.body).to_string()
    }

    pub fn body(&self) -> &[u8] {
        &self.body
    }

    pub fn json(status: u16, value: Value) -> Self {
        Self {
            status,
            content_type: "application/json".to_string(),
            body: serde_json::to_vec(&value).unwrap(),
            retry_after_seconds: None,
        }
    }

    pub fn html(status: u16, value: String) -> Self {
        Self {
            status,
            content_type: "text/html; charset=utf-8".to_string(),
            body: value.into_bytes(),
            retry_after_seconds: None,
        }
    }

    pub fn text(status: u16, content_type: &str, value: String) -> Self {
        Self {
            status,
            content_type: content_type.to_string(),
            body: value.into_bytes(),
            retry_after_seconds: None,
        }
    }

    pub fn bytes(status: u16, content_type: &str, body: Vec<u8>) -> Self {
        Self {
            status,
            content_type: content_type.to_string(),
            body,
            retry_after_seconds: None,
        }
    }

    pub fn from_api_error(err: &ApiError) -> Self {
        let mut response = Self::json(err.status, err.body());
        response.retry_after_seconds = err.retry_after_seconds;
        response
    }

    pub fn with_retry_after(mut self, seconds: u32) -> Self {
        self.retry_after_seconds = Some(seconds);
        self
    }
}

fn write_response(stream: &mut TcpStream, response: HttpResponse) -> anyhow::Result<()> {
    write_response_with_connection(stream, response, false)
}

fn write_response_with_connection(
    stream: &mut TcpStream,
    response: HttpResponse,
    keep_alive: bool,
) -> anyhow::Result<()> {
    let reason = match response.status {
        200 => "OK",
        202 => "Accepted",
        400 => "Bad Request",
        401 => "Unauthorized",
        403 => "Forbidden",
        404 => "Not Found",
        429 => "Too Many Requests",
        503 => "Service Unavailable",
        _ => "OK",
    };
    write!(
        stream,
        "HTTP/1.1 {} {}\r\ncontent-type: {}\r\ncontent-length: {}\r\nconnection: {}\r\n",
        response.status,
        reason,
        response.content_type,
        response.body.len(),
        if keep_alive { "keep-alive" } else { "close" }
    )?;
    if let Some(seconds) = response.retry_after_seconds {
        write!(stream, "retry-after: {seconds}\r\n")?;
    }
    stream.write_all(b"\r\n")?;
    stream.write_all(&response.body)?;
    Ok(())
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
