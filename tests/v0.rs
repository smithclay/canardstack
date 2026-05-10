use canardstack::http;
use canardstack::ingest::Signal;
use canardstack::validation;
use canardstack::{AppState, Config, Scheduler};

mod common;
use chrono::{Duration, TimeZone, Utc};
use common::{log_fixture, metric_fixture, trace_fixture};
use flate2::write::GzEncoder;
use flate2::Compression;
use opentelemetry_proto::tonic::trace::v1::TracesData;
use prost::Message;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::env;
use std::io::Write;
use std::sync::Arc;
use std::thread;
use std::time::Duration as StdDuration;
use std::time::Instant;
use tempfile::tempdir;

#[derive(Clone, PartialEq, Message)]
struct TempoTraceByIdResponseForTest {
    #[prost(message, optional, tag = "1")]
    trace: Option<TracesData>,
}

fn app() -> (tempfile::TempDir, AppState) {
    let dir = tempdir().unwrap();
    let mut config = Config::test(dir.path().join("canardstack.duckdb"));
    config.local_storage_dir = dir.path().join("storage");
    (dir, AppState::new(config).unwrap())
}

fn headers(state: &AppState) -> HashMap<String, String> {
    HashMap::from([
        (
            "authorization".to_string(),
            format!("Bearer {}", state.config.api_key),
        ),
        ("content-type".to_string(), "application/json".to_string()),
        ("accept".to_string(), "application/json".to_string()),
    ])
}

fn admin_headers(state: &AppState) -> HashMap<String, String> {
    HashMap::from([(
        "authorization".to_string(),
        format!("Bearer {}", state.config.admin_api_key),
    )])
}

struct SeededApp {
    _dir: tempfile::TempDir,
    state: AppState,
    headers: HashMap<String, String>,
    from_unix: String,
    to_unix: String,
    at_unix: String,
}

fn seeded_app() -> SeededApp {
    let (_dir, state) = app();
    let now = Utc::now();
    let now_nanos = now.timestamp_nanos_opt().unwrap();
    let headers = headers(&state);

    for (path, body) in [
        ("/v1/logs", log_fixture(now_nanos)),
        ("/v1/traces", trace_fixture(now_nanos)),
        ("/v1/metrics", metric_fixture(now_nanos)),
    ] {
        let response = http::route(
            "POST",
            path,
            &HashMap::new(),
            &headers,
            body.to_string().as_bytes(),
            &state,
        );
        assert_eq!(response.status(), 202, "{path}: {}", response.json_body());
    }
    state.ingestor.flush_all(&state.storage).unwrap();

    let from = now - Duration::minutes(5);
    let to = now + Duration::minutes(5);
    SeededApp {
        _dir,
        state,
        headers,
        from_unix: from.timestamp().to_string(),
        to_unix: to.timestamp().to_string(),
        at_unix: now.timestamp().to_string(),
    }
}

fn compat_get(app: &SeededApp, path: &str, params: HashMap<String, String>) -> http::HttpResponse {
    http::route("GET", path, &params, &app.headers, &[], &app.state)
}

fn assert_success(response: &http::HttpResponse, context: &str) -> Value {
    assert_eq!(
        response.status(),
        200,
        "{context}: {}",
        response.json_body()
    );
    response.json_body()
}

fn assert_trace_search_finds_fixture(body: &Value, context: &str) {
    let traces = body["traces"]
        .as_array()
        .unwrap_or_else(|| panic!("{context}: missing traces array: {body}"));
    assert!(
        traces
            .iter()
            .any(|trace| trace["traceID"] == "11111111111111111111111111111111"),
        "{context}: fixture trace missing: {body}"
    );
}

fn assert_tag_values_include(body: &Value, expected: &str, context: &str) {
    let values = body["tagValues"]
        .as_array()
        .unwrap_or_else(|| panic!("{context}: missing tagValues array: {body}"));
    assert!(
        values.iter().any(|value| value == expected),
        "{context}: expected {expected:?} in {body}"
    );
}

fn unix_range_params(app: &SeededApp) -> [(String, String); 2] {
    [
        ("start".to_string(), app.from_unix.clone()),
        ("end".to_string(), app.to_unix.clone()),
    ]
}

fn gauge_payload(now_nanos: i64, point_count: usize) -> Value {
    let data_points = (0..point_count)
        .map(|idx| {
            json!({
                "timeUnixNano": (now_nanos + idx as i64).to_string(),
                "asDouble": idx as f64,
                "attributes": [{"key": "route", "value": {"stringValue": "/smoke"}}]
            })
        })
        .collect::<Vec<_>>();
    json!({
        "resourceMetrics": [{
            "resource": {"attributes": [
                {"key": "service.name", "value": {"stringValue": "checkout"}},
                {"key": "deployment.environment", "value": {"stringValue": "dev"}}
            ]},
            "scopeMetrics": [{
                "scope": {"name": "smoke", "version": "1"},
                "metrics": [{
                    "name": "bulk.gauge",
                    "description": "bulk gauge",
                    "unit": "1",
                    "gauge": {"dataPoints": data_points}
                }]
            }]
        }]
    })
}

fn metric_gauge_rows(state: &AppState) -> i64 {
    state.storage.logical_rows().unwrap()["metric_gauge"]
        .as_i64()
        .unwrap()
}

fn assert_metric_queue_rows(state: &AppState, expected: usize) {
    let snapshot = state
        .ingestor
        .snapshots()
        .into_iter()
        .find(|snapshot| snapshot.signal == Signal::MetricGauge.as_str())
        .unwrap();
    assert_eq!(snapshot.queued_rows, expected);
}

#[test]
fn auth_rejects_missing_and_bad_keys() {
    let (_dir, state) = app();
    let body = log_fixture(Utc::now().timestamp_nanos_opt().unwrap()).to_string();

    let missing = http::route(
        "POST",
        "/v1/logs",
        &HashMap::new(),
        &HashMap::new(),
        body.as_bytes(),
        &state,
    );
    assert_eq!(missing.status(), 401);

    let mut bad = headers(&state);
    bad.insert("authorization".to_string(), "Bearer wrong".to_string());
    let response = http::route(
        "POST",
        "/v1/logs",
        &HashMap::new(),
        &bad,
        body.as_bytes(),
        &state,
    );
    assert_eq!(response.status(), 403);
}

#[test]
fn removed_dashboard_alert_and_rest_query_routes_are_not_available() {
    let (_dir, state) = app();
    for (method, path) in [
        ("POST", "/api/logs/search"),
        ("POST", "/api/spans/search"),
        ("POST", "/api/metrics/query"),
        ("GET", "/api/dashboards"),
        ("POST", "/api/dashboards"),
        ("GET", "/api/dashboards/local"),
        ("PATCH", "/api/dashboards/local"),
        ("GET", "/api/alerts"),
        ("POST", "/api/alerts"),
        ("PATCH", "/api/alerts/local"),
    ] {
        let response = http::route(
            method,
            path,
            &HashMap::new(),
            &HashMap::new(),
            b"{}",
            &state,
        );
        assert_eq!(response.status(), 404, "{method} {path}");
    }
}

#[test]
fn healthz_is_cheap_and_unauthenticated() {
    let (_dir, state) = app();
    let response = http::route(
        "GET",
        "/healthz",
        &HashMap::new(),
        &HashMap::new(),
        &[],
        &state,
    );
    assert_eq!(response.status(), 200);
    assert_eq!(response.json_body()["status"], "ok");
    assert_eq!(response.json_body()["storage"]["healthy"], true);
}

#[test]
fn invalid_payload_returns_400() {
    let (_dir, state) = app();
    let response = http::route(
        "POST",
        "/v1/logs",
        &HashMap::new(),
        &headers(&state),
        b"{bad json",
        &state,
    );
    assert_eq!(response.status(), 400);
}

#[test]
fn ingest_rejects_missing_or_unparseable_event_timestamps() {
    let (_dir, state) = app();
    let mut body = log_fixture(Utc::now().timestamp_nanos_opt().unwrap());
    let log_record = body["resourceLogs"][0]["scopeLogs"][0]["logRecords"][0]
        .as_object_mut()
        .unwrap();
    log_record.remove("timeUnixNano");
    log_record.remove("observedTimeUnixNano");
    let response = http::route(
        "POST",
        "/v1/logs",
        &HashMap::new(),
        &headers(&state),
        body.to_string().as_bytes(),
        &state,
    );
    assert_eq!(response.status(), 400);
    assert_eq!(response.json_body()["error"], "invalid_timestamp");

    let mut config = Config::test(tempdir().unwrap().path().join("canardstack.duckdb"));
    config.use_ducklake = false;
    let err = validation::validate_timestamp_skew(
        &[json!({"timestamp": "not-a-time"})],
        Signal::Logs,
        &config,
    )
    .unwrap_err();
    assert_eq!(err.reason, "invalid_timestamp");
}

#[test]
fn gzip_payloads_are_capped_after_decompression() {
    let dir = tempdir().unwrap();
    let mut config = Config::test(dir.path().join("canardstack.duckdb"));
    config.max_body_bytes = 128;
    let state = AppState::new(config).unwrap();
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(&vec![b' '; 4096]).unwrap();
    let compressed = encoder.finish().unwrap();
    assert!(
        compressed.len() <= state.config.max_body_bytes,
        "test fixture must be compressed below the request limit"
    );

    let mut request_headers = headers(&state);
    request_headers.insert("content-encoding".to_string(), "gzip".to_string());
    let response = http::route(
        "POST",
        "/v1/logs",
        &HashMap::new(),
        &request_headers,
        &compressed,
        &state,
    );
    assert_eq!(response.status(), 400);
    assert_eq!(response.json_body()["error"], "payload_too_large");
}

#[test]
fn timestamp_skew_returns_400() {
    let (_dir, state) = app();
    let old = (Utc::now() - Duration::days(3))
        .timestamp_nanos_opt()
        .unwrap();
    let body = log_fixture(old).to_string();
    let response = http::route(
        "POST",
        "/v1/logs",
        &HashMap::new(),
        &headers(&state),
        body.as_bytes(),
        &state,
    );
    assert_eq!(response.status(), 400);
    assert_eq!(response.json_body()["error"], "timestamp_too_old");
}

#[test]
fn dependency_unhealthy_returns_503() {
    let dir = tempdir().unwrap();
    let mut config = Config::test(dir.path().join("canardstack.duckdb"));
    config.force_dependency_unhealthy = true;
    let state = AppState::new(config).unwrap();
    let body = log_fixture(Utc::now().timestamp_nanos_opt().unwrap()).to_string();
    let response = http::route(
        "POST",
        "/v1/logs",
        &HashMap::new(),
        &headers(&state),
        body.as_bytes(),
        &state,
    );
    assert_eq!(response.status(), 503);
}

#[test]
fn queue_pressure_returns_429() {
    let dir = tempdir().unwrap();
    let mut config = Config::test(dir.path().join("canardstack.duckdb"));
    config.per_signal_queue_bytes = 16;
    config.process_ingest_bytes = 64;
    config.max_rows_per_flush = 10_000;
    config.max_bytes_per_flush = 10_000;
    let state = AppState::new(config).unwrap();
    let body = log_fixture(Utc::now().timestamp_nanos_opt().unwrap()).to_string();
    let response = http::route(
        "POST",
        "/v1/logs",
        &HashMap::new(),
        &headers(&state),
        body.as_bytes(),
        &state,
    );
    assert_eq!(response.status(), 429);
}

#[test]
fn metrics_request_rejection_does_not_partially_enqueue() {
    let dir = tempdir().unwrap();
    let mut config = Config::test(dir.path().join("canardstack.duckdb"));
    config.per_signal_queue_bytes = 1024 * 1024;
    config.max_rows_per_flush = 10_000;
    config.max_bytes_per_flush = 10_000_000;
    let body = metric_fixture(Utc::now().timestamp_nanos_opt().unwrap()).to_string();
    config.process_ingest_bytes = body.len() + 128;
    let state = AppState::new(config).unwrap();
    let response = http::route(
        "POST",
        "/v1/metrics",
        &HashMap::new(),
        &headers(&state),
        body.as_bytes(),
        &state,
    );
    assert_eq!(response.status(), 429);
    assert_eq!(
        state
            .ingestor
            .snapshots()
            .into_iter()
            .map(|s| s.queued_rows)
            .sum::<usize>(),
        0
    );
}

#[test]
fn query_limit_validation_rejects_excessive_limit() {
    let (_dir, state) = app();
    let now = Utc::now();
    let response = http::route(
        "GET",
        "/loki/api/v1/query_range",
        &HashMap::from([
            (
                "query".to_string(),
                "{service_name=\"checkout\"}".to_string(),
            ),
            (
                "start".to_string(),
                (now - Duration::minutes(5)).to_rfc3339(),
            ),
            ("end".to_string(), now.to_rfc3339()),
            ("limit".to_string(), "5000".to_string()),
        ]),
        &headers(&state),
        &[],
        &state,
    );
    assert_eq!(response.status(), 400);
    assert_eq!(response.json_body()["status"], "error");
    assert_eq!(response.json_body()["errorType"], "limit_too_large");
}

#[test]
fn compatibility_routes_require_auth() {
    let (_dir, state) = app();
    let response = http::route(
        "GET",
        "/api/v1/query",
        &HashMap::from([("query".to_string(), "smoke.gauge".to_string())]),
        &HashMap::new(),
        &[],
        &state,
    );
    assert_eq!(response.status(), 401);
}

#[test]
fn grafana_datasource_probe_routes_are_compatible() {
    let (_dir, state) = app();
    let response = http::route(
        "GET",
        "/api/v1/query",
        &HashMap::from([("query".to_string(), "1+1".to_string())]),
        &headers(&state),
        &[],
        &state,
    );
    assert_eq!(response.status(), 200);
    assert_eq!(response.json_body()["status"], "success");
    assert_eq!(response.json_body()["data"]["result"][0]["value"][1], "2");

    let buildinfo = http::route(
        "GET",
        "/api/status/buildinfo",
        &HashMap::new(),
        &headers(&state),
        &[],
        &state,
    );
    assert_eq!(buildinfo.status(), 200);
    assert_eq!(buildinfo.json_body()["revision"], "canardstack");

    let tempo_tags = http::route(
        "GET",
        "/api/v2/search/tags",
        &HashMap::new(),
        &headers(&state),
        &[],
        &state,
    );
    assert_eq!(tempo_tags.status(), 200);
    assert!(tempo_tags.json_body().to_string().contains("span.name"));
}

#[test]
fn compatibility_envelopes_reject_missing_required_query() {
    let (_dir, state) = app();
    let response = http::route(
        "GET",
        "/api/v1/query_range",
        &HashMap::new(),
        &headers(&state),
        &[],
        &state,
    );
    assert_eq!(response.status(), 400);
    assert_eq!(response.json_body()["status"], "error");
    assert_eq!(response.json_body()["errorType"], "missing_parameter");
}

#[test]
fn compatibility_routes_reject_unsupported_query_subsets() {
    let (_dir, state) = app();
    let now = Utc::now();
    let response = http::route(
        "GET",
        "/api/v1/query_range",
        &HashMap::from([
            (
                "query".to_string(),
                "histogram_quantile(0.9, x)".to_string(),
            ),
            (
                "start".to_string(),
                (now - Duration::minutes(5)).to_rfc3339(),
            ),
            ("end".to_string(), now.to_rfc3339()),
            ("step".to_string(), "60".to_string()),
        ]),
        &headers(&state),
        &[],
        &state,
    );
    assert_eq!(response.status(), 400);
    assert_eq!(response.json_body()["status"], "error");
    assert_eq!(response.json_body()["errorType"], "unsupported_promql");
}

#[test]
fn compatibility_selectors_reject_unsupported_labels_instead_of_widening() {
    let app = seeded_app();

    let mut prom_params = HashMap::from(unix_range_params(&app));
    prom_params.insert(
        "query".to_string(),
        "avg(smoke.gauge{service_name=\"checkout\",pod=\"api-1\"})".to_string(),
    );
    prom_params.insert("step".to_string(), "60".to_string());
    let prom = compat_get(&app, "/api/v1/query_range", prom_params);
    assert_eq!(prom.status(), 400);
    assert_eq!(prom.json_body()["errorType"], "unsupported_promql");

    let mut loki_params = HashMap::from(unix_range_params(&app));
    loki_params.insert(
        "query".to_string(),
        "{service_name=\"checkout\",pod=\"api-1\"} |= \"smoke\"".to_string(),
    );
    let loki = compat_get(&app, "/loki/api/v1/query_range", loki_params);
    assert_eq!(loki.status(), 400);
    assert_eq!(loki.json_body()["errorType"], "unsupported_selector");
}

#[test]
fn prometheus_instant_query_returns_latest_bucket_in_lookback() {
    let (_dir, state) = app();
    let at = Utc.with_ymd_and_hms(1970, 1, 1, 0, 11, 0).unwrap();
    let older = Utc.with_ymd_and_hms(1970, 1, 1, 0, 6, 30).unwrap();
    let newer = Utc.with_ymd_and_hms(1970, 1, 1, 0, 10, 30).unwrap();
    state
        .storage
        .insert_records(
            Signal::MetricGauge,
            &[
                json!({"timestamp": older.timestamp_millis(), "metric_name": "smoke.gauge", "value": 1.0, "service_name": "checkout"}),
                json!({"timestamp": newer.timestamp_millis(), "metric_name": "smoke.gauge", "value": 2.0, "service_name": "checkout"}),
            ],
            "test",
        )
        .unwrap();

    let response = http::route(
        "GET",
        "/api/v1/query",
        &HashMap::from([
            ("query".to_string(), "smoke.gauge".to_string()),
            ("time".to_string(), at.timestamp().to_string()),
        ]),
        &headers(&state),
        &[],
        &state,
    );
    assert_eq!(response.status(), 200, "{}", response.json_body());
    assert_eq!(response.json_body()["data"]["result"][0]["value"][1], "2");
}

#[test]
fn metric_flush_splits_oversized_batch_and_preserves_queue_accounting() {
    let dir = tempdir().unwrap();
    let mut config = Config::test(dir.path().join("canardstack.duckdb"));
    config.local_storage_dir = dir.path().join("storage");
    config.max_rows_per_flush = 2;
    config.max_bytes_per_flush = 10 * 1024 * 1024;
    let state = AppState::new(config).unwrap();
    let body = gauge_payload(Utc::now().timestamp_nanos_opt().unwrap(), 5).to_string();

    let response = http::route(
        "POST",
        "/v1/metrics",
        &HashMap::new(),
        &headers(&state),
        body.as_bytes(),
        &state,
    );
    assert_eq!(response.status(), 202, "{}", response.json_body());
    assert_eq!(metric_gauge_rows(&state), 2);
    assert_metric_queue_rows(&state, 3);

    assert_eq!(
        state
            .ingestor
            .flush_signal(Signal::MetricGauge, &state.storage)
            .unwrap(),
        2
    );
    assert_eq!(metric_gauge_rows(&state), 4);
    assert_metric_queue_rows(&state, 1);

    assert_eq!(
        state
            .ingestor
            .flush_signal(Signal::MetricGauge, &state.storage)
            .unwrap(),
        1
    );
    assert_eq!(metric_gauge_rows(&state), 5);
    assert_metric_queue_rows(&state, 0);
}

#[test]
fn storage_large_metric_insert_commits_across_internal_chunks() {
    let (_dir, state) = app();
    let now = Utc::now().timestamp_millis();
    let rows = (0..6_001)
        .map(|idx| {
            json!({
                "timestamp": now + idx,
                "metric_name": "bulk.gauge",
                "value": idx as f64,
                "service_name": "checkout"
            })
        })
        .collect::<Vec<_>>();

    let inserted = state
        .storage
        .insert_records(Signal::MetricGauge, &rows, "test")
        .unwrap();

    assert_eq!(inserted, rows.len());
    assert_eq!(metric_gauge_rows(&state), rows.len() as i64);
}

#[test]
fn freshness_lag_tracks_ingest_visibility_not_event_time_age() {
    let (_dir, state) = app();
    state
        .storage
        .insert_records(
            Signal::MetricGauge,
            &[json!({
                "timestamp": 0,
                "metric_name": "old.event",
                "value": 1.0,
                "service_name": "checkout"
            })],
            "test",
        )
        .unwrap();

    let watermarks = state.storage.freshness_watermarks().unwrap();
    let metric = &watermarks["metric_gauge"];
    assert!(metric["lag_seconds"].as_f64().unwrap() < 60.0, "{metric}");
    assert!(
        metric["event_lag_seconds"].as_f64().unwrap() > 1_000_000.0,
        "{metric}"
    );
}

#[test]
fn loki_forward_direction_orders_entries_forward_within_stream() {
    let (_dir, state) = app();
    let older = Utc.with_ymd_and_hms(1970, 1, 1, 0, 0, 10).unwrap();
    let newer = Utc.with_ymd_and_hms(1970, 1, 1, 0, 0, 20).unwrap();
    state
        .storage
        .insert_records(
            Signal::Logs,
            &[
                json!({"timestamp": newer.timestamp_millis(), "body": "newer", "service_name": "checkout"}),
                json!({"timestamp": older.timestamp_millis(), "body": "older", "service_name": "checkout"}),
            ],
            "test",
        )
        .unwrap();

    let response = http::route(
        "GET",
        "/loki/api/v1/query_range",
        &HashMap::from([
            (
                "query".to_string(),
                "{service_name=\"checkout\"}".to_string(),
            ),
            (
                "start".to_string(),
                Utc.with_ymd_and_hms(1970, 1, 1, 0, 0, 0)
                    .unwrap()
                    .timestamp()
                    .to_string(),
            ),
            (
                "end".to_string(),
                Utc.with_ymd_and_hms(1970, 1, 1, 0, 1, 0)
                    .unwrap()
                    .timestamp()
                    .to_string(),
            ),
            ("limit".to_string(), "10".to_string()),
            ("direction".to_string(), "forward".to_string()),
        ]),
        &headers(&state),
        &[],
        &state,
    );
    assert_eq!(response.status(), 200, "{}", response.json_body());
    let values = response.json_body()["data"]["result"][0]["values"]
        .as_array()
        .unwrap()
        .clone();
    assert_eq!(values[0][1], "older");
    assert_eq!(values[1][1], "newer");
}

#[test]
fn grafana_prometheus_contract_accepts_dashboard_selectors_and_unix_times() {
    let app = seeded_app();

    let mut range_params = HashMap::from(unix_range_params(&app));
    range_params.insert(
        "query".to_string(),
        "avg({__name__=\"smoke.gauge\",service_name=\"checkout\"})".to_string(),
    );
    range_params.insert("step".to_string(), "60s".to_string());
    let range = assert_success(
        &compat_get(&app, "/api/v1/query_range", range_params),
        "prometheus query_range",
    );
    assert_eq!(range["status"], "success");
    assert_eq!(range["data"]["resultType"], "matrix");
    let series = range["data"]["result"]
        .as_array()
        .unwrap()
        .first()
        .unwrap_or_else(|| panic!("prometheus query_range returned no series: {range}"));
    assert_eq!(series["metric"]["__name__"], "smoke.gauge");
    assert_eq!(series["metric"]["service_name"], "checkout");
    let sample = series["values"]
        .as_array()
        .unwrap()
        .first()
        .unwrap_or_else(|| panic!("prometheus query_range returned no samples: {range}"));
    assert!(
        sample[0].is_number(),
        "Grafana requires numeric sample timestamps: {sample}"
    );
    assert_eq!(sample[1], "42");

    let instant = assert_success(
        &compat_get(
            &app,
            "/api/v1/query",
            HashMap::from([
                ("query".to_string(), "1+1".to_string()),
                ("time".to_string(), app.at_unix.clone()),
            ]),
        ),
        "prometheus datasource probe",
    );
    let value = &instant["data"]["result"][0]["value"];
    assert!(
        value[0].is_number(),
        "probe timestamp must be numeric: {instant}"
    );
    assert_eq!(value[1], "2");
}

#[test]
fn grafana_loki_contract_accepts_unix_ranges_and_label_aliases() {
    let app = seeded_app();

    let mut query_params = HashMap::from(unix_range_params(&app));
    query_params.insert(
        "query".to_string(),
        "{service_name=\"checkout\"} |= \"smoke\"".to_string(),
    );
    query_params.insert("limit".to_string(), "10".to_string());
    query_params.insert("direction".to_string(), "backward".to_string());
    let logs = assert_success(
        &compat_get(&app, "/loki/api/v1/query_range", query_params),
        "loki query_range",
    );
    assert_eq!(logs["status"], "success");
    assert!(
        logs.to_string().contains("smoke payment timeout"),
        "fixture log missing: {logs}"
    );

    let label_values = assert_success(
        &compat_get(
            &app,
            "/loki/api/v1/label/http_route/values",
            HashMap::from(unix_range_params(&app)),
        ),
        "loki label values",
    );
    assert_eq!(label_values["status"], "success");
    assert!(
        label_values["data"]
            .as_array()
            .unwrap()
            .iter()
            .any(|value| value == "/smoke"),
        "http_route label value missing: {label_values}"
    );
}

#[test]
fn grafana_tempo_contract_supports_probe_search_tags_and_trace_lookup() {
    let app = seeded_app();

    let buildinfo = assert_success(
        &compat_get(&app, "/api/status/buildinfo", HashMap::new()),
        "tempo buildinfo probe",
    );
    assert_eq!(buildinfo["revision"], "canardstack");

    for path in ["/api/search/tags", "/api/v2/search/tags"] {
        let tags = assert_success(&compat_get(&app, path, HashMap::new()), path);
        let tag_names = tags["tagNames"].as_array().unwrap();
        for expected in ["service.name", "span.name", "status"] {
            assert!(
                tag_names.iter().any(|tag| tag == expected),
                "{path}: expected {expected:?} in {tags}"
            );
        }
    }

    for (path, expected) in [
        ("/api/search/tag/service.name/values", "checkout"),
        ("/api/v2/search/tag/service.name/values", "checkout"),
        ("/api/v2/search/tag/service-name/values", "checkout"),
        ("/api/v2/search/tag/span-name/values", "GET /smoke"),
        ("/api/v2/search/tag/status/values", "2"),
    ] {
        let values = assert_success(
            &compat_get(&app, path, HashMap::from(unix_range_params(&app))),
            path,
        );
        assert_tag_values_include(&values, expected, path);
    }

    let duration_values = assert_success(
        &compat_get(
            &app,
            "/api/v2/search/tag/duration/values",
            HashMap::from(unix_range_params(&app)),
        ),
        "Grafana duration tag values helper",
    );
    assert_eq!(duration_values["tagValues"].as_array().unwrap().len(), 0);

    for (context, extra_params) in [
        (
            "canonical service tag",
            HashMap::from([("service.name".to_string(), "checkout".to_string())]),
        ),
        (
            "hyphenated service alias",
            HashMap::from([("service-name".to_string(), "checkout".to_string())]),
        ),
        (
            "Grafana dashboard serviceName field",
            HashMap::from([("serviceName".to_string(), "checkout".to_string())]),
        ),
        (
            "canonical span tag",
            HashMap::from([("span.name".to_string(), "GET /smoke".to_string())]),
        ),
        (
            "TraceQL query field",
            HashMap::from([(
                "q".to_string(),
                "{resource.service.name=\"checkout\" && span.name=\"GET /smoke\"}".to_string(),
            )]),
        ),
        (
            "tags query field",
            HashMap::from([(
                "tags".to_string(),
                "{service.name=\"checkout\",span.name=\"GET /smoke\"}".to_string(),
            )]),
        ),
    ] {
        let mut params = HashMap::from(unix_range_params(&app));
        params.extend(extra_params);
        params.insert("limit".to_string(), "20".to_string());
        let search = assert_success(&compat_get(&app, "/api/search", params), context);
        let trace = search["traces"]
            .as_array()
            .unwrap()
            .first()
            .unwrap_or_else(|| panic!("{context}: missing trace: {search}"));
        assert!(
            trace["spanSet"]["spans"].as_array().is_some(),
            "{context}: Grafana expects spanSet.spans to be an array: {trace}"
        );
        assert!(
            trace["startTime"].is_null() && trace["startTimeUnixNano"].as_str().is_some(),
            "{context}: Grafana Tempo search expects startTimeUnixNano, not startTime: {trace}"
        );
        assert_trace_search_finds_fixture(&search, context);
    }

    for path in [
        "/api/traces/11111111111111111111111111111111",
        "/api/v2/traces/11111111111111111111111111111111",
    ] {
        let trace = assert_success(&compat_get(&app, path, HashMap::new()), path);
        assert!(
            trace.to_string().contains("GET /smoke"),
            "{path}: expected fixture span: {trace}"
        );
    }

    let mut grafana_headers = app.headers.clone();
    grafana_headers.remove("accept");
    let trace_proto = http::route(
        "GET",
        "/api/v2/traces/11111111111111111111111111111111",
        &HashMap::new(),
        &grafana_headers,
        &[],
        &app.state,
    );
    assert_eq!(trace_proto.status(), 200, "{}", trace_proto.text_body());
    let decoded = TempoTraceByIdResponseForTest::decode(trace_proto.body())
        .unwrap_or_else(|err| panic!("Grafana trace lookup must be Tempo protobuf: {err}"));
    let trace = decoded
        .trace
        .expect("Tempo trace response should include trace");
    let spans = &trace.resource_spans[0].scope_spans[0].spans;
    assert_eq!(spans[0].name, "GET /smoke");
}

#[test]
fn provisioned_grafana_dashboard_uses_supported_compat_queries() {
    let datasources = include_str!("../config/grafana/provisioning/datasources/canardstack.yaml");
    assert!(
        datasources.contains("uid: canardstack-tempo"),
        "Tempo datasource should be provisioned"
    );
    assert!(
        datasources.contains("streamingEnabled:")
            && datasources.contains("search: false")
            && datasources.contains("metrics: false"),
        "Tempo streaming must stay disabled because canardstack exposes HTTP compatibility APIs, not Tempo gRPC streaming"
    );

    let dashboard: Value = serde_json::from_str(include_str!(
        "../config/grafana/dashboards/canardstack-overview.json"
    ))
    .unwrap();
    let links = dashboard["links"].as_array().unwrap();
    let panels = dashboard["panels"].as_array().unwrap();

    let mut prom_exprs = Vec::new();
    let mut loki_exprs = Vec::new();
    let mut tempo_searches = Vec::new();
    for panel in panels {
        if let Some(targets) = panel["targets"].as_array() {
            for target in targets {
                match target["datasource"]["uid"].as_str() {
                    Some("canardstack-prometheus") => {
                        prom_exprs.push(target["expr"].as_str().unwrap_or_default().to_string());
                    }
                    Some("canardstack-loki") => {
                        loki_exprs.push(target["expr"].as_str().unwrap_or_default().to_string());
                    }
                    Some("canardstack-tempo") => {
                        tempo_searches.push(target.clone());
                    }
                    _ => {}
                }
            }
        }
    }

    assert!(
        prom_exprs
            .iter()
            .any(|expr| expr == "avg({__name__=\"smoke.gauge\",service_name=\"checkout\"})"),
        "dashboard should use the __name__ selector Grafana accepts: {prom_exprs:?}"
    );
    assert!(
        !prom_exprs.iter().any(|expr| expr.contains("smoke.gauge{")),
        "dotted metric names inside bare PromQL selectors are not Grafana-safe: {prom_exprs:?}"
    );
    assert!(
        loki_exprs
            .iter()
            .any(|expr| expr == "{service_name=\"checkout\"} |= \"smoke\""),
        "dashboard should include the smoke log query: {loki_exprs:?}"
    );
    assert!(
        tempo_searches.iter().any(|target| {
            target["queryType"] == "traceqlSearch"
                && target["query"] == "{resource.service.name=\"checkout\"}"
                && target["tableType"] == "traces"
        }),
        "dashboard should provision a dashboard-compatible Tempo TraceQL search target: {tempo_searches:?}"
    );
    assert!(
        links.iter().any(|link| {
            link["title"] == "Explore traces"
                && link["url"]
                    .as_str()
                    .is_some_and(|url| url.contains("traceqlSearch"))
        }),
        "dashboard Explore link should use the same Tempo TraceQL search shape: {links:?}"
    );
}

#[test]
fn ingest_flush_and_query_vertical_slice() {
    let (_dir, state) = app();
    let now = Utc::now();
    let now_nanos = now.timestamp_nanos_opt().unwrap();
    let headers = headers(&state);

    for (path, body) in [
        ("/v1/logs", log_fixture(now_nanos)),
        ("/v1/traces", trace_fixture(now_nanos)),
        ("/v1/metrics", metric_fixture(now_nanos)),
    ] {
        let response = http::route(
            "POST",
            path,
            &HashMap::new(),
            &headers,
            body.to_string().as_bytes(),
            &state,
        );
        assert_eq!(response.status(), 202, "{path}: {}", response.json_body());
    }
    state.ingestor.flush_all(&state.storage).unwrap();

    let from = (now - Duration::minutes(5)).to_rfc3339();
    let to = (now + Duration::minutes(5)).to_rfc3339();
    let health = http::route(
        "GET",
        "/api/admin/health/storage",
        &HashMap::new(),
        &admin_headers(&state),
        &[],
        &state,
    );
    assert_eq!(health.status(), 200);
    assert_eq!(health.json_body()["logical_rows"]["logs"], 1);

    let prom = http::route(
        "GET",
        "/api/v1/query_range",
        &HashMap::from([
            (
                "query".to_string(),
                "avg(smoke.gauge{service_name=\"checkout\"})".to_string(),
            ),
            ("start".to_string(), from.clone()),
            ("end".to_string(), to.clone()),
            ("step".to_string(), "60".to_string()),
        ]),
        &headers,
        &[],
        &state,
    );
    assert_eq!(prom.status(), 200, "{}", prom.json_body());
    assert_eq!(prom.json_body()["status"], "success");
    assert!(prom.json_body()["data"]["result"]
        .as_array()
        .unwrap()
        .iter()
        .any(|series| series.to_string().contains("42")));

    let loki = http::route(
        "GET",
        "/loki/api/v1/query_range",
        &HashMap::from([
            (
                "query".to_string(),
                "{service_name=\"checkout\"} |= \"smoke\"".to_string(),
            ),
            ("start".to_string(), from.clone()),
            ("end".to_string(), to.clone()),
            ("limit".to_string(), "10".to_string()),
        ]),
        &headers,
        &[],
        &state,
    );
    assert_eq!(loki.status(), 200, "{}", loki.json_body());
    assert_eq!(loki.json_body()["status"], "success");
    assert!(loki
        .json_body()
        .to_string()
        .contains("smoke payment timeout"));

    let trace = http::route(
        "GET",
        "/api/v2/traces/11111111111111111111111111111111",
        &HashMap::new(),
        &headers,
        &[],
        &state,
    );
    assert_eq!(trace.status(), 200, "{}", trace.json_body());
    assert!(trace.json_body().to_string().contains("GET /smoke"));

    let trace_v1 = http::route(
        "GET",
        "/api/traces/11111111111111111111111111111111",
        &HashMap::new(),
        &headers,
        &[],
        &state,
    );
    assert_eq!(trace_v1.status(), 200, "{}", trace_v1.json_body());
    assert!(trace_v1.json_body().to_string().contains("GET /smoke"));

    let search = http::route(
        "GET",
        "/api/search",
        &HashMap::from([
            ("start".to_string(), from.clone()),
            ("end".to_string(), to.clone()),
            ("service.name".to_string(), "checkout".to_string()),
            ("limit".to_string(), "10".to_string()),
        ]),
        &headers,
        &[],
        &state,
    );
    assert_eq!(search.status(), 200, "{}", search.json_body());
    assert!(search
        .json_body()
        .to_string()
        .contains("11111111111111111111111111111111"));

    let label_values = http::route(
        "GET",
        "/loki/api/v1/label/http_route/values",
        &HashMap::from([("start".to_string(), from), ("end".to_string(), to)]),
        &headers,
        &[],
        &state,
    );
    assert_eq!(label_values.status(), 200, "{}", label_values.json_body());
    assert!(label_values.json_body().to_string().contains("/smoke"));

    let metrics = http::route(
        "GET",
        "/metrics",
        &HashMap::new(),
        &HashMap::new(),
        &[],
        &state,
    );
    assert_eq!(metrics.status(), 200);
    assert!(metrics
        .text_body()
        .contains("canardstack_query_requests_total"));
}

#[test]
#[ignore = "requires MotherDuck network access and motherduck_token or MOTHERDUCK_TOKEN"]
fn remote_motherduck_ducklake_smoke() {
    let dir = tempdir().unwrap();
    let mut config = Config::test(dir.path().join("canardstack.duckdb"));
    config.local_storage_dir = dir.path().join("storage");
    config.use_ducklake = true;
    config.ducklake_attach_uri = Some(
        env::var("CANARDSTACK_DUCKLAKE_ATTACH_URI")
            .unwrap_or_else(|_| "md:test-ducklake".to_string()),
    );
    config.max_rows_per_flush = 1;
    if let Ok(extension_dir) = env::var("CANARDSTACK_DUCKDB_EXTENSION_DIR") {
        config.duckdb_extension_dir = Some(extension_dir.into());
    }

    let state = AppState::new(config).unwrap();
    let health = state.storage.health();
    assert_eq!(health.mode, "ducklake_motherduck_remote");
    assert!(health.ducklake_available);
    assert!(health.capabilities.insert);
    assert!(!health.capabilities.inlined_flush);

    let now = Utc::now();
    let body = log_fixture(now.timestamp_nanos_opt().unwrap()).to_string();
    let response = http::route(
        "POST",
        "/v1/logs",
        &HashMap::new(),
        &headers(&state),
        body.as_bytes(),
        &state,
    );
    assert_eq!(response.status(), 202, "{}", response.json_body());
    state.ingestor.flush_all(&state.storage).unwrap();

    let logs = http::route(
        "GET",
        "/loki/api/v1/query_range",
        &HashMap::from([
            (
                "query".to_string(),
                "{service_name=\"checkout\"} |= \"smoke\"".to_string(),
            ),
            (
                "start".to_string(),
                (now - Duration::minutes(5)).to_rfc3339(),
            ),
            ("end".to_string(), (now + Duration::minutes(5)).to_rfc3339()),
            ("limit".to_string(), "10".to_string()),
        ]),
        &headers(&state),
        &[],
        &state,
    );
    assert_eq!(logs.status(), 200, "{}", logs.json_body());
    assert!(logs.json_body().to_string().contains("smoke"));
}

#[test]
fn crash_semantics_are_best_effort_until_flush() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("canardstack.duckdb");
    let mut config = Config::test(db_path.clone());
    config.max_rows_per_flush = 10_000;
    let state = AppState::new(config.clone()).unwrap();
    let now = Utc::now();
    let body = log_fixture(now.timestamp_nanos_opt().unwrap()).to_string();
    let response = http::route(
        "POST",
        "/v1/logs",
        &HashMap::new(),
        &headers(&state),
        body.as_bytes(),
        &state,
    );
    assert_eq!(response.status(), 202);
    drop(state);

    let restarted = AppState::new(config).unwrap();
    let result = http::route(
        "GET",
        "/loki/api/v1/query_range",
        &HashMap::from([
            ("query".to_string(), "{} |= \"smoke\"".to_string()),
            (
                "start".to_string(),
                (now - Duration::minutes(5)).to_rfc3339(),
            ),
            ("end".to_string(), (now + Duration::minutes(5)).to_rfc3339()),
            ("limit".to_string(), "10".to_string()),
        ]),
        &headers(&restarted),
        &[],
        &restarted,
    );
    assert_eq!(result.status(), 200);
    assert!(result.json_body()["data"]["result"]
        .as_array()
        .unwrap()
        .is_empty());
}

#[test]
fn scheduler_watchdog_flushes_aged_queue_without_admin_action() {
    let dir = tempdir().unwrap();
    let mut config = Config::test(dir.path().join("canardstack.duckdb"));
    config.local_storage_dir = dir.path().join("storage");
    config.scheduler_enabled = true;
    config.scheduler_watchdog_interval = StdDuration::from_millis(20);
    config.scheduler_flush_interval = StdDuration::from_secs(3_600);
    config.scheduler_retention_interval = StdDuration::from_secs(3_600);
    config.max_age = StdDuration::from_millis(10);
    config.high_pressure_max_age = StdDuration::from_millis(5);
    config.max_rows_per_flush = 10_000;
    config.max_bytes_per_flush = 10_000_000;

    let state = Arc::new(AppState::new(config).unwrap());
    let scheduler = Scheduler::spawn(state.clone());

    let now = Utc::now();
    let body = log_fixture(now.timestamp_nanos_opt().unwrap()).to_string();
    let response = http::route(
        "POST",
        "/v1/logs",
        &HashMap::new(),
        &headers(&state),
        body.as_bytes(),
        &state,
    );
    assert_eq!(response.status(), 202);

    let deadline = Instant::now() + StdDuration::from_secs(3);
    let mut row_count = 0;
    while Instant::now() < deadline {
        thread::sleep(StdDuration::from_millis(40));
        let logs = http::route(
            "GET",
            "/loki/api/v1/query_range",
            &HashMap::from([
                ("query".to_string(), "{} |= \"smoke\"".to_string()),
                (
                    "start".to_string(),
                    (now - Duration::minutes(5)).to_rfc3339(),
                ),
                ("end".to_string(), (now + Duration::minutes(5)).to_rfc3339()),
                ("limit".to_string(), "10".to_string()),
            ]),
            &headers(&state),
            &[],
            &state,
        );
        if logs.status() == 200 {
            row_count = logs.json_body()["data"]["result"]
                .as_array()
                .map(|r| r.len())
                .unwrap_or(0);
            if row_count > 0 {
                break;
            }
        }
    }

    drop(scheduler);
    assert!(
        row_count > 0,
        "watchdog should have flushed aged queue without admin POST"
    );

    let maintenance_health = http::route(
        "GET",
        "/api/admin/health/maintenance",
        &HashMap::new(),
        &admin_headers(&state),
        &[],
        &state,
    );
    let last_runs = &maintenance_health.json_body()["last_runs"];
    assert!(
        last_runs.get("watchdog").is_some(),
        "scheduler should have recorded a watchdog run, got {last_runs}"
    );
}

#[test]
fn retention_run_deletes_whole_day_eligible_rows() {
    let dir = tempdir().unwrap();
    let mut config = Config::test(dir.path().join("canardstack.duckdb"));
    config.local_storage_dir = dir.path().join("storage");
    config.logs_retention_days = 1;
    let state = AppState::new(config).unwrap();
    let old_ms = (Utc::now() - Duration::days(3)).timestamp_millis();
    let fresh_ms = Utc::now().timestamp_millis();
    state
        .storage
        .insert_records(
            Signal::Logs,
            &[
                json!({"timestamp": old_ms, "body": "old retained log"}),
                json!({"timestamp": fresh_ms, "body": "fresh retained log"}),
            ],
            "otlp_json",
        )
        .unwrap();

    let response = http::route(
        "POST",
        "/api/admin/maintenance/retention/run",
        &HashMap::new(),
        &admin_headers(&state),
        &[],
        &state,
    );
    assert_eq!(response.status(), 200, "{}", response.json_body());

    let remaining = state
        .storage
        .with_conn(|conn, prefix| {
            let sql = format!("SELECT count(*) FROM {prefix}logs");
            let count: i64 = conn.query_row(&sql, [], |row| row.get(0))?;
            Ok(count)
        })
        .unwrap();
    assert_eq!(remaining, 1);
}

#[test]
fn malicious_label_values_do_not_inject_sql() {
    // Hostile inputs flow through compat label/tag/value lookups that build
    // SQL via sql_quote. None should cause a 5xx (which would indicate the
    // SQL parser saw injected statements) or a panic. 200/4xx are both fine —
    // the contract is "never let an attacker run their own SQL."
    let app = seeded_app();
    let hostile = [
        "checkout' OR '1'='1",
        "x'; DROP TABLE spans;--",
        "x' UNION SELECT * FROM logs--",
        "checkout\\' OR 1=1",
        "checkout\0",
        "checkout\"; --",
    ];

    for value in hostile {
        for path in [
            "/api/v1/label/service_name/values",
            "/loki/api/v1/label/service_name/values",
            "/api/search/tag/service.name/values",
            "/api/v2/search/tag/service.name/values",
        ] {
            let response = compat_get(
                &app,
                path,
                HashMap::from([
                    ("start".to_string(), app.from_unix.clone()),
                    ("end".to_string(), app.to_unix.clone()),
                    (
                        "match[]".to_string(),
                        format!("{{service_name=\"{value}\"}}"),
                    ),
                ]),
            );
            let status = response.status();
            assert!(
                (200..600).contains(&status),
                "{path} with hostile value {value:?} returned non-HTTP status {status}",
            );
        }

        // TraceQL-shaped search endpoints take values in `q=` / `tags=`.
        for params in [
            HashMap::from([
                ("start".to_string(), app.from_unix.clone()),
                ("end".to_string(), app.to_unix.clone()),
                (
                    "q".to_string(),
                    format!("{{ .service.name = \"{value}\" }}"),
                ),
            ]),
            HashMap::from([
                ("start".to_string(), app.from_unix.clone()),
                ("end".to_string(), app.to_unix.clone()),
                ("tags".to_string(), format!("service.name={value}")),
            ]),
            HashMap::from([
                ("start".to_string(), app.from_unix.clone()),
                ("end".to_string(), app.to_unix.clone()),
                ("service.name".to_string(), value.to_string()),
            ]),
        ] {
            let response = compat_get(&app, "/api/search", params);
            let status = response.status();
            assert!(
                (200..600).contains(&status),
                "/api/search with hostile value {value:?} returned non-HTTP status {status}",
            );
        }

        // Loki query_range takes values via the {label="..."} stream selector.
        let response = compat_get(
            &app,
            "/loki/api/v1/query_range",
            HashMap::from([
                ("start".to_string(), app.from_unix.clone()),
                ("end".to_string(), app.to_unix.clone()),
                ("query".to_string(), format!("{{service_name=\"{value}\"}}")),
                ("limit".to_string(), "10".to_string()),
            ]),
        );
        let status = response.status();
        assert!(
            (200..600).contains(&status),
            "/loki/api/v1/query_range with hostile value {value:?} returned non-HTTP status {status}",
        );
    }

    // Sanity: after all the hostile traffic, the fixture row is still there
    // (no DROP TABLE actually fired).
    let response = compat_get(
        &app,
        "/api/v1/label/service_name/values",
        HashMap::from([
            ("start".to_string(), app.from_unix.clone()),
            ("end".to_string(), app.to_unix.clone()),
        ]),
    );
    assert_eq!(response.status(), 200);
    let body = response.json_body();
    let values = body["data"].as_array().expect("data array");
    assert!(
        values.iter().any(|v| v == "checkout"),
        "expected fixture service_name still present: {body}"
    );
}

#[test]
fn admin_endpoints_reject_data_key_and_unauthenticated_requests() {
    let (_dir, state) = app();
    let admin_routes: &[(&str, &str)] = &[
        ("GET", "/api/admin/health/storage"),
        ("GET", "/api/admin/health/ingest"),
        ("GET", "/api/admin/health/maintenance"),
        ("GET", "/api/admin/health/queries"),
        ("POST", "/api/admin/maintenance/pause"),
        ("POST", "/api/admin/maintenance/resume"),
        ("POST", "/api/admin/maintenance/flush"),
        ("POST", "/api/admin/maintenance/retention/dry-run"),
        ("POST", "/api/admin/maintenance/retention/run"),
    ];

    let data_bearer = HashMap::from([(
        "authorization".to_string(),
        format!("Bearer {}", state.config.api_key),
    )]);
    let data_xapi = HashMap::from([("x-api-key".to_string(), state.config.api_key.clone())]);
    let admin_bearer = HashMap::from([(
        "authorization".to_string(),
        format!("Bearer {}", state.config.admin_api_key),
    )]);

    for (method, path) in admin_routes {
        // 1) No auth header at all -> 401.
        let unauth = http::route(method, path, &HashMap::new(), &HashMap::new(), &[], &state);
        assert_eq!(
            unauth.status(),
            401,
            "{method} {path}: missing auth should 401, got {}",
            unauth.json_body()
        );

        // 2) Data key as bearer must not authorize an admin endpoint.
        let bearer = http::route(method, path, &HashMap::new(), &data_bearer, &[], &state);
        assert_eq!(
            bearer.status(),
            403,
            "{method} {path}: data-key bearer should 403, got {}",
            bearer.json_body()
        );

        // 3) Data key via x-api-key must not authorize an admin endpoint.
        let xapi = http::route(method, path, &HashMap::new(), &data_xapi, &[], &state);
        assert_eq!(
            xapi.status(),
            403,
            "{method} {path}: data-key x-api-key should 403, got {}",
            xapi.json_body()
        );

        // 4) Admin key authorizes (status may be 200 success or 5xx from the
        //    operation itself; we only care that auth doesn't reject it).
        let admin = http::route(method, path, &HashMap::new(), &admin_bearer, &[], &state);
        assert!(
            admin.status() != 401 && admin.status() != 403,
            "{method} {path}: admin-key should pass auth, got {}: {}",
            admin.status(),
            admin.json_body()
        );
    }
}

#[test]
fn empty_configured_api_key_fails_closed() {
    let (_dir, mut state) = {
        let (dir, state) = app();
        (dir, state)
    };
    // Simulate the misconfiguration: a deployment that wiped api_key without
    // realizing it. The runtime must refuse to authorize *any* request, even
    // one whose Authorization header strips down to an empty bearer token.
    state.config.api_key = String::new();
    let unauth_headers = HashMap::from([
        ("authorization".to_string(), "Bearer ".to_string()),
        ("content-type".to_string(), "application/json".to_string()),
    ]);
    let body = log_fixture(Utc::now().timestamp_nanos_opt().unwrap()).to_string();
    let response = http::route(
        "POST",
        "/v1/logs",
        &HashMap::new(),
        &unauth_headers,
        body.as_bytes(),
        &state,
    );
    assert_eq!(response.status(), 403, "{}", response.json_body());
}

#[test]
fn config_validate_rejects_empty_keys_and_collisions() {
    let dir = tempdir().unwrap();
    let mut config = Config::test(dir.path().join("canardstack.duckdb"));

    config.api_key = String::new();
    assert!(config.validate().is_err(), "empty api_key must fail");

    config.api_key = "data-key".to_string();
    config.admin_api_key = String::new();
    assert!(config.validate().is_err(), "empty admin_api_key must fail");

    config.admin_api_key = "data-key".to_string();
    assert!(
        config.validate().is_err(),
        "api_key == admin_api_key must fail to keep the admin gate meaningful"
    );

    config.admin_api_key = "admin-key".to_string();
    assert!(config.validate().is_ok());

    config.query_interactive.concurrency = 0;
    assert!(
        config.validate().is_err(),
        "zero query concurrency must fail"
    );
}
