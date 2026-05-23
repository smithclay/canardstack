use crate::http;
use crate::{AppState, Config};
use chrono::{Duration, Utc};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::thread;
use std::time::{Duration as StdDuration, Instant};

pub fn run() -> anyhow::Result<()> {
    let state = AppState::new(Config::from_env()?)?;
    let now = Utc::now();
    let from = (now - Duration::minutes(10)).to_rfc3339();
    let to = (now + Duration::minutes(1)).to_rfc3339();

    let mut headers = HashMap::new();
    headers.insert(
        "authorization".to_string(),
        format!("Bearer {}", state.config.api_key),
    );
    headers.insert("content-type".to_string(), "application/json".to_string());

    let now_nanos = now
        .timestamp_nanos_opt()
        .unwrap_or(now.timestamp_millis() * 1_000_000);
    let logs = log_fixture(now_nanos);
    let traces = trace_fixture(now_nanos);
    let metrics = metric_fixture(now_nanos);

    let ingest_logs = http::route(
        "POST",
        "/v1/logs",
        &HashMap::new(),
        &headers,
        logs.to_string().as_bytes(),
        &state,
    );
    let ingest_traces = http::route(
        "POST",
        "/v1/traces",
        &HashMap::new(),
        &headers,
        traces.to_string().as_bytes(),
        &state,
    );
    let ingest_metrics = http::route(
        "POST",
        "/v1/metrics",
        &HashMap::new(),
        &headers,
        metrics.to_string().as_bytes(),
        &state,
    );
    let deadline = Instant::now() + StdDuration::from_secs(5);
    while Instant::now() < deadline && state.ingestor.inflight_bytes() > 0 {
        thread::sleep(StdDuration::from_millis(20));
    }
    state
        .ingestor
        .seal_committed_to_storage(&state.storage, &state.metrics)?;

    let mut admin_headers = HashMap::new();
    admin_headers.insert(
        "authorization".to_string(),
        format!("Bearer {}", state.config.admin_api_key),
    );
    let health = http::route(
        "GET",
        "/api/admin/health/storage",
        &HashMap::new(),
        &admin_headers,
        &[],
        &state,
    );
    let prom = http::route(
        "GET",
        "/api/v1/query_range",
        &HashMap::from([
            (
                "query".to_string(),
                "avg by (service_name) (smoke.gauge)".to_string(),
            ),
            ("start".to_string(), from.clone()),
            ("end".to_string(), to.clone()),
            ("step".to_string(), "60".to_string()),
        ]),
        &headers,
        &[],
        &state,
    );
    let loki = http::route(
        "GET",
        "/loki/api/v1/query_range",
        &HashMap::from([
            (
                "query".to_string(),
                "{service_name=\"checkout\"} |= \"smoke\"".to_string(),
            ),
            ("start".to_string(), from),
            ("end".to_string(), to),
            ("limit".to_string(), "10".to_string()),
        ]),
        &headers,
        &[],
        &state,
    );
    let trace = http::route(
        "GET",
        "/api/v2/traces/11111111111111111111111111111111",
        &HashMap::new(),
        &headers,
        &[],
        &state,
    );

    println!(
        "{}",
        json!({
            "ingest_logs": ingest_logs.status(),
            "ingest_traces": ingest_traces.status(),
            "ingest_metrics": ingest_metrics.status(),
            "compatibility": {
                "prometheus_query_range": prom.json_body(),
                "loki_query_range": loki.json_body(),
                "tempo_trace": trace.json_body()
            },
            "storage_health": health.json_body()
        })
    );
    Ok(())
}

pub fn log_fixture(now_nanos: i64) -> Value {
    json!({
        "resourceLogs": [{
            "resource": {"attributes": [
                {"key": "service.name", "value": {"stringValue": "checkout"}},
                {"key": "deployment.environment", "value": {"stringValue": "dev"}}
            ]},
            "scopeLogs": [{
                "scope": {"name": "smoke", "version": "1"},
                "logRecords": [{
                    "timeUnixNano": nanos_ago(now_nanos, 900).to_string(),
                    "observedTimeUnixNano": nanos_ago(now_nanos, 900).to_string(),
                    "severityNumber": 9,
                    "severityText": "INFO",
                    "traceId": "33333333333333333333333333333333",
                    "spanId": "4444444444444444",
                    "body": {"stringValue": "smoke checkout warmed catalog cache"},
                    "attributes": [
                        {"key": "http.route", "value": {"stringValue": "/checkout"}},
                        {"key": "http.request.method", "value": {"stringValue": "GET"}},
                        {"key": "http.response.status_code", "value": {"intValue": "200"}}
                    ]
                }, {
                    "timeUnixNano": nanos_ago(now_nanos, 420).to_string(),
                    "observedTimeUnixNano": nanos_ago(now_nanos, 420).to_string(),
                    "severityNumber": 13,
                    "severityText": "WARN",
                    "traceId": "55555555555555555555555555555555",
                    "spanId": "6666666666666666",
                    "body": {"stringValue": "smoke checkout retrying payment authorization"},
                    "attributes": [
                        {"key": "http.route", "value": {"stringValue": "/checkout"}},
                        {"key": "http.request.method", "value": {"stringValue": "POST"}},
                        {"key": "http.response.status_code", "value": {"intValue": "202"}}
                    ]
                }, {
                    "timeUnixNano": now_nanos.to_string(),
                    "observedTimeUnixNano": now_nanos.to_string(),
                    "severityNumber": 17,
                    "severityText": "ERROR",
                    "traceId": "11111111111111111111111111111111",
                    "spanId": "2222222222222222",
                    "body": {"stringValue": "smoke payment timeout"},
                    "attributes": [
                        {"key": "http.route", "value": {"stringValue": "/smoke"}},
                        {"key": "http.request.method", "value": {"stringValue": "GET"}},
                        {"key": "http.response.status_code", "value": {"intValue": "500"}},
                        {"key": "exception.type", "value": {"stringValue": "SmokeTimeout"}}
                    ]
                }]
            }]
        }, {
            "resource": {"attributes": [
                {"key": "service.name", "value": {"stringValue": "payments"}},
                {"key": "deployment.environment", "value": {"stringValue": "dev"}}
            ]},
            "scopeLogs": [{
                "scope": {"name": "smoke", "version": "1"},
                "logRecords": [{
                    "timeUnixNano": nanos_ago(now_nanos, 720).to_string(),
                    "observedTimeUnixNano": nanos_ago(now_nanos, 720).to_string(),
                    "severityNumber": 9,
                    "severityText": "INFO",
                    "traceId": "33333333333333333333333333333333",
                    "spanId": "7777777777777777",
                    "body": {"stringValue": "smoke payment authorized"},
                    "attributes": [
                        {"key": "http.route", "value": {"stringValue": "/charge"}},
                        {"key": "http.request.method", "value": {"stringValue": "POST"}},
                        {"key": "http.response.status_code", "value": {"intValue": "200"}}
                    ]
                }, {
                    "timeUnixNano": nanos_ago(now_nanos, 60).to_string(),
                    "observedTimeUnixNano": nanos_ago(now_nanos, 60).to_string(),
                    "severityNumber": 13,
                    "severityText": "WARN",
                    "traceId": "11111111111111111111111111111111",
                    "spanId": "8888888888888888",
                    "body": {"stringValue": "smoke payment gateway slow response"},
                    "attributes": [
                        {"key": "http.route", "value": {"stringValue": "/charge"}},
                        {"key": "http.request.method", "value": {"stringValue": "POST"}},
                        {"key": "http.response.status_code", "value": {"intValue": "504"}}
                    ]
                }]
            }]
        }, {
            "resource": {"attributes": [
                {"key": "service.name", "value": {"stringValue": "inventory"}},
                {"key": "deployment.environment", "value": {"stringValue": "dev"}}
            ]},
            "scopeLogs": [{
                "scope": {"name": "smoke", "version": "1"},
                "logRecords": [{
                    "timeUnixNano": nanos_ago(now_nanos, 300).to_string(),
                    "observedTimeUnixNano": nanos_ago(now_nanos, 300).to_string(),
                    "severityNumber": 9,
                    "severityText": "INFO",
                    "traceId": "55555555555555555555555555555555",
                    "spanId": "9999999999999999",
                    "body": {"stringValue": "smoke inventory reservation accepted"},
                    "attributes": [
                        {"key": "http.route", "value": {"stringValue": "/reserve"}},
                        {"key": "http.request.method", "value": {"stringValue": "POST"}},
                        {"key": "http.response.status_code", "value": {"intValue": "200"}}
                    ]
                }]
            }]
        }]
    })
}

pub fn trace_fixture(now_nanos: i64) -> Value {
    json!({
        "resourceSpans": [{
            "resource": {"attributes": [
                {"key": "service.name", "value": {"stringValue": "checkout"}},
                {"key": "deployment.environment", "value": {"stringValue": "dev"}}
            ]},
            "scopeSpans": [{
                "scope": {"name": "smoke", "version": "1"},
                "spans": [{
                    "traceId": "11111111111111111111111111111111",
                    "spanId": "2222222222222222",
                    "parentSpanId": "",
                    "name": "GET /smoke",
                    "kind": 2,
                    "startTimeUnixNano": nanos_ago(now_nanos, 2).to_string(),
                    "endTimeUnixNano": (nanos_ago(now_nanos, 2) + 186_000_000).to_string(),
                    "status": {"code": 2, "message": "smoke timeout"},
                    "attributes": [
                        {"key": "http.request.method", "value": {"stringValue": "GET"}},
                        {"key": "http.response.status_code", "value": {"intValue": "500"}},
                        {"key": "http.route", "value": {"stringValue": "/smoke"}},
                        {"key": "exception.type", "value": {"stringValue": "SmokeTimeout"}}
                    ]
                }, {
                    "traceId": "11111111111111111111111111111111",
                    "spanId": "aaaaaaaaaaaaaaaa",
                    "parentSpanId": "2222222222222222",
                    "name": "GET /catalog",
                    "kind": 3,
                    "startTimeUnixNano": (nanos_ago(now_nanos, 2) + 12_000_000).to_string(),
                    "endTimeUnixNano": (nanos_ago(now_nanos, 2) + 38_000_000).to_string(),
                    "status": {"code": 1},
                    "attributes": [
                        {"key": "http.request.method", "value": {"stringValue": "GET"}},
                        {"key": "http.response.status_code", "value": {"intValue": "200"}},
                        {"key": "http.route", "value": {"stringValue": "/catalog"}}
                    ]
                }, {
                    "traceId": "33333333333333333333333333333333",
                    "spanId": "4444444444444444",
                    "parentSpanId": "",
                    "name": "POST /checkout",
                    "kind": 2,
                    "startTimeUnixNano": nanos_ago(now_nanos, 720).to_string(),
                    "endTimeUnixNano": (nanos_ago(now_nanos, 720) + 74_000_000).to_string(),
                    "status": {"code": 1},
                    "attributes": [
                        {"key": "http.request.method", "value": {"stringValue": "POST"}},
                        {"key": "http.response.status_code", "value": {"intValue": "200"}},
                        {"key": "http.route", "value": {"stringValue": "/checkout"}}
                    ]
                }]
            }]
        }, {
            "resource": {"attributes": [
                {"key": "service.name", "value": {"stringValue": "payments"}},
                {"key": "deployment.environment", "value": {"stringValue": "dev"}}
            ]},
            "scopeSpans": [{
                "scope": {"name": "smoke", "version": "1"},
                "spans": [{
                    "traceId": "11111111111111111111111111111111",
                    "spanId": "8888888888888888",
                    "parentSpanId": "2222222222222222",
                    "name": "POST /charge",
                    "kind": 3,
                    "startTimeUnixNano": (nanos_ago(now_nanos, 2) + 55_000_000).to_string(),
                    "endTimeUnixNano": (nanos_ago(now_nanos, 2) + 181_000_000).to_string(),
                    "status": {"code": 2, "message": "gateway timeout"},
                    "attributes": [
                        {"key": "http.request.method", "value": {"stringValue": "POST"}},
                        {"key": "http.response.status_code", "value": {"intValue": "504"}},
                        {"key": "http.route", "value": {"stringValue": "/charge"}},
                        {"key": "exception.type", "value": {"stringValue": "GatewayTimeout"}}
                    ]
                }, {
                    "traceId": "33333333333333333333333333333333",
                    "spanId": "7777777777777777",
                    "parentSpanId": "4444444444444444",
                    "name": "POST /charge",
                    "kind": 3,
                    "startTimeUnixNano": (nanos_ago(now_nanos, 720) + 20_000_000).to_string(),
                    "endTimeUnixNano": (nanos_ago(now_nanos, 720) + 62_000_000).to_string(),
                    "status": {"code": 1},
                    "attributes": [
                        {"key": "http.request.method", "value": {"stringValue": "POST"}},
                        {"key": "http.response.status_code", "value": {"intValue": "200"}},
                        {"key": "http.route", "value": {"stringValue": "/charge"}}
                    ]
                }]
            }]
        }]
    })
}

pub fn metric_fixture(now_nanos: i64) -> Value {
    json!({
        "resourceMetrics": [{
            "resource": {"attributes": [
                {"key": "service.name", "value": {"stringValue": "checkout"}},
                {"key": "deployment.environment", "value": {"stringValue": "dev"}}
            ]},
            "scopeMetrics": [{
                "scope": {"name": "smoke", "version": "1"},
                "metrics": [{
                    "name": "smoke.gauge",
                    "description": "smoke demo request latency",
                    "unit": "ms",
                    "gauge": {"dataPoints": demo_gauge_points(now_nanos, "/checkout", [28.0, 35.0, 42.0, 33.0, 47.0])}
                }, {
                    "name": "smoke.sum",
                    "description": "smoke demo requests",
                    "unit": "1",
                    "sum": {"aggregationTemporality": 2, "isMonotonic": true, "dataPoints": demo_sum_points(now_nanos, "/checkout", [10, 28, 45, 63, 84])}
                }]
            }]
        }, {
            "resource": {"attributes": [
                {"key": "service.name", "value": {"stringValue": "payments"}},
                {"key": "deployment.environment", "value": {"stringValue": "dev"}}
            ]},
            "scopeMetrics": [{
                "scope": {"name": "smoke", "version": "1"},
                "metrics": [{
                    "name": "smoke.gauge",
                    "description": "smoke demo request latency",
                    "unit": "ms",
                    "gauge": {"dataPoints": demo_gauge_points(now_nanos, "/charge", [12.0, 18.0, 24.0, 16.0, 30.0])}
                }, {
                    "name": "smoke.sum",
                    "description": "smoke demo requests",
                    "unit": "1",
                    "sum": {"aggregationTemporality": 2, "isMonotonic": true, "dataPoints": demo_sum_points(now_nanos, "/charge", [4, 10, 18, 28, 39])}
                }]
            }]
        }, {
            "resource": {"attributes": [
                {"key": "service.name", "value": {"stringValue": "inventory"}},
                {"key": "deployment.environment", "value": {"stringValue": "dev"}}
            ]},
            "scopeMetrics": [{
                "scope": {"name": "smoke", "version": "1"},
                "metrics": [{
                    "name": "smoke.gauge",
                    "description": "smoke demo request latency",
                    "unit": "ms",
                    "gauge": {"dataPoints": demo_gauge_points(now_nanos, "/reserve", [8.0, 11.0, 9.0, 14.0, 13.0])}
                }, {
                    "name": "smoke.sum",
                    "description": "smoke demo requests",
                    "unit": "1",
                    "sum": {"aggregationTemporality": 2, "isMonotonic": true, "dataPoints": demo_sum_points(now_nanos, "/reserve", [2, 5, 9, 14, 21])}
                }]
            }]
        }]
    })
}

fn nanos_ago(now_nanos: i64, seconds: i64) -> i64 {
    now_nanos - seconds * 1_000_000_000
}

fn demo_gauge_points(now_nanos: i64, route: &str, values: [f64; 5]) -> Vec<Value> {
    let offsets = [900, 600, 300, 60, 0];
    offsets
        .into_iter()
        .zip(values)
        .map(|(seconds, value)| {
            json!({
                "timeUnixNano": nanos_ago(now_nanos, seconds).to_string(),
                "asDouble": value,
                "attributes": [{"key": "route", "value": {"stringValue": route}}]
            })
        })
        .collect()
}

fn demo_sum_points(now_nanos: i64, route: &str, values: [i64; 5]) -> Vec<Value> {
    let offsets = [900, 600, 300, 60, 0];
    offsets
        .into_iter()
        .zip(values)
        .map(|(seconds, value)| {
            json!({
                "startTimeUnixNano": nanos_ago(now_nanos, 900).to_string(),
                "timeUnixNano": nanos_ago(now_nanos, seconds).to_string(),
                "asInt": value.to_string(),
                "attributes": [{"key": "route", "value": {"stringValue": route}}]
            })
        })
        .collect()
}
