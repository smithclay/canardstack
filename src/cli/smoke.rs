use crate::http;
use crate::{AppState, Config};
use chrono::{Duration, Utc};
use serde_json::{json, Value};
use std::collections::HashMap;

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
    state.ingestor.flush_all(&state.storage)?;
    state.storage.flush_immutable_segments(true)?;

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
                    "timeUnixNano": now_nanos.to_string(),
                    "observedTimeUnixNano": now_nanos.to_string(),
                    "severityNumber": 17,
                    "severityText": "ERROR",
                    "traceId": "11111111111111111111111111111111",
                    "spanId": "2222222222222222",
                    "body": {"stringValue": "smoke payment timeout"},
                    "attributes": [
                        {"key": "http.route", "value": {"stringValue": "/smoke"}},
                        {"key": "exception.type", "value": {"stringValue": "SmokeTimeout"}}
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
                    "startTimeUnixNano": now_nanos.to_string(),
                    "endTimeUnixNano": (now_nanos + 25_000_000).to_string(),
                    "status": {"code": 2, "message": "smoke timeout"},
                    "attributes": [
                        {"key": "http.request.method", "value": {"stringValue": "GET"}},
                        {"key": "http.response.status_code", "value": {"intValue": "500"}},
                        {"key": "http.route", "value": {"stringValue": "/smoke"}},
                        {"key": "exception.type", "value": {"stringValue": "SmokeTimeout"}}
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
                    "description": "smoke gauge",
                    "unit": "1",
                    "gauge": {"dataPoints": [{
                        "timeUnixNano": now_nanos.to_string(),
                        "asDouble": 42.0,
                        "attributes": [{"key": "route", "value": {"stringValue": "/smoke"}}]
                    }]}
                }, {
                    "name": "smoke.sum",
                    "sum": {"aggregationTemporality": 2, "isMonotonic": true, "dataPoints": [{
                        "timeUnixNano": now_nanos.to_string(),
                        "asInt": "7"
                    }]}
                }]
            }]
        }]
    })
}
