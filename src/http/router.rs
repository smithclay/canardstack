use crate::metrics::{MetricName, Metrics};
use crate::validation::{self, ApiError};
use crate::AppState;
use serde_json::{json, Value};
use std::collections::HashMap;

use super::compat_routes::{match_compat_route, route_compat};
use super::response::HttpResponse;

pub fn route(
    method: &str,
    path: &str,
    query: &HashMap<String, String>,
    headers: &HashMap<String, String>,
    body: &[u8],
    state: &AppState,
) -> HttpResponse {
    route_inner(
        method,
        path,
        query,
        headers,
        RequestBody::Borrowed(body),
        state,
    )
}

pub fn route_owned(
    method: &str,
    path: &str,
    query: &HashMap<String, String>,
    headers: &HashMap<String, String>,
    body: Vec<u8>,
    state: &AppState,
) -> HttpResponse {
    route_inner(
        method,
        path,
        query,
        headers,
        RequestBody::Owned(body),
        state,
    )
}

enum RequestBody<'a> {
    Borrowed(&'a [u8]),
    Owned(Vec<u8>),
}

impl RequestBody<'_> {
    fn as_slice(&self) -> &[u8] {
        match self {
            Self::Borrowed(body) => body,
            Self::Owned(body) => body,
        }
    }
}

fn route_inner(
    method: &str,
    path: &str,
    query: &HashMap<String, String>,
    headers: &HashMap<String, String>,
    body: RequestBody<'_>,
    state: &AppState,
) -> HttpResponse {
    if let Some(matched) = match_compat_route(method, path) {
        return route_compat(matched, query, headers, body.as_slice(), state);
    }

    let result = match (method, path) {
        ("GET", "/healthz") => {
            let probe = state.storage.probe();
            let ok = probe.is_ready();
            let body = json!({
                "status": if ok { "ok" } else { "error" },
                "storage": probe
            });
            return HttpResponse::json(if ok { 200 } else { 503 }, body);
        }
        ("GET", "/metrics") => {
            state.admission.record_metrics(&state.metrics);
            record_storage_operator_gauges(state);
            return HttpResponse::text(
                200,
                "text/plain; charset=utf-8",
                state.metrics.render_prometheus(),
            );
        }
        ("GET", "/api/admin/health/storage") => {
            return admin_health_response(headers, state, || {
                let health = state.storage.health();
                (health.is_ready(), json!(health))
            });
        }
        ("GET", "/api/admin/health/queries") => {
            return admin_health_response(headers, state, || {
                // Queries are only as healthy as DuckDB; 200 here while
                // storage is wedged would mislead the runbook step.
                let mut health = state.queries.health();
                health["admission"] = json!(state.admission.snapshot());
                (state.storage.probe().is_ready(), health)
            });
        }
        _ => Err(ApiError::new(404, "not_found", "route not found")),
    };

    match result {
        Ok(value) => HttpResponse::json(200, value),
        Err(err) => HttpResponse::from_api_error(&err),
    }
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

fn storage_signal_gauge(metrics: &Metrics, name: MetricName, storage_signal: &str, value: f64) {
    metrics.gauge(name, &[("storage_signal", storage_signal)], value);
}

pub(crate) fn record_storage_operator_gauges(state: &AppState) {
    let storage = state.storage.health();
    storage_signal_gauge(
        &state.metrics,
        MetricName::StoragePhysicalBytes,
        "all",
        storage.physical_bytes as f64,
    );
    if let Some(rows) = storage.logical_rows.as_object() {
        for (table, value) in rows {
            if let Some(count) = value.as_i64() {
                storage_signal_gauge(
                    &state.metrics,
                    MetricName::StorageLogicalRows,
                    table.as_str(),
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
                (MetricName::DucklakeActiveDataFiles, "active_data_files"),
                (
                    MetricName::DucklakeActiveDataFileRows,
                    "active_data_file_rows",
                ),
            ] {
                if let Some(count) = value.get(field).and_then(Value::as_i64) {
                    storage_signal_gauge(&state.metrics, metric, table.as_str(), count as f64);
                }
            }
        }
    }
    if let Some(watermarks) = storage.freshness_watermarks.as_object() {
        for (table, value) in watermarks {
            if let Some(epoch) = value.get("epoch_seconds").and_then(Value::as_f64) {
                storage_signal_gauge(
                    &state.metrics,
                    MetricName::FreshnessWatermarkTimestamp,
                    table.as_str(),
                    epoch,
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use tempfile::tempdir;

    #[test]
    fn ingest_route_is_not_served() {
        let dir = tempdir().unwrap();
        let config = Config::test(dir.path().join("canardstack.duckdb"));
        let state = AppState::new(config).unwrap();

        let response = route(
            "POST",
            "/v1/logs",
            &HashMap::new(),
            &HashMap::new(),
            &[],
            &state,
        );
        assert_eq!(response.status(), 404, "{}", response.json_body());
    }
}
