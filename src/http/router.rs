use crate::ingest::{OtlpRequestKind, StorageSignal};
use crate::validation::{self, ApiError};
use crate::AppState;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::time::Instant;

use super::auth::admin;
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

    fn into_vec(self) -> Vec<u8> {
        match self {
            Self::Borrowed(body) => body.to_vec(),
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
        if matched.role_category().serves_queries() && !state.config.serve_role.serves_queries() {
            return HttpResponse::from_api_error(&ApiError::new(
                404,
                "not_found",
                "query routes are disabled for this serve role",
            ));
        }
        return route_compat(matched, query, headers, body.as_slice(), state);
    }

    let result = match (method, path) {
        ("GET", "/healthz") => {
            let probe = state.storage.probe();
            // A wedged raw-spool writer 503s ingest for its signal forever, so
            // the node must report NOT ready even when storage is fine.
            let unhealthy_spools: Vec<Value> = state
                .ingestor
                .raw_spool_health_by_lane()
                .into_iter()
                .filter(|(_, (healthy, _))| !healthy)
                .map(|(lane, (_, error))| json!({"spool_lane": lane, "error": error}))
                .collect();
            let raw_spool_healthy = unhealthy_spools.is_empty();
            let ok = probe.is_ready() && raw_spool_healthy;
            let mut body = json!({
                "status": if ok { "ok" } else { "error" },
                "storage": probe,
                "raw_spool_healthy": raw_spool_healthy,
            });
            if !raw_spool_healthy {
                body["raw_spool_unhealthy"] = json!(unhealthy_spools);
            }
            return HttpResponse::json(if ok { 200 } else { 503 }, body);
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
            if !state.config.serve_role.accepts_ingest() {
                return HttpResponse::from_api_error(&ApiError::new(
                    404,
                    "not_found",
                    "ingest routes are disabled for this serve role",
                ));
            }
            return ingest_response(ingest(
                OtlpRequestKind::Logs,
                headers,
                body.into_vec(),
                state,
            ));
        }
        ("POST", "/v1/traces") => {
            if !state.config.serve_role.accepts_ingest() {
                return HttpResponse::from_api_error(&ApiError::new(
                    404,
                    "not_found",
                    "ingest routes are disabled for this serve role",
                ));
            }
            return ingest_response(ingest(
                OtlpRequestKind::Traces,
                headers,
                body.into_vec(),
                state,
            ));
        }
        ("POST", "/v1/metrics") => {
            if !state.config.serve_role.accepts_ingest() {
                return HttpResponse::from_api_error(&ApiError::new(
                    404,
                    "not_found",
                    "ingest routes are disabled for this serve role",
                ));
            }
            return ingest_response(ingest(
                OtlpRequestKind::Metrics,
                headers,
                body.into_vec(),
                state,
            ));
        }
        ("GET", "/api/admin/health/storage") => {
            return admin_health_response(headers, state, || {
                let health = state.storage.health();
                (health.is_ready(), json!(health))
            });
        }
        ("GET", "/api/admin/health/ingest") => {
            return admin_health_response(headers, state, || {
                let raw_spool_healthy = state.ingestor.raw_spool_healthy();
                let raw_spool = state
                    .ingestor
                    .raw_spool_stats()
                    .map(|stats| json!(stats))
                    .unwrap_or_else(|err| json!({"error": err.to_string()}));
                let raw_spool_by_lane = state
                    .ingestor
                    .raw_spool_stats_by_lane()
                    .map(|stats| json!(stats))
                    .unwrap_or_else(|err| json!({"error": err.to_string()}));
                let body = json!({
                    "raw_spool_healthy": raw_spool_healthy,
                    "queues": state.ingestor.snapshots(),
                    "admission": state.admission.snapshot_for(state.ingestor.freshness_budget_inputs(&state.storage)),
                    "raw_spool": raw_spool,
                    "raw_spool_by_lane": raw_spool_by_lane,
                    "raw_spool_config": {
                        "writer_queue_capacity": state.config.raw_spool_writer_queue_capacity,
                        "group_commit_records": state.config.raw_spool_group_commit_records,
                        "group_commit_ms": state.config.raw_spool_group_commit_delay.as_millis(),
                        "append_sync_ms": state.config.raw_spool_append_sync_interval.as_millis(),
                        "append_sync_bytes": state.config.raw_spool_append_sync_bytes,
                        "checkpoint_fsync_records": state.config.raw_spool_checkpoint_fsync_records,
                        "checkpoint_fsync_ms": state.config.raw_spool_checkpoint_fsync_delay.as_millis()
                    }
                });
                (raw_spool_healthy, body)
            });
        }
        ("GET", "/api/admin/health/maintenance") => {
            return admin_health_response(headers, state, || {
                (state.maintenance.is_ready(), state.maintenance.health())
            });
        }
        ("GET", "/api/admin/health/queries") => {
            return admin_health_response(headers, state, || {
                // Queries are only as healthy as DuckDB; 200 here while
                // storage is wedged would mislead the runbook step.
                let mut health = state.queries.health();
                health["admission"] = json!(state
                    .admission
                    .snapshot_for(state.ingestor.freshness_budget_inputs(&state.storage)));
                (state.storage.probe().is_ready(), health)
            });
        }
        ("POST", "/api/admin/maintenance/pause") => admin(headers, state, || {
            ensure_maintenance_allowed(state)?;
            state.maintenance.pause();
            Ok(json!({"paused": true}))
        }),
        ("POST", "/api/admin/maintenance/resume") => admin(headers, state, || {
            ensure_maintenance_allowed(state)?;
            state.maintenance.resume();
            Ok(json!({"paused": false}))
        }),
        ("POST", "/api/admin/maintenance/seal") => admin(headers, state, || {
            ensure_maintenance_allowed(state)?;
            let started = Instant::now();
            let result = run_seal_with_admission(state);
            record_maintenance_metrics(state, "seal", &result, started);
            result
        }),
        ("POST", "/api/admin/maintenance/retention/dry-run") => admin(headers, state, || {
            ensure_maintenance_allowed(state)?;
            run_maintenance_job(state, "retention", || {
                state.maintenance.retention(&state.storage, true)
            })
        }),
        ("POST", "/api/admin/maintenance/retention/run") => admin(headers, state, || {
            ensure_maintenance_allowed(state)?;
            run_maintenance_job(state, "retention", || {
                state.maintenance.retention(&state.storage, false)
            })
        }),
        _ => Err(ApiError::new(404, "not_found", "route not found")),
    };

    match result {
        Ok(value) => HttpResponse::json(200, value),
        Err(err) => HttpResponse::from_api_error(&err),
    }
}

fn ingest(
    route: OtlpRequestKind,
    headers: &HashMap<String, String>,
    body: Vec<u8>,
    state: &AppState,
) -> Result<Value, ApiError> {
    validation::validate_api_key(headers, &state.config, false)?;
    state.ingestor.ingest(
        route,
        headers,
        body,
        &state.storage,
        &state.admission,
        state.metrics.clone(),
    )
}

fn ingest_response(result: Result<Value, ApiError>) -> HttpResponse {
    // 202 matches the local-spool write acknowledgement the body advertises
    // and the metric label canardstack_ingest_requests_total{status="202"}.
    match result {
        Ok(value) => HttpResponse::json(202, value),
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

fn storage_error(err: anyhow::Error) -> ApiError {
    ApiError::new(503, "storage_operation_failed", err.to_string())
}

fn run_maintenance_job(
    state: &AppState,
    job: &str,
    run: impl FnOnce() -> anyhow::Result<Value>,
) -> Result<Value, ApiError> {
    let started = Instant::now();
    let result = run().map_err(storage_error);
    record_maintenance_metrics(state, job, &result, started);
    result
}

fn run_seal_with_admission(state: &AppState) -> Result<Value, ApiError> {
    let guard = state.admission.reserve_seal(&state.metrics)?;
    let result = state
        .maintenance
        .run_seal(&state.ingestor, &state.storage, &state.metrics)
        .map_err(storage_error);
    guard.finish(&state.metrics);
    result
}

fn ensure_maintenance_allowed(state: &AppState) -> Result<(), ApiError> {
    if state.config.serve_role.allows_maintenance_mutation() {
        Ok(())
    } else {
        Err(ApiError::new(
            404,
            "not_found",
            "maintenance mutations are disabled for this serve role",
        ))
    }
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
    state.ingestor.record_inflight_metrics(&state.metrics);
    state.ingestor.record_raw_spool_metrics(&state.metrics);
    let arrow_write_buffers = state
        .storage
        .arrow_write_buffer_metrics()
        .into_iter()
        .map(|buffer| (buffer.table, buffer))
        .collect::<HashMap<_, _>>();
    for table in [
        StorageSignal::Logs,
        StorageSignal::Spans,
        StorageSignal::MetricGauge,
        StorageSignal::MetricSum,
    ] {
        let rows = arrow_write_buffers
            .get(&table)
            .map(|buffer| buffer.rows)
            .unwrap_or(0);
        let bytes = arrow_write_buffers
            .get(&table)
            .map(|buffer| buffer.bytes)
            .unwrap_or(0);
        let age_seconds = arrow_write_buffers
            .get(&table)
            .map(|buffer| buffer.age_seconds)
            .unwrap_or(0.0);
        state.metrics.gauge(
            "canardstack_arrow_write_buffer_rows",
            &[("table", table.as_str())],
            rows as f64,
        );
        state.metrics.gauge(
            "canardstack_arrow_write_buffer_bytes",
            &[("table", table.as_str())],
            bytes as f64,
        );
        state.metrics.gauge(
            "canardstack_arrow_write_buffer_age_seconds",
            &[("table", table.as_str())],
            age_seconds,
        );
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

pub(crate) fn record_storage_operator_gauges(state: &AppState) {
    let storage = state.storage.health();
    state.metrics.gauge(
        "canardstack_storage_physical_bytes",
        &[("table", "all")],
        storage.physical_bytes as f64,
    );
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
                (
                    "canardstack_ducklake_active_data_files",
                    "active_data_files",
                ),
                (
                    "canardstack_ducklake_active_data_file_rows",
                    "active_data_file_rows",
                ),
            ] {
                if let Some(count) = value.get(field).and_then(Value::as_i64) {
                    state
                        .metrics
                        .gauge(metric, &[("table", table.as_str())], count as f64);
                }
            }
        }
    }
    let mut max_freshness_lag = 0.0f64;
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
                max_freshness_lag = max_freshness_lag.max(lag.max(0.0));
                state.metrics.gauge(
                    "canardstack_ingest_to_query_lag_seconds",
                    &[("table", table.as_str())],
                    lag.max(0.0),
                );
            }
        }
    }
    state
        .admission
        .record_observed_freshness_lag(max_freshness_lag, &state.metrics);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{Config, ServeRole};
    use tempfile::tempdir;

    #[test]
    fn ingest_role_disables_every_registered_compat_route() {
        let dir = tempdir().unwrap();
        let mut config = Config::test(dir.path().join("canardstack.duckdb"));
        config.serve_role = ServeRole::Ingest;
        let state = AppState::new(config).unwrap();

        for (method, path) in super::super::compat_routes::compat_route_examples_for_tests() {
            let response = route(
                &method,
                &path,
                &HashMap::new(),
                &HashMap::new(),
                &[],
                &state,
            );
            assert_eq!(
                response.status(),
                404,
                "{method} {path}: {}",
                response.json_body()
            );
        }
    }
}
