use crate::ingest::Signal;
use crate::validation::{self, ApiError};
use crate::AppState;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::time::Instant;

use super::auth::admin;
use super::compat_routes::route_compat;
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
    if let Some(response) = route_compat(method, path, query, headers, body.as_slice(), state) {
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
            return ingest_response(ingest(Signal::Logs, headers, body.into_vec(), state));
        }
        ("POST", "/v1/traces") => {
            return ingest_response(ingest(Signal::Spans, headers, body.into_vec(), state));
        }
        ("POST", "/v1/metrics") => {
            return ingest_response(ingest(Signal::MetricGauge, headers, body.into_vec(), state));
        }
        ("GET", "/api/admin/health/storage") => {
            return admin_health_response(headers, state, || {
                let health = state.storage.health();
                (health.is_ready(), json!(health))
            });
        }
        ("GET", "/api/admin/health/ingest") => admin(headers, state, || {
            let raw_spool = state
                .ingestor
                .raw_spool_stats()
                .map(|stats| json!(stats))
                .unwrap_or_else(|err| json!({"error": err.to_string()}));
            let raw_spool_by_signal = state
                .ingestor
                .raw_spool_stats_by_signal()
                .map(|stats| json!(stats))
                .unwrap_or_else(|err| json!({"error": err.to_string()}));
            Ok(json!({
                "queues": state.ingestor.snapshots(),
                "raw_spool": raw_spool,
                "raw_spool_by_signal": raw_spool_by_signal,
                "raw_spool_config": {
                    "writer_queue_capacity": state.config.raw_spool_writer_queue_capacity,
                    "group_commit_records": state.config.raw_spool_group_commit_records,
                    "group_commit_ms": state.config.raw_spool_group_commit_delay.as_millis(),
                    "append_sync_ms": state.config.raw_spool_append_sync_interval.as_millis(),
                    "append_sync_bytes": state.config.raw_spool_append_sync_bytes,
                    "checkpoint_fsync_records": state.config.raw_spool_checkpoint_fsync_records,
                    "checkpoint_fsync_ms": state.config.raw_spool_checkpoint_fsync_delay.as_millis()
                }
            }))
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
        ("POST", "/api/admin/maintenance/retention/dry-run") => admin(headers, state, || {
            run_maintenance_job(state, "retention", || {
                state.maintenance.retention(&state.storage, true)
            })
        }),
        ("POST", "/api/admin/maintenance/retention/run") => admin(headers, state, || {
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
    signal: Signal,
    headers: &HashMap<String, String>,
    body: Vec<u8>,
    state: &AppState,
) -> Result<Value, ApiError> {
    validation::validate_api_key(headers, &state.config, false)?;
    state
        .ingestor
        .ingest(signal, headers, body, &state.storage, &state.metrics)
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
    state.ingestor.record_raw_spool_metrics(&state.metrics);
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
