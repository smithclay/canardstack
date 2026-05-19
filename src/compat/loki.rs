use super::params::{
    optional_range, optional_time, parse_any_time_to_utc, parse_usize, required_param, result_rows,
    validate_range,
};
use crate::query::log::parse_loki_query;
use crate::query::plan::{LogPlan, TimeBounds};
use crate::validation::{ApiError, ApiResult};
use crate::AppState;
use chrono::Utc;
use serde_json::{json, Map, Value};
use std::collections::{BTreeMap, BTreeSet, HashMap};

const INTERACTIVE_RANGE_SECS: i64 = 24 * 60 * 60;

pub fn loki_query(state: &AppState, params: &HashMap<String, String>) -> ApiResult<Value> {
    loki_query_inner(state, params, false)
}

pub fn loki_query_range(state: &AppState, params: &HashMap<String, String>) -> ApiResult<Value> {
    loki_query_inner(state, params, true)
}

pub fn loki_query_range_candidates(
    state: &AppState,
    params: &HashMap<String, String>,
) -> ApiResult<Value> {
    let plan = loki_plan(params, true)?;
    let files = state
        .storage
        .ducklake_log_candidate_files(plan.time_bounds.from, plan.time_bounds.to, plan.limit)
        .map_err(|err| ApiError::new(503, "query_storage_unavailable", err.to_string()))?;
    Ok(loki_success(json!({
        "resultType": "ducklake_files",
        "source": "ducklake_metadata",
        "query": required_param(params, "query")?,
        "direction": if plan.direction.is_forward() { "forward" } else { "backward" },
        "time_bounds": {
            "from": plan.time_bounds.from.to_rfc3339(),
            "to": plan.time_bounds.to.to_rfc3339()
        },
        "candidate_file_limit": plan.limit,
        "files": files
    })))
}

pub fn loki_query_range_explain(
    state: &AppState,
    params: &HashMap<String, String>,
) -> ApiResult<Value> {
    let plan = loki_plan(params, true)?;
    if plan.direction.is_forward() {
        return Err(ApiError::new(
            400,
            "unsupported_direction",
            "progressive explain is only implemented for backward Loki query_range",
        ));
    }
    let analyze = params
        .get("analyze")
        .map(|value| matches!(value.as_str(), "1" | "true" | "yes"))
        .unwrap_or(false);
    state
        .queries
        .explain_logs_progressive_window(&state.storage, &plan, analyze)
}

pub fn loki_labels(_state: &AppState, params: &HashMap<String, String>) -> ApiResult<Value> {
    let _ = optional_range(params, INTERACTIVE_RANGE_SECS)?;
    let labels = BTreeSet::from([
        "service_name".to_string(),
        "deployment_environment".to_string(),
        "severity_text".to_string(),
        "trace_id".to_string(),
        "span_id".to_string(),
        "http_route".to_string(),
        "http_method".to_string(),
    ]);
    Ok(loki_success(json!(labels.into_iter().collect::<Vec<_>>())))
}

pub fn loki_label_values(
    state: &AppState,
    name: &str,
    params: &HashMap<String, String>,
) -> ApiResult<Value> {
    let (from, to) = optional_range(params, INTERACTIVE_RANGE_SECS)?;
    let values =
        state
            .metadata
            .loki_label_values(&state.queries, &state.storage, name, from, to)?;
    Ok(loki_success(json!(values)))
}

pub fn loki_series(state: &AppState, params: &HashMap<String, String>) -> ApiResult<Value> {
    let (from, to) = optional_range(params, INTERACTIVE_RANGE_SECS)?;
    let out = state
        .metadata
        .loki_series(&state.queries, &state.storage, from, to)?;
    Ok(loki_success(out))
}
pub(super) fn loki_query_inner(
    state: &AppState,
    params: &HashMap<String, String>,
    range: bool,
) -> ApiResult<Value> {
    let plan = loki_plan(params, range)?;
    let result = execute_loki_log_result(state, &plan, range)?;
    let mut streams: BTreeMap<String, (Map<String, Value>, Vec<Value>)> = BTreeMap::new();
    for row in result_rows(&result) {
        let mut labels = Map::new();
        for label in &plan.stream_labels {
            if let Some(value) = row
                .get(label.as_str())
                .and_then(Value::as_str)
                .filter(|v| !v.is_empty())
            {
                labels.insert(label.to_string(), json!(value));
            }
        }
        let key = serde_json::to_string(&labels).unwrap_or_default();
        let ts = row
            .get("timestamp")
            .and_then(Value::as_str)
            .and_then(parse_any_time_to_utc)
            .unwrap_or(plan.time_bounds.to)
            .timestamp_nanos_opt()
            .unwrap_or_else(|| plan.time_bounds.to.timestamp_micros() * 1000);
        let line = row
            .get("body")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        streams
            .entry(key)
            .or_insert_with(|| (labels, Vec::new()))
            .1
            .push(json!([ts.to_string(), line]));
    }
    let mut result = streams
        .into_values()
        .map(|(stream, values)| json!({"stream": stream, "values": values}))
        .collect::<Vec<_>>();
    if plan.direction.is_forward() {
        result.reverse();
    }
    Ok(loki_success(
        json!({"resultType": "streams", "result": result}),
    ))
}

fn loki_plan(params: &HashMap<String, String>, range: bool) -> ApiResult<LogPlan> {
    let query = required_param(params, "query")?;
    let end = optional_time(params, "end")?.unwrap_or_else(Utc::now);
    let start = optional_time(params, "start")?.unwrap_or(end - chrono::Duration::hours(1));
    validate_range(start, end, INTERACTIVE_RANGE_SECS)?;
    let limit = parse_usize(params.get("limit"), 100, 1000)?;
    let direction = params
        .get("direction")
        .map(String::as_str)
        .unwrap_or("backward");
    let time_bounds = TimeBounds {
        from: start,
        to: if range {
            end
        } else {
            end + chrono::Duration::seconds(1)
        },
    };
    parse_loki_query(query, time_bounds, limit, direction)
}

fn execute_loki_log_result(state: &AppState, plan: &LogPlan, range: bool) -> ApiResult<Value> {
    if range && !plan.direction.is_forward() {
        let (result, report) = state
            .queries
            .execute_logs_progressive_window(&state.storage, plan)?;
        record_loki_progressive_query_report(state, "ok", &report);
        return Ok(result);
    }

    state.queries.execute_logs(&state.storage, plan)
}

fn record_loki_progressive_query_report(
    state: &AppState,
    status: &str,
    report: &crate::query::ProgressiveLogQueryReport,
) {
    state.metrics.inc(
        "canardstack_loki_progressive_query_requests_total",
        &[("status", status)],
        1,
    );
    state.metrics.gauge(
        "canardstack_loki_progressive_query_candidate_files",
        &[],
        report.candidate_files as f64,
    );
    state.metrics.gauge(
        "canardstack_loki_progressive_query_candidate_rows",
        &[],
        report.candidate_rows as f64,
    );
    state.metrics.gauge(
        "canardstack_loki_progressive_query_candidate_bytes",
        &[],
        report.candidate_bytes as f64,
    );
    state.metrics.gauge(
        "canardstack_loki_progressive_query_batch_size",
        &[],
        report.batch_size as f64,
    );
    state.metrics.gauge(
        "canardstack_loki_progressive_query_files_scanned",
        &[],
        report.files_scanned as f64,
    );
    state.metrics.gauge(
        "canardstack_loki_progressive_query_batches_scanned",
        &[],
        report.batches_scanned as f64,
    );
    state.metrics.gauge(
        "canardstack_loki_progressive_query_rows_scanned",
        &[],
        report.rows_scanned as f64,
    );
    state.metrics.gauge(
        "canardstack_loki_progressive_query_bytes_scanned",
        &[],
        report.bytes_scanned as f64,
    );
    state.metrics.gauge(
        "canardstack_loki_progressive_query_result_rows",
        &[],
        report.result_rows as f64,
    );
    state.metrics.gauge(
        "canardstack_loki_progressive_query_truncated",
        &[],
        if report.truncated { 1.0 } else { 0.0 },
    );
    state.metrics.gauge(
        "canardstack_loki_progressive_query_duration_ms",
        &[],
        report.query_duration_ms as f64,
    );
    state.metrics.observe_phase_seconds(
        "logs",
        "loki_progressive_query_candidate_plan",
        Some("/loki/api/v1/query_range"),
        report.candidate_plan_seconds,
    );
    state.metrics.observe_phase_seconds(
        "logs",
        "loki_progressive_query_candidate_execute",
        Some("/loki/api/v1/query_range"),
        report.candidate_execute_seconds,
    );
    state.metrics.observe_phase_seconds(
        "logs",
        "loki_progressive_query_execute",
        Some("/loki/api/v1/query_range"),
        report.query_duration_ms as f64 / 1000.0,
    );
}

fn loki_success(data: Value) -> Value {
    json!({"status": "success", "data": data})
}
