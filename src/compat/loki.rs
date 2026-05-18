use super::params::{
    optional_range, optional_time, parse_any_time_to_utc, parse_usize, required_param, result_rows,
    validate_range,
};
use crate::query::log::parse_loki_query;
use crate::query::plan::TimeBounds;
use crate::validation::ApiResult;
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
    let plan = parse_loki_query(query, time_bounds, limit, direction)?;
    let result = state.queries.execute_logs(&state.storage, &plan)?;
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
            .unwrap_or(end)
            .timestamp_nanos_opt()
            .unwrap_or_else(|| end.timestamp_micros() * 1000);
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

fn loki_success(data: Value) -> Value {
    json!({"status": "success", "data": data})
}
