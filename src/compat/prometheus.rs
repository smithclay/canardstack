use super::params::{
    optional_range, optional_time, parse_any_time_to_utc, parse_step, required_param,
    required_time, result_rows, validate_range,
};
use crate::query::plan::{
    FieldMatcher, MetricAggregation, MetricPlan, MetricSignal, SelectorPlan, SortDirection,
    TimeBounds,
};
use crate::query::prometheus::parse_prom_query;
use crate::validation::ApiResult;
use crate::AppState;
use chrono::Utc;
use serde_json::{json, Map, Value};
use std::collections::{BTreeMap, BTreeSet, HashMap};

const METRIC_RANGE_SECS: i64 = 30 * 24 * 60 * 60;

pub fn prometheus_query(state: &AppState, params: &HashMap<String, String>) -> ApiResult<Value> {
    let query = required_param(params, "query")?;
    let at = optional_time(params, "time")?.unwrap_or_else(Utc::now);
    if let Some(value) = prometheus_probe_scalar(query) {
        return Ok(prom_success(json!({
            "resultType": "vector",
            "result": [{
                "metric": {},
                "value": [at.timestamp(), prom_value(value)]
            }]
        })));
    }
    let from = at - chrono::Duration::minutes(5);
    let prom = parse_prom_query(query)?;
    let rows = execute_metric_rows_with_sum_fallback(
        state,
        MetricPlanInput {
            metric_name: &prom.metric_name,
            signal: prom.signal,
            aggregation: prom.aggregation,
            filters: prom.filters.clone(),
            group_by: prom.group_by.clone(),
            time_bounds: TimeBounds {
                from,
                to: at + chrono::Duration::seconds(1),
            },
            step_seconds: 300,
            limit: 1,
            order: SortDirection::Backward,
        },
    )?;
    if rows.is_empty() {
        return Ok(prom_success(json!({"resultType": "vector", "result": []})));
    }
    let value = rows
        .last()
        .and_then(|row| row.get("value").and_then(Value::as_f64))
        .unwrap_or(0.0);
    Ok(prom_success(json!({
        "resultType": "vector",
        "result": [{
            "metric": prom_metric_labels(&prom.metric_name, rows.last()),
            "value": [at.timestamp(), prom_value(value)]
        }]
    })))
}

pub fn prometheus_query_range(
    state: &AppState,
    params: &HashMap<String, String>,
) -> ApiResult<Value> {
    let query = required_param(params, "query")?;
    let start = required_time(params, "start")?;
    let end = required_time(params, "end")?;
    validate_range(start, end, METRIC_RANGE_SECS)?;
    let step = parse_step(params.get("step").map(String::as_str).unwrap_or("60"))?;
    let prom = parse_prom_query(query)?;
    let group_by = if prom.explicit_grouping {
        prom.group_by.clone()
    } else {
        vec!["service_name".to_string()]
    };
    let rows = execute_metric_rows_with_sum_fallback(
        state,
        MetricPlanInput {
            metric_name: &prom.metric_name,
            signal: prom.signal,
            aggregation: prom.aggregation,
            filters: prom.filters.clone(),
            group_by: group_by.clone(),
            time_bounds: TimeBounds {
                from: start,
                to: end,
            },
            step_seconds: step,
            limit: 5000,
            order: SortDirection::Forward,
        },
    )?;
    let mut series: BTreeMap<String, (Map<String, Value>, Vec<Value>)> = BTreeMap::new();
    for row in rows {
        let mut labels = Map::new();
        labels.insert("__name__".to_string(), json!(prom.metric_name));
        for label in &group_by {
            if let Some(value) = row
                .get(label.as_str())
                .and_then(Value::as_str)
                .filter(|v| !v.is_empty())
            {
                labels.insert(label.to_string(), json!(value));
            }
        }
        let key = serde_json::to_string(&labels).unwrap_or_default();
        series
            .entry(key)
            .or_insert_with(|| (labels, Vec::new()))
            .1
            .push(row.clone());
    }
    let result = series
        .into_iter()
        .map(|(_, (labels, rows))| {
            json!({
                "metric": labels,
                "values": rows.into_iter().filter_map(|row| {
                    let ts = row.get("timestamp").and_then(Value::as_str).and_then(parse_any_time_to_utc)?;
                    let value = row.get("value").and_then(Value::as_f64).unwrap_or(0.0);
                    Some(json!([ts.timestamp(), prom_value(value)]))
                }).collect::<Vec<_>>()
            })
        })
        .collect::<Vec<_>>();
    Ok(prom_success(
        json!({"resultType": "matrix", "result": result}),
    ))
}

pub fn prometheus_labels(_state: &AppState, params: &HashMap<String, String>) -> ApiResult<Value> {
    let _ = optional_range(params, METRIC_RANGE_SECS)?;
    let labels = BTreeSet::from([
        "__name__".to_string(),
        "service_name".to_string(),
        "deployment_environment".to_string(),
    ]);
    Ok(prom_success(json!(labels.into_iter().collect::<Vec<_>>())))
}

pub fn prometheus_label_values(
    state: &AppState,
    name: &str,
    params: &HashMap<String, String>,
) -> ApiResult<Value> {
    let (from, to) = optional_range(params, METRIC_RANGE_SECS)?;
    let values =
        state
            .metadata
            .prometheus_label_values(&state.queries, &state.storage, name, from, to)?;
    Ok(prom_success(json!(values)))
}

pub fn prometheus_series(state: &AppState, params: &HashMap<String, String>) -> ApiResult<Value> {
    let (from, to) = optional_range(params, METRIC_RANGE_SECS)?;
    let out = state
        .metadata
        .prometheus_series(&state.queries, &state.storage, from, to)?;
    Ok(prom_success(out))
}

pub fn prometheus_metadata(state: &AppState) -> ApiResult<Value> {
    let metadata = state
        .metadata
        .prometheus_metric_metadata(&state.queries, &state.storage)?;
    Ok(prom_success(metadata))
}

fn prom_metric_labels(metric_name: &str, row: Option<&Value>) -> Value {
    let mut labels = Map::new();
    labels.insert("__name__".to_string(), json!(metric_name));
    if let Some(service) = row
        .and_then(|r| r.get("service_name"))
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
    {
        labels.insert("service_name".to_string(), json!(service));
    }
    Value::Object(labels)
}

#[derive(Clone)]
struct MetricPlanInput<'a> {
    metric_name: &'a str,
    signal: &'a str,
    aggregation: &'a str,
    filters: BTreeMap<String, String>,
    group_by: Vec<String>,
    time_bounds: TimeBounds,
    step_seconds: i64,
    limit: usize,
    order: SortDirection,
}

fn execute_metric_rows_with_sum_fallback(
    state: &AppState,
    input: MetricPlanInput<'_>,
) -> ApiResult<Vec<Value>> {
    let signal = MetricSignal::parse(input.signal)?;
    let aggregation = MetricAggregation::parse(input.aggregation)?;
    let result = state
        .queries
        .execute_metric(&state.storage, &metric_plan(input.clone())?)?;
    let rows = result_rows(&result);
    if !rows.is_empty() || signal != MetricSignal::Gauge || aggregation == MetricAggregation::Rate {
        return Ok(rows);
    }

    let fallback = MetricPlanInput {
        signal: "sum",
        ..input
    };
    let result = state
        .queries
        .execute_metric(&state.storage, &metric_plan(fallback)?)?;
    let fallback_rows = result_rows(&result);
    if fallback_rows.is_empty() {
        Ok(rows)
    } else {
        Ok(fallback_rows)
    }
}

fn metric_plan(input: MetricPlanInput<'_>) -> ApiResult<MetricPlan> {
    Ok(MetricPlan {
        selector: SelectorPlan {
            resource: Some(input.metric_name.to_string()),
            matchers: input
                .filters
                .into_iter()
                .map(|(field, value)| FieldMatcher { field, value })
                .collect(),
            text_filters: Vec::new(),
        },
        time_bounds: input.time_bounds,
        signal: MetricSignal::parse(input.signal)?,
        aggregation: MetricAggregation::parse(input.aggregation)?,
        group_by: input.group_by,
        step_seconds: input.step_seconds,
        limit: input.limit,
        order: input.order,
    })
}

fn prom_value(value: f64) -> String {
    if value.is_finite() {
        value.to_string()
    } else {
        "NaN".to_string()
    }
}

fn prometheus_probe_scalar(raw: &str) -> Option<f64> {
    match raw.split_whitespace().collect::<String>().as_str() {
        "1" => Some(1.0),
        "1+1" => Some(2.0),
        _ => None,
    }
}

fn prom_success(data: Value) -> Value {
    json!({"status": "success", "data": data})
}
