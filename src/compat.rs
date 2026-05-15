use crate::log_query::parse_loki_query;
use crate::promql::parse_prom_query;
use crate::query_plan::TimeBounds;
use crate::sql::{quote as sql_quote, time_predicate};
use crate::trace_query::plan_tempo_search;
use crate::validation::{self, ApiError, ApiResult};
use crate::AppState;
use chrono::{DateTime, TimeZone, Utc};
use opentelemetry_proto::tonic::common::v1::{any_value, AnyValue, InstrumentationScope, KeyValue};
use opentelemetry_proto::tonic::resource::v1::Resource;
use opentelemetry_proto::tonic::trace::v1::{
    span, status, ResourceSpans, ScopeSpans, Span, Status, TracesData,
};
use prost::Message;
use serde_json::{json, Map, Value};
use std::collections::{BTreeMap, BTreeSet, HashMap};

const INTERACTIVE_RANGE_SECS: i64 = 24 * 60 * 60;
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
    let req = json!({
        "from": from.to_rfc3339(),
        "to": (at + chrono::Duration::seconds(1)).to_rfc3339(),
        "metric_name": prom.metric_name,
        "signal": prom.signal,
        "aggregation": prom.aggregation,
        "step_seconds": 300,
        "filters": prom.filters,
        "group_by": prom.group_by.clone(),
        "order": "desc",
        "limit": 1
    });
    let result = state.queries.metric_query(&state.storage, &req)?;
    let rows = result_rows(&result);
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
    let req = json!({
        "from": start.to_rfc3339(),
        "to": end.to_rfc3339(),
        "metric_name": prom.metric_name,
        "signal": prom.signal,
        "aggregation": prom.aggregation,
        "step_seconds": step,
        "filters": prom.filters,
        "group_by": if prom.explicit_grouping { json!(prom.group_by.clone()) } else { json!(["service_name"]) },
        "limit": 5000
    });
    let result = state.queries.metric_query(&state.storage, &req)?;
    let group_by = req
        .get("group_by")
        .and_then(Value::as_array)
        .map(|values| values.iter().filter_map(Value::as_str).collect::<Vec<_>>())
        .unwrap_or_default();
    let mut series: BTreeMap<String, (Map<String, Value>, Vec<Value>)> = BTreeMap::new();
    for row in result_rows(&result) {
        let mut labels = Map::new();
        labels.insert("__name__".to_string(), json!(prom.metric_name));
        for label in &group_by {
            if let Some(value) = row
                .get(*label)
                .and_then(Value::as_str)
                .filter(|v| !v.is_empty())
            {
                labels.insert((*label).to_string(), json!(value));
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
    let values = metric_label_values(state, name, from, to)?;
    Ok(prom_success(json!(values)))
}

pub fn prometheus_series(state: &AppState, params: &HashMap<String, String>) -> ApiResult<Value> {
    let (from, to) = optional_range(params, METRIC_RANGE_SECS)?;
    let mut out = Vec::new();
    state.queries.run_interactive(&state.storage, |conn, prefix| {
        for table in ["metric_gauge", "metric_sum"] {
            let sql = format!(
                "SELECT metric_name, service_name, deployment_environment, count(*) FROM {prefix}{table} WHERE {} GROUP BY 1,2,3 ORDER BY 4 DESC LIMIT 1000",
                time_predicate(from, to)
            );
            let mut stmt = conn.prepare(&sql)?;
            let rows = stmt.query_map([], |row| {
                let mut labels = Map::new();
                labels.insert("__name__".to_string(), json!(row.get::<_, String>(0)?));
                if let Some(v) = row.get::<_, Option<String>>(1)? {
                    labels.insert("service_name".to_string(), json!(v));
                }
                if let Some(v) = row.get::<_, Option<String>>(2)? {
                    labels.insert("deployment_environment".to_string(), json!(v));
                }
                Ok(Value::Object(labels))
            })?;
            for row in rows {
                out.push(row?);
            }
        }
        Ok(())
    })?;
    Ok(prom_success(json!(out)))
}

pub fn prometheus_metadata(state: &AppState) -> ApiResult<Value> {
    let mut metadata = Map::new();
    state.queries.run_interactive(&state.storage, |conn, prefix| {
        for table in ["metric_gauge", "metric_sum"] {
            let signal_type = if table == "metric_sum" { "counter" } else { "gauge" };
            let sql = format!(
                "SELECT metric_name, max(metric_unit), max(metric_description) FROM {prefix}{table} GROUP BY 1 LIMIT 1000"
            );
            let mut stmt = conn.prepare(&sql)?;
            let rows = stmt.query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, Option<String>>(1)?,
                    row.get::<_, Option<String>>(2)?,
                ))
            })?;
            for row in rows {
                let (name, unit, help) = row?;
                metadata.insert(
                    name,
                    json!([{"type": signal_type, "unit": unit.unwrap_or_default(), "help": help.unwrap_or_default()}]),
                );
            }
        }
        Ok(())
    })?;
    Ok(prom_success(Value::Object(metadata)))
}

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
    let values = log_label_values(state, name, from, to)?;
    Ok(loki_success(json!(values)))
}

pub fn loki_series(state: &AppState, params: &HashMap<String, String>) -> ApiResult<Value> {
    let (from, to) = optional_range(params, INTERACTIVE_RANGE_SECS)?;
    let mut out = Vec::new();
    state.queries.run_interactive(&state.storage, |conn, prefix| {
        let sql = format!(
            "SELECT service_name, deployment_environment, severity_text, count(*) FROM {prefix}logs WHERE {} GROUP BY 1,2,3 ORDER BY 4 DESC LIMIT 1000",
            time_predicate(from, to)
        );
        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map([], |row| {
            let mut labels = Map::new();
            insert_opt(&mut labels, "service_name", row.get::<_, Option<String>>(0)?);
            insert_opt(
                &mut labels,
                "deployment_environment",
                row.get::<_, Option<String>>(1)?,
            );
            insert_opt(&mut labels, "severity_text", row.get::<_, Option<String>>(2)?);
            Ok(Value::Object(labels))
        })?;
        for row in rows {
            out.push(row?);
        }
        Ok(())
    })?;
    Ok(loki_success(json!(out)))
}

pub fn tempo_trace(state: &AppState, trace_id: &str) -> ApiResult<Value> {
    validate_trace_id(trace_id)?;
    let to = Utc::now() + chrono::Duration::minutes(10);
    let from = to - chrono::Duration::days(7);
    let mut spans = Vec::new();
    state.queries.run_interactive(&state.storage, |conn, prefix| {
        let sql = format!(
            "SELECT timestamp::VARCHAR, trace_id, span_id, parent_span_id, service_name, span_name, duration, status_code, http_method, http_status_code FROM {prefix}spans WHERE trace_id = {} AND {} ORDER BY timestamp ASC LIMIT 20000",
            sql_quote(trace_id),
            time_predicate(from, to)
        );
        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map([], crate::sql::span_row)?;
        for row in rows {
            spans.push(row?);
        }
        Ok(())
    })?;
    Ok(json!({
        "batches": [{
            "resource": {},
            "instrumentationLibrarySpans": [{
                "spans": spans.into_iter().map(tempo_span).collect::<Vec<_>>()
            }]
        }]
    }))
}

pub fn tempo_trace_proto(state: &AppState, trace_id: &str) -> ApiResult<Vec<u8>> {
    validate_trace_id(trace_id)?;
    let to = Utc::now() + chrono::Duration::minutes(10);
    let from = to - chrono::Duration::days(7);
    let mut spans = Vec::new();
    state.queries.run_interactive(&state.storage, |conn, prefix| {
        let sql = format!(
            "SELECT timestamp::VARCHAR, trace_id, span_id, parent_span_id, service_name, span_name, duration, status_code, http_method, http_status_code, trace_state, span_kind, status_message, scope_name, scope_version, deployment_environment, http_route, exception_type FROM {prefix}spans WHERE trace_id = {} AND {} ORDER BY timestamp ASC LIMIT 20000",
            sql_quote(trace_id),
            time_predicate(from, to)
        );
        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map([], |row| {
            Ok(json!({
                "timestamp": row.get::<_, String>(0)?,
                "trace_id": row.get::<_, Option<String>>(1)?,
                "span_id": row.get::<_, Option<String>>(2)?,
                "parent_span_id": row.get::<_, Option<String>>(3)?,
                "service_name": row.get::<_, Option<String>>(4)?,
                "span_name": row.get::<_, Option<String>>(5)?,
                "duration": row.get::<_, Option<i64>>(6)?,
                "status_code": row.get::<_, Option<i32>>(7)?,
                "http_method": row.get::<_, Option<String>>(8)?,
                "http_status_code": row.get::<_, Option<i32>>(9)?,
                "trace_state": row.get::<_, Option<String>>(10)?,
                "span_kind": row.get::<_, Option<i32>>(11)?,
                "status_message": row.get::<_, Option<String>>(12)?,
                "scope_name": row.get::<_, Option<String>>(13)?,
                "scope_version": row.get::<_, Option<String>>(14)?,
                "deployment_environment": row.get::<_, Option<String>>(15)?,
                "http_route": row.get::<_, Option<String>>(16)?,
                "exception_type": row.get::<_, Option<String>>(17)?,
            }))
        })?;
        for row in rows {
            spans.push(row?);
        }
        Ok(())
    })?;

    let service_name = spans
        .iter()
        .find_map(|span| span.get("service_name").and_then(Value::as_str))
        .unwrap_or_default()
        .to_string();
    let deployment_environment = spans
        .iter()
        .find_map(|span| span.get("deployment_environment").and_then(Value::as_str))
        .unwrap_or_default()
        .to_string();
    let scope_name = spans
        .iter()
        .find_map(|span| span.get("scope_name").and_then(Value::as_str))
        .unwrap_or_default()
        .to_string();
    let scope_version = spans
        .iter()
        .find_map(|span| span.get("scope_version").and_then(Value::as_str))
        .unwrap_or_default()
        .to_string();

    let trace = TracesData {
        resource_spans: vec![ResourceSpans {
            resource: Some(Resource {
                attributes: compact_key_values([
                    string_attr("service.name", &service_name),
                    string_attr("deployment.environment", &deployment_environment),
                ]),
                dropped_attributes_count: 0,
                entity_refs: vec![],
            }),
            scope_spans: vec![ScopeSpans {
                scope: Some(InstrumentationScope {
                    name: scope_name,
                    version: scope_version,
                    attributes: vec![],
                    dropped_attributes_count: 0,
                }),
                spans: spans.into_iter().map(tempo_proto_span).collect(),
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    };
    Ok(TempoTraceByIdResponse { trace: Some(trace) }.encode_to_vec())
}

/// Tempo's `/api/v2/traces/{traceID}` returns a `tempopb.TraceByIDResponse`
/// proto whose first field is the OTLP `TracesData`.
#[derive(Clone, PartialEq, Message)]
struct TempoTraceByIdResponse {
    #[prost(message, optional, tag = "1")]
    trace: Option<TracesData>,
}

pub fn tempo_search(state: &AppState, params: &HashMap<String, String>) -> ApiResult<Value> {
    let (from, to) = optional_range(params, INTERACTIVE_RANGE_SECS)?;
    let limit = parse_usize(params.get("limit"), 20, 1000)?;
    let plan = plan_tempo_search(
        params,
        TimeBounds {
            from,
            to,
            max_range_secs: INTERACTIVE_RANGE_SECS,
            default_lookback: Some(chrono::Duration::hours(1)),
            instant: false,
        },
        limit,
    )?;
    let rows = state.queries.execute_trace_search(&state.storage, &plan)?;
    let traces = result_rows(&rows)
        .into_iter()
        .map(|row| {
            let start_time_unix_nano = row
                .get("start_time")
                .and_then(Value::as_str)
                .and_then(parse_any_time_to_utc)
                .and_then(|time| time.timestamp_nanos_opt())
                .map(|nanos| nanos.to_string())
                .unwrap_or_default();
            let matched = row
                .get("matched_spans")
                .and_then(Value::as_i64)
                .unwrap_or_default();
            json!({
                "traceID": row.get("trace_id").and_then(Value::as_str).unwrap_or_default(),
                "startTimeUnixNano": start_time_unix_nano,
                "rootServiceName": row.get("service_name").and_then(Value::as_str),
                "rootTraceName": row.get("span_name").and_then(Value::as_str),
                "spanSet": {"spans": [], "matched": matched},
                "spanSets": [{"spans": [], "matched": matched}],
                "durationMs": row.get("duration").and_then(Value::as_i64).unwrap_or_default()
            })
        })
        .collect::<Vec<_>>();
    Ok(json!({"traces": traces, "metrics": {"inspectedTraces": traces.len(), "completedJobs": 1}}))
}

pub fn tempo_tags() -> Value {
    json!({"tagNames": ["service.name", "span.name", "name", "http.route", "status", "status.code", "traceID"]})
}

pub fn tempo_tag_values(
    state: &AppState,
    tag: &str,
    params: &HashMap<String, String>,
) -> ApiResult<Value> {
    if matches!(
        tag,
        "" | "duration" | "min-duration" | "max-duration" | "duration_ms"
    ) {
        return Ok(json!({"tagValues": []}));
    }

    let (from, to) = optional_range(params, INTERACTIVE_RANGE_SECS)?;
    let column = match tag {
        "service.name" | "service_name" | "service-name" => "service_name",
        "name" | "span.name" | "span_name" | "span-name" => "span_name",
        "http.route" | "http_route" | "http-route" => "http_route",
        "status" | "status.code" | "status_code" | "status-code" => "status_code",
        "traceID" | "trace_id" => "trace_id",
        _ => return Ok(json!({"tagValues": []})),
    };
    let mut values = Vec::new();
    state.queries.run_interactive(&state.storage, |conn, prefix| {
        let sql = format!(
            "SELECT DISTINCT {column}::VARCHAR FROM {prefix}spans WHERE {} AND {column} IS NOT NULL ORDER BY 1 LIMIT 1000",
            time_predicate(from, to)
        );
        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map([], |row| row.get::<_, Option<String>>(0))?;
        for row in rows {
            if let Some(value) = row? {
                values.push(value);
            }
        }
        Ok(())
    })?;
    Ok(json!({"tagValues": values}))
}

fn loki_query_inner(
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
        max_range_secs: INTERACTIVE_RANGE_SECS,
        default_lookback: Some(chrono::Duration::hours(1)),
        instant: !range,
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

fn required_param<'a>(
    params: &'a HashMap<String, String>,
    name: &'static str,
) -> ApiResult<&'a str> {
    params
        .get(name)
        .map(String::as_str)
        .filter(|v| !v.is_empty())
        .ok_or_else(|| ApiError::new(400, "missing_parameter", format!("{name} is required")))
}

fn optional_range(
    params: &HashMap<String, String>,
    max_secs: i64,
) -> ApiResult<(DateTime<Utc>, DateTime<Utc>)> {
    let to = optional_time(params, "end")?
        .or_else(|| optional_time(params, "to").ok().flatten())
        .unwrap_or_else(Utc::now);
    let from = optional_time(params, "start")?
        .or_else(|| optional_time(params, "from").ok().flatten())
        .unwrap_or(to - chrono::Duration::hours(1));
    validate_range(from, to, max_secs)?;
    Ok((from, to))
}

fn required_time(params: &HashMap<String, String>, name: &'static str) -> ApiResult<DateTime<Utc>> {
    let raw = required_param(params, name)?;
    parse_time(raw).ok_or_else(|| {
        ApiError::new(
            400,
            "invalid_time_range",
            format!("{name} must be RFC3339, Unix seconds, or Unix nanoseconds"),
        )
    })
}

fn optional_time(
    params: &HashMap<String, String>,
    name: &'static str,
) -> ApiResult<Option<DateTime<Utc>>> {
    match params
        .get(name)
        .map(String::as_str)
        .filter(|v| !v.is_empty())
    {
        Some(raw) => parse_time(raw).map(Some).ok_or_else(|| {
            ApiError::new(
                400,
                "invalid_time_range",
                format!("{name} must be RFC3339, Unix seconds, or Unix nanoseconds"),
            )
        }),
        None => Ok(None),
    }
}

fn parse_time(raw: &str) -> Option<DateTime<Utc>> {
    if let Ok(dt) = DateTime::parse_from_rfc3339(raw) {
        return Some(dt.with_timezone(&Utc));
    }
    if let Ok(value) = raw.parse::<i64>() {
        if value > 10_000_000_000_000 {
            return Some(Utc.timestamp_nanos(value));
        }
        return Utc.timestamp_opt(value, 0).single();
    }
    if let Ok(value) = raw.parse::<f64>() {
        let secs = value.trunc() as i64;
        let nanos = ((value.fract()) * 1_000_000_000.0) as u32;
        return Utc.timestamp_opt(secs, nanos).single();
    }
    None
}

fn parse_any_time_to_utc(raw: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(raw)
        .map(|dt| dt.with_timezone(&Utc))
        .ok()
        .or_else(|| {
            chrono::NaiveDateTime::parse_from_str(raw, "%Y-%m-%d %H:%M:%S%.f")
                .ok()
                .map(|dt| DateTime::<Utc>::from_naive_utc_and_offset(dt, Utc))
        })
        .or_else(|| {
            DateTime::parse_from_str(raw, "%Y-%m-%d %H:%M:%S%.f%#z")
                .map(|dt| dt.with_timezone(&Utc))
                .ok()
        })
}

fn validate_range(from: DateTime<Utc>, to: DateTime<Utc>, max_secs: i64) -> ApiResult<()> {
    validation::validate_range(from, to, max_secs)
}

fn parse_step(raw: &str) -> ApiResult<i64> {
    if let Some(stripped) = raw.strip_suffix('s') {
        return stripped.parse::<i64>().map_err(|_| {
            ApiError::new(
                400,
                "invalid_step",
                "step must be seconds or a duration ending in s",
            )
        });
    }
    raw.parse::<i64>().map_err(|_| {
        ApiError::new(
            400,
            "invalid_step",
            "step must be seconds or a duration ending in s",
        )
    })
}

fn parse_usize(value: Option<&String>, default: usize, max: usize) -> ApiResult<usize> {
    let parsed = value
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(default);
    validation::parse_limit(Some(&json!(parsed)), default, max)
}

fn metric_label_values(
    state: &AppState,
    name: &str,
    from: DateTime<Utc>,
    to: DateTime<Utc>,
) -> ApiResult<Vec<String>> {
    if name == "__name__" {
        return distinct_union(state, "metric_name", "metric_gauge", "metric_sum", from, to);
    }
    if !matches!(name, "service_name" | "deployment_environment") {
        return Ok(Vec::new());
    }
    distinct_union(state, name, "metric_gauge", "metric_sum", from, to)
}

fn log_label_values(
    state: &AppState,
    name: &str,
    from: DateTime<Utc>,
    to: DateTime<Utc>,
) -> ApiResult<Vec<String>> {
    if !matches!(
        name,
        "service_name"
            | "deployment_environment"
            | "severity_text"
            | "trace_id"
            | "span_id"
            | "http_route"
            | "http_method"
    ) {
        return Ok(Vec::new());
    }
    distinct_one(state, name, "logs", from, to)
}

fn distinct_union(
    state: &AppState,
    column: &str,
    table_a: &str,
    table_b: &str,
    from: DateTime<Utc>,
    to: DateTime<Utc>,
) -> ApiResult<Vec<String>> {
    let mut values = BTreeSet::new();
    state.queries.run_interactive(&state.storage, |conn, prefix| {
        for table in [table_a, table_b] {
            let sql = format!(
                "SELECT DISTINCT {column}::VARCHAR FROM {prefix}{table} WHERE {} AND {column} IS NOT NULL ORDER BY 1 LIMIT 1000",
                time_predicate(from, to)
            );
            let mut stmt = conn.prepare(&sql)?;
            let rows = stmt.query_map([], |row| row.get::<_, Option<String>>(0))?;
            for row in rows {
                if let Some(value) = row? {
                    values.insert(value);
                }
            }
        }
        Ok(())
    })?;
    Ok(values.into_iter().collect())
}

fn distinct_one(
    state: &AppState,
    column: &str,
    table: &str,
    from: DateTime<Utc>,
    to: DateTime<Utc>,
) -> ApiResult<Vec<String>> {
    let mut values = BTreeSet::new();
    state.queries.run_interactive(&state.storage, |conn, prefix| {
        let sql = format!(
            "SELECT DISTINCT {column}::VARCHAR FROM {prefix}{table} WHERE {} AND {column} IS NOT NULL ORDER BY 1 LIMIT 1000",
            time_predicate(from, to)
        );
        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map([], |row| row.get::<_, Option<String>>(0))?;
        for row in rows {
            if let Some(value) = row? {
                values.insert(value);
            }
        }
        Ok(())
    })?;
    Ok(values.into_iter().collect())
}

fn result_rows(result: &Value) -> Vec<Value> {
    result
        .get("rows")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default()
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

fn prom_value(value: f64) -> String {
    if value.is_finite() {
        value.to_string()
    } else {
        "NaN".to_string()
    }
}

fn insert_opt(labels: &mut Map<String, Value>, name: &str, value: Option<String>) {
    if let Some(value) = value.filter(|v| !v.is_empty()) {
        labels.insert(name.to_string(), json!(value));
    }
}

fn tempo_span(row: Value) -> Value {
    let span_id = row
        .get("span_id")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let trace_id = row
        .get("trace_id")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let start = row
        .get("timestamp")
        .and_then(Value::as_str)
        .and_then(parse_any_time_to_utc)
        .unwrap_or_else(Utc::now);
    let duration_ms = row.get("duration").and_then(Value::as_i64).unwrap_or(0);
    json!({
        "traceId": trace_id,
        "spanId": span_id,
        "parentSpanId": row.get("parent_span_id").and_then(Value::as_str).unwrap_or_default(),
        "name": row.get("span_name").and_then(Value::as_str).unwrap_or_default(),
        "kind": "SPAN_KIND_UNSPECIFIED",
        "startTimeUnixNano": start.timestamp_nanos_opt().unwrap_or_else(|| start.timestamp_micros() * 1000).to_string(),
        "endTimeUnixNano": (start.timestamp_nanos_opt().unwrap_or_else(|| start.timestamp_micros() * 1000) + duration_ms * 1_000_000).to_string(),
        "attributes": [
            {"key": "service.name", "value": {"stringValue": row.get("service_name").and_then(Value::as_str).unwrap_or_default()}},
            {"key": "http.request.method", "value": {"stringValue": row.get("http_method").and_then(Value::as_str).unwrap_or_default()}},
            {"key": "http.response.status_code", "value": {"intValue": row.get("http_status_code").and_then(Value::as_i64).unwrap_or_default().to_string()}}
        ],
        "status": {"code": row.get("status_code").and_then(Value::as_i64).unwrap_or_default()}
    })
}

fn tempo_proto_span(row: Value) -> Span {
    let start = row
        .get("timestamp")
        .and_then(Value::as_str)
        .and_then(parse_any_time_to_utc)
        .unwrap_or_else(Utc::now);
    let start_nanos = start
        .timestamp_nanos_opt()
        .unwrap_or_else(|| start.timestamp_micros() * 1000) as u64;
    let duration_ms = row
        .get("duration")
        .and_then(Value::as_i64)
        .unwrap_or(0)
        .max(0) as u64;
    let status_code = row
        .get("status_code")
        .and_then(Value::as_i64)
        .map(|code| {
            if code == 2 {
                status::StatusCode::Error as i32
            } else if code == 1 {
                status::StatusCode::Ok as i32
            } else {
                status::StatusCode::Unset as i32
            }
        })
        .unwrap_or(status::StatusCode::Unset as i32);

    Span {
        trace_id: row
            .get("trace_id")
            .and_then(Value::as_str)
            .and_then(hex_to_bytes)
            .unwrap_or_default(),
        span_id: row
            .get("span_id")
            .and_then(Value::as_str)
            .and_then(hex_to_bytes)
            .unwrap_or_default(),
        trace_state: row
            .get("trace_state")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string(),
        parent_span_id: row
            .get("parent_span_id")
            .and_then(Value::as_str)
            .and_then(hex_to_bytes)
            .unwrap_or_default(),
        flags: 0,
        name: row
            .get("span_name")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string(),
        kind: row
            .get("span_kind")
            .and_then(Value::as_i64)
            .map(|kind| kind as i32)
            .unwrap_or(span::SpanKind::Unspecified as i32),
        start_time_unix_nano: start_nanos,
        end_time_unix_nano: start_nanos + duration_ms * 1_000_000,
        attributes: compact_key_values([
            string_attr(
                "service.name",
                row.get("service_name")
                    .and_then(Value::as_str)
                    .unwrap_or_default(),
            ),
            string_attr(
                "http.request.method",
                row.get("http_method")
                    .and_then(Value::as_str)
                    .unwrap_or_default(),
            ),
            int_attr(
                "http.response.status_code",
                row.get("http_status_code").and_then(Value::as_i64),
            ),
            string_attr(
                "http.route",
                row.get("http_route")
                    .and_then(Value::as_str)
                    .unwrap_or_default(),
            ),
            string_attr(
                "exception.type",
                row.get("exception_type")
                    .and_then(Value::as_str)
                    .unwrap_or_default(),
            ),
        ]),
        dropped_attributes_count: 0,
        events: vec![],
        dropped_events_count: 0,
        links: vec![],
        dropped_links_count: 0,
        status: Some(Status {
            message: row
                .get("status_message")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string(),
            code: status_code,
        }),
    }
}

fn compact_key_values(values: impl IntoIterator<Item = Option<KeyValue>>) -> Vec<KeyValue> {
    values.into_iter().flatten().collect()
}

fn string_attr(key: &str, value: &str) -> Option<KeyValue> {
    if value.is_empty() {
        return None;
    }
    Some(KeyValue {
        key: key.to_string(),
        value: Some(AnyValue {
            value: Some(any_value::Value::StringValue(value.to_string())),
        }),
    })
}

fn int_attr(key: &str, value: Option<i64>) -> Option<KeyValue> {
    value.map(|value| KeyValue {
        key: key.to_string(),
        value: Some(AnyValue {
            value: Some(any_value::Value::IntValue(value)),
        }),
    })
}

fn hex_to_bytes(value: &str) -> Option<Vec<u8>> {
    if !value.len().is_multiple_of(2) || !value.chars().all(|c| c.is_ascii_hexdigit()) {
        return None;
    }
    let mut out = Vec::with_capacity(value.len() / 2);
    for chunk in value.as_bytes().chunks_exact(2) {
        let raw = std::str::from_utf8(chunk).ok()?;
        out.push(u8::from_str_radix(raw, 16).ok()?);
    }
    Some(out)
}

fn validate_trace_id(trace_id: &str) -> ApiResult<()> {
    if trace_id.chars().all(|c| c.is_ascii_hexdigit()) && matches!(trace_id.len(), 16 | 32) {
        Ok(())
    } else {
        Err(ApiError::new(
            400,
            "invalid_trace_id",
            "traceID must be 16 or 32 hex characters",
        ))
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

fn loki_success(data: Value) -> Value {
    json!({"status": "success", "data": data})
}

pub fn compat_error(err: ApiError) -> Value {
    json!({"status": "error", "errorType": err.reason, "error": err.message})
}
