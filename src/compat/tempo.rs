use super::params::{optional_range, parse_any_time_to_utc, parse_usize, result_rows};
use crate::db::sql::{quote as sql_quote, span_row, time_predicate};
use crate::query::plan::TimeBounds;
use crate::query::trace::plan_tempo_search;
use crate::validation::{ApiError, ApiResult};
use crate::AppState;
use chrono::Utc;
use opentelemetry_proto::tonic::common::v1::{any_value, AnyValue, InstrumentationScope, KeyValue};
use opentelemetry_proto::tonic::resource::v1::Resource;
use opentelemetry_proto::tonic::trace::v1::{
    span, status, ResourceSpans, ScopeSpans, Span, Status, TracesData,
};
use prost::Message;
use serde_json::{json, Value};
use std::collections::HashMap;

const INTERACTIVE_RANGE_SECS: i64 = 24 * 60 * 60;

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
        let rows = stmt.query_map([], span_row)?;
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
    let plan = plan_tempo_search(params, TimeBounds { from, to }, limit)?;
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
    let values = state
        .metadata
        .tempo_tag_values(&state.queries, &state.storage, tag, from, to)?;
    Ok(json!({"tagValues": values}))
}
pub(super) fn tempo_span(row: Value) -> Value {
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

pub(super) fn tempo_proto_span(row: Value) -> Span {
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

pub(super) fn compact_key_values(
    values: impl IntoIterator<Item = Option<KeyValue>>,
) -> Vec<KeyValue> {
    values.into_iter().flatten().collect()
}

pub(super) fn string_attr(key: &str, value: &str) -> Option<KeyValue> {
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

pub(super) fn int_attr(key: &str, value: Option<i64>) -> Option<KeyValue> {
    value.map(|value| KeyValue {
        key: key.to_string(),
        value: Some(AnyValue {
            value: Some(any_value::Value::IntValue(value)),
        }),
    })
}

pub(super) fn hex_to_bytes(value: &str) -> Option<Vec<u8>> {
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

pub(super) fn validate_trace_id(trace_id: &str) -> ApiResult<()> {
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
