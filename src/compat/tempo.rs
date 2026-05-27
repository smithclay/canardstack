use super::params::{optional_range, parse_any_time_to_utc, parse_usize, result_rows};
use crate::db::sql::{quote as sql_quote, span_row, time_predicate};
use crate::query::plan::TimeBounds;
use crate::query::trace::plan_tempo_search;
use crate::semantic_labels::{self, LabelScope};
use crate::validation::{ApiError, ApiResult};
use crate::AppState;
use chrono::Utc;
use otlp2records::proto_output::{
    encode_tempo_trace_by_id_response, Attribute, AttributeValue, Resource, ResourceSpans, Scope,
    ScopeSpans, Span, SpanStatus, TraceData,
};
use serde_json::{json, Value};
use std::collections::HashMap;

const INTERACTIVE_RANGE_SECS: i64 = 24 * 60 * 60;

/// SQL expression for a span label, sourced from the semantic-label registry so
/// the Tempo projection and the registry's tag discovery cannot drift apart.
fn span_label(canonical: &str) -> String {
    semantic_labels::label_expr(LabelScope::Spans, canonical)
        .unwrap_or_else(|| panic!("span label {canonical} is registered for spans"))
}

pub fn tempo_trace(state: &AppState, trace_id: &str) -> ApiResult<Value> {
    validate_trace_id(trace_id)?;
    let to = Utc::now() + chrono::Duration::minutes(10);
    let from = to - chrono::Duration::days(7);
    let mut spans = Vec::new();
    state.queries.run_interactive(&state.storage, |conn, prefix| {
        // v2 storage holds IDs as BLOBs (otlp2records 0.8.0 `FixedSizeBinary`)
        // and durations as BIGINT nanoseconds (`duration_time_unix_nano`). The
        // public span JSON keys stay stable (`timestamp`, `trace_id`,
        // `span_name`, `duration`) so the Tempo adapter and `span_row` keep
        // their existing positional contract.
        let sql = format!(
            "SELECT start_time_unix_nano::VARCHAR, lower(hex(trace_id)), lower(hex(span_id)), lower(hex(parent_span_id)), service_name, name, duration_time_unix_nano, status_code, {} AS http_method, {} AS http_status_code FROM {prefix}spans WHERE trace_id = unhex({}) AND {} ORDER BY start_time_unix_nano ASC LIMIT 20000",
            span_label("http_method"),
            span_label("http_status_code"),
            sql_quote(trace_id),
            time_predicate("start_time_unix_nano", from, to)
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
        // Same v2 mapping as the JSON sibling above: IDs come out as hex
        // VARCHAR via `lower(hex(...))`, durations are nanoseconds, and the
        // span_name column was renamed `name` while status_message became
        // `status_status_message` in the OTAP schema.
        let sql = format!(
            "SELECT start_time_unix_nano::VARCHAR, lower(hex(trace_id)), lower(hex(span_id)), lower(hex(parent_span_id)), service_name, name, duration_time_unix_nano, status_code, {} AS http_method, {} AS http_status_code, trace_state, kind, status_status_message, scope_name, scope_version, {} AS deployment_environment, {} AS http_route, {} AS exception_type FROM {prefix}spans WHERE trace_id = unhex({}) AND {} ORDER BY start_time_unix_nano ASC LIMIT 20000",
            span_label("http_method"),
            span_label("http_status_code"),
            span_label("deployment_environment"),
            span_label("http_route"),
            span_label("exception_type"),
            sql_quote(trace_id),
            time_predicate("start_time_unix_nano", from, to)
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

    let trace = TraceData {
        resource_spans: vec![ResourceSpans {
            resource: Resource {
                attributes: compact_attrs([
                    string_attr("service.name", &service_name),
                    string_attr("deployment.environment", &deployment_environment),
                ]),
            },
            scope_spans: vec![ScopeSpans {
                scope: Scope {
                    name: scope_name,
                    version: scope_version,
                    attributes: vec![],
                },
                spans: spans.into_iter().map(tempo_proto_span).collect(),
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    };
    encode_tempo_trace_by_id_response(trace).map_err(|err| {
        ApiError::new(
            500,
            "internal_error",
            format!("encode Tempo trace response: {err}"),
        )
    })
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
            // v2 stores `duration_time_unix_nano` in nanoseconds; Tempo
            // wants milliseconds.
            let duration_ns = row.get("duration").and_then(Value::as_i64).unwrap_or(0);
            json!({
                "traceID": row.get("trace_id").and_then(Value::as_str).unwrap_or_default(),
                "startTimeUnixNano": start_time_unix_nano,
                "rootServiceName": row.get("service_name").and_then(Value::as_str),
                "rootTraceName": row.get("span_name").and_then(Value::as_str),
                "spanSet": {"spans": [], "matched": matched},
                "spanSets": [{"spans": [], "matched": matched}],
                "durationMs": duration_ns / 1_000_000
            })
        })
        .collect::<Vec<_>>();
    Ok(json!({"traces": traces, "metrics": {"inspectedTraces": traces.len(), "completedJobs": 1}}))
}

pub fn tempo_tags() -> Value {
    json!({"tagNames": semantic_labels::tempo_tag_names()})
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
    // v2: `duration_time_unix_nano` lands in the span JSON as nanoseconds
    // under the stable `duration` key; downstream just adds it to start_nanos.
    let duration_ns = row.get("duration").and_then(Value::as_i64).unwrap_or(0);
    let start_nanos = start
        .timestamp_nanos_opt()
        .unwrap_or_else(|| start.timestamp_micros() * 1000);
    json!({
        "traceId": trace_id,
        "spanId": span_id,
        "parentSpanId": row.get("parent_span_id").and_then(Value::as_str).unwrap_or_default(),
        "name": row.get("span_name").and_then(Value::as_str).unwrap_or_default(),
        "kind": "SPAN_KIND_UNSPECIFIED",
        "startTimeUnixNano": start_nanos.to_string(),
        "endTimeUnixNano": (start_nanos + duration_ns).to_string(),
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
    // v2: `duration_time_unix_nano` is BIGINT nanoseconds; clamp negatives
    // before widening to u64 the same way the millisecond path used to.
    let duration_ns = row
        .get("duration")
        .and_then(Value::as_i64)
        .unwrap_or(0)
        .max(0) as u64;
    let status_code = row
        .get("status_code")
        .and_then(Value::as_i64)
        .map(|code| {
            if code == 2 {
                2
            } else if code == 1 {
                1
            } else {
                0
            }
        })
        .unwrap_or(0);

    Span {
        trace_id_hex: row
            .get("trace_id")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string(),
        span_id_hex: row
            .get("span_id")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string(),
        trace_state: row
            .get("trace_state")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string(),
        parent_span_id_hex: row
            .get("parent_span_id")
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
            .map(str::to_string),
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
            .unwrap_or(0),
        start_time_unix_nano: start_nanos,
        end_time_unix_nano: start_nanos + duration_ns,
        attributes: compact_attrs([
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
        dropped_events_count: 0,
        dropped_links_count: 0,
        status: SpanStatus {
            message: row
                .get("status_message")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string(),
            code: status_code,
        },
    }
}

pub(super) fn compact_attrs(values: impl IntoIterator<Item = Option<Attribute>>) -> Vec<Attribute> {
    values.into_iter().flatten().collect()
}

pub(super) fn string_attr(key: &str, value: &str) -> Option<Attribute> {
    if value.is_empty() {
        return None;
    }
    Some(Attribute {
        key: key.to_string(),
        value: AttributeValue::String(value.to_string()),
    })
}

pub(super) fn int_attr(key: &str, value: Option<i64>) -> Option<Attribute> {
    value.map(|value| Attribute {
        key: key.to_string(),
        value: AttributeValue::Int(value),
    })
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
