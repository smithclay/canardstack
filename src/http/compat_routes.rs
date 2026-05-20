use crate::compat;
use crate::validation::ApiError;
use crate::AppState;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::time::Instant;

use super::auth::api_auth;
use super::parser::percent_decode;
use super::response::HttpResponse;

pub(super) fn route_compat(
    method: &str,
    path: &str,
    query: &HashMap<String, String>,
    headers: &HashMap<String, String>,
    body: &[u8],
    state: &AppState,
) -> Option<HttpResponse> {
    let started = Instant::now();
    let params = request_params(query, headers, body);
    // `query_class` must be a static route template - never the raw `path` -
    // or the `canardstack_query_*` series cardinality explodes on every
    // distinct trace_id / label / tag in the URL.
    let (query_class, result) = match (method, path) {
        ("GET", "/api/v1/query") | ("POST", "/api/v1/query") => (
            "/api/v1/query",
            api_auth(headers, state, || compat::prometheus_query(state, &params)),
        ),
        ("GET", "/api/v1/query_range") | ("POST", "/api/v1/query_range") => (
            "/api/v1/query_range",
            api_auth(headers, state, || {
                compat::prometheus_query_range(state, &params)
            }),
        ),
        ("GET", "/api/v1/labels") => (
            "/api/v1/labels",
            api_auth(headers, state, || compat::prometheus_labels(state, &params)),
        ),
        ("GET", "/api/v1/series") => (
            "/api/v1/series",
            api_auth(headers, state, || compat::prometheus_series(state, &params)),
        ),
        ("GET", "/api/v1/metadata") => (
            "/api/v1/metadata",
            api_auth(headers, state, || compat::prometheus_metadata(state)),
        ),
        ("GET", "/loki/api/v1/query") => (
            "/loki/api/v1/query",
            api_auth(headers, state, || compat::loki_query(state, &params)),
        ),
        ("GET", "/loki/api/v1/query_range") => (
            "/loki/api/v1/query_range",
            api_auth(headers, state, || compat::loki_query_range(state, &params)),
        ),
        ("GET", "/loki/api/v1/labels") => (
            "/loki/api/v1/labels",
            api_auth(headers, state, || compat::loki_labels(state, &params)),
        ),
        ("GET", "/loki/api/v1/series") => (
            "/loki/api/v1/series",
            api_auth(headers, state, || compat::loki_series(state, &params)),
        ),
        ("GET", "/api/search") => (
            "/api/search",
            api_auth(headers, state, || compat::tempo_search(state, &params)),
        ),
        ("GET", "/api/search/tags") => (
            "/api/search/tags",
            api_auth(headers, state, || Ok(compat::tempo_tags())),
        ),
        ("GET", "/api/v2/search/tags") => (
            "/api/v2/search/tags",
            api_auth(headers, state, || Ok(compat::tempo_tags())),
        ),
        ("GET", "/api/status/buildinfo") => (
            "/api/status/buildinfo",
            api_auth(headers, state, || {
                Ok(json!({
                    "version": env!("CARGO_PKG_VERSION"),
                    "revision": "canardstack",
                    "branch": "local",
                    "buildUser": "canardstack",
                    "buildDate": ""
                }))
            }),
        ),
        _ => {
            if method == "GET" {
                if let Some(name) = path
                    .strip_prefix("/api/v1/label/")
                    .and_then(|s| s.strip_suffix("/values"))
                {
                    return Some(compat_http(
                        state,
                        "/api/v1/label/:name/values",
                        api_auth(headers, state, || {
                            compat::prometheus_label_values(state, name, &params)
                        }),
                        started,
                    ));
                }
                if let Some(name) = path
                    .strip_prefix("/loki/api/v1/label/")
                    .and_then(|s| s.strip_suffix("/values"))
                {
                    return Some(compat_http(
                        state,
                        "/loki/api/v1/label/:name/values",
                        api_auth(headers, state, || {
                            compat::loki_label_values(state, name, &params)
                        }),
                        started,
                    ));
                }
                if let Some(trace_id) = path.strip_prefix("/api/v2/traces/") {
                    return Some(tempo_trace_http(
                        state,
                        "/api/v2/traces/:trace_id",
                        trace_id,
                        headers,
                        started,
                    ));
                }
                if let Some(trace_id) = path.strip_prefix("/api/traces/") {
                    return Some(tempo_trace_http(
                        state,
                        "/api/traces/:trace_id",
                        trace_id,
                        headers,
                        started,
                    ));
                }
                if let Some(tag) = path
                    .strip_prefix("/api/search/tag/")
                    .and_then(|s| s.strip_suffix("/values"))
                {
                    return Some(compat_http(
                        state,
                        "/api/search/tag/:tag/values",
                        api_auth(headers, state, || {
                            compat::tempo_tag_values(state, tag, &params)
                        }),
                        started,
                    ));
                }
                if let Some(tag) = path
                    .strip_prefix("/api/v2/search/tag/")
                    .and_then(|s| s.strip_suffix("/values"))
                {
                    return Some(compat_http(
                        state,
                        "/api/v2/search/tag/:tag/values",
                        api_auth(headers, state, || {
                            compat::tempo_tag_values(state, tag, &params)
                        }),
                        started,
                    ));
                }
            }
            return None;
        }
    };
    Some(compat_http(state, query_class, result, started))
}

fn tempo_trace_http(
    state: &AppState,
    query_class: &'static str,
    trace_id: &str,
    headers: &HashMap<String, String>,
    started: Instant,
) -> HttpResponse {
    let wants_json = headers
        .get("accept")
        .is_some_and(|value| value.contains("application/json"));
    if wants_json {
        return compat_http(
            state,
            query_class,
            api_auth(headers, state, || compat::tempo_trace(state, trace_id)),
            started,
        );
    }

    let result = api_auth(headers, state, || {
        compat::tempo_trace_proto(state, trace_id)
    });
    record_query_metrics(state, query_class, &result, started);
    match result {
        Ok(bytes) => HttpResponse::bytes(200, "application/protobuf", bytes),
        Err(err) => compat_error_response(err),
    }
}

fn compat_error_response(err: ApiError) -> HttpResponse {
    let retry_after = err.retry_after_seconds;
    let status = err.status;
    let body = compat::compat_error(err);
    let mut response = HttpResponse::json(status, body);
    if let Some(seconds) = retry_after {
        response = response.with_retry_after(seconds);
    }
    response
}

fn compat_http(
    state: &AppState,
    query_class: &'static str,
    result: Result<Value, ApiError>,
    started: Instant,
) -> HttpResponse {
    record_query_metrics(state, query_class, &result, started);
    match result {
        Ok(value) => HttpResponse::json(200, value),
        Err(err) => compat_error_response(err),
    }
}

fn record_query_metrics<T>(
    state: &AppState,
    query_class: &'static str,
    result: &Result<T, ApiError>,
    started: Instant,
) {
    let (status, reason) = match &result {
        Ok(_) => (200, "ok"),
        Err(err) => (err.status, err.reason),
    };
    state
        .metrics
        .query_request(query_class, status, reason, started.elapsed().as_secs_f64());
    state.metrics.observe_phase_seconds(
        "query",
        "query_execute",
        Some(query_class),
        started.elapsed().as_secs_f64(),
    );
}

fn request_params(
    query: &HashMap<String, String>,
    headers: &HashMap<String, String>,
    body: &[u8],
) -> HashMap<String, String> {
    let mut params = query.clone();
    if body.is_empty() {
        return params;
    }
    let content_type = headers
        .get("content-type")
        .map(|v| v.split(';').next().unwrap_or(v).trim().to_ascii_lowercase())
        .unwrap_or_default();
    if content_type == "application/json" {
        if let Ok(Value::Object(map)) = serde_json::from_slice::<Value>(body) {
            for (key, value) in map {
                if let Some(value) = value.as_str() {
                    params.insert(key, value.to_string());
                } else if !value.is_null() {
                    params.insert(key, value.to_string());
                }
            }
        }
    } else {
        for pair in String::from_utf8_lossy(body)
            .split('&')
            .filter(|s| !s.is_empty())
        {
            let (k, v) = pair.split_once('=').unwrap_or((pair, ""));
            params.insert(percent_decode(k), percent_decode(v));
        }
    }
    params
}
