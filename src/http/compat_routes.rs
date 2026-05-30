use crate::admission_control::QueryClass;
use crate::compat;
use crate::validation::ApiError;
use crate::AppState;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::time::Instant;

use super::auth::api_auth;
use super::parser::percent_decode;
use super::response::HttpResponse;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum AuthRequirement {
    Api,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum QueryAdmission {
    None,
    Cheap,
    Heavy,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RoutePattern {
    Exact(&'static str),
    PrefixParam {
        prefix: &'static str,
    },
    PrefixSuffixParam {
        prefix: &'static str,
        suffix: &'static str,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CompatHandler {
    PrometheusQuery,
    PrometheusQueryRange,
    PrometheusLabels,
    PrometheusSeries,
    PrometheusMetadata,
    PrometheusLabelValues,
    LokiQuery,
    LokiQueryRange,
    LokiLabels,
    LokiSeries,
    LokiLabelValues,
    TempoSearch,
    TempoTags,
    TempoTrace,
    TempoTagValues,
    BuildInfo,
}

#[derive(Clone, Copy, Debug)]
struct CompatRoute {
    methods: &'static [&'static str],
    template: &'static str,
    pattern: RoutePattern,
    auth: AuthRequirement,
    admission: QueryAdmission,
    handler: CompatHandler,
}

#[derive(Clone, Copy, Debug)]
pub(super) struct CompatRouteMatch<'a> {
    route: &'static CompatRoute,
    param: Option<&'a str>,
}

const COMPAT_ROUTES: &[CompatRoute] = &[
    compat_route(
        &["GET", "POST"],
        "/api/v1/query",
        RoutePattern::Exact("/api/v1/query"),
        QueryAdmission::Cheap,
        CompatHandler::PrometheusQuery,
    ),
    compat_route(
        &["GET", "POST"],
        "/api/v1/query_range",
        RoutePattern::Exact("/api/v1/query_range"),
        QueryAdmission::Heavy,
        CompatHandler::PrometheusQueryRange,
    ),
    compat_route(
        &["GET"],
        "/api/v1/labels",
        RoutePattern::Exact("/api/v1/labels"),
        QueryAdmission::Cheap,
        CompatHandler::PrometheusLabels,
    ),
    compat_route(
        &["GET"],
        "/api/v1/series",
        RoutePattern::Exact("/api/v1/series"),
        QueryAdmission::Cheap,
        CompatHandler::PrometheusSeries,
    ),
    compat_route(
        &["GET"],
        "/api/v1/metadata",
        RoutePattern::Exact("/api/v1/metadata"),
        QueryAdmission::Cheap,
        CompatHandler::PrometheusMetadata,
    ),
    compat_route(
        &["GET"],
        "/api/v1/label/:name/values",
        RoutePattern::PrefixSuffixParam {
            prefix: "/api/v1/label/",
            suffix: "/values",
        },
        QueryAdmission::Cheap,
        CompatHandler::PrometheusLabelValues,
    ),
    compat_route(
        &["GET"],
        "/loki/api/v1/query",
        RoutePattern::Exact("/loki/api/v1/query"),
        QueryAdmission::Cheap,
        CompatHandler::LokiQuery,
    ),
    compat_route(
        &["GET"],
        "/loki/api/v1/query_range",
        RoutePattern::Exact("/loki/api/v1/query_range"),
        QueryAdmission::Heavy,
        CompatHandler::LokiQueryRange,
    ),
    compat_route(
        &["GET"],
        "/loki/api/v1/labels",
        RoutePattern::Exact("/loki/api/v1/labels"),
        QueryAdmission::Cheap,
        CompatHandler::LokiLabels,
    ),
    compat_route(
        &["GET"],
        "/loki/api/v1/series",
        RoutePattern::Exact("/loki/api/v1/series"),
        QueryAdmission::Cheap,
        CompatHandler::LokiSeries,
    ),
    compat_route(
        &["GET"],
        "/loki/api/v1/label/:name/values",
        RoutePattern::PrefixSuffixParam {
            prefix: "/loki/api/v1/label/",
            suffix: "/values",
        },
        QueryAdmission::Cheap,
        CompatHandler::LokiLabelValues,
    ),
    compat_route(
        &["GET"],
        "/api/search",
        RoutePattern::Exact("/api/search"),
        QueryAdmission::Heavy,
        CompatHandler::TempoSearch,
    ),
    compat_route(
        &["GET"],
        "/api/search/tags",
        RoutePattern::Exact("/api/search/tags"),
        QueryAdmission::Cheap,
        CompatHandler::TempoTags,
    ),
    compat_route(
        &["GET"],
        "/api/v2/search/tags",
        RoutePattern::Exact("/api/v2/search/tags"),
        QueryAdmission::Cheap,
        CompatHandler::TempoTags,
    ),
    compat_route(
        &["GET"],
        "/api/v2/traces/:trace_id",
        RoutePattern::PrefixParam {
            prefix: "/api/v2/traces/",
        },
        QueryAdmission::Heavy,
        CompatHandler::TempoTrace,
    ),
    compat_route(
        &["GET"],
        "/api/traces/:trace_id",
        RoutePattern::PrefixParam {
            prefix: "/api/traces/",
        },
        QueryAdmission::Heavy,
        CompatHandler::TempoTrace,
    ),
    compat_route(
        &["GET"],
        "/api/search/tag/:tag/values",
        RoutePattern::PrefixSuffixParam {
            prefix: "/api/search/tag/",
            suffix: "/values",
        },
        QueryAdmission::Cheap,
        CompatHandler::TempoTagValues,
    ),
    compat_route(
        &["GET"],
        "/api/v2/search/tag/:tag/values",
        RoutePattern::PrefixSuffixParam {
            prefix: "/api/v2/search/tag/",
            suffix: "/values",
        },
        QueryAdmission::Cheap,
        CompatHandler::TempoTagValues,
    ),
    compat_route(
        &["GET"],
        "/api/status/buildinfo",
        RoutePattern::Exact("/api/status/buildinfo"),
        QueryAdmission::None,
        CompatHandler::BuildInfo,
    ),
];

const fn compat_route(
    methods: &'static [&'static str],
    template: &'static str,
    pattern: RoutePattern,
    admission: QueryAdmission,
    handler: CompatHandler,
) -> CompatRoute {
    CompatRoute {
        methods,
        template,
        pattern,
        auth: AuthRequirement::Api,
        admission,
        handler,
    }
}

pub(super) fn match_compat_route<'a>(method: &str, path: &'a str) -> Option<CompatRouteMatch<'a>> {
    COMPAT_ROUTES.iter().find_map(|route| {
        if !route.methods.contains(&method) {
            return None;
        }
        match_route_path(route.pattern, path).map(|param| CompatRouteMatch { route, param })
    })
}

pub(super) fn route_compat(
    matched: CompatRouteMatch<'_>,
    query: &HashMap<String, String>,
    headers: &HashMap<String, String>,
    body: &[u8],
    state: &AppState,
) -> HttpResponse {
    let started = Instant::now();
    let params = request_params(query, headers, body);
    if matched.route.handler == CompatHandler::TempoTrace {
        return tempo_trace_http(matched, headers, state, started);
    }

    let result = authorize(matched.route.auth, headers, state, || {
        with_query_admission(state, matched.route.admission, || {
            compat_value(matched.route.handler, matched.param, state, &params)
        })
    });
    compat_http(state, matched.route.template, result, started)
}

fn match_route_path(pattern: RoutePattern, path: &str) -> Option<Option<&str>> {
    match pattern {
        RoutePattern::Exact(exact) => (path == exact).then_some(None),
        RoutePattern::PrefixParam { prefix } => path
            .strip_prefix(prefix)
            .filter(|param| !param.is_empty() && !param.contains('/'))
            .map(Some),
        RoutePattern::PrefixSuffixParam { prefix, suffix } => path
            .strip_prefix(prefix)
            .and_then(|rest| rest.strip_suffix(suffix))
            .filter(|param| !param.is_empty() && !param.contains('/'))
            .map(Some),
    }
}

fn compat_value(
    handler: CompatHandler,
    param: Option<&str>,
    state: &AppState,
    params: &HashMap<String, String>,
) -> Result<Value, ApiError> {
    match handler {
        CompatHandler::PrometheusQuery => compat::prometheus_query(state, params),
        CompatHandler::PrometheusQueryRange => compat::prometheus_query_range(state, params),
        CompatHandler::PrometheusLabels => compat::prometheus_labels(state, params),
        CompatHandler::PrometheusSeries => compat::prometheus_series(state, params),
        CompatHandler::PrometheusMetadata => compat::prometheus_metadata(state),
        CompatHandler::PrometheusLabelValues => {
            compat::prometheus_label_values(state, param.expect("label route has param"), params)
        }
        CompatHandler::LokiQuery => compat::loki_query(state, params),
        CompatHandler::LokiQueryRange => compat::loki_query_range(state, params),
        CompatHandler::LokiLabels => compat::loki_labels(state, params),
        CompatHandler::LokiSeries => compat::loki_series(state, params),
        CompatHandler::LokiLabelValues => {
            compat::loki_label_values(state, param.expect("label route has param"), params)
        }
        CompatHandler::TempoSearch => compat::tempo_search(state, params),
        CompatHandler::TempoTags => Ok(compat::tempo_tags()),
        CompatHandler::TempoTrace => {
            compat::tempo_trace(state, param.expect("trace route has param"))
        }
        CompatHandler::TempoTagValues => {
            compat::tempo_tag_values(state, param.expect("tag route has param"), params)
        }
        CompatHandler::BuildInfo => Ok(json!({
            "version": env!("CARGO_PKG_VERSION"),
            "revision": "canardstack",
            "branch": "local",
            "buildUser": "canardstack",
            "buildDate": ""
        })),
    }
}

fn tempo_trace_http(
    matched: CompatRouteMatch<'_>,
    headers: &HashMap<String, String>,
    state: &AppState,
    started: Instant,
) -> HttpResponse {
    let trace_id = matched.param.expect("trace route has param");
    let wants_json = headers
        .get("accept")
        .is_some_and(|value| value.contains("application/json"));
    if wants_json {
        let result = authorize(matched.route.auth, headers, state, || {
            with_query_admission(state, matched.route.admission, || {
                compat::tempo_trace(state, trace_id)
            })
        });
        return compat_http(state, matched.route.template, result, started);
    }

    let result = authorize(matched.route.auth, headers, state, || {
        with_query_admission(state, matched.route.admission, || {
            compat::tempo_trace_proto(state, trace_id)
        })
    });
    record_query_metrics(state, matched.route.template, &result, started);
    match result {
        Ok(bytes) => HttpResponse::bytes(200, "application/protobuf", bytes),
        Err(err) => compat_error_response(err),
    }
}

fn authorize<T>(
    auth: AuthRequirement,
    headers: &HashMap<String, String>,
    state: &AppState,
    run: impl FnOnce() -> Result<T, ApiError>,
) -> Result<T, ApiError> {
    match auth {
        AuthRequirement::Api => api_auth(headers, state, run),
    }
}

fn with_query_admission<T>(
    state: &AppState,
    admission: QueryAdmission,
    run: impl FnOnce() -> Result<T, ApiError>,
) -> Result<T, ApiError> {
    match admission {
        QueryAdmission::None => run(),
        QueryAdmission::Cheap => {
            let _guard = state.admission.reserve_query_with_wait(
                QueryClass::Cheap,
                state.config.operator.query_admission_wait,
                &state.metrics,
            )?;
            run()
        }
        QueryAdmission::Heavy => {
            let _guard = state.admission.reserve_query_with_wait(
                QueryClass::Heavy,
                state.config.operator.query_admission_wait,
                &state.metrics,
            )?;
            run()
        }
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
    route_template: &'static str,
    result: Result<Value, ApiError>,
    started: Instant,
) -> HttpResponse {
    record_query_metrics(state, route_template, &result, started);
    match result {
        Ok(value) => HttpResponse::json(200, value),
        Err(err) => compat_error_response(err),
    }
}

fn record_query_metrics<T>(
    state: &AppState,
    route_template: &'static str,
    result: &Result<T, ApiError>,
    started: Instant,
) {
    let (status, reason) = match &result {
        Ok(_) => (200, "ok"),
        Err(err) => (err.status, err.reason),
    };
    state.metrics.query_request(
        route_template,
        status,
        reason,
        started.elapsed().as_secs_f64(),
    );
    state.metrics.observe_query_route_phase_seconds(
        route_template,
        "query_execute",
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
