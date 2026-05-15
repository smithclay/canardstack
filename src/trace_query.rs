use crate::query_plan::{
    matchers_from_labels, normalize_labels, SelectorPlan, TimeBounds, TracePlan, TraceSort,
};
use crate::validation::ApiResult;
use std::collections::{BTreeMap, HashMap};

const TEMPO_ALIASES: &[(&str, &str)] = &[
    ("resource.service.name", "service_name"),
    ("service.name", "service_name"),
    ("service_name", "service_name"),
    ("serviceName", "service_name"),
    ("service-name", "service_name"),
    ("name", "span_name"),
    ("span_name", "span_name"),
    ("span.name", "span_name"),
    ("span-name", "span_name"),
    ("http.route", "http_route"),
    ("http-route", "http_route"),
    ("status.code", "status_code"),
    ("status_code", "status_code"),
    ("status", "status_code"),
];

const TEMPO_QUERY_ALIASES: &[(&str, &str)] = &[
    ("resource.service.name", "service_name"),
    ("service.name", "service_name"),
    ("span.name", "span_name"),
    ("name", "span_name"),
    ("http.route", "http_route"),
    ("status.code", "status_code"),
    ("status", "status_code"),
];

pub fn plan_tempo_search(
    params: &HashMap<String, String>,
    time_bounds: TimeBounds,
    limit: usize,
) -> ApiResult<TracePlan> {
    let mut labels = BTreeMap::new();
    for (param, _) in TEMPO_ALIASES {
        if let Some(value) = params.get(*param).filter(|v| !v.is_empty()) {
            labels.insert((*param).to_string(), value.to_string());
        }
    }
    if let Some(q) = params.get("q").or_else(|| params.get("query")) {
        extract_traceql_labels(q, &mut labels);
    }
    if let Some(tags) = params.get("tags") {
        extract_traceql_labels(tags, &mut labels);
    }
    let labels = normalize_labels(labels, TEMPO_ALIASES, "unsupported_selector")?;
    Ok(TracePlan {
        selector: SelectorPlan {
            resource: None,
            matchers: matchers_from_labels(labels),
            text_filters: Vec::new(),
        },
        time_bounds,
        limit,
        sort: TraceSort::TimestampDesc,
    })
}

fn extract_traceql_labels(raw: &str, labels: &mut BTreeMap<String, String>) {
    for (tag, _) in TEMPO_QUERY_ALIASES {
        if let Some(value) = extract_quoted_tag(raw, tag) {
            labels.insert((*tag).to_string(), value);
        }
    }
}

fn extract_quoted_tag(raw: &str, tag: &str) -> Option<String> {
    let marker_start = raw.match_indices(tag).find_map(|(index, _)| {
        let is_tag_boundary = raw[..index]
            .chars()
            .next_back()
            .map(|c| !(c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | '.')))
            .unwrap_or(true);
        let rest = raw[index + tag.len()..].trim_start();
        (is_tag_boundary && rest.starts_with('=')).then_some(index)
    })?;
    let rest = raw[marker_start + tag.len()..]
        .trim_start()
        .strip_prefix('=')?
        .trim_start();
    if let Some(rest) = rest.strip_prefix('"') {
        let end = rest.find('"')?;
        Some(rest[..end].to_string())
    } else {
        let end = rest
            .find(|c: char| c.is_whitespace() || c == ',' || c == '}')
            .unwrap_or(rest.len());
        let value = &rest[..end];
        (!value.is_empty()).then(|| value.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeZone, Utc};

    fn bounds() -> TimeBounds {
        TimeBounds {
            from: Utc.with_ymd_and_hms(1970, 1, 1, 0, 0, 0).unwrap(),
            to: Utc.with_ymd_and_hms(1970, 1, 1, 1, 0, 0).unwrap(),
        }
    }

    #[test]
    fn tempo_search_params_and_traceql_filters_normalize_to_promoted_fields() {
        let params = HashMap::from([
            ("serviceName".to_string(), "checkout".to_string()),
            (
                "q".to_string(),
                r#"{ span.name = "GET /smoke" && http.route = "/smoke" }"#.to_string(),
            ),
        ]);
        let plan = plan_tempo_search(&params, bounds(), 20).unwrap();
        let TracePlan { selector, .. } = plan;

        assert_eq!(selector.matcher_value("service_name"), Some("checkout"));
        assert_eq!(selector.matcher_value("span_name"), Some("GET /smoke"));
        assert_eq!(selector.matcher_value("http_route"), Some("/smoke"));
    }

    #[test]
    fn tempo_search_conflicting_aliases_fail_closed() {
        let params = HashMap::from([
            ("service.name".to_string(), "checkout".to_string()),
            ("serviceName".to_string(), "payments".to_string()),
        ]);
        let err = plan_tempo_search(&params, bounds(), 20).unwrap_err();

        assert_eq!(err.reason, "unsupported_selector");
    }
}
