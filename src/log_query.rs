use crate::query_plan::{
    matchers_from_labels, normalize_labels, parse_selector, unquote, LogPlan, SelectorPlan,
    SortDirection, TextFilter, TimeBounds,
};
use crate::validation::ApiResult;

const LOG_LABEL_ALIASES: &[(&str, &str)] = &[
    ("service_name", "service_name"),
    ("service.name", "service_name"),
    ("deployment_environment", "deployment_environment"),
    ("deployment.environment", "deployment_environment"),
    ("severity_text", "severity_text"),
    ("trace_id", "trace_id"),
    ("span_id", "span_id"),
    ("http_route", "http_route"),
    ("http.route", "http_route"),
    ("http_method", "http_method"),
    ("http.method", "http_method"),
];

const LOKI_STREAM_LABELS: &[&str] = &[
    "service_name",
    "deployment_environment",
    "severity_text",
    "trace_id",
    "span_id",
    "http_route",
];

pub fn parse_loki_query(
    raw: &str,
    time_bounds: TimeBounds,
    limit: usize,
    direction: &str,
) -> ApiResult<LogPlan> {
    let (selector, contains) = raw
        .split_once("|=")
        .map(|(left, right)| (left.trim(), Some(unquote(right.trim()).to_string())))
        .unwrap_or((raw.trim(), None));
    let (_, labels) = parse_selector(selector, "unsupported_selector")?;
    let labels = normalize_labels(labels, LOG_LABEL_ALIASES, "unsupported_selector")?;
    let mut text_filters = Vec::new();
    if let Some(contains) = contains {
        text_filters.push(TextFilter::BodyContains(contains));
    }
    Ok(LogPlan {
        selector: SelectorPlan {
            resource: None,
            matchers: matchers_from_labels(labels),
            text_filters,
        },
        time_bounds,
        limit,
        direction: SortDirection::from_loki(direction),
        stream_labels: LOKI_STREAM_LABELS
            .iter()
            .map(|label| (*label).to_string())
            .collect(),
    })
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
    fn loki_selector_aliases_normalize_to_promoted_fields() {
        let plan = parse_loki_query(
            r#"{service.name="checkout",http.route="/smoke"} |= "timeout""#,
            bounds(),
            100,
            "forward",
        )
        .unwrap();

        assert_eq!(
            plan.selector.matcher_value("service_name"),
            Some("checkout")
        );
        assert_eq!(plan.selector.matcher_value("http_route"), Some("/smoke"));
        assert!(matches!(
            plan.selector.text_filters.as_slice(),
            [TextFilter::BodyContains(term)] if term == "timeout"
        ));
        assert_eq!(plan.direction, SortDirection::Forward);
    }

    #[test]
    fn loki_selector_conflicting_aliases_fail_closed() {
        let err = parse_loki_query(
            r#"{service.name="checkout",service_name="payments"}"#,
            bounds(),
            100,
            "backward",
        )
        .unwrap_err();

        assert_eq!(err.reason, "unsupported_selector");
    }
}
