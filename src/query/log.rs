use crate::query::plan::{
    normalize_matchers, parse_selector_matchers, unquote, LogPlan, SelectorPlan, SortDirection,
    TextFilter, TimeBounds,
};
use crate::semantic_labels::{self, LabelScope};
use crate::validation::ApiResult;

pub fn parse_loki_query(
    raw: &str,
    time_bounds: TimeBounds,
    limit: usize,
    direction: &str,
) -> ApiResult<LogPlan> {
    let (selector, text_filters) = parse_log_pipeline(raw);
    let (_, matchers) = parse_selector_matchers(selector, "unsupported_selector")?;
    let aliases = semantic_labels::alias_pairs(LabelScope::Logs);
    let matchers = normalize_matchers(matchers, &aliases, "unsupported_selector")?;
    Ok(LogPlan {
        selector: SelectorPlan {
            resource: None,
            matchers,
            text_filters,
        },
        time_bounds,
        limit,
        direction: SortDirection::from_loki(direction),
        stream_labels: semantic_labels::loki_stream_labels(),
    })
}

fn parse_log_pipeline(raw: &str) -> (&str, Vec<TextFilter>) {
    let contains = raw.find("|=");
    let regex = raw.find("|~");
    let Some((idx, regex_filter)) = (match (contains, regex) {
        (Some(contains), Some(regex)) if regex < contains => Some((regex, true)),
        (Some(contains), Some(_)) | (Some(contains), None) => Some((contains, false)),
        (None, Some(regex)) => Some((regex, true)),
        (None, None) => None,
    }) else {
        return (raw.trim(), Vec::new());
    };
    let selector = raw[..idx].trim();
    let term = unquote(raw[idx + 2..].trim()).to_string();
    let filter = if regex_filter {
        TextFilter::BodyRegex(term)
    } else {
        TextFilter::BodyContains(term)
    };
    (selector, vec![filter])
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

    #[test]
    fn loki_selector_supports_regex_and_resource_aliases() {
        let plan = parse_loki_query(
            r#"{resource.service.name=~"front.*",resource.service.namespace="demo",severity.text!="DEBUG"} |~ "GET|POST""#,
            bounds(),
            100,
            "backward",
        )
        .unwrap();

        assert_eq!(
            plan.selector.matcher_value("service_namespace"),
            Some("demo")
        );
        assert!(matches!(
            plan.selector.text_filters.as_slice(),
            [TextFilter::BodyRegex(term)] if term == "GET|POST"
        ));
    }

    #[test]
    fn loki_selector_keeps_commas_inside_quoted_regex_matchers() {
        let plan = parse_loki_query(
            r#"{service.name=~"frontend,checkout",severity.text!="DEBUG"}"#,
            bounds(),
            100,
            "backward",
        )
        .unwrap();

        let matcher = plan
            .selector
            .matchers
            .iter()
            .find(|matcher| matcher.field == "service_name")
            .unwrap();
        assert_eq!(matcher.value, "frontend,checkout");
        assert_eq!(matcher.op, crate::query::plan::MatchOp::Regex);
    }
}
