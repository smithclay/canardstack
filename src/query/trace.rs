use crate::query::plan::{
    normalize_matchers, FieldMatcher, MatchOp, SelectorPlan, TimeBounds, TracePlan, TraceSort,
};
use crate::semantic_labels::{self, LabelScope};
use crate::validation::ApiResult;
use std::collections::HashMap;

pub fn plan_tempo_search(
    params: &HashMap<String, String>,
    time_bounds: TimeBounds,
    limit: usize,
) -> ApiResult<TracePlan> {
    let mut matchers = Vec::new();
    let aliases = semantic_labels::alias_pairs(LabelScope::Spans);
    for (param, _) in &aliases {
        if let Some(value) = params.get(*param).filter(|v| !v.is_empty()) {
            matchers.push(FieldMatcher {
                field: (*param).to_string(),
                value: value.to_string(),
                op: MatchOp::Eq,
            });
        }
    }
    if let Some(q) = params.get("q").or_else(|| params.get("query")) {
        extract_traceql_matchers(q, &mut matchers);
    }
    if let Some(tags) = params.get("tags") {
        extract_traceql_matchers(tags, &mut matchers);
    }
    let matchers = normalize_matchers(matchers, &aliases, "unsupported_selector")?;
    Ok(TracePlan {
        selector: SelectorPlan {
            resource: None,
            matchers,
            text_filters: Vec::new(),
        },
        time_bounds,
        limit,
        sort: TraceSort::TimestampDesc,
    })
}

fn extract_traceql_matchers(raw: &str, labels: &mut Vec<FieldMatcher>) {
    for (tag, _) in semantic_labels::alias_pairs(LabelScope::Spans) {
        if let Some((op, value)) = extract_tag_matcher(raw, tag) {
            labels.push(FieldMatcher {
                field: tag.to_string(),
                value,
                op,
            });
        }
    }
}

fn extract_tag_matcher(raw: &str, tag: &str) -> Option<(MatchOp, String)> {
    let marker_start = raw.match_indices(tag).find_map(|(index, _)| {
        let is_tag_boundary = raw[..index]
            .chars()
            .next_back()
            .map(|c| !(c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | '.')))
            .unwrap_or(true);
        let rest = raw[index + tag.len()..].trim_start();
        (is_tag_boundary
            && (rest.starts_with('=')
                || rest.starts_with("!=")
                || rest.starts_with("=~")
                || rest.starts_with("!~")))
        .then_some(index)
    })?;
    let rest = raw[marker_start + tag.len()..].trim_start();
    let (op, rest) = if let Some(rest) = rest.strip_prefix("!=") {
        (MatchOp::NotEq, rest)
    } else if let Some(rest) = rest.strip_prefix("=~") {
        (MatchOp::Regex, rest)
    } else if let Some(rest) = rest.strip_prefix("!~") {
        (MatchOp::NotRegex, rest)
    } else {
        (MatchOp::Eq, rest.strip_prefix('=')?)
    };
    let rest = rest.trim_start();
    if let Some(rest) = rest.strip_prefix('"') {
        let end = rest.find('"')?;
        Some((op, rest[..end].to_string()))
    } else {
        let end = rest
            .find(|c: char| c.is_whitespace() || matches!(c, ',' | '}' | '&' | '|'))
            .unwrap_or(rest.len());
        let value = &rest[..end];
        (!value.is_empty()).then(|| (op, value.to_string()))
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
