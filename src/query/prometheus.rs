use crate::query::plan::{is_ident_char, normalize_labels, parse_selector};
use crate::validation::{ApiError, ApiResult};
use std::collections::BTreeMap;

const PROMOTED_METRIC_LABELS: &[&str] = &["service_name", "deployment_environment"];

#[derive(Debug)]
pub struct PromQuery {
    pub metric_name: String,
    pub signal: &'static str,
    pub aggregation: &'static str,
    pub filters: BTreeMap<String, String>,
    pub group_by: Vec<String>,
    pub explicit_grouping: bool,
}

pub fn parse_prom_query(raw: &str) -> ApiResult<PromQuery> {
    let mut query = raw.trim();
    let mut aggregation = "avg";
    let mut group_by = Vec::new();
    let mut explicit_grouping = false;

    if let Some(expr) = parse_aggregation(query)? {
        aggregation = expr.aggregation;
        group_by = expr.group_by;
        explicit_grouping = expr.explicit_grouping;
        query = expr.inner;
    } else if let Some((func, inner)) = unwrap_func(query) {
        aggregation = aggregation_keyword(func).ok_or_else(unsupported_promql)?;
        query = inner;
    }

    let (mut metric_name, mut filters) = parse_selector(query, "unsupported_promql")?;
    if metric_name.is_empty() {
        metric_name = filters.remove("__name__").ok_or_else(|| {
            ApiError::new(
                400,
                "unsupported_promql",
                "metric queries require a metric name or __name__ label",
            )
        })?;
    }
    let filters = normalize_labels(
        filters,
        &[
            ("service_name", "service_name"),
            ("service.name", "service_name"),
            ("deployment_environment", "deployment_environment"),
            ("deployment.environment", "deployment_environment"),
        ],
        "unsupported_promql",
    )?;
    let signal = if aggregation == "rate"
        || metric_name.ends_with(".sum")
        || metric_name.ends_with("_total")
    {
        "sum"
    } else {
        "gauge"
    };
    Ok(PromQuery {
        metric_name,
        signal,
        aggregation,
        filters,
        group_by,
        explicit_grouping,
    })
}

struct AggregationExpr<'a> {
    aggregation: &'static str,
    group_by: Vec<String>,
    explicit_grouping: bool,
    inner: &'a str,
}

fn parse_aggregation(query: &str) -> ApiResult<Option<AggregationExpr<'_>>> {
    let Some((func, rest)) = split_ident(query) else {
        return Ok(None);
    };
    let aggregation = match aggregation_keyword(func) {
        Some("rate") | None => return Ok(None),
        Some(agg) => agg,
    };
    let rest = rest.trim_start();
    let Some(rest) = rest
        .strip_prefix("by")
        .or_else(|| rest.strip_prefix("without"))
    else {
        return Ok(None);
    };
    let without = query[func.len()..].trim_start().starts_with("without");
    let rest = rest.trim_start();
    let (labels, rest) = parse_label_list(rest)?;
    let rest = rest.trim_start();
    let inner = parenthesized(rest).ok_or_else(unsupported_promql)?;

    let group_by = if without {
        PROMOTED_METRIC_LABELS
            .iter()
            .filter(|label| !labels.iter().any(|removed| removed == **label))
            .map(|label| (*label).to_string())
            .collect()
    } else {
        labels
    };

    Ok(Some(AggregationExpr {
        aggregation,
        group_by,
        explicit_grouping: true,
        inner,
    }))
}

fn aggregation_keyword(func: &str) -> Option<&'static str> {
    Some(match func {
        "avg" => "avg",
        "min" => "min",
        "max" => "max",
        "sum" => "sum",
        "count" => "count",
        "rate" => "rate",
        _ => return None,
    })
}

fn split_ident(raw: &str) -> Option<(&str, &str)> {
    let end = raw
        .char_indices()
        .find_map(|(idx, c)| (!is_ident_char(c)).then_some(idx))?;
    (end > 0).then(|| (&raw[..end], &raw[end..]))
}

fn parse_label_list(raw: &str) -> ApiResult<(Vec<String>, &str)> {
    let inner = raw
        .strip_prefix('(')
        .and_then(|s| s.split_once(')'))
        .ok_or_else(unsupported_promql)?;
    let mut labels = Vec::new();
    for label in inner.0.split(',').map(str::trim).filter(|s| !s.is_empty()) {
        let normalized = normalize_group_label(label)?;
        if !labels.iter().any(|existing| existing == normalized) {
            labels.push(normalized.to_string());
        }
    }
    Ok((labels, inner.1))
}

fn normalize_group_label(label: &str) -> ApiResult<&'static str> {
    match label {
        "service_name" | "service.name" => Ok("service_name"),
        "deployment_environment" | "deployment.environment" => Ok("deployment_environment"),
        _ => Err(ApiError::new(
            400,
            "unsupported_promql",
            format!("unsupported grouping label {label} in v0 PromQL subset"),
        )),
    }
}

fn parenthesized(raw: &str) -> Option<&str> {
    raw.strip_prefix('(')?.strip_suffix(')').map(str::trim)
}

fn unwrap_func(query: &str) -> Option<(&str, &str)> {
    let open = query.find('(')?;
    let close = query.rfind(')')?;
    if close == query.len() - 1 {
        Some((query[..open].trim(), query[open + 1..close].trim()))
    } else {
        None
    }
}

fn unsupported_promql() -> ApiError {
    ApiError::new(
        400,
        "unsupported_promql",
        "supported PromQL subset is metric selectors plus avg/min/max/sum/count/rate(metric) and avg/min/max/sum/count by/without supported labels",
    )
}
