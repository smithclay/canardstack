use crate::query::plan::{
    is_ident_char, normalize_matchers, parse_selector_matchers, FieldMatcher, MatchOp,
};
use crate::semantic_labels::{self, LabelScope};
use crate::validation::{ApiError, ApiResult};

#[derive(Debug)]
pub struct PromQuery {
    pub metric_name: String,
    pub signal: &'static str,
    pub aggregation: &'static str,
    pub filters: Vec<FieldMatcher>,
    pub group_by: Vec<String>,
    pub explicit_grouping: bool,
}

pub fn parse_prom_query(raw: &str) -> ApiResult<PromQuery> {
    let mut query = strip_outer_parens(raw.trim());
    let mut aggregation = "avg";
    let mut group_by = Vec::new();
    let mut explicit_grouping = false;

    if let Some(expr) = parse_aggregation(query)? {
        aggregation = expr.aggregation;
        group_by = expr.group_by;
        explicit_grouping = expr.explicit_grouping;
        query = strip_outer_parens(expr.inner);
    } else if let Some((func, inner)) = unwrap_func(query) {
        aggregation = aggregation_keyword(func).ok_or_else(unsupported_promql)?;
        query = strip_outer_parens(inner);
    }

    if let Some((func, inner)) = unwrap_func(query) {
        aggregation = aggregation_keyword(func).ok_or_else(unsupported_promql)?;
        query = strip_outer_parens(inner);
    }

    query = strip_range_selector(query);
    let (mut metric_name, mut filters) = parse_selector_matchers(query, "unsupported_promql")?;
    if metric_name.is_empty() {
        let name_idx = filters
            .iter()
            .position(|matcher| matcher.field == "__name__" && matcher.op == MatchOp::Eq)
            .ok_or_else(|| {
                ApiError::new(
                    400,
                    "unsupported_promql",
                    "metric queries require a metric name or __name__ label",
                )
            })?;
        metric_name = filters.remove(name_idx).value;
    }
    let metric = normalize_metric_name(&metric_name, aggregation);
    metric_name = metric.name;
    let metric_label_aliases = semantic_labels::alias_pairs(LabelScope::Metrics);
    let filters = normalize_matchers(filters, &metric_label_aliases, "unsupported_promql")?;
    let group_by = group_by
        .into_iter()
        .map(|label| normalize_group_label(&label).map(str::to_string))
        .collect::<ApiResult<Vec<_>>>()?;
    Ok(PromQuery {
        metric_name,
        signal: metric.signal,
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
    let mut rest = rest.trim_start();
    let mut labels = Vec::new();
    let mut without = false;
    let mut explicit_grouping = false;
    if rest.starts_with("by") || rest.starts_with("without") {
        without = rest.starts_with("without");
        rest = rest
            .strip_prefix("by")
            .or_else(|| rest.strip_prefix("without"))
            .unwrap()
            .trim_start();
        let parsed;
        (parsed, rest) = parse_label_list(rest)?;
        labels = parsed;
        explicit_grouping = true;
    }
    let rest = rest.trim_start();
    let (inner, suffix) = parenthesized_with_suffix(rest).ok_or_else(unsupported_promql)?;
    let suffix = suffix.trim_start();
    if !explicit_grouping && (suffix.starts_with("by") || suffix.starts_with("without")) {
        without = suffix.starts_with("without");
        let suffix = suffix
            .strip_prefix("by")
            .or_else(|| suffix.strip_prefix("without"))
            .unwrap()
            .trim_start();
        let parsed;
        let label_rest;
        (parsed, label_rest) = parse_label_list(suffix)?;
        labels = parsed;
        explicit_grouping = true;
        if !label_rest.trim().is_empty() {
            return Err(unsupported_promql());
        }
    } else if !suffix.is_empty() {
        return Err(unsupported_promql());
    }

    let group_by = if without {
        semantic_labels::prometheus_grouping_labels()
            .into_iter()
            .filter(|label| !labels.iter().any(|removed| removed == *label))
            .map(str::to_string)
            .collect()
    } else {
        labels
    };

    Ok(Some(AggregationExpr {
        aggregation,
        group_by,
        explicit_grouping,
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
    semantic_labels::canonical_for_alias(LabelScope::Metrics, label).ok_or_else(|| {
        ApiError::new(
            400,
            "unsupported_promql",
            format!("unsupported grouping label {label} in v0 PromQL subset"),
        )
    })
}

fn parenthesized_with_suffix(raw: &str) -> Option<(&str, &str)> {
    let raw = raw.strip_prefix('(')?;
    let mut depth = 1_i32;
    for (idx, c) in raw.char_indices() {
        match c {
            '(' => depth += 1,
            ')' => {
                depth -= 1;
                if depth == 0 {
                    return Some((raw[..idx].trim(), raw[idx + 1..].trim()));
                }
            }
            _ => {}
        }
    }
    None
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

fn strip_outer_parens(raw: &str) -> &str {
    let mut current = raw.trim();
    loop {
        let Some((inner, suffix)) = parenthesized_with_suffix(current) else {
            return current;
        };
        if suffix.is_empty() {
            current = inner;
        } else {
            return current;
        }
    }
}

fn strip_range_selector(raw: &str) -> &str {
    let raw = raw.trim();
    if let Some(end) = raw.strip_suffix(']').and_then(|s| s.rfind('[')) {
        raw[..end].trim()
    } else {
        raw
    }
}

struct MetricDef {
    name: String,
    signal: &'static str,
}

fn normalize_metric_name(raw: &str, aggregation: &str) -> MetricDef {
    let name = resolve_metric_name(raw);
    let signal = if aggregation == "rate" || raw.ends_with("_total") || name.ends_with(".sum") {
        "sum"
    } else {
        "gauge"
    };
    MetricDef { name, signal }
}

const METRIC_NAME_OVERRIDES: &[(&str, &str)] = &[
    (
        "process_runtime_cpython_cpu_time_seconds_total",
        "process.runtime.cpython.cpu_time",
    ),
    (
        "otelcol_process_uptime_seconds_total",
        "otelcol_process_uptime",
    ),
    (
        "demo_recommendation_requests_total",
        "app_recommendations_counter",
    ),
];

fn resolve_metric_name(raw: &str) -> String {
    if let Some((_, resolved)) = METRIC_NAME_OVERRIDES
        .iter()
        .find(|(prometheus, _)| *prometheus == raw)
    {
        return (*resolved).to_string();
    }
    let without_total = raw.strip_suffix("_total").unwrap_or(raw);
    if raw.starts_with("otelcol_exporter_") || raw.starts_with("otelcol_process_") {
        return without_total.to_string();
    }
    if raw.starts_with("traces_span_metrics_")
        || raw.starts_with("system_")
        || raw.starts_with("process_runtime_")
    {
        return strip_prometheus_unit_suffix(without_total).replace('_', ".");
    }
    raw.to_string()
}

fn strip_prometheus_unit_suffix(raw: &str) -> &str {
    [
        "_seconds",
        "_milliseconds",
        "_bytes",
        "_ratio",
        "_count",
        "_total",
    ]
    .into_iter()
    .find_map(|suffix| raw.strip_suffix(suffix))
    .unwrap_or(raw)
}

fn unsupported_promql() -> ApiError {
    ApiError::new(
        400,
        "unsupported_promql",
        "supported PromQL subset is metric selectors plus avg/min/max/sum/count/rate(metric) and avg/min/max/sum/count by/without supported labels",
    )
}
