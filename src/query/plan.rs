use crate::signal::StorageSignal;
use crate::validation::{ApiError, ApiResult};
use chrono::{DateTime, Utc};
use std::collections::BTreeMap;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SortDirection {
    Forward,
    Backward,
}

impl SortDirection {
    pub fn from_loki(raw: &str) -> Self {
        if raw == "forward" {
            Self::Forward
        } else {
            Self::Backward
        }
    }

    pub fn from_order(raw: Option<&str>) -> Self {
        if raw == Some("desc") {
            Self::Backward
        } else {
            Self::Forward
        }
    }

    pub fn sql(self) -> &'static str {
        match self {
            Self::Forward => "ASC",
            Self::Backward => "DESC",
        }
    }

    pub fn is_forward(self) -> bool {
        matches!(self, Self::Forward)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MatchOp {
    Eq,
    NotEq,
    Regex,
    NotRegex,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FieldMatcher {
    pub field: String,
    pub value: String,
    pub op: MatchOp,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TextFilter {
    BodyContains(String),
    BodyRegex(String),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SelectorPlan {
    pub resource: Option<String>,
    pub matchers: Vec<FieldMatcher>,
    pub text_filters: Vec<TextFilter>,
}

impl SelectorPlan {
    pub fn matcher_value(&self, field: &str) -> Option<&str> {
        self.matchers
            .iter()
            .find(|matcher| matcher.field == field)
            .map(|matcher| matcher.value.as_str())
    }
}

#[derive(Clone, Copy, Debug)]
pub struct TimeBounds {
    pub from: DateTime<Utc>,
    pub to: DateTime<Utc>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MetricSignal {
    Gauge,
    Sum,
}

impl MetricSignal {
    pub fn parse(raw: &str) -> ApiResult<Self> {
        match raw {
            "gauge" => Ok(Self::Gauge),
            "sum" => Ok(Self::Sum),
            _ => Err(ApiError::new(
                400,
                "unsupported_signal",
                "signal must be gauge or sum",
            )),
        }
    }

    pub fn storage_signal(self) -> StorageSignal {
        match self {
            Self::Gauge => StorageSignal::MetricGauge,
            Self::Sum => StorageSignal::MetricSum,
        }
    }

    pub fn table(self) -> &'static str {
        self.storage_signal().as_str()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MetricAggregation {
    Avg,
    Min,
    Max,
    Sum,
    Count,
    Rate,
}

impl MetricAggregation {
    pub fn parse(raw: &str) -> ApiResult<Self> {
        match raw {
            "avg" => Ok(Self::Avg),
            "min" => Ok(Self::Min),
            "max" => Ok(Self::Max),
            "sum" => Ok(Self::Sum),
            "count" => Ok(Self::Count),
            "rate" => Ok(Self::Rate),
            _ => Err(ApiError::new(
                400,
                "unsupported_aggregation",
                "unsupported aggregation",
            )),
        }
    }
}

#[derive(Clone, Debug)]
pub struct MetricPlan {
    pub selector: SelectorPlan,
    pub time_bounds: TimeBounds,
    pub signal: MetricSignal,
    pub aggregation: MetricAggregation,
    pub group_by: Vec<String>,
    pub step_seconds: i64,
    pub limit: usize,
    pub order: SortDirection,
}

#[derive(Clone, Debug)]
pub struct LogPlan {
    pub selector: SelectorPlan,
    pub time_bounds: TimeBounds,
    pub limit: usize,
    pub direction: SortDirection,
    pub stream_labels: Vec<String>,
}

#[derive(Clone, Debug)]
pub struct TracePlan {
    pub selector: SelectorPlan,
    pub time_bounds: TimeBounds,
    pub limit: usize,
    pub sort: TraceSort,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TraceSort {
    TimestampDesc,
    DurationDesc,
}

pub fn parse_selector(
    raw: &str,
    reason: &'static str,
) -> ApiResult<(String, BTreeMap<String, String>)> {
    let (resource, matchers) = parse_selector_matchers(raw, reason)?;
    let mut labels = BTreeMap::new();
    for matcher in matchers {
        if matcher.op != MatchOp::Eq {
            return Err(ApiError::new(
                400,
                reason,
                "regex and negative label filters are not supported",
            ));
        }
        labels.insert(matcher.field, matcher.value);
    }
    Ok((resource, labels))
}

pub fn parse_selector_matchers(
    raw: &str,
    reason: &'static str,
) -> ApiResult<(String, Vec<FieldMatcher>)> {
    let raw = raw.trim();
    if let Some((name, rest)) = raw.split_once('{') {
        let label_part = rest
            .strip_suffix('}')
            .ok_or_else(|| ApiError::new(400, "invalid_selector", "selector must end with }"))?;
        let labels = parse_label_matchers(label_part, reason)?;
        Ok((name.trim().to_string(), labels))
    } else if raw.starts_with('{') {
        let label_part = raw
            .strip_prefix('{')
            .and_then(|s| s.strip_suffix('}'))
            .ok_or_else(|| ApiError::new(400, "invalid_selector", "selector must be {...}"))?;
        Ok(("".to_string(), parse_label_matchers(label_part, reason)?))
    } else if !raw.is_empty() && raw.chars().all(is_ident_char) {
        Ok((raw.to_string(), Vec::new()))
    } else {
        Err(ApiError::new(
            400,
            reason,
            "supported selector subset is name, name{label=\"value\"}, or {label=\"value\"}",
        ))
    }
}

pub fn normalize_matchers(
    matchers: Vec<FieldMatcher>,
    supported: &[(&str, &str)],
    reason: &'static str,
) -> ApiResult<Vec<FieldMatcher>> {
    let mut normalized = Vec::new();
    for matcher in matchers {
        let Some((_, canonical)) = supported.iter().find(|(raw, _)| *raw == matcher.field) else {
            return Err(ApiError::new(
                400,
                reason,
                format!("unsupported label {} in v0 selector", matcher.field),
            ));
        };
        if matcher.op == MatchOp::Eq
            && normalized.iter().any(|existing: &FieldMatcher| {
                existing.field == *canonical
                    && existing.op == MatchOp::Eq
                    && existing.value != matcher.value
            })
        {
            return Err(ApiError::new(
                400,
                reason,
                format!("conflicting values for label {canonical}"),
            ));
        }
        normalized.push(FieldMatcher {
            field: (*canonical).to_string(),
            value: matcher.value,
            op: matcher.op,
        });
    }
    Ok(normalized)
}

pub fn normalize_labels(
    labels: BTreeMap<String, String>,
    supported: &[(&str, &str)],
    reason: &'static str,
) -> ApiResult<BTreeMap<String, String>> {
    let mut normalized = BTreeMap::new();
    for (key, value) in labels {
        let Some((_, canonical)) = supported.iter().find(|(raw, _)| *raw == key) else {
            return Err(ApiError::new(
                400,
                reason,
                format!("unsupported label {key} in v0 selector"),
            ));
        };
        if let Some(existing) = normalized.get(*canonical) {
            if existing != &value {
                return Err(ApiError::new(
                    400,
                    reason,
                    format!("conflicting values for label {canonical}"),
                ));
            }
        }
        normalized.insert((*canonical).to_string(), value);
    }
    Ok(normalized)
}

pub fn matchers_from_labels(labels: BTreeMap<String, String>) -> Vec<FieldMatcher> {
    labels
        .into_iter()
        .map(|(field, value)| FieldMatcher {
            field,
            value,
            op: MatchOp::Eq,
        })
        .collect()
}

pub fn unquote(value: &str) -> &str {
    value.trim().trim_matches('"')
}

pub fn is_ident_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || matches!(c, '_' | ':' | '.')
}

fn parse_label_matchers(raw: &str, reason: &'static str) -> ApiResult<Vec<FieldMatcher>> {
    let mut labels = Vec::new();
    for part in split_label_matchers(raw, reason)? {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        let (op, key, value) = if let Some((key, value)) = part.split_once("!~") {
            (MatchOp::NotRegex, key, value)
        } else if let Some((key, value)) = part.split_once("=~") {
            (MatchOp::Regex, key, value)
        } else if let Some((key, value)) = part.split_once("!=") {
            (MatchOp::NotEq, key, value)
        } else if let Some((key, value)) = part.split_once('=') {
            (MatchOp::Eq, key, value)
        } else {
            return Err(ApiError::new(
                400,
                reason,
                "label filters must use =, !=, =~, or !~",
            ));
        };
        labels.push(FieldMatcher {
            field: key.trim().to_string(),
            value: unquote(value.trim()).to_string(),
            op,
        });
    }
    Ok(labels)
}

fn split_label_matchers<'a>(raw: &'a str, reason: &'static str) -> ApiResult<Vec<&'a str>> {
    let mut parts = Vec::new();
    let mut start = 0;
    let mut in_quote = false;
    let mut escaped = false;
    for (idx, ch) in raw.char_indices() {
        if escaped {
            escaped = false;
            continue;
        }
        match ch {
            '\\' if in_quote => escaped = true,
            '"' => in_quote = !in_quote,
            ',' if !in_quote => {
                parts.push(&raw[start..idx]);
                start = idx + ch.len_utf8();
            }
            _ => {}
        }
    }
    if in_quote {
        return Err(ApiError::new(
            400,
            reason,
            "unterminated quoted label value",
        ));
    }
    parts.push(&raw[start..]);
    Ok(parts)
}
