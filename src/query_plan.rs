use crate::validation::{self, ApiError, ApiResult};
use chrono::{DateTime, Duration, Utc};
use std::collections::BTreeMap;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum QuerySurface {
    PrometheusInstant,
    PrometheusRange,
    PrometheusMetadata,
    LokiInstant,
    LokiRange,
    LokiMetadata,
    TempoTraceById,
    TempoSearch,
    TempoTags,
    TempoTagValues,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SignalKind {
    Metrics,
    Logs,
    Traces,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum QueryLane {
    Interactive,
    Background,
}

impl QueryLane {
    pub fn is_background(self) -> bool {
        matches!(self, Self::Background)
    }
}

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
pub struct FieldMatcher {
    pub field: String,
    pub value: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TextFilter {
    BodyContains(String),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SelectorPlan {
    pub signal: SignalKind,
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

#[derive(Clone, Debug)]
pub struct TimeBounds {
    pub from: DateTime<Utc>,
    pub to: DateTime<Utc>,
    pub max_range_secs: i64,
    pub default_lookback: Option<Duration>,
    pub instant: bool,
}

impl TimeBounds {
    pub fn new(
        from: DateTime<Utc>,
        to: DateTime<Utc>,
        max_range_secs: i64,
        default_lookback: Option<Duration>,
        instant: bool,
    ) -> ApiResult<Self> {
        validation::validate_range(from, to, max_range_secs)?;
        Ok(Self {
            from,
            to,
            max_range_secs,
            default_lookback,
            instant,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryLimitPlan {
    pub limit: usize,
    pub max_limit: usize,
    pub max_groups: Option<usize>,
    pub query_text_len: usize,
    pub lane: QueryLane,
}

#[derive(Clone, Debug)]
pub enum SignalQueryPlan {
    Metric(MetricPlan),
    Logs(LogPlan),
    Traces(TracePlan),
    Metadata(MetadataPlan),
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

    pub fn table(self) -> &'static str {
        match self {
            Self::Gauge => "metric_gauge",
            Self::Sum => "metric_sum",
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Gauge => "gauge",
            Self::Sum => "sum",
        }
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

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Avg => "avg",
            Self::Min => "min",
            Self::Max => "max",
            Self::Sum => "sum",
            Self::Count => "count",
            Self::Rate => "rate",
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
    pub lane: QueryLane,
}

#[derive(Clone, Debug)]
pub struct LogPlan {
    pub selector: SelectorPlan,
    pub time_bounds: TimeBounds,
    pub limit: usize,
    pub direction: SortDirection,
    pub lane: QueryLane,
    pub stream_labels: Vec<String>,
}

#[derive(Clone, Debug)]
pub enum TracePlan {
    Search {
        selector: SelectorPlan,
        time_bounds: TimeBounds,
        limit: usize,
        sort: TraceSort,
        lane: QueryLane,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TraceSort {
    TimestampDesc,
    DurationDesc,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MetadataPlan {
    pub surface: QuerySurface,
    pub selector: Option<SelectorPlan>,
    pub limit: usize,
    pub lane: QueryLane,
}

pub fn parse_selector(
    raw: &str,
    reason: &'static str,
) -> ApiResult<(String, BTreeMap<String, String>)> {
    let raw = raw.trim();
    if let Some((name, rest)) = raw.split_once('{') {
        let label_part = rest
            .strip_suffix('}')
            .ok_or_else(|| ApiError::new(400, "invalid_selector", "selector must end with }"))?;
        let labels = parse_labels(label_part, reason)?;
        Ok((name.trim().to_string(), labels))
    } else if raw.starts_with('{') {
        let label_part = raw
            .strip_prefix('{')
            .and_then(|s| s.strip_suffix('}'))
            .ok_or_else(|| ApiError::new(400, "invalid_selector", "selector must be {...}"))?;
        Ok(("".to_string(), parse_labels(label_part, reason)?))
    } else if !raw.is_empty() && raw.chars().all(is_ident_char) {
        Ok((raw.to_string(), BTreeMap::new()))
    } else {
        Err(ApiError::new(
            400,
            reason,
            "supported selector subset is name, name{label=\"value\"}, or {label=\"value\"}",
        ))
    }
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
        .map(|(field, value)| FieldMatcher { field, value })
        .collect()
}

pub fn unquote(value: &str) -> &str {
    value.trim().trim_matches('"')
}

pub fn is_ident_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || matches!(c, '_' | ':' | '.')
}

fn parse_labels(raw: &str, reason: &'static str) -> ApiResult<BTreeMap<String, String>> {
    let mut labels = BTreeMap::new();
    for part in raw.split(',').map(str::trim).filter(|s| !s.is_empty()) {
        let (key, value) = part.split_once('=').ok_or_else(|| {
            ApiError::new(400, reason, "only equality label filters are supported")
        })?;
        if key.ends_with('!') || key.ends_with('~') || part.contains("=~") || part.contains("!=") {
            return Err(ApiError::new(
                400,
                reason,
                "regex and negative label filters are not supported",
            ));
        }
        labels.insert(key.trim().to_string(), unquote(value.trim()).to_string());
    }
    Ok(labels)
}
