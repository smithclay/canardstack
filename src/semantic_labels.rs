use crate::db::sql::{
    json_attr, logs_deployment_environment_expr, logs_http_method_expr, logs_http_route_expr,
    logs_http_status_code_expr, metrics_deployment_environment_expr, span_id_hex_expr,
    spans_deployment_environment_expr, spans_http_method_expr, spans_http_route_expr,
    spans_http_status_code_expr, trace_id_hex_expr,
};
use std::collections::BTreeSet;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LabelScope {
    Logs,
    Spans,
    Metrics,
}

#[derive(Clone, Copy)]
pub enum LabelSource {
    Column(&'static str),
    ResourceAttr(&'static str),
    SpanAttr(&'static str),
    MetricAttr(&'static str),
    Sql(fn() -> String),
    Missing,
}

#[derive(Clone, Copy)]
pub struct ScopedLabelSource {
    pub scope: LabelScope,
    pub source: LabelSource,
}

pub struct LabelDef {
    pub canonical: &'static str,
    pub aliases: &'static [&'static str],
    pub sources: &'static [ScopedLabelSource],
    pub loki_stream: bool,
    pub tempo_tag: Option<&'static str>,
    /// Promoted to a first-class Prometheus discovery label (`/labels`,
    /// `/label/<name>/values`, metric metadata). Most metric labels are
    /// filter/group-only and stay `false`.
    pub prom_promoted: bool,
    /// Scopes whose result projection is sourced from this registry. Currently
    /// only `Logs` (its query projection is registry-derived); span and metric
    /// projections remain explicit in their adapters.
    pub project: &'static [LabelScope],
}

impl LabelSource {
    pub fn expr(self) -> String {
        match self {
            Self::Column(column) => column.to_string(),
            Self::ResourceAttr(key) => json_attr("resource_attributes", key),
            Self::SpanAttr(key) => json_attr("span_attributes", key),
            Self::MetricAttr(key) => json_attr("metric_attributes", key),
            Self::Sql(expr) => expr(),
            Self::Missing => "NULL".to_string(),
        }
    }
}

pub fn label_expr(scope: LabelScope, canonical: &str) -> Option<String> {
    label_def(scope, canonical).and_then(|def| {
        def.sources
            .iter()
            .find(|source| source.scope == scope)
            .map(|source| source.source.expr())
    })
}

pub fn alias_pairs(scope: LabelScope) -> Vec<(&'static str, &'static str)> {
    let mut out = Vec::new();
    for label in LABELS {
        if !has_scope(label, scope) {
            continue;
        }
        out.push((label.canonical, label.canonical));
        for alias in label.aliases {
            out.push((*alias, label.canonical));
        }
    }
    out
}

pub fn canonical_for_alias(scope: LabelScope, raw: &str) -> Option<&'static str> {
    label_def(scope, raw)
        .map(|label| label.canonical)
        .or_else(|| {
            LABELS
                .iter()
                .filter(|label| has_scope(label, scope))
                .find_map(|label| label.aliases.contains(&raw).then_some(label.canonical))
        })
}

pub fn metadata_labels(scope: LabelScope) -> Vec<(&'static str, String)> {
    LABELS
        .iter()
        .filter(|label| has_scope(label, scope))
        .filter(|label| metadata_label_allowed(scope, label))
        .filter_map(|label| {
            let metadata_name = match scope {
                LabelScope::Spans => label.tempo_tag?,
                LabelScope::Logs | LabelScope::Metrics => label.canonical,
            };
            label_expr(scope, label.canonical).map(|expr| (metadata_name, expr))
        })
        .collect()
}

pub fn loki_stream_labels() -> Vec<String> {
    LABELS
        .iter()
        .filter(|label| label.loki_stream && has_scope(label, LabelScope::Logs))
        .map(|label| label.canonical.to_string())
        .collect()
}

pub fn loki_label_names() -> Vec<String> {
    LABELS
        .iter()
        .filter(|label| has_scope(label, LabelScope::Logs))
        .filter(|label| label.loki_stream)
        .map(|label| label.canonical.to_string())
        .collect()
}

/// Result-projection columns sourced from the registry for a scope whose query
/// projection is registry-derived. Returns ordered `(output_name, sql_expr)`
/// pairs; the query adapter aliases each `expr` as `output_name` and reads the
/// column back by name. See [`LabelDef::project`].
pub fn projected_labels(scope: LabelScope) -> Vec<(&'static str, String)> {
    LABELS
        .iter()
        .filter(|label| label.project.contains(&scope))
        .filter_map(|label| label_expr(scope, label.canonical).map(|expr| (label.canonical, expr)))
        .collect()
}

pub fn prometheus_label_names() -> Vec<String> {
    std::iter::once("__name__".to_string())
        .chain(
            LABELS
                .iter()
                .filter(|label| has_scope(label, LabelScope::Metrics))
                .filter(|label| label.prom_promoted)
                .map(|label| label.canonical.to_string()),
        )
        .collect()
}

/// Labels whose values are effectively unbounded (per-event or per-replica
/// identifiers). They stay selectable as filters and discoverable as label
/// *names*, but their distinct *values* are never materialized into
/// `metadata_summary` — enumerating them is expensive and no client value
/// dropdown wants the result. This is the cost axis, kept orthogonal to the
/// per-protocol surface flags (`loki_stream` / `tempo_tag` / `prom_promoted`):
/// protocol membership decides what is selectable and name-discoverable;
/// `HIGH_CARDINALITY` independently vetoes value materialization.
const HIGH_CARDINALITY: &[&str] = &["trace_id", "span_id", "service_instance_id"];

/// A label's distinct values are materialized into `metadata_summary` (and so
/// returned by `/label/<name>/values`) when it is part of the scope's protocol
/// surface *and* bounded enough to enumerate. See [`HIGH_CARDINALITY`].
fn metadata_label_allowed(scope: LabelScope, label: &LabelDef) -> bool {
    if HIGH_CARDINALITY.contains(&label.canonical) {
        return false;
    }
    match scope {
        LabelScope::Logs => label.loki_stream,
        LabelScope::Spans => label.tempo_tag.is_some(),
        LabelScope::Metrics => label.prom_promoted,
    }
}

pub fn prometheus_grouping_labels() -> Vec<&'static str> {
    LABELS
        .iter()
        .filter(|label| has_scope(label, LabelScope::Metrics))
        .map(|label| label.canonical)
        .collect()
}

pub fn tempo_tag_names() -> Vec<String> {
    let mut names = BTreeSet::new();
    for label in LABELS
        .iter()
        .filter(|label| has_scope(label, LabelScope::Spans))
    {
        if let Some(tag) = label.tempo_tag {
            names.insert(tag.to_string());
            names.insert(label.canonical.to_string());
            names.extend(label.aliases.iter().map(|alias| (*alias).to_string()));
        }
    }
    names.into_iter().collect()
}

pub fn tempo_tag_name(raw: &str) -> Option<&'static str> {
    LABELS.iter().find_map(|label| {
        label
            .tempo_tag
            .filter(|tag| *tag == raw || label.canonical == raw || label.aliases.contains(&raw))
    })
}

fn has_scope(label: &LabelDef, scope: LabelScope) -> bool {
    label.sources.iter().any(|source| source.scope == scope)
}

fn label_def(scope: LabelScope, canonical: &str) -> Option<&'static LabelDef> {
    LABELS
        .iter()
        .find(|label| label.canonical == canonical && has_scope(label, scope))
}

const SERVICE_NAME_SOURCES: &[ScopedLabelSource] = &[
    ScopedLabelSource {
        scope: LabelScope::Logs,
        source: LabelSource::Column("service_name"),
    },
    ScopedLabelSource {
        scope: LabelScope::Spans,
        source: LabelSource::Column("service_name"),
    },
    ScopedLabelSource {
        scope: LabelScope::Metrics,
        source: LabelSource::Column("service_name"),
    },
];

const SERVICE_NAMESPACE_SOURCES: &[ScopedLabelSource] = &[
    ScopedLabelSource {
        scope: LabelScope::Logs,
        source: LabelSource::Column("service_namespace"),
    },
    ScopedLabelSource {
        scope: LabelScope::Spans,
        source: LabelSource::Column("service_namespace"),
    },
    ScopedLabelSource {
        scope: LabelScope::Metrics,
        source: LabelSource::Column("service_namespace"),
    },
];

const SERVICE_INSTANCE_SOURCES: &[ScopedLabelSource] = &[
    ScopedLabelSource {
        scope: LabelScope::Logs,
        source: LabelSource::Column("service_instance_id"),
    },
    ScopedLabelSource {
        scope: LabelScope::Spans,
        source: LabelSource::Column("service_instance_id"),
    },
    ScopedLabelSource {
        scope: LabelScope::Metrics,
        source: LabelSource::Column("service_instance_id"),
    },
];

const DEPLOYMENT_ENV_SOURCES: &[ScopedLabelSource] = &[
    ScopedLabelSource {
        scope: LabelScope::Logs,
        source: LabelSource::Sql(logs_deployment_environment_expr),
    },
    ScopedLabelSource {
        scope: LabelScope::Spans,
        source: LabelSource::Sql(spans_deployment_environment_expr),
    },
    ScopedLabelSource {
        scope: LabelScope::Metrics,
        source: LabelSource::Sql(metrics_deployment_environment_expr),
    },
];

const HOST_NAME_SOURCES: &[ScopedLabelSource] = &[
    ScopedLabelSource {
        scope: LabelScope::Logs,
        source: LabelSource::ResourceAttr("host.name"),
    },
    ScopedLabelSource {
        scope: LabelScope::Spans,
        source: LabelSource::ResourceAttr("host.name"),
    },
    ScopedLabelSource {
        scope: LabelScope::Metrics,
        source: LabelSource::ResourceAttr("host.name"),
    },
];

const K8S_CLUSTER_SOURCES: &[ScopedLabelSource] = &[
    ScopedLabelSource {
        scope: LabelScope::Logs,
        source: LabelSource::ResourceAttr("k8s.cluster.name"),
    },
    ScopedLabelSource {
        scope: LabelScope::Spans,
        source: LabelSource::ResourceAttr("k8s.cluster.name"),
    },
    ScopedLabelSource {
        scope: LabelScope::Metrics,
        source: LabelSource::ResourceAttr("k8s.cluster.name"),
    },
];

const K8S_NODE_SOURCES: &[ScopedLabelSource] = &[
    ScopedLabelSource {
        scope: LabelScope::Logs,
        source: LabelSource::ResourceAttr("k8s.node.name"),
    },
    ScopedLabelSource {
        scope: LabelScope::Spans,
        source: LabelSource::ResourceAttr("k8s.node.name"),
    },
    ScopedLabelSource {
        scope: LabelScope::Metrics,
        source: LabelSource::ResourceAttr("k8s.node.name"),
    },
];

const K8S_STATEFULSET_SOURCES: &[ScopedLabelSource] = &[
    ScopedLabelSource {
        scope: LabelScope::Logs,
        source: LabelSource::ResourceAttr("k8s.statefulset.name"),
    },
    ScopedLabelSource {
        scope: LabelScope::Spans,
        source: LabelSource::ResourceAttr("k8s.statefulset.name"),
    },
    ScopedLabelSource {
        scope: LabelScope::Metrics,
        source: LabelSource::ResourceAttr("k8s.statefulset.name"),
    },
];

const K8S_DEPLOYMENT_SOURCES: &[ScopedLabelSource] = &[
    ScopedLabelSource {
        scope: LabelScope::Logs,
        source: LabelSource::ResourceAttr("k8s.deployment.name"),
    },
    ScopedLabelSource {
        scope: LabelScope::Spans,
        source: LabelSource::ResourceAttr("k8s.deployment.name"),
    },
    ScopedLabelSource {
        scope: LabelScope::Metrics,
        source: LabelSource::ResourceAttr("k8s.deployment.name"),
    },
];

const LOG_SCOPE_SOURCES: &[ScopedLabelSource] = &[ScopedLabelSource {
    scope: LabelScope::Logs,
    source: LabelSource::Column("scope_name"),
}];

const LOG_SEVERITY_SOURCES: &[ScopedLabelSource] = &[ScopedLabelSource {
    scope: LabelScope::Logs,
    source: LabelSource::Column("severity_text"),
}];

// v2 stores trace/span IDs as BLOB; label matching and projection need them as
// lowercase hex strings to stay round-trippable with client query input, so the
// source is the `lower(hex(...))` SQL expression rather than the raw column.
const TRACE_ID_SOURCES: &[ScopedLabelSource] = &[
    ScopedLabelSource {
        scope: LabelScope::Logs,
        source: LabelSource::Sql(trace_id_hex_expr),
    },
    ScopedLabelSource {
        scope: LabelScope::Spans,
        source: LabelSource::Sql(trace_id_hex_expr),
    },
];

const SPAN_ID_SOURCES: &[ScopedLabelSource] = &[
    ScopedLabelSource {
        scope: LabelScope::Logs,
        source: LabelSource::Sql(span_id_hex_expr),
    },
    ScopedLabelSource {
        scope: LabelScope::Spans,
        source: LabelSource::Sql(span_id_hex_expr),
    },
];

const HTTP_ROUTE_SOURCES: &[ScopedLabelSource] = &[
    ScopedLabelSource {
        scope: LabelScope::Logs,
        source: LabelSource::Sql(logs_http_route_expr),
    },
    ScopedLabelSource {
        scope: LabelScope::Spans,
        source: LabelSource::Sql(spans_http_route_expr),
    },
    ScopedLabelSource {
        scope: LabelScope::Metrics,
        source: LabelSource::MetricAttr("http.route"),
    },
];

const HTTP_METHOD_SOURCES: &[ScopedLabelSource] = &[
    ScopedLabelSource {
        scope: LabelScope::Logs,
        source: LabelSource::Sql(logs_http_method_expr),
    },
    ScopedLabelSource {
        scope: LabelScope::Spans,
        source: LabelSource::Sql(spans_http_method_expr),
    },
    ScopedLabelSource {
        scope: LabelScope::Metrics,
        source: LabelSource::MetricAttr("http.request.method"),
    },
];

const HTTP_STATUS_SOURCES: &[ScopedLabelSource] = &[
    ScopedLabelSource {
        scope: LabelScope::Logs,
        source: LabelSource::Sql(logs_http_status_code_expr),
    },
    ScopedLabelSource {
        scope: LabelScope::Spans,
        source: LabelSource::Sql(spans_http_status_code_expr),
    },
];

const HTTP_RESPONSE_STATUS_METRIC_SOURCES: &[ScopedLabelSource] = &[ScopedLabelSource {
    scope: LabelScope::Metrics,
    source: LabelSource::MetricAttr("http.response.status_code"),
}];

const SPAN_NAME_SOURCES: &[ScopedLabelSource] = &[
    // v2 renamed the span name column from `span_name` to `name`; the public
    // canonical label vocabulary still calls it `span_name`.
    ScopedLabelSource {
        scope: LabelScope::Spans,
        source: LabelSource::Column("name"),
    },
    ScopedLabelSource {
        scope: LabelScope::Metrics,
        source: LabelSource::MetricAttr("span.name"),
    },
];

const STATUS_CODE_SOURCES: &[ScopedLabelSource] = &[
    ScopedLabelSource {
        scope: LabelScope::Spans,
        source: LabelSource::Column("status_code"),
    },
    ScopedLabelSource {
        scope: LabelScope::Metrics,
        source: LabelSource::MetricAttr("status.code"),
    },
];

const JOB_SOURCES: &[ScopedLabelSource] = &[ScopedLabelSource {
    scope: LabelScope::Metrics,
    source: LabelSource::Missing,
}];

const INSTANCE_SOURCES: &[ScopedLabelSource] = &[ScopedLabelSource {
    scope: LabelScope::Metrics,
    source: LabelSource::Missing,
}];

const METRIC_STATE_SOURCES: &[ScopedLabelSource] = &[ScopedLabelSource {
    scope: LabelScope::Metrics,
    source: LabelSource::MetricAttr("state"),
}];

const METRIC_TYPE_SOURCES: &[ScopedLabelSource] = &[ScopedLabelSource {
    scope: LabelScope::Metrics,
    source: LabelSource::MetricAttr("type"),
}];

const METRIC_CPU_SOURCES: &[ScopedLabelSource] = &[ScopedLabelSource {
    scope: LabelScope::Metrics,
    source: LabelSource::MetricAttr("cpu"),
}];

const METRIC_EXPORTER_SOURCES: &[ScopedLabelSource] = &[ScopedLabelSource {
    scope: LabelScope::Metrics,
    source: LabelSource::MetricAttr("exporter"),
}];

const METRIC_RECEIVER_SOURCES: &[ScopedLabelSource] = &[ScopedLabelSource {
    scope: LabelScope::Metrics,
    source: LabelSource::MetricAttr("receiver"),
}];

const METRIC_PROCESSOR_SOURCES: &[ScopedLabelSource] = &[ScopedLabelSource {
    scope: LabelScope::Metrics,
    source: LabelSource::MetricAttr("processor"),
}];

const OTEL_SIGNAL_SOURCES: &[ScopedLabelSource] = &[ScopedLabelSource {
    scope: LabelScope::Metrics,
    source: LabelSource::MetricAttr("otel_signal"),
}];

const DATA_TYPE_SOURCES: &[ScopedLabelSource] = &[ScopedLabelSource {
    scope: LabelScope::Metrics,
    source: LabelSource::MetricAttr("data_type"),
}];

const POSTGRES_DB_SOURCES: &[ScopedLabelSource] = &[ScopedLabelSource {
    scope: LabelScope::Metrics,
    source: LabelSource::MetricAttr("postgresql.database.name"),
}];

const RPC_SERVICE_SOURCES: &[ScopedLabelSource] = &[
    ScopedLabelSource {
        scope: LabelScope::Spans,
        source: LabelSource::SpanAttr("rpc.service"),
    },
    ScopedLabelSource {
        scope: LabelScope::Metrics,
        source: LabelSource::MetricAttr("rpc.service"),
    },
];

const RPC_METHOD_SOURCES: &[ScopedLabelSource] = &[
    ScopedLabelSource {
        scope: LabelScope::Spans,
        source: LabelSource::SpanAttr("rpc.method"),
    },
    ScopedLabelSource {
        scope: LabelScope::Metrics,
        source: LabelSource::MetricAttr("rpc.method"),
    },
];

const RPC_GRPC_STATUS_SOURCES: &[ScopedLabelSource] = &[
    ScopedLabelSource {
        scope: LabelScope::Spans,
        source: LabelSource::SpanAttr("rpc.grpc.status_code"),
    },
    ScopedLabelSource {
        scope: LabelScope::Metrics,
        source: LabelSource::MetricAttr("rpc.grpc.status_code"),
    },
];

const SERVER_ADDRESS_SOURCES: &[ScopedLabelSource] = &[
    ScopedLabelSource {
        scope: LabelScope::Spans,
        source: LabelSource::SpanAttr("server.address"),
    },
    ScopedLabelSource {
        scope: LabelScope::Metrics,
        source: LabelSource::MetricAttr("server.address"),
    },
];

const DB_NAMESPACE_SOURCES: &[ScopedLabelSource] = &[
    ScopedLabelSource {
        scope: LabelScope::Spans,
        source: LabelSource::SpanAttr("db.namespace"),
    },
    ScopedLabelSource {
        scope: LabelScope::Metrics,
        source: LabelSource::MetricAttr("db.namespace"),
    },
];

const URL_TEMPLATE_SOURCES: &[ScopedLabelSource] = &[ScopedLabelSource {
    scope: LabelScope::Metrics,
    source: LabelSource::MetricAttr("url.template"),
}];

const EXCEPTION_TYPE_SOURCES: &[ScopedLabelSource] = &[ScopedLabelSource {
    scope: LabelScope::Spans,
    source: LabelSource::SpanAttr("exception.type"),
}];

pub const LABELS: &[LabelDef] = &[
    LabelDef {
        canonical: "service_name",
        aliases: &[
            "service.name",
            "resource.service.name",
            "serviceName",
            "service-name",
        ],
        sources: SERVICE_NAME_SOURCES,
        prom_promoted: true,
        project: &[LabelScope::Logs],
        loki_stream: true,
        tempo_tag: Some("service.name"),
    },
    LabelDef {
        canonical: "service_namespace",
        aliases: &[
            "service.namespace",
            "resource.service.namespace",
            "service-namespace",
        ],
        sources: SERVICE_NAMESPACE_SOURCES,
        prom_promoted: false,
        project: &[LabelScope::Logs],
        loki_stream: true,
        tempo_tag: Some("service.namespace"),
    },
    LabelDef {
        canonical: "service_instance_id",
        aliases: &[
            "service.instance.id",
            "resource.service.instance.id",
            "service-instance-id",
        ],
        sources: SERVICE_INSTANCE_SOURCES,
        prom_promoted: false,
        project: &[LabelScope::Logs],
        loki_stream: true,
        tempo_tag: Some("service.instance.id"),
    },
    LabelDef {
        canonical: "deployment_environment",
        aliases: &[
            "deployment.environment",
            "deployment_environment_name",
            "deployment-environment",
        ],
        sources: DEPLOYMENT_ENV_SOURCES,
        prom_promoted: true,
        project: &[LabelScope::Logs],
        loki_stream: true,
        tempo_tag: Some("deployment.environment"),
    },
    LabelDef {
        canonical: "host_name",
        aliases: &["host.name", "resource.host.name", "host-name"],
        sources: HOST_NAME_SOURCES,
        prom_promoted: false,
        project: &[LabelScope::Logs],
        loki_stream: true,
        tempo_tag: Some("host.name"),
    },
    LabelDef {
        canonical: "k8s_cluster_name",
        aliases: &["k8s.cluster.name"],
        sources: K8S_CLUSTER_SOURCES,
        prom_promoted: false,
        project: &[],
        loki_stream: false,
        tempo_tag: None,
    },
    LabelDef {
        canonical: "k8s_node_name",
        aliases: &["k8s.node.name"],
        sources: K8S_NODE_SOURCES,
        prom_promoted: false,
        project: &[],
        loki_stream: false,
        tempo_tag: None,
    },
    LabelDef {
        canonical: "k8s_statefulset_name",
        aliases: &["k8s.statefulset.name"],
        sources: K8S_STATEFULSET_SOURCES,
        prom_promoted: false,
        project: &[],
        loki_stream: false,
        tempo_tag: None,
    },
    LabelDef {
        canonical: "k8s_deployment_name",
        aliases: &["k8s.deployment.name"],
        sources: K8S_DEPLOYMENT_SOURCES,
        prom_promoted: false,
        project: &[],
        loki_stream: false,
        tempo_tag: Some("k8s.deployment.name"),
    },
    LabelDef {
        canonical: "scope_name",
        aliases: &["instrumentationScope.name", "instrumentation_scope_name"],
        sources: LOG_SCOPE_SOURCES,
        prom_promoted: false,
        project: &[LabelScope::Logs],
        loki_stream: true,
        tempo_tag: None,
    },
    LabelDef {
        canonical: "severity_text",
        aliases: &["severity.text"],
        sources: LOG_SEVERITY_SOURCES,
        prom_promoted: false,
        project: &[LabelScope::Logs],
        loki_stream: true,
        tempo_tag: None,
    },
    LabelDef {
        canonical: "trace_id",
        aliases: &["traceID"],
        sources: TRACE_ID_SOURCES,
        prom_promoted: false,
        project: &[LabelScope::Logs],
        loki_stream: true,
        tempo_tag: Some("traceID"),
    },
    LabelDef {
        canonical: "span_id",
        aliases: &["spanID"],
        sources: SPAN_ID_SOURCES,
        prom_promoted: false,
        project: &[LabelScope::Logs],
        loki_stream: true,
        tempo_tag: Some("spanID"),
    },
    LabelDef {
        canonical: "http_route",
        aliases: &["http.route", "http-route"],
        sources: HTTP_ROUTE_SOURCES,
        prom_promoted: false,
        project: &[LabelScope::Logs],
        loki_stream: true,
        tempo_tag: Some("http.route"),
    },
    LabelDef {
        canonical: "http_method",
        aliases: &["http.method", "http.request.method", "http-method"],
        sources: HTTP_METHOD_SOURCES,
        prom_promoted: false,
        project: &[LabelScope::Logs],
        loki_stream: true,
        tempo_tag: Some("http.method"),
    },
    LabelDef {
        canonical: "http_status_code",
        aliases: &[
            "http.status_code",
            "http.response.status_code",
            "http-status-code",
        ],
        sources: HTTP_STATUS_SOURCES,
        prom_promoted: false,
        project: &[LabelScope::Logs],
        loki_stream: false,
        tempo_tag: Some("http.status_code"),
    },
    LabelDef {
        canonical: "http_response_status_code",
        aliases: &["http.response.status_code"],
        sources: HTTP_RESPONSE_STATUS_METRIC_SOURCES,
        prom_promoted: false,
        project: &[],
        loki_stream: false,
        tempo_tag: None,
    },
    LabelDef {
        canonical: "span_name",
        aliases: &["span.name", "span-name", "name"],
        sources: SPAN_NAME_SOURCES,
        prom_promoted: false,
        project: &[],
        loki_stream: false,
        tempo_tag: Some("span.name"),
    },
    LabelDef {
        canonical: "status_code",
        aliases: &["status.code", "status"],
        sources: STATUS_CODE_SOURCES,
        prom_promoted: false,
        project: &[],
        loki_stream: false,
        tempo_tag: Some("status.code"),
    },
    LabelDef {
        canonical: "job",
        aliases: &[],
        sources: JOB_SOURCES,
        prom_promoted: false,
        project: &[],
        loki_stream: false,
        tempo_tag: None,
    },
    LabelDef {
        canonical: "instance",
        aliases: &[],
        sources: INSTANCE_SOURCES,
        prom_promoted: false,
        project: &[],
        loki_stream: false,
        tempo_tag: None,
    },
    LabelDef {
        canonical: "state",
        aliases: &[],
        sources: METRIC_STATE_SOURCES,
        prom_promoted: false,
        project: &[],
        loki_stream: false,
        tempo_tag: None,
    },
    LabelDef {
        canonical: "type",
        aliases: &[],
        sources: METRIC_TYPE_SOURCES,
        prom_promoted: false,
        project: &[],
        loki_stream: false,
        tempo_tag: None,
    },
    LabelDef {
        canonical: "cpu",
        aliases: &[],
        sources: METRIC_CPU_SOURCES,
        prom_promoted: false,
        project: &[],
        loki_stream: false,
        tempo_tag: None,
    },
    LabelDef {
        canonical: "exporter",
        aliases: &[],
        sources: METRIC_EXPORTER_SOURCES,
        prom_promoted: false,
        project: &[],
        loki_stream: false,
        tempo_tag: None,
    },
    LabelDef {
        canonical: "receiver",
        aliases: &[],
        sources: METRIC_RECEIVER_SOURCES,
        prom_promoted: false,
        project: &[],
        loki_stream: false,
        tempo_tag: None,
    },
    LabelDef {
        canonical: "processor",
        aliases: &[],
        sources: METRIC_PROCESSOR_SOURCES,
        prom_promoted: false,
        project: &[],
        loki_stream: false,
        tempo_tag: None,
    },
    LabelDef {
        canonical: "otel_signal",
        aliases: &[],
        sources: OTEL_SIGNAL_SOURCES,
        prom_promoted: false,
        project: &[],
        loki_stream: false,
        tempo_tag: None,
    },
    LabelDef {
        canonical: "data_type",
        aliases: &[],
        sources: DATA_TYPE_SOURCES,
        prom_promoted: false,
        project: &[],
        loki_stream: false,
        tempo_tag: None,
    },
    LabelDef {
        canonical: "postgresql_database_name",
        aliases: &["postgresql.database.name"],
        sources: POSTGRES_DB_SOURCES,
        prom_promoted: false,
        project: &[],
        loki_stream: false,
        tempo_tag: None,
    },
    LabelDef {
        canonical: "rpc_service",
        aliases: &["rpc.service"],
        sources: RPC_SERVICE_SOURCES,
        prom_promoted: false,
        project: &[],
        loki_stream: false,
        tempo_tag: Some("rpc.service"),
    },
    LabelDef {
        canonical: "rpc_method",
        aliases: &["rpc.method"],
        sources: RPC_METHOD_SOURCES,
        prom_promoted: false,
        project: &[],
        loki_stream: false,
        tempo_tag: Some("rpc.method"),
    },
    LabelDef {
        canonical: "rpc_grpc_status_code",
        aliases: &["rpc.grpc.status_code"],
        sources: RPC_GRPC_STATUS_SOURCES,
        prom_promoted: false,
        project: &[],
        loki_stream: false,
        tempo_tag: Some("rpc.grpc.status_code"),
    },
    LabelDef {
        canonical: "server_address",
        aliases: &["server.address"],
        sources: SERVER_ADDRESS_SOURCES,
        prom_promoted: false,
        project: &[],
        loki_stream: false,
        tempo_tag: Some("server.address"),
    },
    LabelDef {
        canonical: "url_template",
        aliases: &["url.template"],
        sources: URL_TEMPLATE_SOURCES,
        prom_promoted: false,
        project: &[],
        loki_stream: false,
        tempo_tag: None,
    },
    LabelDef {
        canonical: "db_namespace",
        aliases: &["db.namespace"],
        sources: DB_NAMESPACE_SOURCES,
        prom_promoted: false,
        project: &[],
        loki_stream: false,
        tempo_tag: Some("db.namespace"),
    },
    LabelDef {
        canonical: "exception_type",
        aliases: &["exception.type"],
        sources: EXCEPTION_TYPE_SOURCES,
        prom_promoted: false,
        project: &[],
        loki_stream: false,
        tempo_tag: Some("exception.type"),
    },
];

#[cfg(test)]
mod tests {
    use super::*;

    /// The log query projection is registry-derived (`query::log_select_sql` /
    /// `log_row`). This pins the projected set so dropping a `project` marker
    /// fails here rather than silently removing a field from query results.
    #[test]
    fn logs_projection_set_is_pinned_to_registry() {
        let projected: Vec<&str> = projected_labels(LabelScope::Logs)
            .into_iter()
            .map(|(name, _)| name)
            .collect();
        assert_eq!(
            projected,
            [
                "service_name",
                "service_namespace",
                "service_instance_id",
                "deployment_environment",
                "host_name",
                "scope_name",
                "severity_text",
                "trace_id",
                "span_id",
                "http_route",
                "http_method",
                "http_status_code",
            ]
        );
    }

    /// Every label marked `project` for a scope must resolve to a SQL expression
    /// in that scope, otherwise `projected_labels` would silently drop it.
    #[test]
    fn projected_labels_all_resolve_to_an_expression() {
        for scope in [LabelScope::Logs, LabelScope::Spans, LabelScope::Metrics] {
            let marked = LABELS
                .iter()
                .filter(|label| label.project.contains(&scope))
                .count();
            assert_eq!(marked, projected_labels(scope).len(), "scope {scope:?}");
        }
    }

    /// Within a scope, an input alias (or canonical) must resolve to exactly one
    /// canonical. Two `LabelDef`s sharing an alias are only safe while their
    /// scopes stay disjoint; this guards the moment one gains the other's scope
    /// (e.g. giving `http_status_code` a metrics source would collide with
    /// `http_response_status_code` on the `http.response.status_code` alias).
    #[test]
    fn alias_resolves_to_one_canonical_per_scope() {
        use std::collections::HashMap;
        for scope in [LabelScope::Logs, LabelScope::Spans, LabelScope::Metrics] {
            let mut seen: HashMap<&str, &str> = HashMap::new();
            for (raw, canonical) in alias_pairs(scope) {
                if let Some(prev) = seen.insert(raw, canonical) {
                    assert_eq!(prev, canonical, "alias {raw} is ambiguous in {scope:?}");
                }
            }
        }
    }

    /// Each `HIGH_CARDINALITY` entry must name a real canonical, so a typo is a
    /// failing test rather than a silently ineffective veto.
    #[test]
    fn high_cardinality_labels_are_registered() {
        for name in HIGH_CARDINALITY {
            assert!(
                LABELS.iter().any(|label| label.canonical == *name),
                "unknown high-cardinality label {name}"
            );
        }
    }

    /// High-cardinality (id-like) labels stay selectable and name-discoverable
    /// but must never be materialized into `metadata_summary` in any scope.
    #[test]
    fn high_cardinality_labels_are_never_materialized() {
        for label in LABELS
            .iter()
            .filter(|label| HIGH_CARDINALITY.contains(&label.canonical))
        {
            for scope in [LabelScope::Logs, LabelScope::Spans, LabelScope::Metrics] {
                assert!(
                    !metadata_label_allowed(scope, label),
                    "{} materialized in {scope:?}",
                    label.canonical
                );
            }
        }
    }

    /// Pins the bounded set of log labels whose values are materialized for
    /// discovery. The reconciliation of the historically-divergent stream /
    /// discovery / materialization lists lives here: dropping or adding a label
    /// (or flipping its cardinality) must be a deliberate edit to this set.
    #[test]
    fn logs_materialized_set_is_pinned() {
        let names: Vec<&str> = metadata_labels(LabelScope::Logs)
            .into_iter()
            .map(|(name, _)| name)
            .collect();
        assert_eq!(
            names,
            [
                "service_name",
                "service_namespace",
                "deployment_environment",
                "host_name",
                "scope_name",
                "severity_text",
                "http_route",
                "http_method",
            ]
        );
    }
}
