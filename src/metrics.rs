use crate::LockExt;
use std::borrow::Cow;
use std::collections::BTreeMap;
use std::sync::Mutex;
use std::time::Instant;

const METRICS_SHARDS: usize = 16;

pub struct Metrics {
    shards: [Mutex<MetricsInner>; METRICS_SHARDS],
}

impl Default for Metrics {
    fn default() -> Self {
        Self {
            shards: std::array::from_fn(|_| Mutex::new(MetricsInner::default())),
        }
    }
}

#[derive(Default)]
struct MetricsInner {
    counters: BTreeMap<MetricId, u64>,
    gauges: BTreeMap<MetricId, f64>,
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
struct MetricId {
    name: String,
    labels: Vec<(String, String)>,
}

impl MetricId {
    fn new(name: &str, labels: &[(&str, &str)]) -> Self {
        Self {
            name: name.to_string(),
            labels: labels
                .iter()
                .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
                .collect(),
        }
    }

    fn render_prometheus(&self) -> String {
        if self.labels.is_empty() {
            return self.name.clone();
        }
        let rendered = self
            .labels
            .iter()
            .map(|(key, value)| format!("{key}=\"{}\"", escape_label_value(value)))
            .collect::<Vec<_>>()
            .join(",");
        format!("{}{{{rendered}}}", self.name)
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct MetricSample {
    pub name: String,
    pub labels: BTreeMap<String, String>,
    pub value: f64,
    kind: MetricKind,
}

impl MetricSample {
    fn counter(name: String, labels: BTreeMap<String, String>, value: f64) -> Self {
        Self {
            name,
            labels,
            value,
            kind: MetricKind::Counter,
        }
    }

    fn gauge(name: String, labels: BTreeMap<String, String>, value: f64) -> Self {
        Self {
            name,
            labels,
            value,
            kind: MetricKind::Gauge,
        }
    }

    pub fn kind(&self) -> MetricKind {
        self.kind
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MetricKind {
    Counter,
    Gauge,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum MetricShape {
    Counter,
    Gauge,
    Observation,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub enum MetricName {
    AdmissionCapacity,
    AdmissionInUse,
    AdmissionRejectionsTotal,
    DucklakeActiveDataFileRows,
    DucklakeActiveDataFiles,
    FreshnessWatermarkTimestamp,
    HttpConnectionClosesTotal,
    HttpConnectionErrorsTotal,
    HttpConnectionRequestsTotal,
    PhaseDurationSeconds,
    QueryDurationSeconds,
    QueryRequestsTotal,
    QueryTimeoutsTotal,
    StorageLogicalRows,
    StoragePhysicalBytes,
}

impl MetricName {
    #[cfg(test)]
    const ALL: &'static [Self] = &[
        Self::AdmissionCapacity,
        Self::AdmissionInUse,
        Self::AdmissionRejectionsTotal,
        Self::DucklakeActiveDataFileRows,
        Self::DucklakeActiveDataFiles,
        Self::FreshnessWatermarkTimestamp,
        Self::HttpConnectionClosesTotal,
        Self::HttpConnectionErrorsTotal,
        Self::HttpConnectionRequestsTotal,
        Self::PhaseDurationSeconds,
        Self::QueryDurationSeconds,
        Self::QueryRequestsTotal,
        Self::QueryTimeoutsTotal,
        Self::StorageLogicalRows,
        Self::StoragePhysicalBytes,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            Self::AdmissionCapacity => "canardstack_admission_capacity",
            Self::AdmissionInUse => "canardstack_admission_in_use",
            Self::AdmissionRejectionsTotal => "canardstack_admission_rejections_total",
            Self::DucklakeActiveDataFileRows => "canardstack_ducklake_active_data_file_rows",
            Self::DucklakeActiveDataFiles => "canardstack_ducklake_active_data_files",
            Self::FreshnessWatermarkTimestamp => "canardstack_freshness_watermark_timestamp",
            Self::HttpConnectionClosesTotal => "canardstack_http_connection_closes_total",
            Self::HttpConnectionErrorsTotal => "canardstack_http_connection_errors_total",
            Self::HttpConnectionRequestsTotal => "canardstack_http_connection_requests_total",
            Self::PhaseDurationSeconds => "canardstack_phase_duration_seconds",
            Self::QueryDurationSeconds => "canardstack_query_duration_seconds",
            Self::QueryRequestsTotal => "canardstack_query_requests_total",
            Self::QueryTimeoutsTotal => "canardstack_query_timeouts_total",
            Self::StorageLogicalRows => "canardstack_storage_logical_rows",
            Self::StoragePhysicalBytes => "canardstack_storage_physical_bytes",
        }
    }

    fn shape(self) -> MetricShape {
        match self {
            Self::AdmissionCapacity
            | Self::AdmissionInUse
            | Self::DucklakeActiveDataFileRows
            | Self::DucklakeActiveDataFiles
            | Self::FreshnessWatermarkTimestamp
            | Self::StorageLogicalRows
            | Self::StoragePhysicalBytes => MetricShape::Gauge,
            Self::PhaseDurationSeconds | Self::QueryDurationSeconds => MetricShape::Observation,
            Self::AdmissionRejectionsTotal
            | Self::HttpConnectionClosesTotal
            | Self::HttpConnectionErrorsTotal
            | Self::HttpConnectionRequestsTotal
            | Self::QueryRequestsTotal
            | Self::QueryTimeoutsTotal => MetricShape::Counter,
        }
    }

    fn allowed_label_keys(self) -> &'static [&'static str] {
        match self {
            Self::AdmissionCapacity | Self::AdmissionInUse => &["admission"],
            Self::AdmissionRejectionsTotal => &["admission", "reason"],
            Self::DucklakeActiveDataFileRows
            | Self::DucklakeActiveDataFiles
            | Self::FreshnessWatermarkTimestamp
            | Self::StorageLogicalRows
            | Self::StoragePhysicalBytes => &["storage_signal"],
            Self::HttpConnectionClosesTotal | Self::HttpConnectionErrorsTotal => &["reason"],
            Self::HttpConnectionRequestsTotal => &["mode"],
            Self::PhaseDurationSeconds => &[
                "request_kind",
                "route_template",
                "storage_signal",
                "phase",
                "reason",
                "status",
                "path",
            ],
            Self::QueryDurationSeconds | Self::QueryTimeoutsTotal => &["route_template"],
            Self::QueryRequestsTotal => &["route_template", "status", "reason"],
        }
    }
}

impl Metrics {
    fn shard_for(&self, metric_id: &MetricId) -> &Mutex<MetricsInner> {
        let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
        hash_metric_part(&mut hash, metric_id.name.as_bytes());
        for (key, value) in &metric_id.labels {
            hash_metric_part(&mut hash, key.as_bytes());
            hash_metric_part(&mut hash, value.as_bytes());
        }
        &self.shards[(hash as usize) & (METRICS_SHARDS - 1)]
    }

    pub fn inc(&self, name: MetricName, labels: &[(&str, &str)], by: u64) {
        assert_eq!(name.shape(), MetricShape::Counter);
        validate_metric_labels(name, labels);
        let metric_id = MetricId::new(name.as_str(), labels);
        let mut inner = self.shard_for(&metric_id).lock_or_poisoned();
        *inner.counters.entry(metric_id).or_default() += by;
    }

    pub fn gauge(&self, name: MetricName, labels: &[(&str, &str)], value: f64) {
        assert_eq!(name.shape(), MetricShape::Gauge);
        validate_metric_labels(name, labels);
        let metric_id = MetricId::new(name.as_str(), labels);
        self.shard_for(&metric_id)
            .lock_or_poisoned()
            .gauges
            .insert(metric_id, value);
    }

    pub fn observe_seconds(&self, name: MetricName, labels: &[(&str, &str)], seconds: f64) {
        assert_eq!(name.shape(), MetricShape::Observation);
        validate_metric_labels(name, labels);
        let count_id = MetricId::new(&format!("{}_count", name.as_str()), labels);
        let sum_id = MetricId::new(&format!("{}_sum", name.as_str()), labels);
        {
            let mut inner = self.shard_for(&count_id).lock_or_poisoned();
            *inner.counters.entry(count_id).or_default() += 1;
        }
        let mut inner = self.shard_for(&sum_id).lock_or_poisoned();
        let current = inner.gauges.get(&sum_id).copied().unwrap_or(0.0);
        inner.gauges.insert(sum_id, current + seconds);
    }

    pub fn observe_query_route_phase_seconds(
        &self,
        route_template: &str,
        phase: &str,
        seconds: f64,
    ) {
        self.observe_seconds(
            MetricName::PhaseDurationSeconds,
            &[("route_template", route_template), ("phase", phase)],
            seconds,
        );
    }

    pub fn query_request(&self, route_template: &str, status: u16, reason: &str, seconds: f64) {
        let status = status_label(status);
        self.inc(
            MetricName::QueryRequestsTotal,
            &[
                ("route_template", route_template),
                ("status", status.as_ref()),
                ("reason", reason),
            ],
            1,
        );
        self.observe_seconds(
            MetricName::QueryDurationSeconds,
            &[("route_template", route_template)],
            seconds,
        );
        if reason == "query_timeout" {
            self.inc(
                MetricName::QueryTimeoutsTotal,
                &[("route_template", route_template)],
                1,
            );
        }
    }

    pub fn render_prometheus(&self) -> String {
        let (counters, gauges) = self.merged_shards();
        let mut out = String::new();
        for (id, value) in &counters {
            out.push_str(&id.render_prometheus());
            out.push(' ');
            out.push_str(&value.to_string());
            out.push('\n');
        }
        for (id, value) in &gauges {
            out.push_str(&id.render_prometheus());
            out.push(' ');
            out.push_str(&format!("{value:.6}"));
            out.push('\n');
        }
        out
    }

    pub fn snapshot(&self) -> Vec<MetricSample> {
        let (counters, gauges) = self.merged_shards();
        counters
            .iter()
            .map(|(id, value)| {
                MetricSample::counter(id.name.clone(), metric_labels_map(id), *value as f64)
            })
            .chain(gauges.iter().map(|(id, value)| {
                MetricSample::gauge(id.name.clone(), metric_labels_map(id), *value)
            }))
            .collect()
    }

    fn merged_shards(&self) -> (BTreeMap<MetricId, u64>, BTreeMap<MetricId, f64>) {
        let mut counters: BTreeMap<MetricId, u64> = BTreeMap::new();
        let mut gauges: BTreeMap<MetricId, f64> = BTreeMap::new();
        for shard in &self.shards {
            let inner = shard.lock_or_poisoned();
            counters.extend(
                inner
                    .counters
                    .iter()
                    .map(|(key, value)| (key.clone(), *value)),
            );
            gauges.extend(
                inner
                    .gauges
                    .iter()
                    .map(|(key, value)| (key.clone(), *value)),
            );
        }
        (counters, gauges)
    }
}

pub struct Timer {
    start: Instant,
}

impl Timer {
    pub fn start() -> Self {
        Self {
            start: Instant::now(),
        }
    }

    pub fn elapsed_ms(&self) -> u128 {
        self.start.elapsed().as_millis()
    }
}

fn status_label(status: u16) -> Cow<'static, str> {
    match status {
        200 => Cow::Borrowed("200"),
        400 => Cow::Borrowed("400"),
        429 => Cow::Borrowed("429"),
        500 => Cow::Borrowed("500"),
        503 => Cow::Borrowed("503"),
        other => Cow::Owned(other.to_string()),
    }
}

fn validate_metric_labels(name: MetricName, labels: &[(&str, &str)]) {
    let allowed = name.allowed_label_keys();
    for (key, _) in labels {
        assert!(
            allowed.contains(key),
            "metric `{}` emitted unsupported label key `{key}`; allowed keys: {allowed:?}",
            name.as_str()
        );
    }
    for index in 0..labels.len() {
        assert!(
            !labels[index + 1..]
                .iter()
                .any(|(key, _)| *key == labels[index].0),
            "metric `{}` emitted duplicate label key `{}`",
            name.as_str(),
            labels[index].0
        );
    }
}

fn hash_metric_part(hash: &mut u64, bytes: &[u8]) {
    for byte in bytes {
        *hash ^= u64::from(*byte);
        *hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    *hash ^= 0xff;
    *hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
}

fn metric_labels_map(metric_id: &MetricId) -> BTreeMap<String, String> {
    metric_id.labels.iter().cloned().collect()
}

fn escape_label_value(value: &str) -> String {
    value.replace('\\', "\\\\").replace('"', "\\\"")
}

#[cfg(test)]
mod snapshot_tests {
    use super::*;
    use std::collections::BTreeSet;

    fn base_name(line: &str) -> Option<String> {
        let head = line.split([' ', '{']).next()?;
        if !head.starts_with("canardstack_") {
            return None;
        }
        let base = head
            .strip_suffix("_count")
            .or_else(|| head.strip_suffix("_sum"))
            .unwrap_or(head);
        Some(base.to_string())
    }

    fn rendered_base_names(metrics: &Metrics) -> BTreeSet<String> {
        metrics
            .render_prometheus()
            .lines()
            .filter_map(base_name)
            .collect()
    }

    fn emit_representative_surface(metrics: &Metrics) {
        metrics.query_request("/api/v1/query_range", 200, "ok", 0.1);
        metrics.query_request("/api/v1/query_range", 504, "query_timeout", 0.1);
        metrics.observe_query_route_phase_seconds("/api/v1/query_range", "query_execute", 0.1);
        metrics.gauge(
            MetricName::AdmissionCapacity,
            &[("admission", "query_cheap")],
            1.0,
        );
        metrics.gauge(
            MetricName::AdmissionInUse,
            &[("admission", "query_cheap")],
            0.0,
        );
        metrics.inc(
            MetricName::AdmissionRejectionsTotal,
            &[
                ("admission", "query"),
                ("reason", "heavy_query_admission_full"),
            ],
            1,
        );
        metrics.gauge(
            MetricName::StoragePhysicalBytes,
            &[("storage_signal", "all")],
            0.0,
        );
        metrics.gauge(
            MetricName::StorageLogicalRows,
            &[("storage_signal", "logs")],
            0.0,
        );
        metrics.gauge(
            MetricName::DucklakeActiveDataFiles,
            &[("storage_signal", "logs")],
            0.0,
        );
        metrics.gauge(
            MetricName::DucklakeActiveDataFileRows,
            &[("storage_signal", "logs")],
            0.0,
        );
        metrics.gauge(
            MetricName::FreshnessWatermarkTimestamp,
            &[("storage_signal", "logs")],
            0.0,
        );
        metrics.inc(
            MetricName::HttpConnectionRequestsTotal,
            &[("mode", "keep_alive")],
            1,
        );
        metrics.inc(
            MetricName::HttpConnectionClosesTotal,
            &[("reason", "client")],
            1,
        );
        metrics.inc(
            MetricName::HttpConnectionErrorsTotal,
            &[("reason", "io_error")],
            1,
        );
    }

    fn expected_surface() -> BTreeSet<String> {
        [
            "canardstack_admission_capacity",
            "canardstack_admission_in_use",
            "canardstack_admission_rejections_total",
            "canardstack_ducklake_active_data_file_rows",
            "canardstack_ducklake_active_data_files",
            "canardstack_freshness_watermark_timestamp",
            "canardstack_http_connection_closes_total",
            "canardstack_http_connection_errors_total",
            "canardstack_http_connection_requests_total",
            "canardstack_phase_duration_seconds",
            "canardstack_query_duration_seconds",
            "canardstack_query_requests_total",
            "canardstack_query_timeouts_total",
            "canardstack_storage_logical_rows",
            "canardstack_storage_physical_bytes",
        ]
        .into_iter()
        .map(str::to_string)
        .collect()
    }

    #[test]
    fn representative_metric_surface_matches_registry() {
        let metrics = Metrics::default();
        emit_representative_surface(&metrics);

        assert_eq!(rendered_base_names(&metrics), expected_surface());
        let registry = MetricName::ALL
            .iter()
            .map(|name| name.as_str().to_string())
            .collect::<BTreeSet<_>>();
        assert_eq!(registry, expected_surface());
    }

    #[test]
    #[should_panic(expected = "unsupported label key")]
    fn rejects_unknown_label_keys() {
        let metrics = Metrics::default();
        metrics.gauge(MetricName::AdmissionCapacity, &[("kind", "cheap")], 1.0);
    }

    #[test]
    fn snapshot_preserves_metric_kind() {
        let metrics = Metrics::default();
        metrics.query_request("/api/v1/query", 200, "ok", 0.1);
        metrics.gauge(
            MetricName::AdmissionCapacity,
            &[("admission", "query_cheap")],
            1.0,
        );

        let samples = metrics.snapshot();
        assert!(samples
            .iter()
            .any(|sample| sample.kind() == MetricKind::Counter));
        assert!(samples
            .iter()
            .any(|sample| sample.kind() == MetricKind::Gauge));
    }
}
