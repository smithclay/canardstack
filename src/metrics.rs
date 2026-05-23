use crate::ingest::Signal;
use crate::storage::Storage;
use crate::LockExt;
use anyhow::{Context, Result};
use arrow58::array::{
    ArrayRef, BooleanArray, Float64Array, Int32Array, Int64Array, StringArray,
    TimestampMicrosecondArray,
};
use arrow58::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow58::record_batch::RecordBatch;
use chrono::Utc;
use serde_json::{json, Map, Value};
use std::borrow::Cow;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Instant;

/// Number of independent lock shards. A power of two so shard selection is a
/// cheap mask. Sized to cover the connection-thread + ingest-worker fan-out so
/// the hot ingest path stops serializing on a single metrics mutex.
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

    /// Discriminates how `value` is interpreted: a `Counter` is cumulative and
    /// routes to `metric_sum`, a `Gauge` is instantaneous and routes to
    /// `metric_gauge`.
    pub fn kind(&self) -> MetricKind {
        self.kind
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MetricKind {
    Counter,
    Gauge,
}

impl Metrics {
    /// Select the lock shard for a metric id. A given id always maps
    /// to the same shard, so read-modify-write counters/gauges remain correct
    /// without a single global lock. FNV-1a keeps this branch-free and cheap.
    fn shard_for(&self, metric_id: &MetricId) -> &Mutex<MetricsInner> {
        let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
        hash_metric_part(&mut hash, metric_id.name.as_bytes());
        for (key, value) in &metric_id.labels {
            hash_metric_part(&mut hash, key.as_bytes());
            hash_metric_part(&mut hash, value.as_bytes());
        }
        &self.shards[(hash as usize) & (METRICS_SHARDS - 1)]
    }

    pub fn inc(&self, name: &str, labels: &[(&str, &str)], by: u64) {
        let metric_id = MetricId::new(name, labels);
        let mut inner = self.shard_for(&metric_id).lock_or_poisoned();
        *inner.counters.entry(metric_id).or_default() += by;
    }

    pub fn set_counter(&self, name: &str, labels: &[(&str, &str)], value: u64) {
        let metric_id = MetricId::new(name, labels);
        self.shard_for(&metric_id)
            .lock_or_poisoned()
            .counters
            .insert(metric_id, value);
    }

    pub fn gauge(&self, name: &str, labels: &[(&str, &str)], value: f64) {
        let metric_id = MetricId::new(name, labels);
        self.shard_for(&metric_id)
            .lock_or_poisoned()
            .gauges
            .insert(metric_id, value);
    }

    pub fn gauge_max(&self, name: &str, labels: &[(&str, &str)], value: f64) {
        let metric_id = MetricId::new(name, labels);
        let mut inner = self.shard_for(&metric_id).lock_or_poisoned();
        let current = inner.gauges.get(&metric_id).copied().unwrap_or(value);
        inner.gauges.insert(metric_id, current.max(value));
    }

    pub fn observe_seconds(&self, name: &str, labels: &[(&str, &str)], seconds: f64) {
        self.observe_seconds_n(name, labels, 1, seconds);
    }

    pub fn observe_seconds_n(&self, name: &str, labels: &[(&str, &str)], count: u64, seconds: f64) {
        if count == 0 {
            return;
        }
        let count_id = MetricId::new(&format!("{name}_count"), labels);
        let sum_id = MetricId::new(&format!("{name}_sum"), labels);
        {
            let mut inner = self.shard_for(&count_id).lock_or_poisoned();
            *inner.counters.entry(count_id).or_default() += count;
        }
        let mut inner = self.shard_for(&sum_id).lock_or_poisoned();
        let current = inner.gauges.get(&sum_id).copied().unwrap_or(0.0);
        inner.gauges.insert(sum_id, current + seconds);
    }

    pub fn set_observation(&self, name: &str, labels: &[(&str, &str)], count: u64, seconds: f64) {
        let count_id = MetricId::new(&format!("{name}_count"), labels);
        let sum_id = MetricId::new(&format!("{name}_sum"), labels);
        self.shard_for(&count_id)
            .lock_or_poisoned()
            .counters
            .insert(count_id, count);
        self.shard_for(&sum_id)
            .lock_or_poisoned()
            .gauges
            .insert(sum_id, seconds);
    }

    pub fn observe_phase_seconds(
        &self,
        signal: &str,
        phase: &str,
        query_class: Option<&str>,
        seconds: f64,
    ) {
        if let Some(query_class) = query_class {
            self.observe_seconds(
                "canardstack_phase_duration_seconds",
                &[
                    ("signal", signal),
                    ("phase", phase),
                    ("query_class", query_class),
                ],
                seconds,
            );
        } else {
            self.observe_seconds(
                "canardstack_phase_duration_seconds",
                &[("signal", signal), ("phase", phase)],
                seconds,
            );
        }
    }

    pub fn observe_phase_seconds_n(
        &self,
        signal: &str,
        phase: &str,
        query_class: Option<&str>,
        count: u64,
        seconds: f64,
    ) {
        if let Some(query_class) = query_class {
            self.observe_seconds_n(
                "canardstack_phase_duration_seconds",
                &[
                    ("signal", signal),
                    ("phase", phase),
                    ("query_class", query_class),
                ],
                count,
                seconds,
            );
        } else {
            self.observe_seconds_n(
                "canardstack_phase_duration_seconds",
                &[("signal", signal), ("phase", phase)],
                count,
                seconds,
            );
        }
    }

    pub fn ingest_request(&self, signal: &str, status: u16, reason: &str) {
        let status = status_label(status);
        self.inc(
            "canardstack_ingest_requests_total",
            &[
                ("signal", signal),
                ("status", status.as_ref()),
                ("reason", reason),
            ],
            1,
        );
        if status == "429" || status == "503" {
            self.inc(
                "canardstack_ingest_rejections_total",
                &[
                    ("signal", signal),
                    ("status", status.as_ref()),
                    ("reason", reason),
                ],
                1,
            );
        }
    }

    pub fn query_request(&self, query_class: &str, status: u16, reason: &str, seconds: f64) {
        let status = status_label(status);
        self.inc(
            "canardstack_query_requests_total",
            &[
                ("query_class", query_class),
                ("status", status.as_ref()),
                ("reason", reason),
            ],
            1,
        );
        self.observe_seconds(
            "canardstack_query_duration_seconds",
            &[("query_class", query_class)],
            seconds,
        );
        if status == "429" {
            self.inc(
                "canardstack_query_rejections_total",
                &[("query_class", query_class), ("reason", reason)],
                1,
            );
        }
        if reason == "query_timeout" {
            self.inc(
                "canardstack_query_timeouts_total",
                &[("query_class", query_class)],
                1,
            );
        }
    }

    pub fn maintenance_run(&self, job: &str, status: &str, reason: &str, seconds: f64) {
        self.inc(
            "canardstack_maintenance_runs_total",
            &[("job", job), ("status", status), ("reason", reason)],
            1,
        );
        self.observe_seconds(
            "canardstack_maintenance_duration_seconds",
            &[("job", job), ("table", "all")],
            seconds,
        );
    }

    pub fn render_prometheus(&self) -> String {
        let (counters, gauges) = self.merged_shards();
        let mut out = String::new();
        for (id, v) in &counters {
            out.push_str(&id.render_prometheus());
            out.push(' ');
            out.push_str(&v.to_string());
            out.push('\n');
        }
        for (id, v) in &gauges {
            out.push_str(&id.render_prometheus());
            out.push(' ');
            out.push_str(&format!("{v:.6}"));
            out.push('\n');
        }
        out
    }

    pub fn snapshot(&self) -> Vec<MetricSample> {
        let (counters, gauges) = self.merged_shards();
        counters
            .iter()
            .map(|(id, v)| MetricSample::counter(id.name.clone(), metric_labels_map(id), *v as f64))
            .chain(
                gauges
                    .iter()
                    .map(|(id, v)| MetricSample::gauge(id.name.clone(), metric_labels_map(id), *v)),
            )
            .collect()
    }

    /// Merge all shards into sorted maps for a stable, deduplicated read. Each
    /// key lives in exactly one shard, so the merge never collides. Locks one
    /// shard at a time, so it cannot deadlock against the per-key writers.
    fn merged_shards(&self) -> (BTreeMap<MetricId, u64>, BTreeMap<MetricId, f64>) {
        let mut counters: BTreeMap<MetricId, u64> = BTreeMap::new();
        let mut gauges: BTreeMap<MetricId, f64> = BTreeMap::new();
        for shard in &self.shards {
            let inner = shard.lock_or_poisoned();
            counters.extend(inner.counters.iter().map(|(k, v)| (k.clone(), *v)));
            gauges.extend(inner.gauges.iter().map(|(k, v)| (k.clone(), *v)));
        }
        (counters, gauges)
    }

    pub fn write_snapshot_to_storage(&self, storage: &Storage) -> Result<usize> {
        let samples = self.snapshot();
        if samples.is_empty() {
            return Ok(0);
        }
        let counters = samples
            .iter()
            .filter(|sample| sample.kind() == MetricKind::Counter)
            .cloned()
            .collect::<Vec<_>>();
        let gauges = samples
            .iter()
            .filter(|sample| sample.kind() == MetricKind::Gauge)
            .cloned()
            .collect::<Vec<_>>();
        let mut rows = 0;
        if !counters.is_empty() {
            let batch = metric_samples_batch(&counters, Signal::MetricSum)?;
            rows += storage.buffer_arrow_records(
                Signal::MetricSum,
                &batch,
                "canardstack_operator_metrics",
            )?;
        }
        if !gauges.is_empty() {
            let batch = metric_samples_batch(&gauges, Signal::MetricGauge)?;
            rows += storage.buffer_arrow_records(
                Signal::MetricGauge,
                &batch,
                "canardstack_operator_metrics",
            )?;
        }
        Ok(rows)
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

/// Render an HTTP status as a label without allocating for the handful of
/// statuses the hot ingest/query paths actually emit.
fn status_label(status: u16) -> Cow<'static, str> {
    match status {
        200 => Cow::Borrowed("200"),
        202 => Cow::Borrowed("202"),
        400 => Cow::Borrowed("400"),
        413 => Cow::Borrowed("413"),
        415 => Cow::Borrowed("415"),
        429 => Cow::Borrowed("429"),
        500 => Cow::Borrowed("500"),
        503 => Cow::Borrowed("503"),
        other => Cow::Owned(other.to_string()),
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

fn metric_samples_batch(samples: &[MetricSample], signal: Signal) -> Result<RecordBatch> {
    let rows = samples.len();
    let timestamp = Utc::now().timestamp_micros();
    let resource_attributes = json!({"service.name": "canardstack"}).to_string();
    let mut fields = vec![
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        ),
        Field::new("start_timestamp", DataType::Int64, true),
        Field::new("metric_name", DataType::Utf8, true),
        Field::new("metric_description", DataType::Utf8, true),
        Field::new("metric_unit", DataType::Utf8, true),
        Field::new("value", DataType::Float64, true),
        Field::new("service_name", DataType::Utf8, true),
        Field::new("service_namespace", DataType::Utf8, true),
        Field::new("service_instance_id", DataType::Utf8, true),
        Field::new("resource_attributes", DataType::Utf8, true),
        Field::new("scope_name", DataType::Utf8, true),
        Field::new("scope_version", DataType::Utf8, true),
        Field::new("scope_attributes", DataType::Utf8, true),
        Field::new("metric_attributes", DataType::Utf8, true),
        Field::new("flags", DataType::Int32, true),
        Field::new("exemplars_json", DataType::Utf8, true),
    ];
    let mut arrays: Vec<ArrayRef> = vec![
        Arc::new(TimestampMicrosecondArray::from(vec![Some(timestamp); rows])),
        Arc::new(Int64Array::from(vec![None; rows])),
        Arc::new(StringArray::from(
            samples
                .iter()
                .map(|sample| Some(sample.name.clone()))
                .collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from(vec![None::<String>; rows])),
        Arc::new(StringArray::from(vec![None::<String>; rows])),
        Arc::new(Float64Array::from(
            samples
                .iter()
                .map(|sample| Some(sample.value))
                .collect::<Vec<_>>(),
        )),
        Arc::new(StringArray::from(vec![
            Some("canardstack".to_string());
            rows
        ])),
        Arc::new(StringArray::from(vec![None::<String>; rows])),
        Arc::new(StringArray::from(vec![None::<String>; rows])),
        Arc::new(StringArray::from(vec![Some(resource_attributes); rows])),
        Arc::new(StringArray::from(vec![
            Some("canardstack".to_string());
            rows
        ])),
        Arc::new(StringArray::from(vec![None::<String>; rows])),
        Arc::new(StringArray::from(vec![Some("{}".to_string()); rows])),
        Arc::new(StringArray::from(
            samples
                .iter()
                .map(|sample| Some(labels_json(&sample.labels).to_string()))
                .collect::<Vec<_>>(),
        )),
        Arc::new(Int32Array::from(vec![None; rows])),
        Arc::new(StringArray::from(vec![None::<String>; rows])),
    ];
    if signal == Signal::MetricSum {
        fields.push(Field::new("aggregation_temporality", DataType::Int32, true));
        fields.push(Field::new("is_monotonic", DataType::Boolean, true));
        arrays.push(Arc::new(Int32Array::from(vec![Some(2); rows])));
        arrays.push(Arc::new(BooleanArray::from(vec![Some(true); rows])));
    }
    RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)
        .context("build operator metrics RecordBatch")
}

fn labels_json(labels: &BTreeMap<String, String>) -> Value {
    let mut map = Map::new();
    for (k, v) in labels {
        map.insert(k.clone(), Value::String(v.clone()));
    }
    Value::Object(map)
}
