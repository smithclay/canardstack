use crate::signal::StorageSignal;
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

    pub fn observe_request_phase_seconds(&self, request_kind: &str, phase: &str, seconds: f64) {
        self.observe_labeled_phase_seconds(&[("request_kind", request_kind)], phase, seconds);
    }

    pub fn observe_request_phase_seconds_n(
        &self,
        request_kind: &str,
        phase: &str,
        count: u64,
        seconds: f64,
    ) {
        self.observe_labeled_phase_seconds_n(
            &[("request_kind", request_kind)],
            phase,
            count,
            seconds,
        );
    }

    pub fn observe_storage_signal_phase_seconds(
        &self,
        storage_signal: &str,
        phase: &str,
        seconds: f64,
    ) {
        self.observe_labeled_phase_seconds(&[("storage_signal", storage_signal)], phase, seconds);
    }

    pub fn observe_query_route_phase_seconds(
        &self,
        route_template: &str,
        phase: &str,
        seconds: f64,
    ) {
        self.observe_labeled_phase_seconds(&[("route_template", route_template)], phase, seconds);
    }

    fn observe_labeled_phase_seconds(&self, labels: &[(&str, &str)], phase: &str, seconds: f64) {
        self.observe_labeled_phase_seconds_n(labels, phase, 1, seconds);
    }

    fn observe_labeled_phase_seconds_n(
        &self,
        labels: &[(&str, &str)],
        phase: &str,
        count: u64,
        seconds: f64,
    ) {
        let mut phase_labels = Vec::with_capacity(labels.len() + 1);
        phase_labels.extend_from_slice(labels);
        phase_labels.push(("phase", phase));
        self.observe_seconds_n(
            "canardstack_phase_duration_seconds",
            &phase_labels,
            count,
            seconds,
        );
    }

    pub fn ingest_request(&self, request_kind: &str, status: u16, reason: &str) {
        let status = status_label(status);
        self.inc(
            "canardstack_ingest_requests_total",
            &[
                ("request_kind", request_kind),
                ("status", status.as_ref()),
                ("reason", reason),
            ],
            1,
        );
    }

    pub fn query_request(&self, route_template: &str, status: u16, reason: &str, seconds: f64) {
        let status = status_label(status);
        self.inc(
            "canardstack_query_requests_total",
            &[
                ("route_template", route_template),
                ("status", status.as_ref()),
                ("reason", reason),
            ],
            1,
        );
        self.observe_seconds(
            "canardstack_query_duration_seconds",
            &[("route_template", route_template)],
            seconds,
        );
        if reason == "query_timeout" {
            self.inc(
                "canardstack_query_timeouts_total",
                &[("route_template", route_template)],
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
            &[("job", job), ("storage_signal", "all")],
            seconds,
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
        // Sanctioned best-effort producer: operator self-telemetry is queryable
        // when enabled, but it has no raw-spool replay record.
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
            let batch = metric_samples_batch(&counters, StorageSignal::MetricSum)?;
            rows += storage.buffer_operator_metrics_snapshot(
                StorageSignal::MetricSum,
                &batch,
                "canardstack_operator_metrics",
            )?;
        }
        if !gauges.is_empty() {
            let batch = metric_samples_batch(&gauges, StorageSignal::MetricGauge)?;
            rows += storage.buffer_operator_metrics_snapshot(
                StorageSignal::MetricGauge,
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

fn metric_samples_batch(samples: &[MetricSample], signal: StorageSignal) -> Result<RecordBatch> {
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
    if signal == StorageSignal::MetricSum {
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

#[cfg(test)]
mod snapshot_tests {
    use super::*;
    use std::collections::BTreeSet;

    /// Reduce a rendered `/metrics` line to its metric base name: drop the value,
    /// drop the `{labels}`, and collapse the histogram `_count`/`_sum` pair onto
    /// the base name so a histogram counts as one metric.
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

    /// Drive a fixed, representative sequence of metric emissions that mirrors
    /// what production code emits (one sample per metric name), so the rendered
    /// surface can be pinned exactly. This is the post-diet contract guard: any
    /// future add/drop of a metric name must update this set deliberately.
    fn emit_representative_surface(metrics: &Metrics) {
        // Ingest request + query request envelopes.
        metrics.ingest_request("logs", 202, "accepted");
        metrics.ingest_request("logs", 429, "raw_spool_full");
        metrics.query_request("/api/v1/query_range", 200, "ok", 0.1);
        metrics.query_request("/api/v1/query_range", 504, "query_timeout", 0.1);

        // Worker dispatch + worker completion + storage insert.
        metrics.inc(
            "canardstack_ingest_worker_dispatch_total",
            &[("request_kind", "logs"), ("outcome", "queued")],
            1,
        );
        metrics.inc(
            "canardstack_ingest_worker_completed_total",
            &[("request_kind", "logs"), ("status", "buffered")],
            1,
        );
        metrics.inc(
            "canardstack_ingest_storage_insert_total",
            &[("request_kind", "logs"), ("status", "ok")],
            1,
        );
        metrics.gauge(
            "canardstack_ingest_worker_queue_capacity",
            &[("state", "capacity")],
            1024.0,
        );

        // Accepted-body + transform + buffered counters.
        metrics.inc(
            "canardstack_ingest_request_bytes_total",
            &[("request_kind", "logs"), ("encoding", "identity")],
            10,
        );
        metrics.inc(
            "canardstack_ingest_decoded_bytes_total",
            &[("request_kind", "logs"), ("encoding", "identity")],
            10,
        );
        metrics.inc(
            "canardstack_ingest_records_total",
            &[("request_kind", "logs")],
            1,
        );
        metrics.inc(
            "canardstack_ingest_transformed_rows_total",
            &[("storage_signal", "logs"), ("request_kind", "logs")],
            1,
        );
        metrics.inc(
            "canardstack_ingest_unsupported_histograms_total",
            &[("request_kind", "metrics")],
            1,
        );
        metrics.inc(
            "canardstack_ingest_buffered_rows_total",
            &[("storage_signal", "logs")],
            1,
        );
        metrics.inc(
            "canardstack_ingest_buffered_bytes_total",
            &[("storage_signal", "logs")],
            10,
        );

        // Ingest durable-boundary funnel (per request) and seal-boundary funnel
        // (per seal operation).
        metrics.inc(
            "canardstack_ingest_stage_total",
            &[("request_kind", "logs"), ("stage", "spooled")],
            1,
        );
        metrics.inc(
            "canardstack_ingest_seal_stage_total",
            &[("stage", "committed")],
            1,
        );

        // In-flight gauge (the `_max`, `_pressure`, and `_capacity_bytes`
        // derivatives were dropped).
        metrics.gauge(
            "canardstack_ingest_inflight_bytes",
            &[("storage_signal", "logs")],
            0.0,
        );

        // Runtime memory.
        metrics.gauge("canardstack_runtime_rss_bytes", &[], 1.0);
        metrics.gauge("canardstack_runtime_memory_limit_bytes", &[], 1.0);
        metrics.inc(
            "canardstack_ingest_runtime_memory_unknown_total",
            &[("request_kind", "logs")],
            1,
        );

        // Raw spool per-`request_kind` surface (aggregate no-label copies dropped).
        metrics.inc(
            "canardstack_raw_spool_records_total",
            &[("request_kind", "logs"), ("status", "spooled")],
            1,
        );
        metrics.inc(
            "canardstack_raw_spool_bytes_total",
            &[("request_kind", "logs")],
            10,
        );
        metrics.inc(
            "canardstack_raw_spool_append_batches_total",
            &[("request_kind", "logs")],
            1,
        );
        metrics.inc(
            "canardstack_raw_spool_append_batch_records_total",
            &[("request_kind", "logs")],
            1,
        );
        metrics.inc(
            "canardstack_raw_spool_append_batch_encoded_bytes_total",
            &[("request_kind", "logs")],
            10,
        );
        metrics.set_counter(
            "canardstack_raw_spool_append_syncs_total",
            &[("request_kind", "logs")],
            1,
        );
        metrics.set_counter(
            "canardstack_raw_spool_append_sync_failures_total",
            &[("request_kind", "logs")],
            0,
        );
        metrics.set_counter(
            "canardstack_raw_spool_append_file_fsyncs_total",
            &[("request_kind", "logs")],
            1,
        );
        metrics.inc(
            "canardstack_raw_spool_checkpoint_batches_total",
            &[("request_kind", "logs")],
            1,
        );
        metrics.inc(
            "canardstack_raw_spool_checkpoint_batch_records_total",
            &[("request_kind", "logs")],
            1,
        );
        metrics.inc(
            "canardstack_raw_spool_checkpoint_batch_commands_total",
            &[("request_kind", "logs")],
            1,
        );
        metrics.inc(
            "canardstack_raw_spool_replayed_records_total",
            &[("request_kind", "logs"), ("status", "ok")],
            1,
        );
        metrics.inc(
            "canardstack_raw_spool_checkpointed_records_total",
            &[("request_kind", "logs"), ("reason", "storage_committed")],
            1,
        );
        for name in [
            "canardstack_raw_spool_segment_bytes",
            "canardstack_raw_spool_segments",
            "canardstack_raw_spool_pending_records",
            "canardstack_raw_spool_pending_bytes",
            "canardstack_raw_spool_unsynced_records",
            "canardstack_raw_spool_unsynced_bytes",
            "canardstack_raw_spool_unsynced_age_seconds",
            "canardstack_raw_spool_healthy",
        ] {
            metrics.gauge(name, &[("request_kind", "logs")], 0.0);
        }

        // Coarse always-on phases (the fine spool micro-timings are gated).
        for phase in [
            "decompress",
            "otlp_transform",
            "timestamp_validation",
            "storage_buffer",
            "ingest_worker",
            "raw_spool_append",
            "raw_spool_checkpoint",
            "query_execute",
            "writer_lock_wait",
        ] {
            metrics.observe_request_phase_seconds("logs", phase, 0.001);
        }
        for phase in [
            "storage_prepare",
            "storage_arrow_write_buffer",
            "storage_duckdb_arrow_append",
            "storage_ducklake_commit",
        ] {
            metrics.observe_storage_signal_phase_seconds("logs", phase, 0.001);
        }
        // The coarse batch-checkpoint phase is emitted once, label-free.
        metrics.observe_seconds(
            "canardstack_phase_duration_seconds",
            &[("phase", "raw_spool_checkpoint")],
            0.001,
        );

        // Fine spool micro-timings: only emitted with `detailed-metrics`.
        #[cfg(feature = "detailed-metrics")]
        for phase in [
            "raw_spool_append_queue_wait",
            "raw_spool_append_batch_wait",
            "raw_spool_append_encode",
            "raw_spool_append_write",
            "raw_spool_append_fsync",
            "raw_spool_checkpoint_queue_wait",
            "raw_spool_checkpoint_batch_wait",
        ] {
            metrics.observe_request_phase_seconds("logs", phase, 0.001);
        }

        // DuckDB / Arrow flush counters.
        metrics.inc(
            "canardstack_duckdb_arrow_appends_total",
            &[("storage_signal", "logs")],
            1,
        );
        metrics.inc(
            "canardstack_duckdb_arrow_appended_rows_total",
            &[("storage_signal", "logs")],
            1,
        );
        metrics.inc(
            "canardstack_arrow_flushes_total",
            &[("storage_signal", "logs")],
            1,
        );
        metrics.inc(
            "canardstack_arrow_flush_rows_total",
            &[("storage_signal", "logs")],
            1,
        );

        // Admission surface (the per-kind rejection/reduction snapshot counters
        // were consolidated into the live inc-counter + one reductions counter).
        metrics.gauge(
            "canardstack_admission_capacity",
            &[("admission", "seal")],
            1.0,
        );
        metrics.gauge(
            "canardstack_admission_in_use",
            &[("admission", "seal")],
            0.0,
        );
        metrics.inc(
            "canardstack_admission_rejections_total",
            &[("admission", "query"), ("reason", "freshness_debt")],
            1,
        );
        metrics.set_counter("canardstack_admission_reductions_total", &[], 1);
        metrics.gauge("canardstack_seal_ewma_bytes_per_second", &[], 1.0);
        metrics.gauge("canardstack_projected_seal_seconds", &[], 0.0);
        metrics.gauge("canardstack_projected_buffer_seconds", &[], 0.0);
        metrics.gauge("canardstack_projected_visibility_seconds", &[], 0.0);
        metrics.gauge("canardstack_observed_freshness_lag_seconds", &[], 0.0);
        metrics.gauge("canardstack_ingest_inflight_memory_bound_bytes", &[], 1.0);

        // Maintenance.
        metrics.maintenance_run("seal", "ok", "ok", 0.01);
        metrics.inc(
            "canardstack_maintenance_failures_total",
            &[("job", "seal"), ("reason", "seal_failed")],
            1,
        );
        metrics.gauge(
            "canardstack_maintenance_consecutive_failures",
            &[("job", "seal")],
            0.0,
        );
        metrics.gauge("canardstack_maintenance_paused", &[], 0.0);

        // Arrow write buffer operator gauges.
        metrics.gauge(
            "canardstack_arrow_write_buffer_rows",
            &[("storage_signal", "logs")],
            0.0,
        );
        metrics.gauge(
            "canardstack_arrow_write_buffer_bytes",
            &[("storage_signal", "logs")],
            0.0,
        );
        metrics.gauge(
            "canardstack_arrow_write_buffer_age_seconds",
            &[("storage_signal", "logs")],
            0.0,
        );

        // Storage / freshness operator gauges.
        metrics.gauge(
            "canardstack_storage_physical_bytes",
            &[("storage_signal", "all")],
            0.0,
        );
        metrics.gauge(
            "canardstack_storage_physical_bytes",
            &[("table", "all")],
            0.0,
        );
        metrics.gauge(
            "canardstack_storage_logical_rows",
            &[("storage_signal", "logs")],
            0.0,
        );
        metrics.gauge(
            "canardstack_storage_logical_rows",
            &[("table", "logs")],
            0.0,
        );
        metrics.gauge(
            "canardstack_ducklake_active_data_files",
            &[("storage_signal", "logs")],
            0.0,
        );
        metrics.gauge(
            "canardstack_ducklake_active_data_files",
            &[("table", "logs")],
            0.0,
        );
        metrics.gauge(
            "canardstack_ducklake_active_data_file_rows",
            &[("storage_signal", "logs")],
            0.0,
        );
        metrics.gauge(
            "canardstack_ducklake_active_data_file_rows",
            &[("table", "logs")],
            0.0,
        );
        metrics.gauge(
            "canardstack_freshness_watermark_timestamp",
            &[("storage_signal", "logs")],
            0.0,
        );
        metrics.gauge(
            "canardstack_freshness_watermark_timestamp",
            &[("table", "logs")],
            0.0,
        );
        metrics.gauge(
            "canardstack_ingest_to_query_lag_seconds",
            &[("storage_signal", "logs")],
            0.0,
        );
        metrics.gauge(
            "canardstack_ingest_to_query_lag_seconds",
            &[("table", "logs")],
            0.0,
        );

        // HTTP connection counters.
        metrics.inc(
            "canardstack_http_connection_requests_total",
            &[("mode", "keep_alive")],
            1,
        );
        metrics.inc(
            "canardstack_http_connection_closes_total",
            &[("reason", "client")],
            1,
        );
        metrics.inc(
            "canardstack_http_connection_errors_total",
            &[("reason", "io_error")],
            1,
        );
    }

    /// The exact post-diet metric base-name set the lean default build renders.
    fn expected_lean_surface() -> BTreeSet<String> {
        [
            "canardstack_admission_capacity",
            "canardstack_admission_in_use",
            "canardstack_admission_reductions_total",
            "canardstack_admission_rejections_total",
            "canardstack_arrow_flush_rows_total",
            "canardstack_arrow_flushes_total",
            "canardstack_arrow_write_buffer_age_seconds",
            "canardstack_arrow_write_buffer_bytes",
            "canardstack_arrow_write_buffer_rows",
            "canardstack_duckdb_arrow_appended_rows_total",
            "canardstack_duckdb_arrow_appends_total",
            "canardstack_ducklake_active_data_file_rows",
            "canardstack_ducklake_active_data_files",
            "canardstack_freshness_watermark_timestamp",
            "canardstack_http_connection_closes_total",
            "canardstack_http_connection_errors_total",
            "canardstack_http_connection_requests_total",
            "canardstack_ingest_buffered_bytes_total",
            "canardstack_ingest_buffered_rows_total",
            "canardstack_ingest_decoded_bytes_total",
            "canardstack_ingest_inflight_bytes",
            "canardstack_ingest_inflight_memory_bound_bytes",
            "canardstack_ingest_records_total",
            "canardstack_ingest_request_bytes_total",
            "canardstack_ingest_requests_total",
            "canardstack_ingest_runtime_memory_unknown_total",
            "canardstack_ingest_seal_stage_total",
            "canardstack_ingest_stage_total",
            "canardstack_ingest_storage_insert_total",
            "canardstack_ingest_to_query_lag_seconds",
            "canardstack_ingest_transformed_rows_total",
            "canardstack_ingest_unsupported_histograms_total",
            "canardstack_ingest_worker_completed_total",
            "canardstack_ingest_worker_dispatch_total",
            "canardstack_ingest_worker_queue_capacity",
            "canardstack_maintenance_consecutive_failures",
            "canardstack_maintenance_duration_seconds",
            "canardstack_maintenance_failures_total",
            "canardstack_maintenance_paused",
            "canardstack_maintenance_runs_total",
            "canardstack_observed_freshness_lag_seconds",
            "canardstack_phase_duration_seconds",
            "canardstack_projected_buffer_seconds",
            "canardstack_projected_seal_seconds",
            "canardstack_projected_visibility_seconds",
            "canardstack_query_duration_seconds",
            "canardstack_query_requests_total",
            "canardstack_query_timeouts_total",
            "canardstack_raw_spool_append_batch_encoded_bytes_total",
            "canardstack_raw_spool_append_batch_records_total",
            "canardstack_raw_spool_append_batches_total",
            "canardstack_raw_spool_append_file_fsyncs_total",
            "canardstack_raw_spool_append_sync_failures_total",
            "canardstack_raw_spool_append_syncs_total",
            "canardstack_raw_spool_bytes_total",
            "canardstack_raw_spool_checkpoint_batch_commands_total",
            "canardstack_raw_spool_checkpoint_batch_records_total",
            "canardstack_raw_spool_checkpoint_batches_total",
            "canardstack_raw_spool_checkpointed_records_total",
            "canardstack_raw_spool_healthy",
            "canardstack_raw_spool_pending_bytes",
            "canardstack_raw_spool_pending_records",
            "canardstack_raw_spool_records_total",
            "canardstack_raw_spool_replayed_records_total",
            "canardstack_raw_spool_segment_bytes",
            "canardstack_raw_spool_segments",
            "canardstack_raw_spool_unsynced_age_seconds",
            "canardstack_raw_spool_unsynced_bytes",
            "canardstack_raw_spool_unsynced_records",
            "canardstack_runtime_memory_limit_bytes",
            "canardstack_runtime_rss_bytes",
            "canardstack_seal_ewma_bytes_per_second",
            "canardstack_storage_logical_rows",
            "canardstack_storage_physical_bytes",
        ]
        .into_iter()
        .map(str::to_string)
        .collect()
    }

    /// Metric names removed by the diet must never reappear on the surface.
    const DROPPED_NAMES: &[&str] = &[
        "canardstack_ingest_inflight_bytes_max",
        "canardstack_ingest_inflight_pressure_max",
        "canardstack_ingest_inflight_capacity_bytes",
        "canardstack_ingest_inflight_pressure",
        "canardstack_raw_spool_append_batch_records",
        "canardstack_raw_spool_append_batch_encoded_bytes",
        "canardstack_ingest_rejections_total",
        "canardstack_query_rejections_total",
        "canardstack_query_admission_rejections_total",
        "canardstack_query_admission_reductions_total",
        "canardstack_ingest_freshness_budget_rejections_total",
        "canardstack_ingest_requests_queued_total",
    ];

    #[test]
    fn render_prometheus_matches_post_diet_surface() {
        let metrics = Metrics::default();
        emit_representative_surface(&metrics);
        let rendered = metrics.render_prometheus();
        let names = rendered_base_names(&metrics);

        assert_eq!(
            names,
            expected_lean_surface(),
            "metric base-name surface drifted from the pinned post-diet set"
        );

        // Match each dropped name at a metric-name boundary (`{` for a labeled
        // series or ` ` for a bare one) so a surviving longer name that merely
        // shares a prefix (e.g. `_total`) is not a false positive.
        for dropped in DROPPED_NAMES {
            let with_labels = format!("{dropped}{{");
            let bare = format!("{dropped} ");
            assert!(
                !rendered.contains(&with_labels) && !rendered.contains(&bare),
                "dropped metric `{dropped}` reappeared on the surface"
            );
        }

        // The retired `spool_lane` label key must be gone everywhere.
        assert!(
            !rendered.contains("spool_lane="),
            "retired `spool_lane` label key is still emitted"
        );
    }

    #[test]
    fn coarse_phases_are_always_present() {
        let metrics = Metrics::default();
        emit_representative_surface(&metrics);
        let rendered = metrics.render_prometheus();
        for phase in [
            "phase=\"decompress\"",
            "phase=\"otlp_transform\"",
            "phase=\"storage_buffer\"",
            "phase=\"ingest_worker\"",
            "phase=\"raw_spool_append\"",
            "phase=\"raw_spool_checkpoint\"",
            "phase=\"query_execute\"",
            "phase=\"writer_lock_wait\"",
        ] {
            assert!(rendered.contains(phase), "missing coarse phase {phase}");
        }
    }

    #[cfg(not(feature = "detailed-metrics"))]
    #[test]
    fn fine_phases_absent_in_lean_build() {
        let metrics = Metrics::default();
        emit_representative_surface(&metrics);
        let rendered = metrics.render_prometheus();
        for phase in [
            "phase=\"raw_spool_append_queue_wait\"",
            "phase=\"raw_spool_append_batch_wait\"",
            "phase=\"raw_spool_append_encode\"",
            "phase=\"raw_spool_append_write\"",
            "phase=\"raw_spool_append_fsync\"",
            "phase=\"raw_spool_checkpoint_queue_wait\"",
            "phase=\"raw_spool_checkpoint_batch_wait\"",
        ] {
            assert!(
                !rendered.contains(phase),
                "fine phase {phase} must be gated out of the lean build"
            );
        }
    }

    #[cfg(feature = "detailed-metrics")]
    #[test]
    fn fine_phases_present_with_detailed_metrics() {
        let metrics = Metrics::default();
        emit_representative_surface(&metrics);
        let rendered = metrics.render_prometheus();
        for phase in [
            "phase=\"raw_spool_append_queue_wait\"",
            "phase=\"raw_spool_append_batch_wait\"",
            "phase=\"raw_spool_append_encode\"",
            "phase=\"raw_spool_append_write\"",
            "phase=\"raw_spool_append_fsync\"",
            "phase=\"raw_spool_checkpoint_queue_wait\"",
            "phase=\"raw_spool_checkpoint_batch_wait\"",
        ] {
            assert!(
                rendered.contains(phase),
                "fine phase {phase} must be present with detailed-metrics"
            );
        }
    }
}
