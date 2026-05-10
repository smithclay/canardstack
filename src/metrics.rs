use crate::ingest::Signal;
use crate::LockExt;
use std::collections::BTreeMap;
use std::sync::Mutex;
use std::time::Instant;

#[derive(Default)]
pub struct Metrics {
    inner: Mutex<MetricsInner>,
}

#[derive(Default)]
struct MetricsInner {
    counters: BTreeMap<String, u64>,
    gauges: BTreeMap<String, f64>,
}

impl Metrics {
    pub fn inc(&self, name: &str, labels: &[(&str, &str)], by: u64) {
        let mut inner = self.inner.lock_or_poisoned();
        *inner.counters.entry(key(name, labels)).or_default() += by;
    }

    pub fn gauge(&self, name: &str, labels: &[(&str, &str)], value: f64) {
        self.inner
            .lock_or_poisoned()
            .gauges
            .insert(key(name, labels), value);
    }

    pub fn observe_seconds(&self, name: &str, labels: &[(&str, &str)], seconds: f64) {
        let mut inner = self.inner.lock_or_poisoned();
        *inner
            .counters
            .entry(key(&format!("{name}_count"), labels))
            .or_default() += 1;
        let sum_key = key(&format!("{name}_sum"), labels);
        let current = inner.gauges.get(&sum_key).copied().unwrap_or(0.0);
        inner.gauges.insert(sum_key, current + seconds);
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

    pub fn ingest_request(&self, signal: Signal, status: u16, reason: &str) {
        self.inc(
            "canardstack_ingest_requests_total",
            &[
                ("signal", signal.as_str()),
                ("status", &status.to_string()),
                ("reason", reason),
            ],
            1,
        );
        if status == 429 || status == 503 {
            self.inc(
                "canardstack_ingest_rejections_total",
                &[
                    ("signal", signal.as_str()),
                    ("status", &status.to_string()),
                    ("reason", reason),
                ],
                1,
            );
        }
    }

    pub fn query_request(&self, query_class: &str, status: u16, reason: &str, seconds: f64) {
        self.inc(
            "canardstack_query_requests_total",
            &[
                ("query_class", query_class),
                ("status", &status.to_string()),
                ("reason", reason),
            ],
            1,
        );
        self.observe_seconds(
            "canardstack_query_duration_seconds",
            &[("query_class", query_class)],
            seconds,
        );
        if status == 429 {
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
        let inner = self.inner.lock_or_poisoned();
        let mut out = String::new();
        for (k, v) in &inner.counters {
            out.push_str(k);
            out.push(' ');
            out.push_str(&v.to_string());
            out.push('\n');
        }
        for (k, v) in &inner.gauges {
            out.push_str(k);
            out.push(' ');
            out.push_str(&format!("{v:.6}"));
            out.push('\n');
        }
        out
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

fn key(name: &str, labels: &[(&str, &str)]) -> String {
    if labels.is_empty() {
        return name.to_string();
    }
    let rendered = labels
        .iter()
        .map(|(k, v)| format!("{k}=\"{}\"", v.replace('"', "\\\"")))
        .collect::<Vec<_>>()
        .join(",");
    format!("{name}{{{rendered}}}")
}
