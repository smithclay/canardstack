use crate::app::AppState;
use crate::config::Config;
use crate::ingest::Ingestor;
use crate::metrics::Metrics;
use crate::storage::{RetentionPolicy, Storage};
use crate::LockExt;
use anyhow::Result;
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

#[derive(Clone, Debug)]
struct FailureRecord {
    at: String,
    reason: String,
    consecutive: u32,
}

pub struct Maintenance {
    paused: AtomicBool,
    last_runs: Mutex<BTreeMap<String, String>>,
    last_failures: Mutex<BTreeMap<String, FailureRecord>>,
    retention: RetentionPolicy,
    compaction_min_files: usize,
}

impl Maintenance {
    pub fn new(config: &Config) -> Self {
        Self {
            paused: AtomicBool::new(false),
            last_runs: Mutex::new(BTreeMap::new()),
            last_failures: Mutex::new(BTreeMap::new()),
            retention: RetentionPolicy {
                logs_days: config.logs_retention_days,
                spans_days: config.spans_retention_days,
                metrics_days: config.metrics_retention_days,
            },
            compaction_min_files: config.ducklake_compaction_min_files,
        }
    }

    pub fn pause(&self) {
        self.paused.store(true, Ordering::SeqCst);
    }

    pub fn resume(&self) {
        self.paused.store(false, Ordering::SeqCst);
    }

    pub fn is_paused(&self) -> bool {
        self.paused.load(Ordering::SeqCst)
    }

    /// Page when any job hits this many consecutive failures.
    const CONSECUTIVE_FAILURE_PAGE_THRESHOLD: u32 = 3;

    pub fn is_ready(&self) -> bool {
        let failures = self.last_failures.lock_or_poisoned();
        failures
            .values()
            .all(|r| r.consecutive < Self::CONSECUTIVE_FAILURE_PAGE_THRESHOLD)
    }

    pub fn health(&self) -> Value {
        let failures = self.last_failures.lock_or_poisoned();
        let failures_json: BTreeMap<String, Value> = failures
            .iter()
            .map(|(job, rec)| {
                (
                    job.clone(),
                    json!({
                        "at": rec.at,
                        "reason": rec.reason,
                        "consecutive": rec.consecutive,
                    }),
                )
            })
            .collect();
        json!({
            "paused": self.is_paused(),
            "singleton_role": "in_process_dev_lease",
            "last_runs": self.last_runs.lock_or_poisoned().clone(),
            "last_failures": failures_json,
            "retention_days": {
                "logs": self.retention.logs_days,
                "spans": self.retention.spans_days,
                "metrics": self.retention.metrics_days
            },
            "compaction_min_files": self.compaction_min_files,
            "priority_order": ["queue_watchdog", "flush_inlined_data", "merge_adjacent_files", "retention"]
        })
    }

    pub fn run_flush(
        &self,
        ingestor: &Ingestor,
        storage: &Storage,
        table: Option<&str>,
        metrics: Option<&Metrics>,
        force_immutable_segments: bool,
    ) -> Result<Value> {
        if self.is_paused() {
            return Ok(json!({"status": "paused"}));
        }
        let started = Instant::now();
        let process_rows = ingestor.flush_all(storage)?;
        let immutable_segments = storage.flush_immutable_segments(force_immutable_segments)?;
        if let Some(metrics) = metrics {
            observe_immutable_segment_timings(metrics, &immutable_segments);
        }
        let ducklake_started = Instant::now();
        let ducklake = storage.flush_inlined_data(table)?;
        if let Some(metrics) = metrics {
            metrics.observe_seconds(
                "canardstack_ducklake_flush_inlined_duration_seconds",
                &[("table", table.unwrap_or("all"))],
                ducklake_started.elapsed().as_secs_f64(),
            );
        }
        self.record_run("flush");
        Ok(json!({
            "status": "ok",
            "process_rows_flushed": process_rows,
            "immutable_segments": immutable_segments,
            "ducklake": ducklake,
            "duration_ms": started.elapsed().as_millis()
        }))
    }

    pub fn run_compaction(
        &self,
        storage: &Storage,
        table: Option<&str>,
        metrics: Option<&Metrics>,
    ) -> Result<Value> {
        if self.is_paused() {
            return Ok(json!({"status": "paused"}));
        }
        let started = Instant::now();
        let decision = storage.compaction_decision(table, self.compaction_min_files)?;
        if !decision
            .get("should_compact")
            .and_then(Value::as_bool)
            .unwrap_or(false)
        {
            self.record_run("compaction");
            return Ok(json!({
                "status": "skipped",
                "reason": decision.get("status").and_then(Value::as_str).unwrap_or("not_needed"),
                "decision": decision,
                "duration_ms": started.elapsed().as_millis()
            }));
        }

        let compaction_started = Instant::now();
        let compaction = storage.merge_adjacent_files(table)?;
        if let Some(metrics) = metrics {
            metrics.observe_seconds(
                "canardstack_ducklake_compaction_duration_seconds",
                &[("table", table.unwrap_or("all"))],
                compaction_started.elapsed().as_secs_f64(),
            );
        }
        self.record_run("compaction");
        Ok(json!({
            "status": "ok",
            "decision": decision,
            "compaction": compaction,
            "duration_ms": started.elapsed().as_millis()
        }))
    }

    pub fn retention(&self, storage: &Storage, dry_run: bool) -> Result<Value> {
        if self.is_paused() {
            return Ok(json!({"status": "paused"}));
        }
        let started = Instant::now();
        let retention = storage.enforce_retention(&self.retention, dry_run)?;
        let snapshot_expiration = if dry_run {
            json!({"supported": storage.health().capabilities.snapshot_expiration, "dry_run": true})
        } else {
            storage.expire_snapshots(self.retention.metrics_days)?
        };
        let cleanup = storage.cleanup_old_files(dry_run)?;
        if !dry_run {
            self.record_run("retention");
        } else {
            self.record_run("retention_dry_run");
        }
        Ok(json!({
            "status": "ok",
            "dry_run": dry_run,
            "retention": retention,
            "snapshot_expiration": snapshot_expiration,
            "cleanup": cleanup,
            "duration_ms": started.elapsed().as_millis()
        }))
    }

    pub fn run_watchdog(
        &self,
        ingestor: &Ingestor,
        storage: &Storage,
        metrics: &Metrics,
    ) -> Result<Value> {
        if self.is_paused() {
            return Ok(json!({"status": "paused"}));
        }
        let started = Instant::now();
        let flushed = ingestor.flush_due(storage, Some(metrics))?;
        let immutable_segments = storage.flush_immutable_segments(false)?;
        observe_immutable_segment_timings(metrics, &immutable_segments);
        if !flushed.is_empty() {
            self.record_run("watchdog");
        }
        let by_signal: BTreeMap<String, usize> = flushed
            .into_iter()
            .map(|(s, n)| (s.as_str().to_string(), n))
            .collect();
        Ok(json!({
            "status": "ok",
            "flushed": by_signal,
            "immutable_segments": immutable_segments,
            "duration_ms": started.elapsed().as_millis()
        }))
    }

    fn record_run(&self, job: &str) {
        self.last_runs
            .lock_or_poisoned()
            .insert(job.to_string(), chrono::Utc::now().to_rfc3339());
        self.last_failures.lock_or_poisoned().remove(job);
    }

    fn record_failure(&self, job: &str, reason: &str) -> u32 {
        let mut failures = self.last_failures.lock_or_poisoned();
        let prev_consecutive = failures.get(job).map(|r| r.consecutive).unwrap_or(0);
        let consecutive = prev_consecutive.saturating_add(1);
        failures.insert(
            job.to_string(),
            FailureRecord {
                at: chrono::Utc::now().to_rfc3339(),
                reason: reason.to_string(),
                consecutive,
            },
        );
        consecutive
    }

    fn consecutive_failures(&self, job: &str) -> u32 {
        self.last_failures
            .lock_or_poisoned()
            .get(job)
            .map(|r| r.consecutive)
            .unwrap_or(0)
    }
}

fn observe_immutable_segment_timings(metrics: &Metrics, result: &Value) {
    let Some(timings) = result.get("timings").and_then(Value::as_array) else {
        return;
    };
    for timing in timings {
        let Some(table) = timing.get("table").and_then(Value::as_str) else {
            continue;
        };
        let Some(phase) = timing.get("phase").and_then(Value::as_str) else {
            continue;
        };
        let Some(seconds) = timing.get("seconds").and_then(Value::as_f64) else {
            continue;
        };
        metrics.observe_phase_seconds(table, phase, None, seconds);
    }
}

pub struct Scheduler {
    stop: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
}

impl Scheduler {
    pub fn spawn(state: Arc<AppState>) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let stop_for_thread = stop.clone();
        let handle = thread::Builder::new()
            .name("canardstack-scheduler".to_string())
            .spawn(move || scheduler_loop(state, stop_for_thread))
            .expect("spawn scheduler thread");
        Self {
            stop,
            handle: Some(handle),
        }
    }
}

impl Drop for Scheduler {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        if let Some(h) = self.handle.take() {
            let _ = h.join();
        }
    }
}

fn scheduler_loop(state: Arc<AppState>, stop: Arc<AtomicBool>) {
    let watchdog_every = state.config.scheduler_watchdog_interval;
    let flush_every = state.config.scheduler_flush_interval;
    let metadata_every = state.config.scheduler_metadata_interval;
    let metrics_every = state.config.scheduler_metrics_interval;
    let compaction_every = state.config.scheduler_compaction_interval;
    let retention_every = state.config.scheduler_retention_interval;
    let tick = watchdog_every
        .min(Duration::from_millis(500))
        .max(Duration::from_millis(10));
    let mut next_watchdog = Instant::now();
    let mut next_flush = Instant::now();
    let mut next_metadata = Instant::now() + metadata_every;
    let mut next_metrics = Instant::now() + metrics_every;
    let mut next_compaction = Instant::now() + compaction_every;
    let mut next_retention = Instant::now() + retention_every;

    loop {
        if stop.load(Ordering::SeqCst) {
            return;
        }
        let flush_requested = state.ingestor.wait_for_flush_or_timeout(tick, &stop);
        if stop.load(Ordering::SeqCst) {
            return;
        }
        let now = Instant::now();

        if flush_requested || now >= next_watchdog {
            let ok = run_job(&state, "watchdog", |s| {
                s.maintenance
                    .run_watchdog(&s.ingestor, &s.storage, &s.metrics)
            });
            next_watchdog = now + next_interval(&state, "watchdog", watchdog_every, ok);
        }

        if now >= next_flush {
            let ok = run_job(&state, "flush", |s| {
                s.maintenance
                    .run_flush(&s.ingestor, &s.storage, None, Some(&s.metrics), false)
            });
            next_flush = now + next_interval(&state, "flush", flush_every, ok);
        }

        if now >= next_metadata {
            let ok = run_job(&state, "metadata_refresh", |s| {
                if s.maintenance.is_paused() {
                    return Ok(json!({"status": "paused"}));
                }
                let buckets = s.storage.refresh_metadata()?;
                Ok(json!({"status": "ok", "buckets": buckets}))
            });
            next_metadata = now + next_interval(&state, "metadata_refresh", metadata_every, ok);
        }

        if now >= next_metrics {
            let ok = run_job(&state, "metrics_snapshot", |s| {
                crate::http::record_operator_gauges(s);
                let rows = s.metrics.write_snapshot_to_storage(&s.storage)?;
                Ok(json!({"status": "ok", "rows": rows}))
            });
            next_metrics = now + next_interval(&state, "metrics_snapshot", metrics_every, ok);
        }

        if now >= next_compaction {
            let ok = run_job(&state, "compaction", |s| {
                s.maintenance
                    .run_compaction(&s.storage, None, Some(&s.metrics))
            });
            next_compaction = now + next_interval(&state, "compaction", compaction_every, ok);
        }

        if now >= next_retention {
            let ok = run_job(&state, "retention", |s| {
                s.maintenance.retention(&s.storage, false)
            });
            next_retention = now + next_interval(&state, "retention", retention_every, ok);
        }
    }
}

fn run_job<F>(state: &AppState, job: &'static str, f: F) -> bool
where
    F: FnOnce(&AppState) -> Result<Value>,
{
    let started = Instant::now();
    let ok = match f(state) {
        Ok(_) => {
            state
                .metrics
                .maintenance_run(job, "ok", "ok", started.elapsed().as_secs_f64());
            true
        }
        Err(err) => {
            if let Some((partial_signal, committed)) = crate::ingest::partial_commit_info(&err) {
                if committed > 0 {
                    state.metrics.inc(
                        "canardstack_ingest_partial_commit_rows_total",
                        &[("signal", partial_signal.as_str()), ("triggered_by", job)],
                        committed as u64,
                    );
                }
            }
            let reason = classify_job_error(&err, job);
            let consecutive = state.maintenance.record_failure(job, reason);
            let consecutive_str = consecutive.to_string();
            let err_str = err.to_string();
            crate::log_event(
                "error",
                "scheduler_job_failed",
                &[
                    ("job", job),
                    ("reason", reason),
                    ("consecutive", &consecutive_str),
                    ("error", &err_str),
                ],
            );
            state
                .metrics
                .maintenance_run(job, "error", reason, started.elapsed().as_secs_f64());
            state.metrics.inc(
                "canardstack_maintenance_failures_total",
                &[("job", job), ("reason", reason)],
                1,
            );
            false
        }
    };
    state.metrics.gauge(
        "canardstack_maintenance_consecutive_failures",
        &[("job", job)],
        state.maintenance.consecutive_failures(job) as f64,
    );
    ok
}

fn next_interval(state: &AppState, job: &str, base: Duration, ok: bool) -> Duration {
    if ok {
        return base;
    }
    // Exponential backoff capped at 5 minutes so a perma-broken catalog doesn't
    // pin a worker thread re-failing every tick and flooding stderr.
    let consecutive = state.maintenance.consecutive_failures(job).min(8);
    let multiplier = 1u64 << consecutive.saturating_sub(1);
    let backoff = base.saturating_mul(multiplier as u32);
    backoff.min(Duration::from_secs(300)).max(base)
}

/// Bounded `reason` label. Most reasons derive from `job` (a static str we
/// control); only `disk_full` substring-matches OS/DuckDB messages.
fn classify_job_error(err: &anyhow::Error, job: &str) -> &'static str {
    let lower = err.to_string().to_ascii_lowercase();
    if lower.contains("no space left") || lower.contains("disk full") {
        return "disk_full";
    }
    match job {
        "flush" | "watchdog" => "flush_failed",
        "metadata_refresh" => "metadata_refresh_failed",
        "metrics_snapshot" => "metrics_snapshot_failed",
        "compaction" => "compaction_failed",
        "retention" | "retention_dry_run" => "retention_failed",
        _ => "scheduler_job_failed",
    }
}
