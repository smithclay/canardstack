use crate::app::AppState;
use crate::config::Config;
use crate::seal::SealDriver;
use crate::storage::{RetentionPolicy, Storage};
use crate::LockExt;
use anyhow::Result;
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

const SCHEDULER_METADATA_REFRESH_BUCKET_LIMIT: usize = 1;
/// Fraction of the freshness-budget SLA (max Arrow-write-buffer age / SLA) at
/// which the scheduler yields the metadata-refresh tick, so discovery
/// re-aggregation never competes with the writer while a seal is approaching
/// due.
const METADATA_REFRESH_BUFFER_AGE_YIELD: f64 = 0.70;
/// Fraction of the freshness-budget SLA at which the single scheduler thread
/// stops running non-seal maintenance (metadata refresh, metrics snapshot,
/// retention) for the tick. Past this point the Arrow-write-buffer age is close
/// enough to the SLA that letting a slow maintenance job hold the thread could
/// delay the next due seal and burn the freshness budget.
const SEAL_PRIORITY_SLA_FRACTION: f64 = 0.5;

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
}

impl Maintenance {
    pub fn new(config: &Config) -> Self {
        Self {
            paused: AtomicBool::new(false),
            last_runs: Mutex::new(BTreeMap::new()),
            last_failures: Mutex::new(BTreeMap::new()),
            retention: RetentionPolicy {
                logs_days: config.operator.logs_retention_days,
                spans_days: config.operator.spans_retention_days,
                metrics_days: config.operator.metrics_retention_days,
            },
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
        failures.iter().all(|(job, r)| {
            !failure_affects_readiness(job)
                || r.consecutive < Self::CONSECUTIVE_FAILURE_PAGE_THRESHOLD
        })
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
            "scheduler_jobs": ["metadata_refresh", "metrics_snapshot", "retention"]
        })
    }

    /// Record a successful seal run. The seal operation itself lives in
    /// [`crate::seal::run`]; this exposes the private run bookkeeping so that
    /// single entry point can mark the `seal` job as having succeeded.
    pub(crate) fn record_seal_run(&self) {
        self.record_run("seal");
    }

    pub fn retention(&self, storage: &Storage, dry_run: bool) -> Result<Value> {
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
            // Physical file compaction is a deliberate v0 non-goal, reported as
            // `supported:false, enabled:false` rather than implemented. DuckLake's
            // `ducklake_merge_adjacent_files` is intentionally NOT called: it stays
            // disabled until proven stable for this append/seal write pattern.
            // v0 therefore tolerates many small Parquet segment files; the Arrow
            // write buffer's size/age coalescing (one seal -> one segment per
            // signal) is the only file-count mitigation. Do not add a compaction
            // code path here without revisiting that decision.
            "physical_file_compaction": {
                "supported": false,
                "enabled": false,
                "reason": "ducklake_merge_adjacent_files is disabled until proven stable"
            },
            "cleanup": cleanup,
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
    let mut seal_driver = SealDriver::new(&state.config, Instant::now());
    let seal_cadence = state.config.mechanics.scheduler_seal_interval;
    let metadata_every = state.config.mechanics.scheduler_metadata_interval;
    let metrics_every = state.config.mechanics.scheduler_metrics_interval;
    let retention_every = state.config.mechanics.scheduler_retention_interval;
    let freshness_budget_sla_seconds = state.config.operator.freshness_budget_sla.as_secs_f64();
    // Poll fast enough to seal on the freshness cadence and to catch a
    // size-due buffer promptly; the maintenance jobs are gated by their own
    // (coarse) timers regardless of how often we wake.
    let tick = seal_cadence
        .min(metadata_every)
        .min(Duration::from_millis(250))
        .max(Duration::from_millis(10));
    let mut next_metadata = Instant::now() + metadata_every;
    let mut next_metrics = Instant::now() + metrics_every;
    let mut next_retention = Instant::now() + retention_every;

    loop {
        if stop.load(Ordering::SeqCst) {
            return;
        }
        thread::sleep(tick);
        if stop.load(Ordering::SeqCst) {
            return;
        }
        let now = Instant::now();

        if state.maintenance.is_paused() {
            continue;
        }

        let seal_tick = seal_driver.tick(&state, now, run_seal_tick);
        let max_buffer_age_seconds = seal_tick.max_buffer_age_seconds;

        // Seal priority: when the oldest Arrow write buffer is within
        // SEAL_PRIORITY_SLA_FRACTION of the freshness SLA, skip the non-seal
        // maintenance jobs this tick so a slow metadata/metrics/retention job
        // cannot hold the scheduler thread and delay the next due seal. The
        // skipped jobs do NOT advance their `next_*` timers, so they run as soon
        // as the buffer-age pressure clears.
        let buffer_age_pressure = if freshness_budget_sla_seconds > 0.0 {
            max_buffer_age_seconds / freshness_budget_sla_seconds
        } else {
            0.0
        };
        let seal_priority_active = buffer_age_pressure >= SEAL_PRIORITY_SLA_FRACTION;

        if !seal_priority_active && now >= next_metadata {
            let ok = run_job(&state, "metadata_refresh", |s| {
                if metadata_refresh_should_yield_to_ingest(buffer_age_pressure) {
                    return Ok(json!({
                        "status": "skipped",
                        "reason": "ingest_pressure",
                        "buffer_age_pressure": buffer_age_pressure
                    }));
                }
                let outcome = s
                    .storage
                    .refresh_metadata_limited(SCHEDULER_METADATA_REFRESH_BUCKET_LIMIT)?;
                s.metrics.observe_seconds(
                    "canardstack_phase_duration_seconds",
                    &[("phase", "writer_lock_wait"), ("path", "metadata_refresh")],
                    outcome.writer_lock_wait_seconds,
                );
                Ok(json!({"status": "ok", "buckets": outcome.buckets}))
            });
            next_metadata = now + next_interval(&state, "metadata_refresh", metadata_every, ok);
        }

        if !seal_priority_active && now >= next_metrics {
            let ok = run_job(&state, "metrics_snapshot", |s| {
                crate::http::record_operator_gauges(s);
                crate::http::record_storage_operator_gauges(s);
                // The operator gauges above always refresh. Persisting a snapshot
                // into the metric_gauge / metric_sum storage tables is opt-in to
                // avoid the extra write load and the canardstack_operator_metrics
                // rows by default.
                if s.config.mechanics.operator_metrics_to_storage {
                    let rows = s.metrics.write_snapshot_to_storage(&s.storage)?;
                    Ok(json!({"status": "ok", "rows": rows}))
                } else {
                    Ok(json!({
                        "status": "ok",
                        "rows": 0,
                        "operator_metrics_to_storage": false
                    }))
                }
            });
            next_metrics = now + next_interval(&state, "metrics_snapshot", metrics_every, ok);
        }

        if !seal_priority_active && now >= next_retention {
            let ok = run_job(&state, "retention", |s| {
                s.maintenance.retention(&s.storage, false)
            });
            next_retention = now + next_interval(&state, "retention", retention_every, ok);
        }
    }
}

fn run_seal_tick(state: &AppState) -> bool {
    run_job(state, "seal", |s| {
        crate::seal::run(s).map_err(|err| anyhow::anyhow!(err.message.clone()))
    })
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
            let reason = classify_job_error(&err, job);
            let consecutive = state.maintenance.record_failure(job, reason);
            tracing::error!(
                event = "scheduler_job_failed",
                job,
                reason,
                consecutive,
                error = %err
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

fn failure_affects_readiness(job: &str) -> bool {
    !matches!(job, "metrics_snapshot")
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

/// Yield the metadata-refresh tick when Arrow-write-buffer age pressure (max
/// buffer age / freshness SLA) crosses the yield threshold, so discovery
/// re-aggregation never competes with the writer while a seal is approaching
/// due.
fn metadata_refresh_should_yield_to_ingest(buffer_age_pressure: f64) -> bool {
    buffer_age_pressure >= METADATA_REFRESH_BUFFER_AGE_YIELD
}

/// Bounded `reason` label. Most reasons derive from `job` (a static str we
/// control); only `disk_full` substring-matches OS/DuckDB messages.
fn classify_job_error(err: &anyhow::Error, job: &str) -> &'static str {
    let lower = err.to_string().to_ascii_lowercase();
    if lower.contains("no space left") || lower.contains("disk full") {
        return "disk_full";
    }
    match job {
        "seal" => "seal_failed",
        "metadata_refresh" => "metadata_refresh_failed",
        "metrics_snapshot" => "metrics_snapshot_failed",
        "retention" | "retention_dry_run" => "retention_failed",
        _ => "scheduler_job_failed",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metadata_refresh_runs_when_buffer_age_pressure_below_threshold() {
        assert!(!metadata_refresh_should_yield_to_ingest(0.69));
    }

    #[test]
    fn metadata_refresh_yields_to_high_buffer_age_pressure_alone() {
        // The oldest buffer is past the yield threshold fraction of the SLA ->
        // yield so the seal stays ahead of discovery re-aggregation.
        assert!(metadata_refresh_should_yield_to_ingest(
            METADATA_REFRESH_BUFFER_AGE_YIELD
        ));
    }
}
