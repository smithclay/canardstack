//! Query admission control.
//!
//! canardstack is query-only: admission protects the embedded DuckDB process by
//! reserving fixed slots for cheap metadata/instant-style queries and separate
//! slots for heavier range/search/trace queries.

use crate::config::Config;
use crate::metrics::{MetricName, Metrics};
use crate::validation::{ApiError, ApiResult};
use crate::LockExt;
use serde::Serialize;
use std::sync::{Condvar, Mutex};
use std::time::{Duration, Instant};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum QueryClass {
    Cheap,
    Heavy,
}

impl QueryClass {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Cheap => "cheap",
            Self::Heavy => "heavy",
        }
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct AdmissionSnapshot {
    pub cheap_query_active: usize,
    pub cheap_query_capacity: usize,
    pub heavy_query_active: usize,
    pub heavy_query_capacity: usize,
    pub query_rejections_total: u64,
}

pub struct AdmissionController {
    inner: Mutex<AdmissionState>,
    query_available: Condvar,
    cheap_query_capacity: usize,
    heavy_query_capacity: usize,
}

#[derive(Debug)]
struct AdmissionState {
    cheap_query_active: usize,
    heavy_query_active: usize,
    query_rejections_total: u64,
}

impl AdmissionController {
    pub fn new(config: &Config) -> Self {
        let cheap_query_capacity = config
            .operator
            .cheap_query_admission_capacity
            .min(
                config
                    .operator
                    .query_interactive
                    .concurrency
                    .saturating_sub(1),
            )
            .max(1);
        let heavy_query_capacity = config
            .operator
            .query_interactive
            .concurrency
            .saturating_sub(cheap_query_capacity)
            .max(1);
        Self {
            inner: Mutex::new(AdmissionState {
                cheap_query_active: 0,
                heavy_query_active: 0,
                query_rejections_total: 0,
            }),
            query_available: Condvar::new(),
            cheap_query_capacity,
            heavy_query_capacity,
        }
    }

    pub fn reserve_query(
        &self,
        class: QueryClass,
        metrics: &Metrics,
    ) -> ApiResult<QueryAdmissionGuard<'_>> {
        self.reserve_query_with_wait(class, Duration::ZERO, metrics)
    }

    pub fn reserve_query_with_wait(
        &self,
        class: QueryClass,
        max_wait: Duration,
        metrics: &Metrics,
    ) -> ApiResult<QueryAdmissionGuard<'_>> {
        let mut state = self.inner.lock_or_poisoned();
        let deadline = Instant::now() + max_wait;
        let result = loop {
            if self.try_reserve_query_locked(class, &mut state) {
                break Ok(QueryAdmissionGuard {
                    admission: self,
                    class,
                });
            }
            if max_wait.is_zero() {
                break self.reject_query_locked(class, &mut state, metrics);
            }
            let now = Instant::now();
            if now >= deadline {
                break self.reject_query_locked(class, &mut state, metrics);
            }
            let remaining = deadline.saturating_duration_since(now);
            let wait_result = self
                .query_available
                .wait_timeout(state, remaining)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            state = wait_result.0;
        };
        let snapshot = self.snapshot_from_state(&state);
        drop(state);
        self.emit_admission_gauges(metrics, &snapshot);
        result
    }

    fn try_reserve_query_locked(&self, class: QueryClass, state: &mut AdmissionState) -> bool {
        match class {
            QueryClass::Cheap if state.cheap_query_active < self.cheap_query_capacity => {
                state.cheap_query_active += 1;
                true
            }
            QueryClass::Heavy if state.heavy_query_active < self.heavy_query_capacity => {
                state.heavy_query_active += 1;
                true
            }
            _ => false,
        }
    }

    fn reject_query_locked(
        &self,
        class: QueryClass,
        state: &mut AdmissionState,
        metrics: &Metrics,
    ) -> ApiResult<QueryAdmissionGuard<'_>> {
        let reason = query_rejection_reason(class);
        state.query_rejections_total += 1;
        metrics.inc(
            MetricName::AdmissionRejectionsTotal,
            &[("admission", "query"), ("reason", reason)],
            1,
        );
        Err(ApiError::new(429, reason, query_rejection_message(class)).with_retry_after(1))
    }

    pub fn snapshot(&self) -> AdmissionSnapshot {
        let state = self.inner.lock_or_poisoned();
        self.snapshot_from_state(&state)
    }

    pub fn record_metrics(&self, metrics: &Metrics) {
        let snapshot = self.snapshot();
        self.emit_admission_gauges(metrics, &snapshot);
    }

    fn snapshot_from_state(&self, state: &AdmissionState) -> AdmissionSnapshot {
        AdmissionSnapshot {
            cheap_query_active: state.cheap_query_active,
            cheap_query_capacity: self.cheap_query_capacity,
            heavy_query_active: state.heavy_query_active,
            heavy_query_capacity: self.heavy_query_capacity,
            query_rejections_total: state.query_rejections_total,
        }
    }

    fn emit_admission_gauges(&self, metrics: &Metrics, snapshot: &AdmissionSnapshot) {
        metrics.gauge(
            MetricName::AdmissionCapacity,
            &[("admission", "query_cheap")],
            snapshot.cheap_query_capacity as f64,
        );
        metrics.gauge(
            MetricName::AdmissionInUse,
            &[("admission", "query_cheap")],
            snapshot.cheap_query_active as f64,
        );
        metrics.gauge(
            MetricName::AdmissionCapacity,
            &[("admission", "query_heavy")],
            snapshot.heavy_query_capacity as f64,
        );
        metrics.gauge(
            MetricName::AdmissionInUse,
            &[("admission", "query_heavy")],
            snapshot.heavy_query_active as f64,
        );
    }
}

pub struct QueryAdmissionGuard<'a> {
    admission: &'a AdmissionController,
    class: QueryClass,
}

impl Drop for QueryAdmissionGuard<'_> {
    fn drop(&mut self) {
        let mut state = self.admission.inner.lock_or_poisoned();
        match self.class {
            QueryClass::Cheap => {
                state.cheap_query_active = state.cheap_query_active.saturating_sub(1);
            }
            QueryClass::Heavy => {
                state.heavy_query_active = state.heavy_query_active.saturating_sub(1);
            }
        }
        drop(state);
        self.admission.query_available.notify_one();
    }
}

fn query_rejection_reason(class: QueryClass) -> &'static str {
    match class {
        QueryClass::Cheap => "cheap_query_admission_full",
        QueryClass::Heavy => "heavy_query_admission_full",
    }
}

fn query_rejection_message(class: QueryClass) -> &'static str {
    match class {
        QueryClass::Cheap => "cheap query admission capacity is exhausted",
        QueryClass::Heavy => "heavy query admission capacity is exhausted",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use std::sync::Arc;
    use tempfile::tempdir;

    fn controller() -> AdmissionController {
        let dir = tempdir().unwrap();
        let mut config = Config::test(dir.path().join("canardstack.duckdb"));
        config.operator.query_interactive.concurrency = 4;
        config.operator.cheap_query_admission_capacity = 1;
        AdmissionController::new(&config)
    }

    #[test]
    fn cheap_and_heavy_queries_use_separate_capacity() {
        let admission = controller();
        let metrics = Metrics::default();
        let _cheap = admission
            .reserve_query(QueryClass::Cheap, &metrics)
            .unwrap();
        let _heavy1 = admission
            .reserve_query(QueryClass::Heavy, &metrics)
            .unwrap();
        let _heavy2 = admission
            .reserve_query(QueryClass::Heavy, &metrics)
            .unwrap();
        let _heavy3 = admission
            .reserve_query(QueryClass::Heavy, &metrics)
            .unwrap();

        let err = match admission.reserve_query(QueryClass::Heavy, &metrics) {
            Ok(_) => panic!("heavy query should reject when capacity is full"),
            Err(err) => err,
        };
        assert_eq!(err.status, 429);
        assert_eq!(err.reason, "heavy_query_admission_full");

        let err = match admission.reserve_query(QueryClass::Cheap, &metrics) {
            Ok(_) => panic!("cheap query should reject when capacity is full"),
            Err(err) => err,
        };
        assert_eq!(err.status, 429);
        assert_eq!(err.reason, "cheap_query_admission_full");
    }

    #[test]
    fn query_waits_for_capacity_before_rejecting() {
        let admission = Arc::new(controller());
        let metrics = Arc::new(Metrics::default());
        let first = admission
            .reserve_query(QueryClass::Cheap, &metrics)
            .unwrap();
        let waiting_admission = admission.clone();
        let waiting_metrics = metrics.clone();
        let waiting = std::thread::spawn(move || {
            waiting_admission
                .reserve_query_with_wait(
                    QueryClass::Cheap,
                    std::time::Duration::from_millis(250),
                    &waiting_metrics,
                )
                .map(|_guard| ())
        });

        std::thread::sleep(std::time::Duration::from_millis(20));
        drop(first);

        assert!(waiting.join().unwrap().is_ok());
    }

    #[test]
    fn snapshot_reports_active_and_capacity() {
        let admission = controller();
        let metrics = Metrics::default();
        let _cheap = admission
            .reserve_query(QueryClass::Cheap, &metrics)
            .unwrap();
        let snapshot = admission.snapshot();

        assert_eq!(snapshot.cheap_query_active, 1);
        assert_eq!(snapshot.cheap_query_capacity, 1);
        assert_eq!(snapshot.heavy_query_capacity, 3);
    }
}
