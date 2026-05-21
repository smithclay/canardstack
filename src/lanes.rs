use crate::config::Config;
use crate::metrics::Metrics;
use crate::validation::{ApiError, ApiResult};
use crate::LockExt;
use serde::Serialize;
use std::sync::Mutex;
use std::time::Instant;

const EWMA_ALPHA: f64 = 0.20;
const HEAVY_QUERY_DEGRADE_FRACTION: f64 = 1.00;
const HEAVY_QUERY_REJECT_FRACTION: f64 = 1.50;
const INGEST_FRESHNESS_BUDGET_FRACTION: f64 = 0.95;
const INGEST_FRESHNESS_BUDGET_WITH_HEAVY_FRACTION: f64 = 0.90;

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

#[derive(Clone, Copy, Debug)]
pub struct FreshnessInputs {
    pub queued_bytes: usize,
    pub incoming_bytes: usize,
    pub oldest_queue_age_seconds: f64,
    pub buffered_bytes: usize,
    pub buffered_active_count: usize,
    pub oldest_buffer_age_seconds: f64,
}

#[derive(Clone, Debug, Serialize)]
pub struct LaneSnapshot {
    pub flush_active: usize,
    pub flush_capacity: usize,
    pub cheap_query_active: usize,
    pub cheap_query_capacity: usize,
    pub heavy_query_active: usize,
    pub heavy_query_capacity: usize,
    pub heavy_query_effective_capacity: usize,
    pub ewma_flush_bytes_per_second: f64,
    pub projected_flush_seconds: f64,
    pub projected_buffer_seconds: f64,
    pub projected_visibility_seconds: f64,
    pub observed_freshness_lag_seconds: f64,
    pub freshness_sla_seconds: f64,
    pub heavy_query_reductions_total: u64,
    pub query_rejections_total: u64,
    pub ingest_freshness_rejections_total: u64,
}

pub struct LaneController {
    inner: Mutex<LaneState>,
    flush_capacity: usize,
    cheap_query_capacity: usize,
    heavy_query_capacity: usize,
    heavy_query_degraded_capacity: usize,
    freshness_sla_seconds: f64,
    visibility_buffer_target_bytes: usize,
    visibility_buffer_max_age_seconds: f64,
    visibility_flush_bytes_per_second: f64,
}

#[derive(Debug)]
struct LaneState {
    flush_active: usize,
    cheap_query_active: usize,
    heavy_query_active: usize,
    ewma_flush_bytes_per_second: f64,
    projected_flush_seconds: f64,
    projected_buffer_seconds: f64,
    projected_visibility_seconds: f64,
    observed_freshness_lag_seconds: f64,
    heavy_query_reductions_total: u64,
    query_rejections_total: u64,
    ingest_freshness_rejections_total: u64,
}

impl LaneController {
    pub fn new(config: &Config) -> Self {
        let flush_capacity = config.lane_flush_capacity.max(1);
        let cheap_query_capacity = config
            .lane_cheap_query_capacity
            .min(config.query_interactive.concurrency)
            .max(1);
        let reserved_query_capacity = flush_capacity.saturating_add(cheap_query_capacity);
        let heavy_query_capacity = config
            .query_interactive
            .concurrency
            .saturating_sub(reserved_query_capacity)
            .max(1);
        let heavy_query_degraded_capacity = config
            .lane_heavy_query_degraded_capacity
            .min(heavy_query_capacity)
            .max(1);
        let initial_flush_bytes_per_second =
            config.max_bytes_per_flush as f64 / config.max_age.as_secs_f64().max(0.001);
        let visibility_flush_bytes_per_second = config.immutable_segment_target_bytes as f64
            / config.immutable_segment_max_age.as_secs_f64().max(0.001);
        Self {
            inner: Mutex::new(LaneState {
                flush_active: 0,
                cheap_query_active: 0,
                heavy_query_active: 0,
                ewma_flush_bytes_per_second: initial_flush_bytes_per_second.max(1.0),
                projected_flush_seconds: 0.0,
                projected_buffer_seconds: 0.0,
                projected_visibility_seconds: 0.0,
                observed_freshness_lag_seconds: 0.0,
                heavy_query_reductions_total: 0,
                query_rejections_total: 0,
                ingest_freshness_rejections_total: 0,
            }),
            flush_capacity,
            cheap_query_capacity,
            heavy_query_capacity,
            heavy_query_degraded_capacity,
            freshness_sla_seconds: config.lane_freshness_sla.as_secs_f64(),
            visibility_buffer_target_bytes: config.immutable_segment_target_bytes.max(1),
            visibility_buffer_max_age_seconds: config.immutable_segment_max_age.as_secs_f64(),
            visibility_flush_bytes_per_second: visibility_flush_bytes_per_second.max(1.0),
        }
    }

    pub fn reserve_flush(&self, metrics: &Metrics) -> ApiResult<FlushLaneGuard<'_>> {
        let mut state = self.inner.lock_or_poisoned();
        if state.flush_active >= self.flush_capacity {
            metrics.inc(
                "canardstack_lane_rejections_total",
                &[("lane", "flush"), ("reason", "flush_lane_full")],
                1,
            );
            return Err(
                ApiError::new(503, "flush_lane_full", "flush lane capacity is exhausted")
                    .with_retry_after(1),
            );
        }
        state.flush_active += 1;
        drop(state);
        self.record_metrics(metrics, FreshnessInputs::default());
        Ok(FlushLaneGuard {
            lanes: self,
            started: Instant::now(),
            bytes: 0,
            released: false,
        })
    }

    pub fn reserve_query(
        &self,
        class: QueryClass,
        inputs: FreshnessInputs,
        metrics: &Metrics,
    ) -> ApiResult<QueryLaneGuard<'_>> {
        let mut state = self.inner.lock_or_poisoned();
        self.update_projection_locked(&mut state, inputs);
        let effective_heavy = self.effective_heavy_capacity_locked(&state);
        let rejection_reason = query_rejection_reason(class, &state, effective_heavy);
        let accepted = match class {
            QueryClass::Cheap => {
                if state.cheap_query_active < self.cheap_query_capacity {
                    state.cheap_query_active += 1;
                    true
                } else {
                    false
                }
            }
            QueryClass::Heavy => {
                if self.freshness_at_risk_locked(&state) {
                    false
                } else if state.heavy_query_active < effective_heavy {
                    if effective_heavy < self.heavy_query_capacity {
                        state.heavy_query_reductions_total += 1;
                    }
                    state.heavy_query_active += 1;
                    true
                } else {
                    false
                }
            }
        };
        if !accepted {
            state.query_rejections_total += 1;
            metrics.inc(
                "canardstack_lane_rejections_total",
                &[("lane", "query"), ("reason", rejection_reason)],
                1,
            );
            drop(state);
            self.record_metrics(metrics, inputs);
            return Err(
                ApiError::new(429, rejection_reason, query_rejection_message(class))
                    .with_retry_after(1),
            );
        }
        drop(state);
        self.record_metrics(metrics, inputs);
        Ok(QueryLaneGuard { lanes: self, class })
    }

    pub fn admit_ingest(&self, inputs: FreshnessInputs, metrics: &Metrics) -> ApiResult<()> {
        let mut state = self.inner.lock_or_poisoned();
        self.update_projection_locked(&mut state, inputs);
        let budget_fraction = if state.heavy_query_active > 0 {
            INGEST_FRESHNESS_BUDGET_WITH_HEAVY_FRACTION
        } else {
            INGEST_FRESHNESS_BUDGET_FRACTION
        };
        let budget = self.freshness_sla_seconds * budget_fraction;
        if state.projected_visibility_seconds > budget {
            state.ingest_freshness_rejections_total += 1;
            metrics.inc(
                "canardstack_ingest_rejections_total",
                &[
                    ("signal", "all"),
                    ("status", "429"),
                    ("reason", "freshness_budget_exceeded"),
                ],
                1,
            );
            metrics.inc(
                "canardstack_lane_rejections_total",
                &[("lane", "ingest"), ("reason", "freshness_budget_exceeded")],
                1,
            );
            drop(state);
            self.record_metrics(metrics, inputs);
            return Err(ApiError::new(
                429,
                "freshness_budget_exceeded",
                "projected flush visibility exceeds freshness budget",
            )
            .with_retry_after(5));
        }
        drop(state);
        self.record_metrics(metrics, inputs);
        Ok(())
    }

    pub fn record_observed_freshness_lag(&self, lag_seconds: f64, metrics: &Metrics) {
        let mut state = self.inner.lock_or_poisoned();
        state.observed_freshness_lag_seconds = lag_seconds.max(0.0);
        drop(state);
        self.record_metrics(metrics, FreshnessInputs::default());
    }

    pub fn record_flush_bytes(&self, bytes: usize, seconds: f64, metrics: &Metrics) {
        if bytes == 0 || seconds <= 0.0 {
            self.record_metrics(metrics, FreshnessInputs::default());
            return;
        }
        let observed = bytes as f64 / seconds.max(0.001);
        let mut state = self.inner.lock_or_poisoned();
        state.ewma_flush_bytes_per_second =
            EWMA_ALPHA * observed + (1.0 - EWMA_ALPHA) * state.ewma_flush_bytes_per_second;
        drop(state);
        self.record_metrics(metrics, FreshnessInputs::default());
    }

    pub fn snapshot_for(&self, inputs: FreshnessInputs) -> LaneSnapshot {
        let mut state = self.inner.lock_or_poisoned();
        self.update_projection_locked(&mut state, inputs);
        let heavy_query_effective_capacity = self.effective_heavy_capacity_locked(&state);
        snapshot_locked(
            &state,
            self.flush_capacity,
            self.cheap_query_capacity,
            self.heavy_query_capacity,
            heavy_query_effective_capacity,
            self.freshness_sla_seconds,
        )
    }

    pub fn record_metrics(&self, metrics: &Metrics, inputs: FreshnessInputs) {
        let snapshot = self.snapshot_for(inputs);
        metrics.gauge(
            "canardstack_lane_capacity",
            &[("lane", "flush")],
            snapshot.flush_capacity as f64,
        );
        metrics.gauge(
            "canardstack_lane_in_use",
            &[("lane", "flush")],
            snapshot.flush_active as f64,
        );
        metrics.gauge(
            "canardstack_lane_capacity",
            &[("lane", "query_cheap")],
            snapshot.cheap_query_capacity as f64,
        );
        metrics.gauge(
            "canardstack_lane_in_use",
            &[("lane", "query_cheap")],
            snapshot.cheap_query_active as f64,
        );
        metrics.gauge(
            "canardstack_lane_capacity",
            &[("lane", "query_heavy")],
            snapshot.heavy_query_effective_capacity as f64,
        );
        metrics.gauge(
            "canardstack_lane_in_use",
            &[("lane", "query_heavy")],
            snapshot.heavy_query_active as f64,
        );
        metrics.gauge(
            "canardstack_flush_ewma_bytes_per_second",
            &[],
            snapshot.ewma_flush_bytes_per_second,
        );
        metrics.gauge(
            "canardstack_projected_flush_seconds",
            &[],
            snapshot.projected_flush_seconds,
        );
        metrics.gauge(
            "canardstack_projected_buffer_seconds",
            &[],
            snapshot.projected_buffer_seconds,
        );
        metrics.gauge(
            "canardstack_projected_visibility_seconds",
            &[],
            snapshot.projected_visibility_seconds,
        );
        metrics.gauge(
            "canardstack_observed_freshness_lag_seconds",
            &[],
            snapshot.observed_freshness_lag_seconds,
        );
        metrics.set_counter(
            "canardstack_query_lane_reductions_total",
            &[],
            snapshot.heavy_query_reductions_total,
        );
        metrics.set_counter(
            "canardstack_query_lane_rejections_total",
            &[],
            snapshot.query_rejections_total,
        );
        metrics.set_counter(
            "canardstack_ingest_freshness_budget_rejections_total",
            &[],
            snapshot.ingest_freshness_rejections_total,
        );
    }

    fn effective_heavy_capacity_locked(&self, state: &LaneState) -> usize {
        if state.projected_visibility_seconds
            >= self.freshness_sla_seconds * HEAVY_QUERY_DEGRADE_FRACTION
        {
            self.heavy_query_degraded_capacity
        } else {
            self.heavy_query_capacity
        }
    }

    fn freshness_at_risk_locked(&self, state: &LaneState) -> bool {
        state.projected_visibility_seconds
            >= self.freshness_sla_seconds * HEAVY_QUERY_REJECT_FRACTION
    }

    fn update_projection_locked(&self, state: &mut LaneState, inputs: FreshnessInputs) {
        let queued_bytes = inputs.queued_bytes.saturating_add(inputs.incoming_bytes);
        state.projected_flush_seconds =
            queued_bytes as f64 / state.ewma_flush_bytes_per_second.max(1.0);
        let queue_visibility_seconds =
            inputs.oldest_queue_age_seconds + state.projected_flush_seconds;
        let allowed_buffer_bytes = self
            .visibility_buffer_target_bytes
            .saturating_mul(inputs.buffered_active_count);
        let excess_buffer_bytes = inputs.buffered_bytes.saturating_sub(allowed_buffer_bytes);
        let buffer_size_debt_seconds =
            excess_buffer_bytes as f64 / self.visibility_flush_bytes_per_second.max(1.0);
        let buffer_age_debt_seconds =
            (inputs.oldest_buffer_age_seconds - self.visibility_buffer_max_age_seconds).max(0.0);
        state.projected_buffer_seconds = buffer_size_debt_seconds + buffer_age_debt_seconds;
        let buffer_visibility_seconds = state.projected_buffer_seconds;
        state.projected_visibility_seconds =
            queue_visibility_seconds.max(buffer_visibility_seconds);
    }
}

impl Default for FreshnessInputs {
    fn default() -> Self {
        Self {
            queued_bytes: 0,
            incoming_bytes: 0,
            oldest_queue_age_seconds: 0.0,
            buffered_bytes: 0,
            buffered_active_count: 0,
            oldest_buffer_age_seconds: 0.0,
        }
    }
}

pub struct FlushLaneGuard<'a> {
    lanes: &'a LaneController,
    started: Instant,
    bytes: usize,
    released: bool,
}

impl FlushLaneGuard<'_> {
    pub fn record_bytes(&mut self, bytes: usize) {
        self.bytes = self.bytes.saturating_add(bytes);
    }

    pub fn finish(mut self, metrics: &Metrics) {
        let bytes = self.bytes;
        let seconds = self.started.elapsed().as_secs_f64();
        self.release();
        self.lanes.record_flush_bytes(bytes, seconds, metrics);
    }

    fn release(&mut self) {
        if self.released {
            return;
        }
        let mut state = self.lanes.inner.lock_or_poisoned();
        state.flush_active = state.flush_active.saturating_sub(1);
        self.released = true;
    }
}

impl Drop for FlushLaneGuard<'_> {
    fn drop(&mut self) {
        self.release();
    }
}

pub struct QueryLaneGuard<'a> {
    lanes: &'a LaneController,
    class: QueryClass,
}

impl Drop for QueryLaneGuard<'_> {
    fn drop(&mut self) {
        let mut state = self.lanes.inner.lock_or_poisoned();
        match self.class {
            QueryClass::Cheap => {
                state.cheap_query_active = state.cheap_query_active.saturating_sub(1);
            }
            QueryClass::Heavy => {
                state.heavy_query_active = state.heavy_query_active.saturating_sub(1);
            }
        }
    }
}

fn snapshot_locked(
    state: &LaneState,
    flush_capacity: usize,
    cheap_query_capacity: usize,
    heavy_query_capacity: usize,
    heavy_query_effective_capacity: usize,
    freshness_sla_seconds: f64,
) -> LaneSnapshot {
    LaneSnapshot {
        flush_active: state.flush_active,
        flush_capacity,
        cheap_query_active: state.cheap_query_active,
        cheap_query_capacity,
        heavy_query_active: state.heavy_query_active,
        heavy_query_capacity,
        heavy_query_effective_capacity,
        ewma_flush_bytes_per_second: state.ewma_flush_bytes_per_second,
        projected_flush_seconds: state.projected_flush_seconds,
        projected_buffer_seconds: state.projected_buffer_seconds,
        projected_visibility_seconds: state.projected_visibility_seconds,
        observed_freshness_lag_seconds: state.observed_freshness_lag_seconds,
        freshness_sla_seconds,
        heavy_query_reductions_total: state.heavy_query_reductions_total,
        query_rejections_total: state.query_rejections_total,
        ingest_freshness_rejections_total: state.ingest_freshness_rejections_total,
    }
}

fn query_rejection_reason(
    class: QueryClass,
    state: &LaneState,
    effective_heavy_capacity: usize,
) -> &'static str {
    match class {
        QueryClass::Cheap => "cheap_query_lane_full",
        QueryClass::Heavy if state.heavy_query_active >= effective_heavy_capacity => {
            "heavy_query_lane_full"
        }
        QueryClass::Heavy if state.projected_visibility_seconds > 0.0 => "freshness_debt",
        QueryClass::Heavy => "heavy_query_lane_full",
    }
}

fn query_rejection_message(class: QueryClass) -> &'static str {
    match class {
        QueryClass::Cheap => "cheap query lane capacity is exhausted",
        QueryClass::Heavy => "heavy query lane capacity is exhausted or freshness is at risk",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use tempfile::tempdir;

    fn controller() -> LaneController {
        let dir = tempdir().unwrap();
        let mut config = Config::test(dir.path().join("canardstack.duckdb"));
        config.query_interactive.concurrency = 4;
        config.lane_freshness_sla = std::time::Duration::from_secs(10);
        config.max_bytes_per_flush = 1_000;
        config.max_age = std::time::Duration::from_secs(1);
        config.immutable_segment_target_bytes = 1_000;
        config.immutable_segment_max_age = std::time::Duration::from_secs(1);
        LaneController::new(&config)
    }

    #[test]
    fn flush_lane_is_independent_of_query_saturation() {
        let lanes = controller();
        let metrics = Metrics::default();
        let inputs = FreshnessInputs::default();
        let _heavy = lanes
            .reserve_query(QueryClass::Heavy, inputs, &metrics)
            .unwrap();
        let _cheap = lanes
            .reserve_query(QueryClass::Cheap, inputs, &metrics)
            .unwrap();

        assert!(lanes.reserve_flush(&metrics).is_ok());
    }

    #[test]
    fn heavy_query_rejects_when_freshness_debt_exceeds_sla() {
        let lanes = controller();
        let metrics = Metrics::default();
        let inputs = FreshnessInputs {
            queued_bytes: 16_000,
            incoming_bytes: 0,
            oldest_queue_age_seconds: 0.0,
            ..FreshnessInputs::default()
        };
        let err = match lanes.reserve_query(QueryClass::Heavy, inputs, &metrics) {
            Ok(_) => panic!("heavy query should reject under freshness debt"),
            Err(err) => err,
        };

        assert_eq!(err.status, 429);
        assert_eq!(err.reason, "freshness_debt");
    }

    #[test]
    fn cheap_query_keeps_protected_lane_under_freshness_debt() {
        let lanes = controller();
        let metrics = Metrics::default();
        let inputs = FreshnessInputs {
            queued_bytes: 16_000,
            incoming_bytes: 0,
            oldest_queue_age_seconds: 0.0,
            ..FreshnessInputs::default()
        };

        assert!(lanes
            .reserve_query(QueryClass::Cheap, inputs, &metrics)
            .is_ok());
    }

    #[test]
    fn heavy_query_keeps_full_capacity_before_late_degradation() {
        let lanes = controller();
        let metrics = Metrics::default();
        let inputs = FreshnessInputs {
            queued_bytes: 9_000,
            incoming_bytes: 0,
            oldest_queue_age_seconds: 0.0,
            ..FreshnessInputs::default()
        };

        let _first = lanes
            .reserve_query(QueryClass::Heavy, inputs, &metrics)
            .unwrap();
        assert!(lanes
            .reserve_query(QueryClass::Heavy, inputs, &metrics)
            .is_ok());
    }

    #[test]
    fn heavy_query_degrades_before_hard_freshness_rejection() {
        let lanes = controller();
        let metrics = Metrics::default();
        let inputs = FreshnessInputs {
            queued_bytes: 12_000,
            incoming_bytes: 0,
            oldest_queue_age_seconds: 0.0,
            ..FreshnessInputs::default()
        };

        let _first = lanes
            .reserve_query(QueryClass::Heavy, inputs, &metrics)
            .unwrap();
        let err = match lanes.reserve_query(QueryClass::Heavy, inputs, &metrics) {
            Ok(_) => panic!("second heavy query should hit degraded capacity"),
            Err(err) => err,
        };

        assert_eq!(err.status, 429);
        assert_eq!(err.reason, "heavy_query_lane_full");
    }

    #[test]
    fn ingest_rejects_when_projected_visibility_exceeds_budget() {
        let lanes = controller();
        let metrics = Metrics::default();
        let err = lanes
            .admit_ingest(
                FreshnessInputs {
                    queued_bytes: 60_000,
                    incoming_bytes: 1,
                    oldest_queue_age_seconds: 0.0,
                    ..FreshnessInputs::default()
                },
                &metrics,
            )
            .unwrap_err();

        assert_eq!(err.status, 429);
        assert_eq!(err.reason, "freshness_budget_exceeded");
    }

    #[test]
    fn ingest_rejects_when_buffer_visibility_debt_exceeds_budget() {
        let lanes = controller();
        let metrics = Metrics::default();
        let err = lanes
            .admit_ingest(
                FreshnessInputs {
                    queued_bytes: 0,
                    buffered_bytes: 12_000,
                    incoming_bytes: 1,
                    oldest_queue_age_seconds: 0.0,
                    ..FreshnessInputs::default()
                },
                &metrics,
            )
            .unwrap_err();

        assert_eq!(err.status, 429);
        assert_eq!(err.reason, "freshness_budget_exceeded");
    }

    #[test]
    fn cached_visible_freshness_lag_does_not_block_empty_queue_ingest() {
        let lanes = controller();
        let metrics = Metrics::default();
        lanes.record_observed_freshness_lag(12.0, &metrics);

        lanes
            .admit_ingest(
                FreshnessInputs {
                    queued_bytes: 0,
                    incoming_bytes: 1,
                    oldest_queue_age_seconds: 0.0,
                    ..FreshnessInputs::default()
                },
                &metrics,
            )
            .unwrap();
    }

    #[test]
    fn cached_visible_freshness_lag_does_not_latch_existing_queue_debt() {
        let lanes = controller();
        let metrics = Metrics::default();
        lanes.record_observed_freshness_lag(12.0, &metrics);

        lanes
            .admit_ingest(
                FreshnessInputs {
                    queued_bytes: 1,
                    incoming_bytes: 1,
                    oldest_queue_age_seconds: 0.0,
                    ..FreshnessInputs::default()
                },
                &metrics,
            )
            .unwrap();
    }
}
