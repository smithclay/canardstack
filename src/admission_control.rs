//! Freshness-first admission control.
//!
//! Every admission decision is driven by one projection of how far behind the
//! seal pipeline is, expressed in seconds of expected query-visibility delay:
//!
//! ```text
//! projected_seal_seconds   = (inflight_bytes + incoming_bytes) / observed_seal_rate
//! projected_buffer_seconds = buffer_size_debt + buffer_age_debt
//!     buffer_size_debt = max(0, buffered_bytes - target*active) / observed_seal_rate
//!     buffer_age_debt  = max(0, oldest_buffer_age - buffer_max_age)
//! projected_visibility_seconds = max(projected_seal_seconds, projected_buffer_seconds)
//! ```
//!
//! `observed_seal_rate` is a single EWMA of measured seal throughput
//! (`ewma_seal_bytes_per_second`), seeded from `max(seal_rate_seed, target/max_age)`
//! and used by BOTH the seal debt and the buffer-size debt so the two terms
//! share one drain estimate.
//!
//! The projection feeds a single freshness band, relative to the configured SLA
//! (`freshness_budget_sla_seconds`):
//!
//! - Ingest is admitted while projected visibility stays under 0.95x the SLA
//!   ([`FreshnessModel::INGEST_FRESHNESS_BUDGET_FRACTION`]), tightened to 0.90x
//!   while a heavy query is in flight
//!   ([`FreshnessModel::INGEST_FRESHNESS_BUDGET_WITH_HEAVY_FRACTION`]); over the
//!   budget it returns 429.
//! - Heavy queries keep full capacity below the SLA, degrade to reduced capacity
//!   at >= 1.0x the SLA ([`FreshnessModel::HEAVY_QUERY_DEGRADE_FRACTION`]), and
//!   are rejected outright at >= 1.5x the SLA
//!   ([`FreshnessModel::HEAVY_QUERY_REJECT_FRACTION`]).
//! - Cheap queries keep protected admission and seal capacity is reserved ahead
//!   of all query capacity.

use crate::config::Config;
use crate::metrics::Metrics;
use crate::validation::{ApiError, ApiResult};
use crate::LockExt;
use serde::Serialize;
use std::sync::Mutex;
use std::time::Instant;

/// Compile-time tuning of the freshness projection and admission band.
///
/// These are deliberately constants, not config knobs: they define the SHAPE of
/// the band, not an operator-tunable budget. The band, in plain language:
/// ingest accepts while projected visibility stays under 0.95x the SLA (0.90x
/// when a heavy query is in flight); heavy queries degrade to reduced capacity
/// at >= 1.0x the SLA and are rejected outright at >= 1.5x the SLA.
struct FreshnessModel;

impl FreshnessModel {
    /// Smoothing factor for the observed seal-rate EWMA.
    const EWMA_ALPHA: f64 = 0.20;
    /// Heavy queries degrade to reduced capacity at >= this multiple of the SLA.
    const HEAVY_QUERY_DEGRADE_FRACTION: f64 = 1.00;
    /// Heavy queries are rejected outright at >= this multiple of the SLA.
    const HEAVY_QUERY_REJECT_FRACTION: f64 = 1.50;
    /// Ingest headroom: accept while projected visibility < this multiple of SLA.
    const INGEST_FRESHNESS_BUDGET_FRACTION: f64 = 0.95;
    /// Tighter ingest headroom while a heavy query is in flight.
    const INGEST_FRESHNESS_BUDGET_WITH_HEAVY_FRACTION: f64 = 0.90;
}

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
pub struct FreshnessBudgetInputs {
    pub inflight_bytes: usize,
    pub incoming_bytes: usize,
    pub buffered_bytes: usize,
    pub buffered_active_count: usize,
    pub oldest_buffer_age_seconds: f64,
}

#[derive(Clone, Debug, Serialize)]
pub struct AdmissionSnapshot {
    pub seal_active: usize,
    pub seal_capacity: usize,
    pub cheap_query_active: usize,
    pub cheap_query_capacity: usize,
    pub heavy_query_active: usize,
    pub heavy_query_capacity: usize,
    pub heavy_query_effective_capacity: usize,
    pub ewma_seal_bytes_per_second: f64,
    pub projected_seal_seconds: f64,
    pub projected_buffer_seconds: f64,
    pub projected_visibility_seconds: f64,
    pub observed_freshness_lag_seconds: f64,
    pub freshness_budget_sla_seconds: f64,
    pub heavy_query_reductions_total: u64,
    pub query_rejections_total: u64,
    pub ingest_freshness_rejections_total: u64,
}

pub struct AdmissionController {
    inner: Mutex<AdmissionState>,
    seal_capacity: usize,
    cheap_query_capacity: usize,
    heavy_query_capacity: usize,
    heavy_query_degraded_capacity: usize,
    freshness_budget_sla_seconds: f64,
    arrow_write_buffer_target_bytes: usize,
    arrow_write_buffer_max_age_seconds: f64,
}

#[derive(Debug)]
struct AdmissionState {
    seal_active: usize,
    cheap_query_active: usize,
    heavy_query_active: usize,
    ewma_seal_bytes_per_second: f64,
    projected_seal_seconds: f64,
    projected_buffer_seconds: f64,
    projected_visibility_seconds: f64,
    observed_freshness_lag_seconds: f64,
    heavy_query_reductions_total: u64,
    query_rejections_total: u64,
    ingest_freshness_rejections_total: u64,
}

impl AdmissionController {
    pub fn new(config: &Config) -> Self {
        let seal_capacity = config.seal_admission_capacity.max(1);
        let cheap_query_capacity = config
            .cheap_query_admission_capacity
            .min(config.query_interactive.concurrency)
            .max(1);
        let reserved_query_capacity = seal_capacity.saturating_add(cheap_query_capacity);
        let heavy_query_capacity = config
            .query_interactive
            .concurrency
            .saturating_sub(reserved_query_capacity)
            .max(1);
        let heavy_query_degraded_capacity = config
            .heavy_query_degraded_capacity
            .min(heavy_query_capacity)
            .max(1);
        // Seed the single observed seal-rate EWMA from the larger of the seed
        // rate and the target/max-age drain rate, so the buffer-size debt (which
        // also divides by this EWMA, see update_projection_locked) starts no
        // slower than the static drain target.
        let seal_rate_seed_rate = config.seal_rate_seed_bytes as f64
            / config.seal_rate_seed_window.as_secs_f64().max(0.001);
        let visibility_drain_rate = config.arrow_write_buffer_target_bytes as f64
            / config.arrow_write_buffer_max_age.as_secs_f64().max(0.001);
        let initial_seal_bytes_per_second = seal_rate_seed_rate.max(visibility_drain_rate);
        Self {
            inner: Mutex::new(AdmissionState {
                seal_active: 0,
                cheap_query_active: 0,
                heavy_query_active: 0,
                ewma_seal_bytes_per_second: initial_seal_bytes_per_second.max(1.0),
                projected_seal_seconds: 0.0,
                projected_buffer_seconds: 0.0,
                projected_visibility_seconds: 0.0,
                observed_freshness_lag_seconds: 0.0,
                heavy_query_reductions_total: 0,
                query_rejections_total: 0,
                ingest_freshness_rejections_total: 0,
            }),
            seal_capacity,
            cheap_query_capacity,
            heavy_query_capacity,
            heavy_query_degraded_capacity,
            freshness_budget_sla_seconds: config.freshness_budget_sla.as_secs_f64(),
            arrow_write_buffer_target_bytes: config.arrow_write_buffer_target_bytes.max(1),
            arrow_write_buffer_max_age_seconds: config.arrow_write_buffer_max_age.as_secs_f64(),
        }
    }

    pub fn reserve_seal(&self, metrics: &Metrics) -> ApiResult<SealAdmissionGuard<'_>> {
        let mut state = self.inner.lock_or_poisoned();
        if state.seal_active >= self.seal_capacity {
            metrics.inc(
                "canardstack_admission_rejections_total",
                &[("admission", "seal"), ("reason", "seal_admission_full")],
                1,
            );
            return Err(ApiError::new(
                503,
                "seal_admission_full",
                "seal admission capacity is exhausted",
            )
            .with_retry_after(1));
        }
        state.seal_active += 1;
        drop(state);
        self.record_metrics(metrics, FreshnessBudgetInputs::default());
        Ok(SealAdmissionGuard {
            admission: self,
            started: Instant::now(),
            bytes: 0,
            released: false,
        })
    }

    pub fn reserve_query(
        &self,
        class: QueryClass,
        inputs: FreshnessBudgetInputs,
        metrics: &Metrics,
    ) -> ApiResult<QueryAdmissionGuard<'_>> {
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
                "canardstack_admission_rejections_total",
                &[("admission", "query"), ("reason", rejection_reason)],
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
        Ok(QueryAdmissionGuard {
            admission: self,
            class,
        })
    }

    pub fn admit_ingest(&self, inputs: FreshnessBudgetInputs, metrics: &Metrics) -> ApiResult<()> {
        let mut state = self.inner.lock_or_poisoned();
        self.update_projection_locked(&mut state, inputs);
        let budget_fraction = if state.heavy_query_active > 0 {
            FreshnessModel::INGEST_FRESHNESS_BUDGET_WITH_HEAVY_FRACTION
        } else {
            FreshnessModel::INGEST_FRESHNESS_BUDGET_FRACTION
        };
        let budget = self.freshness_budget_sla_seconds * budget_fraction;
        if state.projected_visibility_seconds > budget {
            state.ingest_freshness_rejections_total += 1;
            metrics.inc(
                "canardstack_admission_rejections_total",
                &[
                    ("admission", "freshness_budget"),
                    ("reason", "freshness_budget_exceeded"),
                ],
                1,
            );
            drop(state);
            self.record_metrics(metrics, inputs);
            return Err(ApiError::new(
                429,
                "freshness_budget_exceeded",
                "projected seal visibility exceeds freshness budget",
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
        self.record_metrics(metrics, FreshnessBudgetInputs::default());
    }

    pub fn record_seal_bytes(&self, bytes: usize, seconds: f64, metrics: &Metrics) {
        if bytes == 0 || seconds <= 0.0 {
            self.record_metrics(metrics, FreshnessBudgetInputs::default());
            return;
        }
        let observed = bytes as f64 / seconds.max(0.001);
        let mut state = self.inner.lock_or_poisoned();
        state.ewma_seal_bytes_per_second = FreshnessModel::EWMA_ALPHA * observed
            + (1.0 - FreshnessModel::EWMA_ALPHA) * state.ewma_seal_bytes_per_second;
        drop(state);
        self.record_metrics(metrics, FreshnessBudgetInputs::default());
    }

    pub fn snapshot_for(&self, inputs: FreshnessBudgetInputs) -> AdmissionSnapshot {
        let mut state = self.inner.lock_or_poisoned();
        self.update_projection_locked(&mut state, inputs);
        let heavy_query_effective_capacity = self.effective_heavy_capacity_locked(&state);
        snapshot_locked(
            &state,
            self.seal_capacity,
            self.cheap_query_capacity,
            self.heavy_query_capacity,
            heavy_query_effective_capacity,
            self.freshness_budget_sla_seconds,
        )
    }

    pub fn record_metrics(&self, metrics: &Metrics, inputs: FreshnessBudgetInputs) {
        let snapshot = self.snapshot_for(inputs);
        metrics.gauge(
            "canardstack_admission_capacity",
            &[("admission", "seal")],
            snapshot.seal_capacity as f64,
        );
        metrics.gauge(
            "canardstack_admission_in_use",
            &[("admission", "seal")],
            snapshot.seal_active as f64,
        );
        metrics.gauge(
            "canardstack_admission_capacity",
            &[("admission", "query_cheap")],
            snapshot.cheap_query_capacity as f64,
        );
        metrics.gauge(
            "canardstack_admission_in_use",
            &[("admission", "query_cheap")],
            snapshot.cheap_query_active as f64,
        );
        metrics.gauge(
            "canardstack_admission_capacity",
            &[("admission", "query_heavy")],
            snapshot.heavy_query_effective_capacity as f64,
        );
        metrics.gauge(
            "canardstack_admission_in_use",
            &[("admission", "query_heavy")],
            snapshot.heavy_query_active as f64,
        );
        metrics.gauge(
            "canardstack_seal_ewma_bytes_per_second",
            &[],
            snapshot.ewma_seal_bytes_per_second,
        );
        metrics.gauge(
            "canardstack_projected_seal_seconds",
            &[],
            snapshot.projected_seal_seconds,
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
            "canardstack_admission_reductions_total",
            &[],
            snapshot.heavy_query_reductions_total,
        );
    }

    fn effective_heavy_capacity_locked(&self, state: &AdmissionState) -> usize {
        if state.projected_visibility_seconds
            >= self.freshness_budget_sla_seconds * FreshnessModel::HEAVY_QUERY_DEGRADE_FRACTION
        {
            self.heavy_query_degraded_capacity
        } else {
            self.heavy_query_capacity
        }
    }

    fn freshness_at_risk_locked(&self, state: &AdmissionState) -> bool {
        state.projected_visibility_seconds
            >= self.freshness_budget_sla_seconds * FreshnessModel::HEAVY_QUERY_REJECT_FRACTION
    }

    fn update_projection_locked(&self, state: &mut AdmissionState, inputs: FreshnessBudgetInputs) {
        let projected_bytes = inputs.inflight_bytes.saturating_add(inputs.incoming_bytes);
        state.projected_seal_seconds =
            projected_bytes as f64 / state.ewma_seal_bytes_per_second.max(1.0);
        let allowed_buffer_bytes = self
            .arrow_write_buffer_target_bytes
            .saturating_mul(inputs.buffered_active_count);
        let excess_buffer_bytes = inputs.buffered_bytes.saturating_sub(allowed_buffer_bytes);
        // Buffer-size debt drains at the same observed seal rate as seal debt.
        let buffer_size_debt_seconds =
            excess_buffer_bytes as f64 / state.ewma_seal_bytes_per_second.max(1.0);
        let buffer_age_debt_seconds =
            (inputs.oldest_buffer_age_seconds - self.arrow_write_buffer_max_age_seconds).max(0.0);
        state.projected_buffer_seconds = buffer_size_debt_seconds + buffer_age_debt_seconds;
        state.projected_visibility_seconds = state
            .projected_seal_seconds
            .max(state.projected_buffer_seconds);
    }
}

impl Default for FreshnessBudgetInputs {
    fn default() -> Self {
        Self {
            inflight_bytes: 0,
            incoming_bytes: 0,
            buffered_bytes: 0,
            buffered_active_count: 0,
            oldest_buffer_age_seconds: 0.0,
        }
    }
}

pub struct SealAdmissionGuard<'a> {
    admission: &'a AdmissionController,
    started: Instant,
    bytes: usize,
    released: bool,
}

impl SealAdmissionGuard<'_> {
    pub fn record_bytes(&mut self, bytes: usize) {
        self.bytes = self.bytes.saturating_add(bytes);
    }

    pub fn finish(mut self, metrics: &Metrics) {
        let bytes = self.bytes;
        let seconds = self.started.elapsed().as_secs_f64();
        self.release();
        self.admission.record_seal_bytes(bytes, seconds, metrics);
    }

    fn release(&mut self) {
        if self.released {
            return;
        }
        let mut state = self.admission.inner.lock_or_poisoned();
        state.seal_active = state.seal_active.saturating_sub(1);
        self.released = true;
    }
}

impl Drop for SealAdmissionGuard<'_> {
    fn drop(&mut self) {
        self.release();
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
    }
}

fn snapshot_locked(
    state: &AdmissionState,
    seal_capacity: usize,
    cheap_query_capacity: usize,
    heavy_query_capacity: usize,
    heavy_query_effective_capacity: usize,
    freshness_budget_sla_seconds: f64,
) -> AdmissionSnapshot {
    AdmissionSnapshot {
        seal_active: state.seal_active,
        seal_capacity,
        cheap_query_active: state.cheap_query_active,
        cheap_query_capacity,
        heavy_query_active: state.heavy_query_active,
        heavy_query_capacity,
        heavy_query_effective_capacity,
        ewma_seal_bytes_per_second: state.ewma_seal_bytes_per_second,
        projected_seal_seconds: state.projected_seal_seconds,
        projected_buffer_seconds: state.projected_buffer_seconds,
        projected_visibility_seconds: state.projected_visibility_seconds,
        observed_freshness_lag_seconds: state.observed_freshness_lag_seconds,
        freshness_budget_sla_seconds,
        heavy_query_reductions_total: state.heavy_query_reductions_total,
        query_rejections_total: state.query_rejections_total,
        ingest_freshness_rejections_total: state.ingest_freshness_rejections_total,
    }
}

fn query_rejection_reason(
    class: QueryClass,
    state: &AdmissionState,
    effective_heavy_capacity: usize,
) -> &'static str {
    match class {
        QueryClass::Cheap => "cheap_query_admission_full",
        QueryClass::Heavy if state.heavy_query_active >= effective_heavy_capacity => {
            "heavy_query_admission_full"
        }
        QueryClass::Heavy if state.projected_visibility_seconds > 0.0 => "freshness_debt",
        QueryClass::Heavy => "heavy_query_admission_full",
    }
}

fn query_rejection_message(class: QueryClass) -> &'static str {
    match class {
        QueryClass::Cheap => "cheap query admission capacity is exhausted",
        QueryClass::Heavy => "heavy query admission capacity is exhausted or freshness is at risk",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use tempfile::tempdir;

    fn controller() -> AdmissionController {
        let dir = tempdir().unwrap();
        let mut config = Config::test(dir.path().join("canardstack.duckdb"));
        config.query_interactive.concurrency = 4;
        config.freshness_budget_sla = std::time::Duration::from_secs(10);
        config.seal_rate_seed_bytes = 1_000;
        config.seal_rate_seed_window = std::time::Duration::from_secs(1);
        config.arrow_write_buffer_target_bytes = 1_000;
        config.arrow_write_buffer_max_age = std::time::Duration::from_secs(1);
        AdmissionController::new(&config)
    }

    #[test]
    fn seal_admission_is_independent_of_query_saturation() {
        let admission = controller();
        let metrics = Metrics::default();
        let inputs = FreshnessBudgetInputs::default();
        let _heavy = admission
            .reserve_query(QueryClass::Heavy, inputs, &metrics)
            .unwrap();
        let _cheap = admission
            .reserve_query(QueryClass::Cheap, inputs, &metrics)
            .unwrap();

        assert!(admission.reserve_seal(&metrics).is_ok());
    }

    #[test]
    fn heavy_query_rejects_when_freshness_debt_exceeds_sla() {
        let admission = controller();
        let metrics = Metrics::default();
        let inputs = FreshnessBudgetInputs {
            inflight_bytes: 16_000,
            incoming_bytes: 0,
            ..FreshnessBudgetInputs::default()
        };
        let err = match admission.reserve_query(QueryClass::Heavy, inputs, &metrics) {
            Ok(_) => panic!("heavy query should reject under freshness debt"),
            Err(err) => err,
        };

        assert_eq!(err.status, 429);
        assert_eq!(err.reason, "freshness_debt");
    }

    #[test]
    fn cheap_query_keeps_protected_admission_under_freshness_debt() {
        let admission = controller();
        let metrics = Metrics::default();
        let inputs = FreshnessBudgetInputs {
            inflight_bytes: 16_000,
            incoming_bytes: 0,
            ..FreshnessBudgetInputs::default()
        };

        assert!(admission
            .reserve_query(QueryClass::Cheap, inputs, &metrics)
            .is_ok());
    }

    #[test]
    fn heavy_query_keeps_full_capacity_before_late_degradation() {
        let admission = controller();
        let metrics = Metrics::default();
        let inputs = FreshnessBudgetInputs {
            inflight_bytes: 9_000,
            incoming_bytes: 0,
            ..FreshnessBudgetInputs::default()
        };

        let _first = admission
            .reserve_query(QueryClass::Heavy, inputs, &metrics)
            .unwrap();
        assert!(admission
            .reserve_query(QueryClass::Heavy, inputs, &metrics)
            .is_ok());
    }

    #[test]
    fn heavy_query_degrades_before_hard_freshness_rejection() {
        let admission = controller();
        let metrics = Metrics::default();
        let inputs = FreshnessBudgetInputs {
            inflight_bytes: 12_000,
            incoming_bytes: 0,
            ..FreshnessBudgetInputs::default()
        };

        let _first = admission
            .reserve_query(QueryClass::Heavy, inputs, &metrics)
            .unwrap();
        let err = match admission.reserve_query(QueryClass::Heavy, inputs, &metrics) {
            Ok(_) => panic!("second heavy query should hit degraded capacity"),
            Err(err) => err,
        };

        assert_eq!(err.status, 429);
        assert_eq!(err.reason, "heavy_query_admission_full");
    }

    #[test]
    fn ingest_rejects_when_projected_visibility_exceeds_budget() {
        let admission = controller();
        let metrics = Metrics::default();
        let err = admission
            .admit_ingest(
                FreshnessBudgetInputs {
                    inflight_bytes: 60_000,
                    incoming_bytes: 1,
                    ..FreshnessBudgetInputs::default()
                },
                &metrics,
            )
            .unwrap_err();

        assert_eq!(err.status, 429);
        assert_eq!(err.reason, "freshness_budget_exceeded");
    }

    #[test]
    fn ingest_rejects_when_buffer_visibility_debt_exceeds_budget() {
        let admission = controller();
        let metrics = Metrics::default();
        let err = admission
            .admit_ingest(
                FreshnessBudgetInputs {
                    inflight_bytes: 0,
                    buffered_bytes: 12_000,
                    incoming_bytes: 1,
                    ..FreshnessBudgetInputs::default()
                },
                &metrics,
            )
            .unwrap_err();

        assert_eq!(err.status, 429);
        assert_eq!(err.reason, "freshness_budget_exceeded");
    }

    #[test]
    fn cached_visible_freshness_lag_does_not_block_empty_queue_ingest() {
        let admission = controller();
        let metrics = Metrics::default();
        admission.record_observed_freshness_lag(12.0, &metrics);

        admission
            .admit_ingest(
                FreshnessBudgetInputs {
                    inflight_bytes: 0,
                    incoming_bytes: 1,
                    ..FreshnessBudgetInputs::default()
                },
                &metrics,
            )
            .unwrap();
    }

    #[test]
    fn cached_visible_freshness_lag_does_not_latch_existing_queue_debt() {
        let admission = controller();
        let metrics = Metrics::default();
        admission.record_observed_freshness_lag(12.0, &metrics);

        admission
            .admit_ingest(
                FreshnessBudgetInputs {
                    inflight_bytes: 1,
                    incoming_bytes: 1,
                    ..FreshnessBudgetInputs::default()
                },
                &metrics,
            )
            .unwrap();
    }

    // ---------------------------------------------------------------------
    // Golden characterization of admission behavior AFTER A-refactor.
    //
    // A1 removed the (always-zero) queue term; A2 unified the buffer-drain rate
    // onto the single observed seal EWMA. The two buffer-SIZE-debt rows below
    // changed accordingly (their projected_buffer halved as the 2000 B/s EWMA
    // replaced the static 1000 B/s drain rate) and are annotated inline. The
    // seal-debt and buffer-AGE-debt rows were left unchanged, as A2 required.
    //
    // The whole point of this grid is to pin the numeric outputs so future
    // changes reveal their behavior delta as explicit, reviewed expected-value
    // edits rather than as a silent shift.
    // ---------------------------------------------------------------------
    mod characterization {
        use super::*;

        const EPSILON: f64 = 1e-6;

        /// A characterization controller whose seed config DIVERGED across the
        /// two former byte-rate estimates, which is what made A2's unification
        /// observable:
        ///
        /// - seed seal rate = `seal_rate_seed_bytes / seal_rate_seed_window` =
        ///   2000 / 1s = 2000 B/s.
        /// - target/max-age drain rate = `arrow_write_buffer_target_bytes /
        ///   max_age` = 1000 / 1s = 1000 B/s.
        ///
        /// Pre-A2, seal debt divided by 2000 B/s (the EWMA) while buffer-SIZE
        /// debt divided by the static 1000 B/s. Post-A2 there is ONE rate: the
        /// EWMA seeded from `max(2000, 1000) = 2000 B/s`, used by both debts. So
        /// the buffer-SIZE-debt rows now divide by 2000 instead of 1000 (their
        /// debt halves), while the seal-debt and buffer-AGE-debt rows are
        /// unaffected.
        ///
        /// Capacity math for this config (concurrency=4, seal=1, cheap=1,
        /// degraded=1): heavy full capacity = 4 - (1 + 1) = 2, heavy degraded
        /// capacity = 1. So `heavy_query_effective_capacity` is 2 (full) or 1
        /// (degraded).
        fn characterization_controller() -> AdmissionController {
            let dir = tempdir().unwrap();
            let mut config = Config::test(dir.path().join("canardstack.duckdb"));
            config.query_interactive.concurrency = 4;
            config.freshness_budget_sla = std::time::Duration::from_secs(10);
            // Seed seal rate = 2000 / 1s = 2000 B/s; post-A2 this is the single
            // observed rate (seeded from max(2000, target/max-age=1000)).
            config.seal_rate_seed_bytes = 2_000;
            config.seal_rate_seed_window = std::time::Duration::from_secs(1);
            // target/max-age = 1000 / 1s = 1000 B/s; pre-A2 this drove buffer
            // size debt, post-A2 it only floors the EWMA seed (max wins -> 2000).
            config.arrow_write_buffer_target_bytes = 1_000;
            config.arrow_write_buffer_max_age = std::time::Duration::from_secs(1);
            AdmissionController::new(&config)
        }

        #[derive(Clone, Copy, Debug, Eq, PartialEq)]
        enum IngestOutcome {
            Accept,
            RejectFreshnessBudget,
        }

        #[derive(Clone, Copy, Debug, Eq, PartialEq)]
        enum HeavyOutcome {
            AcceptFull,
            AcceptDegraded,
            RejectFreshnessDebt,
        }

        struct Scenario {
            label: &'static str,
            // Inputs. A1 removed the always-zero queue-age input; this grid
            // intentionally never relied on it.
            inflight_bytes: usize,
            incoming_bytes: usize,
            buffered_bytes: usize,
            buffered_active_count: usize,
            oldest_buffer_age_seconds: f64,
            // Expected current outputs (empirically locked from this build).
            expected_ingest: IngestOutcome,
            expected_heavy: HeavyOutcome,
            expected_projected_seal_seconds: f64,
            expected_projected_buffer_seconds: f64,
            expected_projected_visibility_seconds: f64,
        }

        impl Scenario {
            fn inputs(&self) -> FreshnessBudgetInputs {
                FreshnessBudgetInputs {
                    inflight_bytes: self.inflight_bytes,
                    incoming_bytes: self.incoming_bytes,
                    buffered_bytes: self.buffered_bytes,
                    buffered_active_count: self.buffered_active_count,
                    oldest_buffer_age_seconds: self.oldest_buffer_age_seconds,
                }
            }
        }

        fn approx_eq(actual: f64, expected: f64, label: &str, field: &str) {
            assert!(
                (actual - expected).abs() <= EPSILON,
                "[{label}] {field}: expected {expected}, got {actual}",
            );
        }

        /// Classify the heavy-query decision on a FRESH controller (each call is
        /// independent). AcceptFull vs AcceptDegraded is distinguished via
        /// `snapshot_for(..).heavy_query_effective_capacity` (2 = full, 1 =
        /// degraded for this config) on its own fresh controller.
        fn classify_heavy(scenario: &Scenario) -> HeavyOutcome {
            let inputs = scenario.inputs();

            let admission = characterization_controller();
            let metrics = Metrics::default();
            let reservation = admission.reserve_query(QueryClass::Heavy, inputs, &metrics);
            let outcome = match &reservation {
                Err(err) => {
                    assert_eq!(
                        err.status, 429,
                        "[{}] heavy rejection should be 429",
                        scenario.label
                    );
                    assert_eq!(
                        err.reason, "freshness_debt",
                        "[{}] heavy rejection reason",
                        scenario.label
                    );
                    HeavyOutcome::RejectFreshnessDebt
                }
                Ok(_guard) => {
                    let probe = characterization_controller();
                    let effective = probe.snapshot_for(inputs).heavy_query_effective_capacity;
                    match effective {
                        2 => HeavyOutcome::AcceptFull,
                        1 => HeavyOutcome::AcceptDegraded,
                        other => panic!(
                            "[{}] unexpected effective heavy capacity {other}",
                            scenario.label
                        ),
                    }
                }
            };
            // Drop the reservation (and its guard, if any) before the
            // controller it borrows from goes out of scope.
            drop(reservation);
            outcome
        }

        fn classify_ingest(scenario: &Scenario) -> IngestOutcome {
            let admission = characterization_controller();
            let metrics = Metrics::default();
            match admission.admit_ingest(scenario.inputs(), &metrics) {
                Ok(()) => IngestOutcome::Accept,
                Err(err) => {
                    assert_eq!(
                        err.status, 429,
                        "[{}] ingest rejection should be 429",
                        scenario.label
                    );
                    assert_eq!(
                        err.reason, "freshness_budget_exceeded",
                        "[{}] ingest rejection reason",
                        scenario.label
                    );
                    IngestOutcome::RejectFreshnessBudget
                }
            }
        }

        // Each row is independent: a fresh controller is built per assertion so
        // accumulated counters / EWMA drift cannot leak between rows.
        //
        // A2 history: the two rows whose projected_buffer is driven by SIZE debt
        // (excess bytes / drain rate) changed when A2 swapped the static 1000
        // B/s rate for the single observed 2000 B/s EWMA -- they are annotated
        // "CHANGED BY A2" inline. Rows driven by seal debt or buffer-AGE debt
        // were unaffected.
        fn scenarios() -> Vec<Scenario> {
            vec![
                // idle: everything ~0. Stays fixed under A2.
                Scenario {
                    label: "idle",
                    inflight_bytes: 0,
                    incoming_bytes: 0,
                    buffered_bytes: 0,
                    buffered_active_count: 0,
                    oldest_buffer_age_seconds: 0.0,
                    expected_ingest: IngestOutcome::Accept,
                    expected_heavy: HeavyOutcome::AcceptFull,
                    expected_projected_seal_seconds: 0.0,
                    expected_projected_buffer_seconds: 0.0,
                    expected_projected_visibility_seconds: 0.0,
                },
                // seal-debt dominated: large inflight, no buffer pressure.
                // projected_seal = 4000 / 2000 = 2.0s. Stays fixed under A2
                // (already uses the EWMA).
                Scenario {
                    label: "seal_debt_dominated",
                    inflight_bytes: 4_000,
                    incoming_bytes: 0,
                    buffered_bytes: 0,
                    buffered_active_count: 0,
                    oldest_buffer_age_seconds: 0.0,
                    expected_ingest: IngestOutcome::Accept,
                    expected_heavy: HeavyOutcome::AcceptFull,
                    expected_projected_seal_seconds: 2.0,
                    expected_projected_buffer_seconds: 0.0,
                    expected_projected_visibility_seconds: 2.0,
                },
                // buffer-SIZE-debt dominated (age small): buffered 5000 with 1
                // active slot -> allowed = 1000, excess = 4000.
                // CHANGED BY A2: buffer-size debt now divides by the single
                // observed seal rate (EWMA 2000 B/s) instead of the static
                // 1000 B/s drain rate, so 4000 / 2000 = 2.0s (was 4.0s);
                // visibility tracks it to 2.0s. Decisions unchanged (still
                // Accept / AcceptFull, well under the band).
                Scenario {
                    label: "buffer_size_debt_dominated",
                    inflight_bytes: 0,
                    incoming_bytes: 0,
                    buffered_bytes: 5_000,
                    buffered_active_count: 1,
                    oldest_buffer_age_seconds: 0.0,
                    expected_ingest: IngestOutcome::Accept,
                    expected_heavy: HeavyOutcome::AcceptFull,
                    expected_projected_seal_seconds: 0.0,
                    expected_projected_buffer_seconds: 2.0,
                    expected_projected_visibility_seconds: 2.0,
                },
                // buffer-AGE-debt dominated (small bytes, large age): bytes within
                // allowance so size debt = 0; age 6s - max_age 1s = 5.0s.
                // Stays fixed under A2 (age term untouched).
                Scenario {
                    label: "buffer_age_debt_dominated",
                    inflight_bytes: 0,
                    incoming_bytes: 0,
                    buffered_bytes: 500,
                    buffered_active_count: 1,
                    oldest_buffer_age_seconds: 6.0,
                    expected_ingest: IngestOutcome::Accept,
                    expected_heavy: HeavyOutcome::AcceptFull,
                    expected_projected_seal_seconds: 0.0,
                    expected_projected_buffer_seconds: 5.0,
                    expected_projected_visibility_seconds: 5.0,
                },
                // ingest-budget boundary, just UNDER 0.95*sla = 9.5s.
                // inflight 18999 + incoming 0 -> seal = 18999/2000 = 9.4995s < 9.5.
                // Stays fixed under A2 (seal-driven).
                Scenario {
                    label: "ingest_budget_just_under",
                    inflight_bytes: 18_999,
                    incoming_bytes: 0,
                    buffered_bytes: 0,
                    buffered_active_count: 0,
                    oldest_buffer_age_seconds: 0.0,
                    expected_ingest: IngestOutcome::Accept,
                    expected_heavy: HeavyOutcome::AcceptFull,
                    expected_projected_seal_seconds: 9.4995,
                    expected_projected_buffer_seconds: 0.0,
                    expected_projected_visibility_seconds: 9.4995,
                },
                // ingest-budget boundary, just OVER 0.95*sla = 9.5s.
                // inflight 19001 -> seal = 9.5005s > 9.5 -> reject. Still below
                // 1.0*sla=10 so heavy is full. Stays fixed under A2 (seal-driven).
                Scenario {
                    label: "ingest_budget_just_over",
                    inflight_bytes: 19_001,
                    incoming_bytes: 0,
                    buffered_bytes: 0,
                    buffered_active_count: 0,
                    oldest_buffer_age_seconds: 0.0,
                    expected_ingest: IngestOutcome::RejectFreshnessBudget,
                    expected_heavy: HeavyOutcome::AcceptFull,
                    expected_projected_seal_seconds: 9.5005,
                    expected_projected_buffer_seconds: 0.0,
                    expected_projected_visibility_seconds: 9.5005,
                },
                // heavy-degrade band: >= 1.0*sla (10s), < 1.5*sla (15s).
                // inflight 24000 -> seal = 12.0s. Ingest rejects (over 9.5),
                // heavy accepts but degraded (effective capacity = 1).
                // Stays fixed under A2 (seal-driven).
                Scenario {
                    label: "heavy_degrade_band",
                    inflight_bytes: 24_000,
                    incoming_bytes: 0,
                    buffered_bytes: 0,
                    buffered_active_count: 0,
                    oldest_buffer_age_seconds: 0.0,
                    expected_ingest: IngestOutcome::RejectFreshnessBudget,
                    expected_heavy: HeavyOutcome::AcceptDegraded,
                    expected_projected_seal_seconds: 12.0,
                    expected_projected_buffer_seconds: 0.0,
                    expected_projected_visibility_seconds: 12.0,
                },
                // heavy-reject band: >= 1.5*sla (15s).
                // inflight 32000 -> seal = 16.0s. Ingest rejects, heavy rejects
                // with freshness_debt. Stays fixed under A2 (seal-driven).
                Scenario {
                    label: "heavy_reject_band",
                    inflight_bytes: 32_000,
                    incoming_bytes: 0,
                    buffered_bytes: 0,
                    buffered_active_count: 0,
                    oldest_buffer_age_seconds: 0.0,
                    expected_ingest: IngestOutcome::RejectFreshnessBudget,
                    expected_heavy: HeavyOutcome::RejectFreshnessDebt,
                    expected_projected_seal_seconds: 16.0,
                    expected_projected_buffer_seconds: 0.0,
                    expected_projected_visibility_seconds: 16.0,
                },
                // buffer-SIZE-debt that USED to be pushed into the heavy-reject
                // band by the static drain rate. buffered 17000 over 1 slot ->
                // excess 16000.
                // CHANGED BY A2: dividing by the single observed seal rate (EWMA
                // 2000 B/s) instead of the static 1000 B/s halves the size debt:
                // 16000 / 2000 = 8.0s (was 16.0s). 8.0s is below the 0.95*sla
                // ingest budget (9.5s) and below 1.0*sla (10s), so the decisions
                // FLIP: ingest Reject -> Accept and heavy Reject -> AcceptFull.
                // This row is the sharpest A2 signal.
                Scenario {
                    label: "buffer_size_debt_into_reject_band",
                    inflight_bytes: 0,
                    incoming_bytes: 0,
                    buffered_bytes: 17_000,
                    buffered_active_count: 1,
                    oldest_buffer_age_seconds: 0.0,
                    expected_ingest: IngestOutcome::Accept,
                    expected_heavy: HeavyOutcome::AcceptFull,
                    expected_projected_seal_seconds: 0.0,
                    expected_projected_buffer_seconds: 8.0,
                    expected_projected_visibility_seconds: 8.0,
                },
            ]
        }

        #[test]
        fn admission_decisions_match_golden_grid() {
            for scenario in scenarios() {
                let label = scenario.label;
                let inputs = scenario.inputs();

                // Projection triple from a fresh controller.
                let admission = characterization_controller();
                let snapshot = admission.snapshot_for(inputs);
                approx_eq(
                    snapshot.projected_seal_seconds,
                    scenario.expected_projected_seal_seconds,
                    label,
                    "projected_seal_seconds",
                );
                approx_eq(
                    snapshot.projected_buffer_seconds,
                    scenario.expected_projected_buffer_seconds,
                    label,
                    "projected_buffer_seconds",
                );
                approx_eq(
                    snapshot.projected_visibility_seconds,
                    scenario.expected_projected_visibility_seconds,
                    label,
                    "projected_visibility_seconds",
                );

                // Ingest decision on a fresh controller.
                assert_eq!(
                    classify_ingest(&scenario),
                    scenario.expected_ingest,
                    "[{label}] ingest outcome",
                );

                // Heavy-query decision on a fresh controller.
                assert_eq!(
                    classify_heavy(&scenario),
                    scenario.expected_heavy,
                    "[{label}] heavy outcome",
                );
            }
        }
    }
}
