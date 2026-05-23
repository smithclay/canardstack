use crate::app::AppState;
use crate::validation::{ApiError, ApiResult};
use serde_json::{json, Value};
use std::time::Instant;

/// The whole seal, in one place: reserve seal capacity, flush+commit+checkpoint
/// the Arrow write buffer to DuckLake, feed the observed throughput back into the
/// admission EWMA, and record the run. The single named entry point for "perform
/// a seal", used by both the scheduler tick and the admin maintenance route.
pub fn run(state: &AppState) -> ApiResult<Value> {
    let started = Instant::now();
    let pending_bytes: usize = state
        .storage
        .arrow_write_buffer_metrics()
        .iter()
        .map(|metric| metric.bytes)
        .sum();
    let mut guard = state.admission.reserve_seal(&state.metrics)?;
    guard.record_bytes(pending_bytes);
    let arrow_flush = state
        .ingestor
        .seal_committed_to_storage(&state.storage, &state.metrics)
        .map_err(|err| ApiError::new(503, "storage_operation_failed", err.to_string()));
    guard.finish(&state.metrics);
    let arrow_flush = arrow_flush?;
    state.maintenance.record_seal_run();
    Ok(json!({
        "status": "ok",
        "arrow_flush": arrow_flush.to_json(),
        "duration_ms": started.elapsed().as_millis()
    }))
}
