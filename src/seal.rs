use crate::app::AppState;
use crate::validation::{ApiError, ApiResult};
use serde_json::{json, Value};
use std::time::Instant;

/// The whole seal, in one place: reserve seal capacity, flush+commit+checkpoint
/// the Arrow write buffer to DuckLake, feed the observed throughput back into the
/// admission EWMA, and record the run. The single named entry point for "perform
/// a seal", used by both the scheduler tick and the admin maintenance route.
///
/// Delivery semantics: the raw-spool checkpoint happens AFTER the DuckLake COMMIT
/// (capture before flush, checkpoint after commit). This ordering is deliberate
/// and load-bearing for at-least-once — we never checkpoint rows that were not
/// storage-committed. The consequence is that a crash between COMMIT and
/// checkpoint replays those records on restart, producing duplicate ROWS in
/// storage, which v0 surfaces to queries without dedup.
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
