use super::queue::{self, PendingBatch, QueueMap};
use super::Signal;
use crate::config::Config;
use crate::memory;
use crate::metrics::Metrics;
use crate::validation::{ApiError, ApiResult};
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

pub(super) struct RuntimeMemoryReservation {
    reserved_bytes: Arc<AtomicUsize>,
    bytes: usize,
    limit: Option<usize>,
}

impl RuntimeMemoryReservation {
    pub(super) fn disabled(reserved_bytes: Arc<AtomicUsize>) -> Self {
        Self {
            reserved_bytes,
            bytes: 0,
            limit: None,
        }
    }

    pub(super) fn with_limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }

    pub(super) fn reserve_at_least(
        &mut self,
        target_bytes: usize,
        signal: Signal,
        metrics: &Metrics,
    ) -> ApiResult<()> {
        let Some(limit) = self.limit else {
            return Ok(());
        };
        if target_bytes <= self.bytes {
            return Ok(());
        }
        let delta = target_bytes - self.bytes;
        loop {
            let Some(rss) = memory::runtime_rss_bytes() else {
                metrics.inc(
                    "canardstack_ingest_runtime_memory_unknown_total",
                    &[("signal", signal.as_str())],
                    1,
                );
                return Ok(());
            };
            metrics.gauge("canardstack_runtime_rss_bytes", &[], rss as f64);
            metrics.gauge("canardstack_runtime_memory_limit_bytes", &[], limit as f64);

            let current_reserved = self.reserved_bytes.load(Ordering::Acquire);
            let other_reserved = current_reserved.saturating_sub(self.bytes);
            let projected = rss.saturating_add(other_reserved).saturating_add(delta);
            if rss >= limit || projected > limit {
                return Err(ApiError::new(
                    429,
                    "runtime_memory_full",
                    "process runtime memory limit would be exceeded",
                )
                .with_retry_after(5));
            }
            let new_reserved = current_reserved.saturating_add(delta);
            match self.reserved_bytes.compare_exchange(
                current_reserved,
                new_reserved,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    self.bytes = target_bytes;
                    return Ok(());
                }
                Err(_) => continue,
            }
        }
    }
}

impl Drop for RuntimeMemoryReservation {
    fn drop(&mut self) {
        if self.bytes > 0 {
            self.reserved_bytes.fetch_sub(self.bytes, Ordering::AcqRel);
        }
    }
}

pub(super) struct QueueAdmission {
    pub(super) accepted: usize,
    pub(super) should_request_flush: bool,
}

pub(super) fn admit_and_enqueue(
    queues: &mut QueueMap,
    batches: Vec<PendingBatch>,
    config: &Config,
) -> ApiResult<QueueAdmission> {
    let process_bytes = queue::process_bytes(queues);
    let added_process_bytes = queue::added_process_bytes(&batches);
    let added_by_signal: HashMap<Signal, usize> = queue::added_bytes_by_signal(&batches);
    for (signal, added_bytes) in &added_by_signal {
        let queued_bytes = queue::queued_bytes_for_signal(queues, *signal);
        if queued_bytes + added_bytes > config.per_signal_queue_bytes {
            let queued_bytes_str = queued_bytes.to_string();
            let added_str = added_bytes.to_string();
            let cap_str = config.per_signal_queue_bytes.to_string();
            crate::log_event(
                "warn",
                "ingest_queue_full",
                &[
                    ("signal", signal.as_str()),
                    ("queued_bytes", &queued_bytes_str),
                    ("incoming_bytes", &added_str),
                    ("cap_bytes", &cap_str),
                ],
            );
            return Err(
                ApiError::new(429, "signal_queue_full", format!("{signal} queue is full"))
                    .with_retry_after(5),
            );
        }
    }
    if process_bytes + added_process_bytes > config.process_ingest_bytes {
        let process_str = process_bytes.to_string();
        let added_str = added_process_bytes.to_string();
        let cap_str = config.process_ingest_bytes.to_string();
        crate::log_event(
            "warn",
            "ingest_process_memory_full",
            &[
                ("process_bytes", &process_str),
                ("incoming_bytes", &added_str),
                ("cap_bytes", &cap_str),
            ],
        );
        return Err(ApiError::new(
            429,
            "process_ingest_memory_full",
            "process ingest memory cap would be exceeded",
        )
        .with_retry_after(5));
    }

    let accepted = queue::enqueue_batches(queues, batches);
    Ok(QueueAdmission {
        accepted,
        should_request_flush: queue::has_threshold_due_queue(queues, config),
    })
}

pub(super) fn decode_reservation_bytes(
    headers: &HashMap<String, String>,
    compressed_body_bytes: usize,
    max_body_bytes: usize,
) -> usize {
    let compressed_body_bytes = compressed_body_bytes.max(1);
    match headers
        .get("content-encoding")
        .map(|value| value.to_ascii_lowercase())
        .as_deref()
    {
        Some("gzip") => compressed_body_bytes.saturating_add(max_body_bytes),
        _ => compressed_body_bytes.saturating_mul(2),
    }
}
