use crate::config::Config;
use crate::lanes::{FreshnessInputs, LaneController};
use crate::metrics::Metrics;
use crate::otlp::{self, Transformed};
use crate::storage::Storage;
use crate::validation::{self, ApiError, ApiResult};
use crate::LockExt;
use anyhow::{Context, Result};
use serde::Serialize;
use serde_json::{json, Value};
use spool::{FlushRef, Options, RecordId, Writer};
use std::borrow::Cow;
use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::sync::atomic::AtomicUsize;
use std::sync::mpsc::TrySendError;
use std::sync::{Arc, Mutex};
use std::time::Instant;

mod admission;
mod queue;
pub mod spool;
mod topology;

pub use queue::IngestSnapshot;
use topology::{IngestTopologyDispatcher, StorageSinkDispatcher, StorageSinkWork};

#[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize)]
pub enum Signal {
    Logs,
    Spans,
    MetricGauge,
    MetricSum,
}

impl Signal {
    pub const ALL: [Signal; 4] = [
        Signal::Logs,
        Signal::Spans,
        Signal::MetricGauge,
        Signal::MetricSum,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            Signal::Logs => "logs",
            Signal::Spans => "spans",
            Signal::MetricGauge => "metric_gauge",
            Signal::MetricSum => "metric_sum",
        }
    }

    fn is_metric(self) -> bool {
        matches!(self, Signal::MetricGauge | Signal::MetricSum)
    }
}

impl fmt::Display for Signal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

pub struct Ingestor {
    runtime_memory_reserved_bytes: Arc<AtomicUsize>,
    queue_credits: Arc<Mutex<admission::QueueCreditLedger>>,
    raw_spools: BTreeMap<Signal, Writer>,
    metric_raw_spool_next: AtomicUsize,
    raw_spool_flush_refs: Arc<Mutex<BTreeMap<(Signal, RecordId), FlushRef>>>,
    topology: Mutex<Option<IngestTopologyDispatcher>>,
    storage_sink: Mutex<Option<StorageSinkDispatcher>>,
    config: Config,
}

pub(in crate::ingest) struct SpooledIngestWork {
    pub(in crate::ingest) signal: Signal,
    headers: HashMap<String, String>,
    compressed_body: Vec<u8>,
    raw_spool_ref: spool::AppendRef,
    queue_credit_reservation: admission::QueueCreditReservation,
    runtime_memory_reservation: admission::RuntimeMemoryReservation,
    pub(in crate::ingest) metrics: Arc<Metrics>,
    // Payload already decompressed, transformed, and skew-validated on the
    // connection thread. `None` on the replay path, where only the raw
    // compressed body survives a restart and must be re-decoded by the worker.
    prepared: Option<PreparedIngest>,
}

struct PreparedIngest {
    transformed: otlp::Transformed,
    decoded_body_len: usize,
    decoded_body_materialized_bytes: usize,
}

impl Ingestor {
    pub fn new(config: Config) -> Result<Self> {
        let mut raw_spools = BTreeMap::new();
        for signal in all_signals() {
            raw_spools.insert(signal, spawn_raw_spool_writer(&config, signal)?);
        }
        Ok(Self {
            runtime_memory_reserved_bytes: Arc::new(AtomicUsize::new(0)),
            queue_credits: Arc::new(Mutex::new(admission::QueueCreditLedger::new(&config))),
            raw_spools,
            metric_raw_spool_next: AtomicUsize::new(0),
            raw_spool_flush_refs: Arc::new(Mutex::new(BTreeMap::new())),
            topology: Mutex::new(None),
            storage_sink: Mutex::new(None),
            config,
        })
    }

    pub fn ingest(
        &self,
        signal: Signal,
        headers: &HashMap<String, String>,
        compressed_body: Vec<u8>,
        storage: &Storage,
        lanes: &LaneController,
        metrics: Arc<Metrics>,
    ) -> ApiResult<Value> {
        let async_metrics = Arc::clone(&metrics);
        let metrics = metrics.as_ref();
        if let Err(err) = validation::validate_body_size(compressed_body.len(), &self.config) {
            metrics.ingest_request(signal, err.status, err.reason);
            return Err(err);
        }
        if let Err(err) = validation::validate_content_type(headers) {
            metrics.ingest_request(signal, err.status, err.reason);
            return Err(err);
        }
        let prepared =
            match self.validate_request_payload(signal, headers, &compressed_body, metrics) {
                Ok(prepared) => prepared,
                Err(err) => {
                    metrics.ingest_request(signal, err.status, err.reason);
                    return Err(err);
                }
            };
        if !storage.accepts_memory_ingest() || self.config.force_dependency_unhealthy {
            metrics.ingest_request(signal, 503, "dependency_unhealthy");
            return Err(ApiError::new(
                503,
                "dependency_unhealthy",
                "storage dependency is unhealthy",
            )
            .with_retry_after(10));
        }
        let mut queue_credit_reservation = match self.reserve_queue_credit_estimate(
            signal,
            headers,
            compressed_body.len(),
            storage,
            lanes,
            metrics,
        ) {
            Ok(reservation) => reservation,
            Err(err) => {
                self.record_queue_metrics(metrics);
                metrics.ingest_request(signal, err.status, err.reason);
                return Err(err);
            }
        };
        let (raw_spool_ref, compressed_body) =
            match self.append_raw_spool(signal, headers, compressed_body, metrics) {
                Ok(appended) => appended,
                Err(err) => {
                    self.release_queue_credit_reservation(&mut queue_credit_reservation);
                    metrics.ingest_request(signal, err.status, err.reason);
                    return Err(err);
                }
            };
        let runtime_memory_reservation =
            match self.admit_runtime_memory(signal, headers, compressed_body.len(), metrics) {
                Ok(reservation) => reservation,
                Err(err) => {
                    self.release_queue_credit_reservation(&mut queue_credit_reservation);
                    self.checkpoint_raw_spool_terminal(
                        raw_spool_ref,
                        signal,
                        "memory_rejected",
                        metrics,
                    )?;
                    metrics.ingest_request(signal, err.status, err.reason);
                    return Err(err);
                }
            };
        let spooled = SpooledIngestWork {
            signal,
            headers: headers.clone(),
            compressed_body,
            raw_spool_ref,
            queue_credit_reservation,
            runtime_memory_reservation,
            metrics: async_metrics,
            prepared: Some(prepared),
        };
        self.dispatch_topology_work(spooled, metrics)
    }

    pub(in crate::ingest) fn process_spooled_ingest(
        &self,
        work: SpooledIngestWork,
        record_request_status: bool,
    ) -> ApiResult<Value> {
        let SpooledIngestWork {
            signal,
            headers,
            compressed_body,
            raw_spool_ref,
            mut queue_credit_reservation,
            mut runtime_memory_reservation,
            metrics,
            prepared,
        } = work;
        let metrics_arc = metrics;
        let metrics = metrics_arc.as_ref();
        let (transformed, decoded_body_len, decoded_body_materialized_bytes) = match prepared {
            // Normal ingest path: the connection thread already decompressed,
            // transformed, and skew-validated this payload in
            // validate_request_payload. Reuse it rather than redoing the work.
            Some(PreparedIngest {
                transformed,
                decoded_body_len,
                decoded_body_materialized_bytes,
            }) => (
                transformed,
                decoded_body_len,
                decoded_body_materialized_bytes,
            ),
            // Replay path: only the raw compressed body survived the restart, so
            // decode and validate it here in the worker.
            None => {
                let started = Instant::now();
                let body_result = otlp::decompress_if_needed(
                    &headers,
                    &compressed_body,
                    self.config.max_body_bytes,
                );
                metrics.observe_phase_seconds(
                    signal.as_str(),
                    "decompress",
                    None,
                    started.elapsed().as_secs_f64(),
                );
                let body = match body_result {
                    Ok(body) => body,
                    Err(err) => {
                        self.release_queue_credit_reservation(&mut queue_credit_reservation);
                        if record_request_status {
                            metrics.ingest_request(signal, err.status, err.reason);
                        }
                        return Err(err);
                    }
                };
                let decoded_body_materialized_bytes = match &body {
                    Cow::Borrowed(_) => 0,
                    Cow::Owned(bytes) => bytes.len(),
                };
                if let Err(err) = validation::validate_body_size(body.len(), &self.config) {
                    self.release_queue_credit_reservation(&mut queue_credit_reservation);
                    if record_request_status {
                        metrics.ingest_request(signal, err.status, err.reason);
                    }
                    return Err(err);
                }
                let started = Instant::now();
                #[cfg(feature = "otlp2records-observer")]
                let transformed_result = otlp::transform_observed(signal, &headers, &body, metrics);
                #[cfg(not(feature = "otlp2records-observer"))]
                let transformed_result = otlp::transform(signal, &headers, &body);
                metrics.observe_phase_seconds(
                    signal.as_str(),
                    "otlp_transform",
                    None,
                    started.elapsed().as_secs_f64(),
                );
                let transformed = match transformed_result {
                    Ok(transformed) => transformed,
                    Err(err) => {
                        self.release_queue_credit_reservation(&mut queue_credit_reservation);
                        if record_request_status {
                            metrics.ingest_request(signal, err.status, err.reason);
                        }
                        return Err(err);
                    }
                };
                let started = Instant::now();
                let skew_result = self.validate_skew(&transformed);
                metrics.observe_phase_seconds(
                    signal.as_str(),
                    "timestamp_validation",
                    None,
                    started.elapsed().as_secs_f64(),
                );
                if let Err(err) = skew_result {
                    self.release_queue_credit_reservation(&mut queue_credit_reservation);
                    if record_request_status {
                        metrics.ingest_request(signal, err.status, err.reason);
                    }
                    return Err(err);
                }
                let decoded_body_len = body.len();
                (
                    transformed,
                    decoded_body_len,
                    decoded_body_materialized_bytes,
                )
            }
        };
        for (output_signal, rows) in transformed_rows_by_signal(&transformed) {
            metrics.inc(
                "canardstack_ingest_transformed_rows_total",
                &[
                    ("signal", output_signal.as_str()),
                    ("request_signal", signal.as_str()),
                ],
                rows as u64,
            );
        }

        let request_bytes = compressed_body.len();
        let unsupported_histograms = transformed.unsupported_histograms;
        let mut batches = queue::pending_batches(transformed);
        let buffered_totals = pending_batch_totals(&batches);
        let exact_queue_credits = admission::credit_bytes_by_signal(&batches);
        if let Err(err) =
            self.adjust_queue_credit_reservation(&mut queue_credit_reservation, exact_queue_credits)
        {
            self.release_queue_credit_reservation(&mut queue_credit_reservation);
            self.record_queue_metrics(metrics);
            if record_request_status {
                metrics.ingest_request(signal, err.status, err.reason);
            }
            return Err(err);
        }
        let pending_bytes = batches.iter().map(|b| b.approx_bytes).sum::<usize>();
        let peak_bytes = request_bytes
            .saturating_add(decoded_body_materialized_bytes)
            .saturating_add(pending_bytes);
        if let Err(err) = runtime_memory_reservation.reserve_at_least(peak_bytes, signal, metrics) {
            self.release_queue_credit_reservation(&mut queue_credit_reservation);
            if record_request_status {
                metrics.ingest_request(signal, err.status, err.reason);
            }
            return Err(err);
        }
        if batches.is_empty() {
            self.release_queue_credit_reservation(&mut queue_credit_reservation);
            self.checkpoint_raw_spool_terminal(raw_spool_ref, signal, "transform_empty", metrics)?;
        } else {
            self.track_raw_spool_batches(raw_spool_ref, signal, &mut batches);
        }
        let accepted = buffered_totals
            .values()
            .map(|(rows, _)| *rows)
            .sum::<usize>();
        let sets = storage_sink_sets(batches);
        if let Err(err) =
            self.dispatch_storage_sink(signal, sets, metrics, &mut queue_credit_reservation)
        {
            self.untrack_raw_spool_record(raw_spool_ref);
            self.release_queue_credit_reservation(&mut queue_credit_reservation);
            self.record_queue_metrics(metrics);
            if record_request_status {
                metrics.ingest_request(signal, err.status, err.reason);
            }
            return Err(err);
        }
        if record_request_status {
            metrics.ingest_request(signal, 202, "accepted");
        }
        self.record_accepted_body_metrics(
            signal,
            &headers,
            request_bytes,
            decoded_body_len,
            decoded_body_materialized_bytes,
            metrics,
        );
        metrics.inc(
            "canardstack_ingest_records_total",
            &[("signal", signal.as_str())],
            accepted as u64,
        );
        for (output_signal, (rows, bytes)) in buffered_totals {
            metrics.inc(
                "canardstack_ingest_buffered_rows_total",
                &[("signal", output_signal.as_str())],
                rows as u64,
            );
            metrics.inc(
                "canardstack_ingest_buffered_bytes_total",
                &[("signal", output_signal.as_str())],
                bytes as u64,
            );
        }

        Ok(json!({
            "accepted": true,
            "records": accepted,
            "acknowledgement": "locally_spooled_storage_sink",
            "unsupported_histograms": unsupported_histograms
        }))
    }

    fn dispatch_storage_sink(
        &self,
        request_signal: Signal,
        sets: Vec<(queue::QueueKey, Vec<queue::QueuedBatch>)>,
        metrics: &Metrics,
        queue_credit_reservation: &mut admission::QueueCreditReservation,
    ) -> ApiResult<()> {
        if sets.iter().all(|(_, batches)| batches.is_empty()) {
            return Ok(());
        }
        let work = StorageSinkWork {
            request_signal,
            sets,
        };
        let result = {
            let storage_sink = self.storage_sink.lock_or_poisoned();
            let Some(dispatcher) = storage_sink.as_ref() else {
                return Err(ApiError::new(
                    503,
                    "storage_sink_unavailable",
                    "storage sink is not available",
                )
                .with_retry_after(5));
            };
            let Some(commands) = dispatcher.commands.as_ref() else {
                return Err(ApiError::new(
                    503,
                    "storage_sink_unavailable",
                    "storage sink is stopped",
                )
                .with_retry_after(5));
            };
            commands.try_send(work)
        };
        match result {
            Ok(()) => {
                self.release_queue_credit_reservation(queue_credit_reservation);
                metrics.inc(
                    "canardstack_storage_sink_requests_total",
                    &[("signal", request_signal.as_str()), ("status", "queued")],
                    1,
                );
                Ok(())
            }
            Err(TrySendError::Full(work)) => {
                self.release_queue_credit_reservation(queue_credit_reservation);
                self.untrack_storage_sink_work(&work);
                metrics.inc(
                    "canardstack_storage_sink_requests_total",
                    &[
                        ("signal", request_signal.as_str()),
                        ("status", "buffer_full"),
                    ],
                    1,
                );
                Err(ApiError::new(
                    429,
                    "storage_sink_buffer_full",
                    "storage sink buffer is full",
                )
                .with_retry_after(5))
            }
            Err(TrySendError::Disconnected(work)) => {
                self.release_queue_credit_reservation(queue_credit_reservation);
                self.untrack_storage_sink_work(&work);
                metrics.inc(
                    "canardstack_storage_sink_requests_total",
                    &[
                        ("signal", request_signal.as_str()),
                        ("status", "disconnected"),
                    ],
                    1,
                );
                Err(
                    ApiError::new(503, "storage_sink_unavailable", "storage sink is stopped")
                        .with_retry_after(5),
                )
            }
        }
    }

    pub(in crate::ingest) fn untrack_storage_sink_work(&self, work: &StorageSinkWork) {
        let mut refs = self.raw_spool_flush_refs.lock_or_poisoned();
        for (key, batches) in &work.sets {
            for batch in batches {
                if let Some(id) = batch.raw_spool_id {
                    let lane = batch.raw_spool_lane.unwrap_or(key.signal);
                    refs.remove(&(lane, id));
                }
            }
        }
    }

    fn dispatch_topology_work(
        &self,
        work: SpooledIngestWork,
        metrics: &Metrics,
    ) -> ApiResult<Value> {
        let signal = work.signal;
        let send_result = {
            let mut topology = self.topology.lock_or_poisoned();
            let Some(dispatcher) = topology.as_mut() else {
                return Err(ApiError::new(
                    503,
                    "ingest_topology_unavailable",
                    "ingest topology workers are not available",
                )
                .with_retry_after(5));
            };
            if dispatcher.commands.is_empty() {
                return Err(ApiError::new(
                    503,
                    "ingest_topology_unavailable",
                    "ingest topology workers are stopped",
                )
                .with_retry_after(5));
            }
            let start = dispatcher.next_worker % dispatcher.commands.len();
            let mut work = work;
            let mut disconnected = false;
            for offset in 0..dispatcher.commands.len() {
                let worker_idx = (start + offset) % dispatcher.commands.len();
                match dispatcher.commands[worker_idx].try_send(work) {
                    Ok(()) => {
                        dispatcher.next_worker = worker_idx.wrapping_add(1);
                        return self.topology_work_queued(signal, metrics);
                    }
                    Err(TrySendError::Full(returned)) => {
                        work = returned;
                    }
                    Err(TrySendError::Disconnected(returned)) => {
                        disconnected = true;
                        work = returned;
                    }
                }
            }
            if disconnected {
                Err(TrySendError::Disconnected(work))
            } else {
                Err(TrySendError::Full(work))
            }
        };
        match send_result {
            Ok(value) => Ok(value),
            Err(TrySendError::Full(work)) => {
                let mut queue_credit_reservation = work.queue_credit_reservation;
                self.release_queue_credit_reservation(&mut queue_credit_reservation);
                self.record_queue_metrics(metrics);
                metrics.ingest_request(signal, 429, "ingest_buffer_full");
                metrics.inc(
                    "canardstack_ingest_topology_requests_total",
                    &[("signal", signal.as_str()), ("status", "buffer_full")],
                    1,
                );
                Err(
                    ApiError::new(429, "ingest_buffer_full", "ingest worker buffer is full")
                        .with_retry_after(5),
                )
            }
            Err(TrySendError::Disconnected(work)) => {
                let mut queue_credit_reservation = work.queue_credit_reservation;
                self.release_queue_credit_reservation(&mut queue_credit_reservation);
                metrics.ingest_request(signal, 503, "ingest_topology_unavailable");
                metrics.inc(
                    "canardstack_ingest_topology_requests_total",
                    &[("signal", signal.as_str()), ("status", "disconnected")],
                    1,
                );
                Err(ApiError::new(
                    503,
                    "ingest_topology_unavailable",
                    "ingest topology workers are stopped",
                )
                .with_retry_after(5))
            }
        }
    }

    fn topology_work_queued(&self, signal: Signal, metrics: &Metrics) -> ApiResult<Value> {
        metrics.ingest_request(signal, 202, "accepted");
        metrics.inc(
            "canardstack_ingest_topology_requests_total",
            &[("signal", signal.as_str()), ("status", "queued")],
            1,
        );
        Ok(json!({
            "accepted": true,
            "records": 0,
            "acknowledgement": "locally_spooled_topology_handoff",
            "unsupported_histograms": 0
        }))
    }

    fn reserve_queue_credit_estimate(
        &self,
        signal: Signal,
        headers: &HashMap<String, String>,
        compressed_body_bytes: usize,
        storage: &Storage,
        lanes: &LaneController,
        metrics: &Metrics,
    ) -> ApiResult<admission::QueueCreditReservation> {
        let ledger = self.queue_credits.lock_or_poisoned();
        let estimated = ledger.estimate_for_request(
            signal,
            headers,
            compressed_body_bytes,
            self.config.max_body_bytes,
        );
        let projected_total = ledger.projected_reserved_total_bytes(&estimated);
        let queued_total = ledger.total_reserved_bytes();
        drop(ledger);
        let mut inputs = self.lane_freshness_inputs(storage);
        inputs.queued_bytes = queued_total;
        inputs.incoming_bytes = projected_total.saturating_sub(queued_total);
        lanes.admit_ingest(inputs, metrics)?;
        self.queue_credits.lock_or_poisoned().reserve_estimate(
            signal,
            headers,
            compressed_body_bytes,
            self.config.max_body_bytes,
        )
    }

    fn adjust_queue_credit_reservation(
        &self,
        reservation: &mut admission::QueueCreditReservation,
        desired: BTreeMap<Signal, usize>,
    ) -> ApiResult<()> {
        self.queue_credits
            .lock_or_poisoned()
            .adjust_reservation(reservation, desired)
    }

    fn release_queue_credit_reservation(
        &self,
        reservation: &mut admission::QueueCreditReservation,
    ) {
        self.queue_credits
            .lock_or_poisoned()
            .release_reservation(reservation);
    }

    fn admit_runtime_memory(
        &self,
        signal: Signal,
        headers: &HashMap<String, String>,
        compressed_body_bytes: usize,
        metrics: &Metrics,
    ) -> ApiResult<admission::RuntimeMemoryReservation> {
        let mut reservation = admission::RuntimeMemoryReservation::disabled(
            self.runtime_memory_reserved_bytes.clone(),
        );
        let Some(limit) = self.config.runtime_memory_limit_bytes else {
            return Ok(reservation);
        };
        reservation = reservation.with_limit(limit);
        reservation.reserve_at_least(
            admission::decode_reservation_bytes(
                headers,
                compressed_body_bytes,
                self.config.max_body_bytes,
            ),
            signal,
            metrics,
        )?;
        Ok(reservation)
    }

    pub fn snapshots(&self) -> Vec<IngestSnapshot> {
        let credit_snapshots = self.queue_credits.lock_or_poisoned().snapshots();
        Signal::ALL
            .into_iter()
            .map(|signal| {
                let credit = credit_snapshots.get(&signal);
                let reserved = credit.map(|c| c.reserved_bytes).unwrap_or_default();
                let capacity = credit
                    .map(|c| c.capacity_bytes)
                    .unwrap_or(self.config.per_signal_queue_bytes);
                IngestSnapshot {
                    signal: signal.as_str(),
                    buffered_rows: 0,
                    buffered_bytes: reserved,
                    queue_credit_reserved_bytes: reserved,
                    queue_credit_available_bytes: credit
                        .map(|c| c.available_bytes)
                        .unwrap_or(capacity.saturating_sub(reserved)),
                    queue_credit_capacity_bytes: capacity,
                    queue_credit_closed: credit.map(|c| c.closed).unwrap_or(false),
                    visibility_debt_seconds: credit.map(|c| c.flush_debt_seconds).unwrap_or(0.0),
                    oldest_age_seconds: 0.0,
                    pressure: if capacity == 0 {
                        0.0
                    } else {
                        reserved as f64 / capacity as f64
                    },
                }
            })
            .collect()
    }

    pub fn record_queue_metrics(&self, metrics: &Metrics) {
        for snapshot in self.snapshots() {
            metrics.gauge(
                "canardstack_ingest_queue_rows",
                &[("signal", snapshot.signal)],
                snapshot.buffered_rows as f64,
            );
            metrics.gauge_max(
                "canardstack_ingest_queue_rows_max",
                &[("signal", snapshot.signal)],
                snapshot.buffered_rows as f64,
            );
            metrics.gauge(
                "canardstack_ingest_queue_bytes",
                &[("signal", snapshot.signal)],
                snapshot.buffered_bytes as f64,
            );
            metrics.gauge_max(
                "canardstack_ingest_queue_bytes_max",
                &[("signal", snapshot.signal)],
                snapshot.buffered_bytes as f64,
            );
            metrics.gauge(
                "canardstack_ingest_queue_oldest_age_seconds",
                &[("signal", snapshot.signal)],
                snapshot.oldest_age_seconds,
            );
            metrics.gauge_max(
                "canardstack_ingest_queue_oldest_age_seconds_max",
                &[("signal", snapshot.signal)],
                snapshot.oldest_age_seconds,
            );
            metrics.gauge(
                "canardstack_ingest_queue_pressure",
                &[("signal", snapshot.signal)],
                snapshot.pressure,
            );
            metrics.gauge_max(
                "canardstack_ingest_queue_pressure_max",
                &[("signal", snapshot.signal)],
                snapshot.pressure,
            );
            metrics.gauge(
                "canardstack_ingest_queue_credit_reserved_bytes",
                &[("signal", snapshot.signal)],
                snapshot.queue_credit_reserved_bytes as f64,
            );
            metrics.gauge(
                "canardstack_ingest_queue_credit_available_bytes",
                &[("signal", snapshot.signal)],
                snapshot.queue_credit_available_bytes as f64,
            );
            metrics.gauge(
                "canardstack_ingest_queue_credit_capacity_bytes",
                &[("signal", snapshot.signal)],
                snapshot.queue_credit_capacity_bytes as f64,
            );
            metrics.gauge(
                "canardstack_ingest_queue_credit_closed",
                &[("signal", snapshot.signal)],
                if snapshot.queue_credit_closed {
                    1.0
                } else {
                    0.0
                },
            );
            metrics.gauge(
                "canardstack_ingest_visibility_debt_seconds",
                &[("signal", snapshot.signal)],
                snapshot.visibility_debt_seconds,
            );
        }
    }

    pub fn lane_freshness_inputs(&self, storage: &Storage) -> FreshnessInputs {
        let (buffered_bytes, buffered_active_count, oldest_buffer_age_seconds) = storage
            .immutable_buffer_metrics()
            .into_iter()
            .fold((0usize, 0usize, 0.0f64), |(bytes, count, age), metric| {
                (
                    bytes.saturating_add(metric.bytes),
                    count.saturating_add(usize::from(metric.bytes > 0)),
                    age.max(metric.age_seconds),
                )
            });
        FreshnessInputs {
            queued_bytes: self.total_reserved_queue_bytes(),
            incoming_bytes: 0,
            oldest_queue_age_seconds: self.max_oldest_queue_age_seconds(),
            buffered_bytes,
            buffered_active_count,
            oldest_buffer_age_seconds,
        }
    }

    pub fn total_reserved_queue_bytes(&self) -> usize {
        self.queue_credits.lock_or_poisoned().total_reserved_bytes()
    }

    fn max_oldest_queue_age_seconds(&self) -> f64 {
        0.0
    }

    fn record_accepted_body_metrics(
        &self,
        signal: Signal,
        headers: &HashMap<String, String>,
        request_bytes: usize,
        decoded_bytes: usize,
        decoded_body_materialized_bytes: usize,
        metrics: &Metrics,
    ) {
        let encoding = headers
            .get("content-encoding")
            .map(String::as_str)
            .unwrap_or("identity");
        metrics.inc(
            "canardstack_ingest_request_bytes_total",
            &[("signal", signal.as_str()), ("encoding", encoding)],
            request_bytes as u64,
        );
        metrics.inc(
            "canardstack_ingest_decoded_bytes_total",
            &[("signal", signal.as_str()), ("encoding", encoding)],
            decoded_bytes as u64,
        );
        metrics.inc(
            "canardstack_ingest_materialized_bytes_total",
            &[
                ("signal", signal.as_str()),
                ("component", "decoded_body"),
                ("kind", encoding),
            ],
            decoded_body_materialized_bytes as u64,
        );
    }

    fn validate_skew(&self, transformed: &Transformed) -> ApiResult<()> {
        for (signal, batch) in transformed.signal_batches() {
            if let Some(batch) = batch {
                validation::validate_arrow_timestamp_skew(batch, signal, &self.config)?;
            }
        }
        Ok(())
    }

    fn validate_request_payload(
        &self,
        signal: Signal,
        headers: &HashMap<String, String>,
        compressed_body: &[u8],
        metrics: &Metrics,
    ) -> ApiResult<PreparedIngest> {
        let started = Instant::now();
        let body = otlp::decompress_if_needed(headers, compressed_body, self.config.max_body_bytes);
        metrics.observe_phase_seconds(
            signal.as_str(),
            "request_validation_decompress",
            None,
            started.elapsed().as_secs_f64(),
        );
        let body = body?;
        validation::validate_body_size(body.len(), &self.config)?;
        let decoded_body_len = body.len();
        let decoded_body_materialized_bytes = match &body {
            Cow::Borrowed(_) => 0,
            Cow::Owned(bytes) => bytes.len(),
        };

        let started = Instant::now();
        #[cfg(feature = "otlp2records-observer")]
        let transformed = otlp::transform_observed(signal, headers, &body, metrics)?;
        #[cfg(not(feature = "otlp2records-observer"))]
        let transformed = otlp::transform(signal, headers, &body)?;
        metrics.observe_phase_seconds(
            signal.as_str(),
            "request_validation_transform",
            None,
            started.elapsed().as_secs_f64(),
        );
        self.validate_skew(&transformed)?;
        Ok(PreparedIngest {
            transformed,
            decoded_body_len,
            decoded_body_materialized_bytes,
        })
    }
}

fn storage_sink_sets(
    batches: Vec<queue::PendingBatch>,
) -> Vec<(queue::QueueKey, Vec<queue::QueuedBatch>)> {
    let mut sets = HashMap::<queue::QueueKey, Vec<queue::QueuedBatch>>::new();
    for batch in batches {
        sets.entry(batch.key)
            .or_default()
            .push(queue::QueuedBatch::from_pending(batch));
    }
    sets.into_iter().collect()
}

fn transformed_rows_by_signal(transformed: &Transformed) -> Vec<(Signal, usize)> {
    transformed
        .signal_batches()
        .into_iter()
        .filter_map(|(signal, batch)| {
            let rows = batch.map(|batch| batch.num_rows()).unwrap_or(0);
            (rows > 0).then_some((signal, rows))
        })
        .collect()
}

fn all_signals() -> [Signal; 4] {
    Signal::ALL
}

fn spawn_raw_spool_writer(config: &Config, signal: Signal) -> Result<Writer> {
    Writer::spawn(
        Options {
            dir: config.raw_spool_dir.join(signal.as_str()),
            max_segment_bytes: config.raw_spool_max_segment_bytes as u64,
            max_record_bytes: config.raw_spool_max_record_bytes as u64,
            max_total_bytes: config.raw_spool_max_total_bytes as u64,
            append_sync_interval: config.raw_spool_append_sync_interval,
            append_sync_bytes: config.raw_spool_append_sync_bytes as u64,
            checkpoint_fsync_records: config.raw_spool_checkpoint_fsync_records,
            checkpoint_fsync_delay: config.raw_spool_checkpoint_fsync_delay,
        },
        config.raw_spool_writer_queue_capacity,
        config.raw_spool_group_commit_records,
        config.raw_spool_group_commit_delay,
    )
    .with_context(|| format!("spawn {signal} raw spool writer"))
}

fn pending_batch_totals(batches: &[queue::PendingBatch]) -> BTreeMap<Signal, (usize, usize)> {
    let mut totals = BTreeMap::new();
    for batch in batches {
        let (rows, bytes) = totals.entry(batch.key.signal).or_insert((0, 0));
        *rows += batch.batch.num_rows();
        *bytes += batch.approx_bytes;
    }
    totals
}
