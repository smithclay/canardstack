use crate::config::Config;
use crate::lanes::{FreshnessInputs, LaneController};
use crate::metrics::Metrics;
use crate::otlp::{self, Transformed};
use crate::storage::Storage;
use crate::validation::{self, ApiError, ApiResult};
use crate::LockExt;
use anyhow::{Context, Result};
use raw_spool::RawSpoolFlushRef;
use serde::Serialize;
use serde_json::{json, Value};
use spool::{RawSpoolOptions, RawSpoolRecordId, RawSpoolWriter};
use std::borrow::Cow;
use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

mod admission;
mod flush;
mod queue;
mod raw_spool;
pub mod spool;

pub use flush::{partial_commit_info, PartialFlushError};
pub use queue::IngestSnapshot;

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
    queues: Arc<Mutex<queue::QueueMap>>,
    flush_lock: Arc<Mutex<()>>,
    flush_signal: Arc<FlushSignal>,
    runtime_memory_reserved_bytes: Arc<AtomicUsize>,
    queue_credits: Arc<Mutex<admission::QueueCreditLedger>>,
    raw_spools: BTreeMap<Signal, RawSpoolWriter>,
    metric_raw_spool_next: AtomicUsize,
    raw_spool_flush_refs: Arc<Mutex<BTreeMap<(Signal, RawSpoolRecordId), RawSpoolFlushRef>>>,
    config: Config,
}

#[derive(Default)]
struct FlushSignal {
    requested: Mutex<bool>,
    ready: Condvar,
}

impl Ingestor {
    pub fn new(config: Config) -> Result<Self> {
        let mut raw_spools = BTreeMap::new();
        for signal in all_signals() {
            raw_spools.insert(signal, spawn_raw_spool_writer(&config, signal)?);
        }
        Ok(Self {
            queues: Arc::new(Mutex::new(HashMap::new())),
            flush_lock: Arc::new(Mutex::new(())),
            flush_signal: Arc::new(FlushSignal::default()),
            runtime_memory_reserved_bytes: Arc::new(AtomicUsize::new(0)),
            queue_credits: Arc::new(Mutex::new(admission::QueueCreditLedger::new(&config))),
            raw_spools,
            metric_raw_spool_next: AtomicUsize::new(0),
            raw_spool_flush_refs: Arc::new(Mutex::new(BTreeMap::new())),
            config,
        })
    }

    pub fn request_flush(&self) -> bool {
        let mut requested = self.flush_signal.requested.lock_or_poisoned();
        let already_requested = *requested;
        *requested = true;
        if !already_requested {
            self.flush_signal.ready.notify_one();
        }
        already_requested
    }

    fn request_flush_observed(&self, triggered_by: Signal, metrics: &Metrics) {
        let started = Instant::now();
        let already_requested = self.request_flush();
        let status = if already_requested {
            "coalesced"
        } else {
            "queued"
        };
        metrics.inc(
            "canardstack_ingest_flush_requests_total",
            &[("triggered_by", triggered_by.as_str())],
            1,
        );
        metrics.inc(
            "canardstack_ingest_flush_request_events_total",
            &[("triggered_by", triggered_by.as_str()), ("status", status)],
            1,
        );
        metrics.observe_seconds(
            "canardstack_phase_duration_seconds",
            &[
                ("signal", triggered_by.as_str()),
                ("phase", "flush_request"),
                ("status", status),
            ],
            started.elapsed().as_secs_f64(),
        );
    }

    pub fn wait_for_flush_or_timeout(&self, timeout: Duration, stop: &AtomicBool) -> bool {
        let mut requested = self.flush_signal.requested.lock_or_poisoned();
        if !*requested && !stop.load(Ordering::SeqCst) {
            let (guard, _) = self
                .flush_signal
                .ready
                .wait_timeout(requested, timeout)
                .unwrap_or_else(|e| e.into_inner());
            requested = guard;
        }
        let was_requested = *requested;
        *requested = false;
        was_requested
    }

    pub fn ingest(
        &self,
        signal: Signal,
        headers: &HashMap<String, String>,
        compressed_body: Vec<u8>,
        storage: &Storage,
        lanes: &LaneController,
        metrics: &Metrics,
    ) -> ApiResult<Value> {
        validation::validate_body_size(compressed_body.len(), &self.config)?;
        validation::validate_content_type(headers)?;
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
            lanes,
            metrics,
        ) {
            Ok(reservation) => reservation,
            Err(err) => {
                self.request_flush_observed(signal, metrics);
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
        let mut runtime_memory_reservation =
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

        let started = Instant::now();
        let body_result =
            otlp::decompress_if_needed(headers, &compressed_body, self.config.max_body_bytes);
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
                self.checkpoint_raw_spool_terminal(
                    raw_spool_ref,
                    signal,
                    "decompress_rejected",
                    metrics,
                )?;
                return Err(err);
            }
        };
        let decoded_body_materialized_bytes = match &body {
            Cow::Borrowed(_) => 0,
            Cow::Owned(bytes) => bytes.len(),
        };
        if let Err(err) = validation::validate_body_size(body.len(), &self.config) {
            self.release_queue_credit_reservation(&mut queue_credit_reservation);
            self.checkpoint_raw_spool_terminal(
                raw_spool_ref,
                signal,
                "decoded_size_rejected",
                metrics,
            )?;
            return Err(err);
        }
        let started = Instant::now();
        #[cfg(feature = "otlp2records-observer")]
        let transformed_result = otlp::transform_observed(signal, headers, &body, metrics);
        #[cfg(not(feature = "otlp2records-observer"))]
        let transformed_result = otlp::transform(signal, headers, &body);
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
                self.checkpoint_raw_spool_terminal(
                    raw_spool_ref,
                    signal,
                    "transform_rejected",
                    metrics,
                )?;
                return Err(err);
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
            self.checkpoint_raw_spool_terminal(
                raw_spool_ref,
                signal,
                "timestamp_rejected",
                metrics,
            )?;
            return Err(err);
        }

        let request_bytes = compressed_body.len();
        let unsupported_histograms = transformed.unsupported_histograms;
        let mut batches = queue::pending_batches(transformed);
        let enqueued_totals = pending_batch_totals(&batches);
        let exact_queue_credits = admission::credit_bytes_by_signal(&batches);
        if let Err(err) =
            self.adjust_queue_credit_reservation(&mut queue_credit_reservation, exact_queue_credits)
        {
            self.release_queue_credit_reservation(&mut queue_credit_reservation);
            self.checkpoint_raw_spool_terminal(raw_spool_ref, signal, "queue_rejected", metrics)?;
            self.request_flush_observed(signal, metrics);
            self.record_queue_metrics(metrics);
            metrics.ingest_request(signal, err.status, err.reason);
            return Err(err);
        }
        let pending_bytes = batches.iter().map(|b| b.approx_bytes).sum::<usize>();
        let peak_bytes = request_bytes
            .saturating_add(decoded_body_materialized_bytes)
            .saturating_add(pending_bytes);
        if let Err(err) = runtime_memory_reservation.reserve_at_least(peak_bytes, signal, metrics) {
            self.release_queue_credit_reservation(&mut queue_credit_reservation);
            self.checkpoint_raw_spool_terminal(raw_spool_ref, signal, "memory_rejected", metrics)?;
            metrics.ingest_request(signal, err.status, err.reason);
            return Err(err);
        }
        if batches.is_empty() {
            self.release_queue_credit_reservation(&mut queue_credit_reservation);
            self.checkpoint_raw_spool_terminal(raw_spool_ref, signal, "transform_empty", metrics)?;
        } else {
            self.track_raw_spool_batches(raw_spool_ref, signal, &mut batches);
        }
        let accepted = match self.enqueue(signal, batches, metrics) {
            Ok(accepted) => accepted,
            Err(err) => {
                self.untrack_raw_spool_record(raw_spool_ref);
                self.release_queue_credit_reservation(&mut queue_credit_reservation);
                self.checkpoint_raw_spool_terminal(
                    raw_spool_ref,
                    signal,
                    "queue_rejected",
                    metrics,
                )?;
                self.record_queue_metrics(metrics);
                metrics.ingest_request(signal, err.status, err.reason);
                return Err(err);
            }
        };
        queue_credit_reservation.commit_to_queue();
        metrics.ingest_request(signal, 202, "accepted");
        self.record_accepted_body_metrics(
            signal,
            headers,
            request_bytes,
            body.len(),
            decoded_body_materialized_bytes,
            metrics,
        );
        metrics.inc(
            "canardstack_ingest_records_total",
            &[("signal", signal.as_str())],
            accepted as u64,
        );
        for (output_signal, (rows, bytes)) in enqueued_totals {
            metrics.inc(
                "canardstack_ingest_enqueued_rows_total",
                &[("signal", output_signal.as_str())],
                rows as u64,
            );
            metrics.inc(
                "canardstack_ingest_enqueued_bytes_total",
                &[("signal", output_signal.as_str())],
                bytes as u64,
            );
        }
        self.record_queue_metrics(metrics);

        Ok(json!({
            "accepted": true,
            "records": accepted,
            "acknowledgement": "locally_spooled_pending_periodic_sync",
            "unsupported_histograms": unsupported_histograms
        }))
    }

    fn reserve_queue_credit_estimate(
        &self,
        signal: Signal,
        headers: &HashMap<String, String>,
        compressed_body_bytes: usize,
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
        drop(ledger);
        let oldest_age_seconds = self.max_oldest_queue_age_seconds();
        lanes.admit_ingest(
            FreshnessInputs {
                queued_bytes: projected_total,
                incoming_bytes: 0,
                oldest_age_seconds,
            },
            metrics,
        )?;
        self.queue_credits.lock_or_poisoned().reserve_estimate(
            signal,
            headers,
            compressed_body_bytes,
            self.config.max_body_bytes,
        )
    }

    fn reserve_queue_credit_exact(
        &self,
        bytes_by_signal: BTreeMap<Signal, usize>,
    ) -> ApiResult<admission::QueueCreditReservation> {
        self.queue_credits
            .lock_or_poisoned()
            .reserve_exact(bytes_by_signal)
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

    fn release_queue_credits_for_batches(
        &self,
        sets: &[(queue::QueueKey, Vec<queue::QueuedBatch>)],
    ) {
        let mut bytes_by_signal = BTreeMap::<Signal, usize>::new();
        for (key, batches) in sets {
            for batch in batches {
                if batch.len() > 0 {
                    *bytes_by_signal.entry(key.signal).or_default() += batch.credit_bytes;
                }
            }
        }
        if !bytes_by_signal.is_empty() {
            self.queue_credits
                .lock_or_poisoned()
                .release_bytes(&bytes_by_signal);
        }
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
        let queues = self.queues.lock_or_poisoned();
        let mut snapshots = queue::snapshots(&queues, &self.config);
        drop(queues);

        let credit_snapshots = self.queue_credits.lock_or_poisoned().snapshots();
        for snapshot in &mut snapshots {
            let Some((_, credit)) = credit_snapshots
                .iter()
                .find(|(signal, _)| signal.as_str() == snapshot.signal)
            else {
                continue;
            };
            snapshot.queue_credit_reserved_bytes = credit.reserved_bytes;
            snapshot.queue_credit_available_bytes = credit.available_bytes;
            snapshot.queue_credit_capacity_bytes = credit.capacity_bytes;
            snapshot.queue_credit_closed = credit.closed;
            snapshot.flush_debt_seconds = credit.flush_debt_seconds;
            snapshot.pressure = if credit.capacity_bytes == 0 {
                0.0
            } else {
                credit.reserved_bytes as f64 / credit.capacity_bytes as f64
            };
        }
        snapshots
    }

    pub fn record_queue_metrics(&self, metrics: &Metrics) {
        for snapshot in self.snapshots() {
            metrics.gauge(
                "canardstack_ingest_queue_rows",
                &[("signal", snapshot.signal)],
                snapshot.queued_rows as f64,
            );
            metrics.gauge_max(
                "canardstack_ingest_queue_rows_max",
                &[("signal", snapshot.signal)],
                snapshot.queued_rows as f64,
            );
            metrics.gauge(
                "canardstack_ingest_queue_bytes",
                &[("signal", snapshot.signal)],
                snapshot.queued_bytes as f64,
            );
            metrics.gauge_max(
                "canardstack_ingest_queue_bytes_max",
                &[("signal", snapshot.signal)],
                snapshot.queued_bytes as f64,
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
                "canardstack_ingest_flush_debt_seconds",
                &[("signal", snapshot.signal)],
                snapshot.flush_debt_seconds,
            );
        }
    }

    pub fn lane_freshness_inputs(&self) -> FreshnessInputs {
        FreshnessInputs {
            queued_bytes: self.total_reserved_queue_bytes(),
            incoming_bytes: 0,
            oldest_age_seconds: self.max_oldest_queue_age_seconds(),
        }
    }

    pub fn total_reserved_queue_bytes(&self) -> usize {
        self.queue_credits.lock_or_poisoned().total_reserved_bytes()
    }

    fn max_oldest_queue_age_seconds(&self) -> f64 {
        let queues = self.queues.lock_or_poisoned();
        queue::snapshots(&queues, &self.config)
            .into_iter()
            .map(|snapshot| snapshot.oldest_age_seconds)
            .fold(0.0, f64::max)
    }

    fn enqueue(
        &self,
        request_signal: Signal,
        batches: Vec<queue::PendingBatch>,
        metrics: &Metrics,
    ) -> ApiResult<usize> {
        if batches.is_empty() {
            return Ok(0);
        }
        let started = Instant::now();
        let queue_result = {
            let mut queues = self.queues.lock_or_poisoned();
            admission::admit_and_enqueue(&mut queues, batches, &self.config)
        };
        metrics.observe_phase_seconds(
            request_signal.as_str(),
            "queue_admission",
            None,
            started.elapsed().as_secs_f64(),
        );
        let queue_result = queue_result?;
        if queue_result.should_request_flush {
            self.request_flush_observed(request_signal, metrics);
        }
        Ok(queue_result.accepted)
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

fn spawn_raw_spool_writer(config: &Config, signal: Signal) -> Result<RawSpoolWriter> {
    RawSpoolWriter::spawn(
        RawSpoolOptions {
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
