use anyhow::{anyhow, bail, Context, Result};
use chrono::{Duration as ChronoDuration, Utc};
use otlp2records::fixtures::{encode_logs, encode_metrics, encode_traces, FixtureProfile};
use serde::Serialize;
use serde_json::Value;
use std::borrow::Cow;
use std::collections::BTreeMap;
use std::env;
use std::fs;
use std::io::{ErrorKind, Read, Write};
use std::net::{SocketAddr, TcpStream, ToSocketAddrs};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

const BENCH_NAME: &str = "throughput_iteration";
const BENCH_VERSION: &str = "0.3.13";
const SCENARIO_NAME: &str = "throughput-iteration";
const USAGE: &str = "cargo bench --bench throughput_iteration -- [--base-url URL] [--warmup 2m] [--duration 20m] [--target-gb-day 100] [--profile ingest-only|mixed-query] [--signals all|logs|spans|metrics] [--ingest-concurrency 1] [--connection-mode close|persistent] [--query-pressure off|low|medium|high] [--query-interval 5s] [--query-concurrency 1] [--services 1] [--items-per-batch 256] [--log-records 8] [--log-body-bytes 120000] [--trace-spans 16] [--trace-attribute-bytes 48000] [--metric-series 40] [--metric-description-bytes 192] [--timestamp-mode fixed|advancing] [--freshness-sla 15s] [--progress-interval 30s] [--max-runtime 27m] [--no-queries] [--report-dir DIR] [--server-pid PID] [--resource-sample-interval 5s]";
const DETERMINISTIC_SEED: u64 = 0xCA4A_D57A_C5AC;
const DEFAULT_TARGET_GB_PER_DAY: f64 = 100.0;
const DEFAULT_BASE_URL: &str = "http://127.0.0.1:4318";
const DEFAULT_WARMUP: Duration = Duration::from_secs(120);
const DEFAULT_DURATION: Duration = Duration::from_secs(20 * 60);
const DEFAULT_QUERY_INTERVAL: Duration = Duration::from_secs(5);
const DEFAULT_QUERY_CONCURRENCY: usize = 1;
const DEFAULT_PROGRESS_INTERVAL: Duration = Duration::from_secs(30);
const DEFAULT_MAX_RUNTIME_GRACE: Duration = Duration::from_secs(5 * 60);
const NEAR_TIMEOUT_MS: f64 = 25_000.0;
const CLIENT_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
const PERSISTENT_IDLE_RECONNECT: Duration = Duration::from_secs(25);
const DEFAULT_SERVICE_COUNT: usize = 1;
const DEFAULT_LOG_RECORD_COUNT: usize = 8;
const DEFAULT_LOG_BODY_BYTES: usize = 120_000;
const DEFAULT_TRACE_SPAN_COUNT: usize = 16;
const DEFAULT_TRACE_ATTRIBUTE_BYTES: usize = 48_000;
const DEFAULT_METRIC_SERIES_COUNT: usize = 40;
const DEFAULT_METRIC_DESCRIPTION_BYTES: usize = 192;

fn main() {
    match run() {
        Ok((report, path)) => {
            print_summary(&report, &path);
            if !report.pass {
                std::process::exit(1);
            }
        }
        Err(err) => {
            eprintln!("throughput_iteration failed before completing a reportable run: {err:#}");
            std::process::exit(1);
        }
    }
}

fn run() -> Result<(Report, PathBuf)> {
    let args = Args::parse(env::args().skip(1))?;
    let api_key =
        env::var("CANARDSTACK_API_KEY").unwrap_or_else(|_| "dev-canardstack-key".to_string());
    let admin_key = env::var("CANARDSTACK_ADMIN_API_KEY")
        .unwrap_or_else(|_| "dev-canardstack-admin-key".to_string());
    let client = Client::new(&args.base_url, args.connection_mode)?;
    ensure_reachable(&client)?;
    let resource_envelope = ResourceEnvelope::detect();
    let storage_config = fetch_storage_config(&client, &admin_key);

    let run_started = Utc::now();
    let workload = Workload::new(
        run_started,
        args.workload.clone(),
        args.signals,
        args.timestamp_mode,
    );
    let target_bytes_per_sec = args.target_decoded_bytes_per_sec();
    let guard_deadline = Instant::now() + args.max_runtime();

    eprintln!(
        "throughput_iteration: warmup={} measured={} target={:.0} decoded B/s base_url={} profile={} ingest_concurrency={} connection_mode={} query_concurrency={} progress={} max_runtime={}",
        fmt_duration(args.warmup),
        fmt_duration(args.duration),
        target_bytes_per_sec,
        args.base_url,
        args.profile.as_str(),
        args.ingest_concurrency,
        args.connection_mode.as_str(),
        args.query_concurrency,
        fmt_duration(args.progress_interval),
        fmt_duration(args.max_runtime())
    );

    let mut query_plan = QueryPlan::new(run_started, args.signals);
    let _warmup = run_phase(
        &client,
        &api_key,
        &workload,
        &mut query_plan,
        PhaseConfig {
            phase: "warmup",
            duration: args.warmup,
            target_bytes_per_sec,
            ingest_concurrency: args.ingest_concurrency,
            query_interval: args.query_interval,
            query_concurrency: args.query_concurrency,
            no_queries: args.no_queries(),
            measured: false,
            signals: args.signals,
            progress_interval: args.progress_interval,
            guard_deadline,
        },
        None,
    );
    if _warmup.guard_exceeded {
        bail!(
            "benchmark max-runtime guard {} expired during warmup",
            fmt_duration(args.max_runtime())
        );
    }
    let measured_start = Instant::now();
    let mut resource_sampler = ResourceSampler::spawn(
        args.server_pid,
        args.resource_sample_interval,
        measured_start,
    );
    let mut metric_samples = Vec::new();
    metric_samples.push(MetricSample::capture(
        "start",
        measured_start.elapsed(),
        &client,
    ));
    let mut midpoint_sample = None;
    let measured = run_phase(
        &client,
        &api_key,
        &workload,
        &mut query_plan,
        PhaseConfig {
            phase: "measured",
            duration: args.duration,
            target_bytes_per_sec,
            ingest_concurrency: args.ingest_concurrency,
            query_interval: args.query_interval,
            query_concurrency: args.query_concurrency,
            no_queries: args.no_queries(),
            measured: true,
            signals: args.signals,
            progress_interval: args.progress_interval,
            guard_deadline,
        },
        Some(&mut midpoint_sample),
    );
    let resource_samples = resource_sampler.stop();
    if let Some(sample) = midpoint_sample {
        metric_samples.push(sample);
    }
    metric_samples.push(MetricSample::capture(
        "end",
        measured_start.elapsed(),
        &client,
    ));

    let _ = client.post_body(
        "/api/admin/maintenance/seal",
        Some(&admin_key),
        "application/json",
        b"{}",
    );
    let metrics = client.get("/metrics", None).ok();
    let scraped = metrics
        .as_ref()
        .filter(|response| response.status == 200)
        .map(|response| scrape_metrics(&response.body));
    metric_samples.push(MetricSample {
        label: "final".to_string(),
        seconds_from_measured_start: measured_start.elapsed().as_secs_f64(),
        metrics: scraped.clone(),
    });

    let report = build_report(BuildReportInput {
        args: args.clone(),
        workload,
        stats: measured,
        scraped,
        metric_samples,
        resource_samples,
        resource_envelope,
        storage_config,
    });
    let path = write_report(&report, args.report_dir.as_deref())?;
    Ok((report, path))
}

fn ensure_reachable(client: &Client) -> Result<()> {
    let response = client
        .get("/healthz", None)
        .context("canardstack is unreachable; start it or pass --base-url")?;
    if response.status != 200 {
        bail!(
            "canardstack health check returned HTTP {} with body {}",
            response.status,
            response.body
        );
    }
    Ok(())
}

fn run_phase(
    client: &Client,
    api_key: &str,
    workload: &Workload,
    query_plan: &mut QueryPlan,
    config: PhaseConfig,
    mut midpoint_sample: Option<&mut Option<MetricSample>>,
) -> RunStats {
    let mut stats = RunStats::default();
    if config.duration.is_zero() {
        return stats;
    }
    if config.ingest_concurrency > 1 {
        return run_phase_concurrent_ingest(client, api_key, workload, query_plan, config);
    }

    let started = Instant::now();
    let deadline = started + config.duration;
    let midpoint = started + config.duration / 2;
    let mut sent_by_signal: BTreeMap<&'static str, usize> = BTreeMap::new();
    let mut next_query = Instant::now() + config.query_interval.min(Duration::from_secs(1));
    let mut next_progress = started + config.progress_interval;

    while Instant::now() < deadline {
        let now = Instant::now();
        if now >= config.guard_deadline {
            stats.guard_exceeded = true;
            stats.errors.push(format!(
                "{} phase exceeded benchmark max-runtime guard",
                config.phase
            ));
            break;
        }
        if now >= next_progress {
            print_progress(
                config.phase,
                started,
                config.duration,
                &stats,
                client,
                config.signals,
            );
            while next_progress <= now {
                next_progress += config.progress_interval;
            }
        }
        if config.measured {
            if let Some(slot) = midpoint_sample.as_deref_mut() {
                if slot.is_none() && now >= midpoint {
                    *slot = Some(MetricSample::capture("mid", started.elapsed(), client));
                }
            }
        }
        if !config.no_queries && now >= next_query {
            run_query_pressure(
                client,
                api_key,
                query_plan,
                config.query_concurrency,
                &mut stats,
            );
            next_query += config.query_interval.max(Duration::from_millis(1));
            continue;
        }

        let elapsed = started.elapsed().as_secs_f64();
        let Some(payload_idx) =
            workload.next_payload_index(elapsed, config.target_bytes_per_sec, &sent_by_signal)
        else {
            let slept = sleep_until_next_tick(deadline);
            stats.record_pacing_wait(slept);
            continue;
        };
        let template = &workload.payloads[payload_idx];
        *sent_by_signal.entry(template.signal).or_default() += template.decoded_bytes;

        let payload = workload.prepare_payload(payload_idx);
        let outcome = send_ingest_payload(client, api_key, &payload, config.measured);
        stats.record_ingest_outcome(&payload, outcome, config.measured);
    }

    stats.elapsed = started.elapsed();
    print_progress(
        config.phase,
        started,
        config.duration,
        &stats,
        client,
        config.signals,
    );
    stats
}

fn run_phase_concurrent_ingest(
    client: &Client,
    api_key: &str,
    workload: &Workload,
    query_plan: &mut QueryPlan,
    config: PhaseConfig,
) -> RunStats {
    let stats = Arc::new(Mutex::new(RunStats::default()));
    let sent_by_signal = Arc::new(Mutex::new(BTreeMap::new()));
    let workload = Arc::new(workload.clone());
    let started = Instant::now();
    let deadline = started + config.duration;
    let mut handles = Vec::new();

    for _ in 0..config.ingest_concurrency {
        let worker = IngestWorker {
            client: client.clone(),
            api_key: api_key.to_string(),
            workload: workload.clone(),
            sent_by_signal: sent_by_signal.clone(),
            stats: stats.clone(),
            started,
            deadline,
            config,
        };
        handles.push(thread::spawn(move || ingest_worker(worker)));
    }

    let mut next_query = Instant::now() + config.query_interval.min(Duration::from_secs(1));
    let mut next_progress = started + config.progress_interval;
    while Instant::now() < deadline {
        let now = Instant::now();
        if now >= config.guard_deadline {
            let mut stats = stats.lock().expect("lock benchmark stats");
            stats.guard_exceeded = true;
            stats.errors.push(format!(
                "{} phase exceeded benchmark max-runtime guard",
                config.phase
            ));
            break;
        }
        if now >= next_progress {
            let stats_snapshot = stats.lock().expect("lock benchmark stats").clone();
            print_progress(
                config.phase,
                started,
                config.duration,
                &stats_snapshot,
                client,
                config.signals,
            );
            while next_progress <= now {
                next_progress += config.progress_interval;
            }
        }
        if !config.no_queries && now >= next_query {
            let mut query_stats = RunStats::default();
            run_query_pressure(
                client,
                api_key,
                query_plan,
                config.query_concurrency,
                &mut query_stats,
            );
            stats
                .lock()
                .expect("lock benchmark stats")
                .merge(query_stats);
            next_query += config.query_interval.max(Duration::from_millis(1));
            continue;
        }
        let _ = sleep_until_next_tick(deadline);
    }

    for handle in handles {
        if handle.join().is_err() {
            let mut stats = stats.lock().expect("lock benchmark stats");
            stats.transport_errors += 1;
            stats
                .errors
                .push("ingest worker thread panicked".to_string());
        }
    }

    let mut stats = Arc::try_unwrap(stats)
        .map_err(|_| ())
        .expect("benchmark stats still referenced")
        .into_inner()
        .expect("lock benchmark stats");
    stats.elapsed = started.elapsed();
    print_progress(
        config.phase,
        started,
        config.duration,
        &stats,
        client,
        config.signals,
    );
    stats
}

struct IngestWorker {
    client: Client,
    api_key: String,
    workload: Arc<Workload>,
    sent_by_signal: Arc<Mutex<BTreeMap<&'static str, usize>>>,
    stats: Arc<Mutex<RunStats>>,
    started: Instant,
    deadline: Instant,
    config: PhaseConfig,
}

fn ingest_worker(worker: IngestWorker) {
    while Instant::now() < worker.deadline {
        let now = Instant::now();
        if now >= worker.config.guard_deadline {
            return;
        }
        let elapsed = now.duration_since(worker.started).as_secs_f64();
        let payload_idx = {
            let mut sent_by_signal = worker
                .sent_by_signal
                .lock()
                .expect("lock sent bytes by signal");
            let Some(payload_idx) = worker.workload.next_payload_index(
                elapsed,
                worker.config.target_bytes_per_sec,
                &sent_by_signal,
            ) else {
                drop(sent_by_signal);
                let slept = sleep_until_next_tick(worker.deadline);
                worker
                    .stats
                    .lock()
                    .expect("lock benchmark stats")
                    .record_pacing_wait(slept);
                continue;
            };
            let payload = &worker.workload.payloads[payload_idx];
            *sent_by_signal.entry(payload.signal).or_default() += payload.decoded_bytes;
            payload_idx
        };
        let payload = worker.workload.prepare_payload(payload_idx);
        let outcome = send_ingest_payload(
            &worker.client,
            &worker.api_key,
            &payload,
            worker.config.measured,
        );
        worker
            .stats
            .lock()
            .expect("lock benchmark stats")
            .record_ingest_outcome(&payload, outcome, worker.config.measured);
    }
}

fn print_progress(
    phase: &str,
    started: Instant,
    duration: Duration,
    stats: &RunStats,
    client: &Client,
    signals: SignalSelection,
) {
    let elapsed = started.elapsed();
    let throughput = if elapsed.as_secs_f64() > 0.0 {
        stats.accepted_decoded_bytes as f64 / elapsed.as_secs_f64()
    } else {
        0.0
    };
    let metrics = MetricSample::capture("progress", elapsed, client).metrics;
    let queue = metrics
        .as_ref()
        .and_then(|metrics| metrics.queue.max_oldest_age_seconds)
        .map(|oldest| format!(" queue_oldest={oldest:.1}s"))
        .unwrap_or_default();
    let freshness = metrics
        .as_ref()
        .and_then(|metrics| max_freshness_lag_for_signals(&metrics.freshness_lag_seconds, signals))
        .map(|lag| format!(" freshness_lag={lag:.1}s"))
        .unwrap_or_default();
    eprintln!(
        "throughput_iteration progress phase={} elapsed={}/{} accepted={:.0}B/s status_counts={} queries={}/{} transport_errors={}{}{}",
        phase,
        fmt_duration(elapsed.min(duration)),
        fmt_duration(duration),
        throughput,
        fmt_status_counts(&stats.status_counts),
        stats.query_requests.saturating_sub(stats.query_failures),
        stats.query_requests,
        stats.transport_errors,
        queue,
        freshness
    );
}

fn run_query_pressure(
    client: &Client,
    api_key: &str,
    query_plan: &mut QueryPlan,
    concurrency: usize,
    stats: &mut RunStats,
) {
    let mut handles = Vec::new();
    for _ in 0..concurrency.max(1) {
        let client = client.clone();
        let api_key = api_key.to_string();
        let path = query_plan.next();
        handles.push(thread::spawn(move || run_query(client, api_key, path)));
    }
    for handle in handles {
        match handle.join() {
            Ok(outcome) => record_query_outcome(outcome, stats),
            Err(_) => {
                stats.query_requests += 1;
                stats.query_failures += 1;
                stats.transport_errors += 1;
                stats
                    .errors
                    .push("query worker thread panicked".to_string());
            }
        }
    }
}

fn run_query(client: Client, api_key: String, path: String) -> QueryOutcome {
    let started = Instant::now();
    match client.get(&path, Some(&api_key)) {
        Ok(response) => {
            let elapsed_ms = started.elapsed().as_secs_f64() * 1000.0;
            QueryOutcome {
                status: Some(response.status),
                elapsed_ms,
                error: (response.status != 200).then(|| {
                    format!(
                        "query returned HTTP {} path={} elapsed_ms={elapsed_ms:.1} body={}",
                        response.status,
                        path,
                        response.body.trim()
                    )
                }),
                transport_error: false,
            }
        }
        Err(err) => {
            thread::sleep(Duration::from_millis(100));
            let elapsed_ms = started.elapsed().as_secs_f64() * 1000.0;
            QueryOutcome {
                status: None,
                elapsed_ms,
                error: Some(format!(
                    "query transport error path={path} elapsed_ms={elapsed_ms:.1}: {}",
                    format_error_chain(&err)
                )),
                transport_error: true,
            }
        }
    }
}

fn record_query_outcome(outcome: QueryOutcome, stats: &mut RunStats) {
    if let Some(status) = outcome.status {
        stats.status_counts_inc(status);
    }
    stats.query_requests += 1;
    stats.query_latency_ms.push(outcome.elapsed_ms);
    if let Some(error) = outcome.error {
        stats.query_failures += 1;
        stats.errors.push(error);
    }
    if outcome.transport_error {
        stats.transport_errors += 1;
    }
}

fn send_ingest_payload(
    client: &Client,
    api_key: &str,
    payload: &PreparedPayload<'_>,
    measured: bool,
) -> IngestOutcome {
    let started_request = Instant::now();
    match client.post_body(
        payload.path,
        Some(api_key),
        payload.content_type,
        payload.body.as_ref(),
    ) {
        Ok(response) => IngestOutcome {
            status: Some(response.status),
            elapsed_ms: started_request.elapsed().as_secs_f64() * 1000.0,
            records: (response.status == 202).then(|| {
                serde_json::from_str::<Value>(&response.body)
                    .ok()
                    .and_then(|body| body.get("records").and_then(Value::as_u64))
                    .unwrap_or(payload.records_per_request)
            }),
            error: None,
            transport_error: false,
        },
        Err(err) => {
            thread::sleep(Duration::from_millis(100));
            let elapsed_ms = started_request.elapsed().as_secs_f64() * 1000.0;
            let detail = format!(
                "ingest transport error signal={} path={} decoded_bytes={} request_bytes={} elapsed_ms={elapsed_ms:.1}: {}",
                payload.signal,
                payload.path,
                payload.decoded_bytes,
                payload.body.len(),
                format_error_chain(&err)
            );
            eprintln!("throughput_iteration {detail}");
            IngestOutcome {
                status: None,
                elapsed_ms,
                records: None,
                error: measured.then_some(detail),
                transport_error: true,
            }
        }
    }
}

struct IngestOutcome {
    status: Option<u16>,
    elapsed_ms: f64,
    records: Option<u64>,
    error: Option<String>,
    transport_error: bool,
}

struct QueryOutcome {
    status: Option<u16>,
    elapsed_ms: f64,
    error: Option<String>,
    transport_error: bool,
}

fn format_error_chain(err: &anyhow::Error) -> String {
    err.chain()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(": ")
}

fn sleep_until_next_tick(deadline: Instant) -> Duration {
    let remaining = deadline.saturating_duration_since(Instant::now());
    if remaining.is_zero() {
        return Duration::ZERO;
    }
    let sleep_for = remaining.min(Duration::from_millis(10));
    thread::sleep(sleep_for);
    sleep_for
}

#[derive(Clone, Copy)]
struct PhaseConfig {
    phase: &'static str,
    duration: Duration,
    target_bytes_per_sec: f64,
    ingest_concurrency: usize,
    query_interval: Duration,
    query_concurrency: usize,
    no_queries: bool,
    measured: bool,
    signals: SignalSelection,
    progress_interval: Duration,
    guard_deadline: Instant,
}

struct BuildReportInput {
    args: Args,
    workload: Workload,
    stats: RunStats,
    scraped: Option<ScrapedMetrics>,
    metric_samples: Vec<MetricSample>,
    resource_samples: Vec<ResourceSample>,
    resource_envelope: ResourceEnvelope,
    storage_config: Option<Value>,
}

fn build_report(input: BuildReportInput) -> Report {
    let BuildReportInput {
        args,
        workload,
        stats,
        scraped,
        metric_samples,
        resource_samples,
        resource_envelope,
        storage_config,
    } = input;
    let measured_seconds = stats.elapsed.as_secs_f64().max(0.001);
    let target_decoded_bytes_per_sec = args.target_decoded_bytes_per_sec();
    let actual_decoded_bytes_per_sec = stats.accepted_decoded_bytes as f64 / measured_seconds;
    let request_bytes_per_sec = stats.request_bytes_sent as f64 / measured_seconds;
    let generator = GeneratorReport {
        ingest_concurrency: args.ingest_concurrency,
        pacing_wait_count: stats.pacing_wait_count,
        pacing_wait_seconds: stats.pacing_wait_seconds,
        pacing_wait_fraction_of_worker_time: (args.ingest_concurrency > 0).then_some(
            stats.pacing_wait_seconds / (measured_seconds * args.ingest_concurrency as f64),
        ),
        target_utilization: Some(actual_decoded_bytes_per_sec / target_decoded_bytes_per_sec),
        likely_generator_or_schedule_limited: actual_decoded_bytes_per_sec
            < target_decoded_bytes_per_sec * 0.98
            && stats.pacing_wait_seconds > measured_seconds * 0.05,
    };
    let accepted_records_per_sec_by_signal = stats
        .accepted_records_by_signal
        .iter()
        .map(|(signal, records)| (signal.clone(), *records as f64 / measured_seconds))
        .collect();

    let mut failure_reasons = Vec::new();
    let mut smell_observations = Vec::new();
    if stats.status_counts.get(&503).copied().unwrap_or(0) > 0 {
        failure_reasons.push("HTTP 503 observed".to_string());
        smell_observations.push("backpressure or dependency failure returned HTTP 503".to_string());
    }
    if stats.status_counts.get(&429).copied().unwrap_or(0) > 0 {
        failure_reasons.push("HTTP 429 observed".to_string());
        smell_observations
            .push("admission control rejected load before the target was sustained".to_string());
    }
    if actual_decoded_bytes_per_sec < target_decoded_bytes_per_sec * 0.90 {
        failure_reasons.push(format!(
            "accepted decoded throughput {:.0} B/s below 90% of target {:.0} B/s",
            actual_decoded_bytes_per_sec, target_decoded_bytes_per_sec
        ));
        smell_observations.push("throughput collapsed below the modest v0 target".to_string());
    }
    if args.no_queries() {
        smell_observations.push("query interference gate disabled by --no-queries".to_string());
    } else if stats.query_requests == 0 {
        failure_reasons.push("no query requests completed".to_string());
    } else if stats.query_failures > 0 {
        failure_reasons.push(format!(
            "{} of {} query requests failed",
            stats.query_failures, stats.query_requests
        ));
        smell_observations.push("queries failed while ingest was running".to_string());
    }
    if stats.transport_errors > 0 {
        failure_reasons.push(format!(
            "{} transport errors observed",
            stats.transport_errors
        ));
    }
    if generator.likely_generator_or_schedule_limited {
        smell_observations.push(
            "generator pacing suggests the benchmark schedule may be limiting throughput"
                .to_string(),
        );
    }
    if args.server_pid.is_none() {
        smell_observations.push(
            "server process resource sampling unavailable; pass --server-pid or set CANARDSTACK_BENCHMARK_SERVER_PID".to_string(),
        );
    }
    if !resource_samples
        .iter()
        .any(|sample| sample.process == "server" && sample.available)
    {
        smell_observations.push("server CPU/RSS samples unavailable".to_string());
    }
    if stats.guard_exceeded {
        failure_reasons.push("benchmark max-runtime guard expired".to_string());
        smell_observations.push("benchmark stopped by wall-clock guard".to_string());
    }
    for err in stats.errors.iter().take(5) {
        failure_reasons.push(err.clone());
    }

    let ingest_latency_ms = LatencySummary::from_samples(stats.ingest_latency_ms);
    let query_latency_ms = LatencySummary::from_samples(stats.query_latency_ms);

    if ingest_latency_ms
        .p99
        .is_some_and(|p99| p99 >= NEAR_TIMEOUT_MS)
    {
        failure_reasons.push(format!(
            "ingest p99 latency {}ms is near the 30s client timeout",
            fmt_optional(ingest_latency_ms.p99)
        ));
        smell_observations.push("unstable ingest tail latency near timeout territory".to_string());
    }
    if !args.no_queries()
        && query_latency_ms
            .p99
            .is_some_and(|p99| p99 >= NEAR_TIMEOUT_MS)
    {
        failure_reasons.push(format!(
            "query p99 latency {}ms is near the 30s client timeout",
            fmt_optional(query_latency_ms.p99)
        ));
        smell_observations.push("query tail latency approached timeout territory".to_string());
    }

    let trend_samples = metric_samples
        .iter()
        .filter(|sample| sample.label != "final")
        .cloned()
        .collect::<Vec<_>>();
    let queue_oldest_age_trend = trend_from_samples(&trend_samples, |metrics| {
        metrics.queue.max_oldest_age_seconds
    });
    let freshness_lag_trend = trend_from_samples(&trend_samples, |metrics| {
        max_freshness_lag_for_signals(&metrics.freshness_lag_seconds, args.signals)
    });
    let max_measured_freshness_lag_seconds =
        max_freshness_lag_from_samples(&trend_samples, args.signals);
    let queue_rows_trend = trend_from_samples(&trend_samples, |metrics| metrics.queue.max_rows);
    let queue_bytes_trend = trend_from_samples(&trend_samples, |metrics| metrics.queue.max_bytes);

    if queue_oldest_age_trend.clearly_increasing {
        failure_reasons.push("queue oldest age increased across the measured window".to_string());
        smell_observations.push("queue age grew instead of draining under modest load".to_string());
    }
    if freshness_lag_trend.clearly_increasing {
        failure_reasons.push("freshness lag increased across the measured window".to_string());
        smell_observations.push("ingest-to-query freshness lag grew without recovery".to_string());
    }
    if let Some(sla) = args.freshness_sla {
        match max_measured_freshness_lag_seconds {
            Some(max_lag) if max_lag > sla.as_secs_f64() => {
                failure_reasons.push(format!(
                    "query-visible freshness lag {:.3}s exceeded SLA {:.3}s",
                    max_lag,
                    sla.as_secs_f64()
                ));
            }
            Some(_) => {}
            None => smell_observations.push(
                "freshness SLA configured but freshness samples were unavailable".to_string(),
            ),
        }
    }

    let metric_snapshots = metric_samples
        .iter()
        .map(MetricSnapshotReport::from_sample)
        .collect::<Vec<_>>();
    let stage_throughput = StageThroughputReport::from_samples(&metric_samples);
    if !stage_throughput.available {
        smell_observations.push(
            "measured-window stage throughput deltas unavailable from /metrics samples".to_string(),
        );
    } else {
        let buffered_rows = stage_throughput
            .totals
            .get("buffered_rows")
            .copied()
            .unwrap_or(0.0);
        let visible_rows = stage_throughput
            .totals
            .get("storage_visible_rows")
            .copied()
            .unwrap_or(0.0);
        if buffered_rows > 0.0 && visible_rows < buffered_rows * 0.80 {
            smell_observations.push(format!(
                "storage-visible rows advanced only {:.0}/{:.0} measured-window buffered rows",
                visible_rows, buffered_rows
            ));
        }
    }

    let accepted_mib = stats.accepted_decoded_bytes as f64 / (1024.0 * 1024.0);
    let (
        freshness_lag_seconds,
        queue,
        storage,
        mut server_phase_timing,
        transform_counters,
        ingest_buffer_counters,
        ingest_buffer_gauges,
        ducklake_maintenance_timing,
    ) = match scraped {
        Some(scraped) => (
            scraped.freshness_lag_seconds,
            Some(scraped.queue),
            Some(scraped.storage),
            scraped.server_phase_timing,
            scraped.transform_counters,
            scraped.ingest_buffer_counters,
            scraped.ingest_buffer_gauges,
            scraped.ducklake_maintenance_timing,
        ),
        None => (
            BTreeMap::new(),
            None,
            None,
            BTreeMap::new(),
            BTreeMap::new(),
            BTreeMap::new(),
            BTreeMap::new(),
            BTreeMap::new(),
        ),
    };

    for phase in server_phase_timing.values_mut() {
        phase.seconds_per_mib = (accepted_mib > 0.0).then_some(phase.sum_seconds / accepted_mib);
        phase.wall_time_share = (stats.elapsed.as_secs_f64() > 0.0)
            .then_some(phase.sum_seconds / stats.elapsed.as_secs_f64());
    }
    for (phase, timing) in top_phase_timings(&server_phase_timing).into_iter().take(3) {
        if timing.wall_time_share.unwrap_or(0.0) >= 0.50 {
            smell_observations.push(format!(
                "phase timing dominance: {phase} consumed {:.0}% of measured wall time",
                timing.wall_time_share.unwrap_or(0.0) * 100.0
            ));
        }
    }

    if !queue_oldest_age_trend.available {
        smell_observations
            .push("queue oldest-age trend unavailable from server metrics".to_string());
    }
    if !freshness_lag_trend.available {
        smell_observations.push("freshness lag trend unavailable from server metrics".to_string());
    }

    let mut pass_fail_criteria = vec![
        PassFailCriterionReport::new(
            "throughput_at_least_90_percent_target",
            actual_decoded_bytes_per_sec >= target_decoded_bytes_per_sec * 0.90,
            format!(
                "actual={:.0}B/s target={:.0}B/s",
                actual_decoded_bytes_per_sec, target_decoded_bytes_per_sec
            ),
        ),
        PassFailCriterionReport::new(
            "no_503_storage_or_dependency_errors",
            stats.status_counts.get(&503).copied().unwrap_or(0) == 0,
            format!(
                "http_503={}",
                stats.status_counts.get(&503).copied().unwrap_or(0)
            ),
        ),
        PassFailCriterionReport::new(
            "no_429_admission_rejections",
            stats.status_counts.get(&429).copied().unwrap_or(0) == 0,
            format!(
                "http_429={}",
                stats.status_counts.get(&429).copied().unwrap_or(0)
            ),
        ),
        PassFailCriterionReport::new(
            "queue_oldest_age_not_clearly_increasing",
            !queue_oldest_age_trend.clearly_increasing,
            format!(
                "available={} increasing={}",
                queue_oldest_age_trend.available, queue_oldest_age_trend.clearly_increasing
            ),
        ),
        PassFailCriterionReport::new(
            "freshness_lag_not_clearly_increasing",
            !freshness_lag_trend.clearly_increasing,
            format!(
                "available={} increasing={}",
                freshness_lag_trend.available, freshness_lag_trend.clearly_increasing
            ),
        ),
        PassFailCriterionReport::new(
            "query_interference_within_limits",
            args.no_queries() || (stats.query_requests > 0 && stats.query_failures == 0),
            if args.no_queries() {
                "disabled".to_string()
            } else {
                format!(
                    "query_requests={} query_failures={}",
                    stats.query_requests, stats.query_failures
                )
            },
        ),
        PassFailCriterionReport::new(
            "tail_latency_not_near_client_timeout",
            ingest_latency_ms
                .p99
                .is_none_or(|p99| p99 < NEAR_TIMEOUT_MS)
                && (args.no_queries()
                    || query_latency_ms.p99.is_none_or(|p99| p99 < NEAR_TIMEOUT_MS)),
            format!(
                "ingest_p99_ms={} query_p99_ms={}",
                fmt_optional(ingest_latency_ms.p99),
                fmt_optional(query_latency_ms.p99)
            ),
        ),
    ];
    if let Some(sla) = args.freshness_sla {
        pass_fail_criteria.push(PassFailCriterionReport::new(
            "query_visible_freshness_within_sla",
            max_measured_freshness_lag_seconds.is_some_and(|max_lag| max_lag <= sla.as_secs_f64()),
            format!(
                "max_measured_lag_seconds={} sla_seconds={:.3}",
                fmt_optional(max_measured_freshness_lag_seconds),
                sla.as_secs_f64()
            ),
        ));
    }

    let pass = failure_reasons.is_empty();
    let loki_progressive_query =
        LokiProgressiveQueryReport::from_samples(&metric_samples, &server_phase_timing);

    Report {
        git_sha: git_sha(),
        benchmark_name: BENCH_NAME.to_string(),
        benchmark_version: BENCH_VERSION.to_string(),
        base_url: args.base_url.clone(),
        resource_envelope,
        storage_config,
        generator,
        workload_profile: args.workload.clone(),
        query_profile: QueryProfileReport {
            profile: args.profile.as_str().to_string(),
            pressure: args.query_pressure.as_str().to_string(),
            enabled: !args.no_queries(),
            interval_seconds: args.query_interval.as_secs_f64(),
            concurrency: args.query_concurrency,
        },
        scenario: ScenarioReport {
            name: SCENARIO_NAME.to_string(),
            deterministic_seed: DETERMINISTIC_SEED,
            target_gb_per_day: args.target_gb_per_day,
            signals: args.signals.as_str().to_string(),
            timestamp_mode: args.timestamp_mode.as_str().to_string(),
            connection_mode: args.connection_mode.as_str().to_string(),
            byte_mix: workload
                .payloads
                .iter()
                .map(|payload| (payload.signal.to_string(), payload.ratio))
                .collect(),
            payloads: workload
                .payloads
                .iter()
                .map(|payload| PayloadReport {
                    signal: payload.signal.to_string(),
                    decoded_bytes: payload.decoded_bytes as u64,
                    records_per_request: payload.records_per_request,
                })
                .collect(),
        },
        warmup_duration_seconds: args.warmup.as_secs_f64(),
        measured_duration_seconds: stats.elapsed.as_secs_f64(),
        target_decoded_bytes_per_sec,
        actual_decoded_bytes_per_sec,
        request_bytes_per_sec,
        accepted_records_per_sec_by_signal,
        http_status_counts: stats
            .status_counts
            .iter()
            .map(|(status, count)| (status.to_string(), *count))
            .collect(),
        ingest_latency_ms,
        query_latency_ms,
        freshness_lag_seconds,
        freshness_sla_seconds: args.freshness_sla.map(|sla| sla.as_secs_f64()),
        max_measured_freshness_lag_seconds,
        queue,
        storage,
        server_phase_timing,
        transform_counters,
        ingest_buffer_counters,
        ingest_buffer_gauges,
        ducklake_maintenance_timing,
        resource_samples,
        metric_snapshots,
        stage_throughput,
        loki_progressive_query,
        queue_oldest_age_trend,
        queue_rows_trend,
        queue_bytes_trend,
        freshness_lag_trend,
        pass_fail_criteria,
        smell_observations,
        pass,
        failure_reasons,
    }
}

fn write_report(report: &Report, report_dir: Option<&Path>) -> Result<PathBuf> {
    let stamp = Utc::now().format("%Y%m%dT%H%M%SZ").to_string();
    let dir = report_dir
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            PathBuf::from("target")
                .join("canardstack-bench")
                .join(BENCH_NAME)
        })
        .join(stamp);
    fs::create_dir_all(&dir)?;
    let path = dir.join("report.json");
    fs::write(&path, serde_json::to_vec_pretty(report)?)?;
    Ok(path)
}

fn print_summary(report: &Report, path: &Path) {
    println!(
        "throughput_iteration scenario={} pass={} actual={:.0}B/s target={:.0}B/s profile={} query_concurrency={} timestamp_mode={}",
        report.scenario.name,
        report.pass,
        report.actual_decoded_bytes_per_sec,
        report.target_decoded_bytes_per_sec,
        report.query_profile.profile,
        report.query_profile.concurrency,
        report.scenario.timestamp_mode
    );
    println!(
        "workload services={} log_records={} log_body_bytes={} trace_spans={} trace_attribute_bytes={} metric_series={} metric_description_bytes={}",
        report.workload_profile.service_count,
        report.workload_profile.log_record_count,
        report.workload_profile.log_body_bytes,
        report.workload_profile.trace_span_count,
        report.workload_profile.trace_attribute_bytes,
        report.workload_profile.metric_series_count,
        report.workload_profile.metric_description_bytes
    );
    println!(
        "resource_envelope cpu_limit={} memory_limit={} note={}",
        report
            .resource_envelope
            .configured_cpu_limit
            .as_deref()
            .unwrap_or("n/a"),
        report
            .resource_envelope
            .configured_memory_limit
            .as_deref()
            .unwrap_or("n/a"),
        report
            .resource_envelope
            .configured_note
            .as_deref()
            .unwrap_or("n/a")
    );
    println!("status_counts={:?}", report.http_status_counts);
    println!(
        "generator pacing_wait_seconds={:.3} pacing_wait_fraction={} target_utilization={} schedule_limited={}",
        report.generator.pacing_wait_seconds,
        report
            .generator
            .pacing_wait_fraction_of_worker_time
            .map(|value| format!("{value:.3}"))
            .unwrap_or_else(|| "n/a".to_string()),
        report
            .generator
            .target_utilization
            .map(|value| format!("{value:.3}"))
            .unwrap_or_else(|| "n/a".to_string()),
        report.generator.likely_generator_or_schedule_limited
    );
    for (process, sample) in peak_resource_samples(&report.resource_samples) {
        println!(
            "resource_peak process={} cpu_percent={} memory_percent={} rss_mib={}",
            process,
            sample
                .cpu_percent
                .map(|value| format!("{value:.1}"))
                .unwrap_or_else(|| "n/a".to_string()),
            sample
                .memory_percent
                .map(|value| format!("{value:.1}"))
                .unwrap_or_else(|| "n/a".to_string()),
            sample
                .rss_kib
                .map(|value| format!("{:.1}", value as f64 / 1024.0))
                .unwrap_or_else(|| "n/a".to_string())
        );
    }
    println!(
        "ingest_latency_ms p50={} p95={} p99={} count={}",
        fmt_optional(report.ingest_latency_ms.p50),
        fmt_optional(report.ingest_latency_ms.p95),
        fmt_optional(report.ingest_latency_ms.p99),
        report.ingest_latency_ms.count
    );
    println!(
        "query_latency_ms p50={} p95={} p99={} count={}",
        fmt_optional(report.query_latency_ms.p50),
        fmt_optional(report.query_latency_ms.p95),
        fmt_optional(report.query_latency_ms.p99),
        report.query_latency_ms.count
    );
    let top_phases = top_phase_timings(&report.server_phase_timing)
        .into_iter()
        .take(5)
        .map(|(phase, timing)| {
            format!(
                "{} sum={:.3}s share={}",
                phase,
                timing.sum_seconds,
                timing
                    .wall_time_share
                    .map(|share| format!("{:.0}%", share * 100.0))
                    .unwrap_or_else(|| "n/a".to_string())
            )
        })
        .collect::<Vec<_>>();
    println!("phase_top={}", top_phases.join("; "));
    if let Some(storage) = &report.storage {
        if !storage.ducklake_active_data_files.is_empty() {
            println!(
                "ducklake_layout active_data_files={:?} active_data_file_rows={:?}",
                storage.ducklake_active_data_files, storage.ducklake_active_data_file_rows
            );
        }
    }
    if report.stage_throughput.available {
        println!(
            "stage_rates_per_sec accepted_decoded_bytes={:.0} transformed_rows={:.0} buffered_rows={:.0} storage_visible_rows={:.0}",
            report
                .stage_throughput
                .totals_per_second
                .get("accepted_decoded_bytes")
                .copied()
                .unwrap_or(0.0),
            report
                .stage_throughput
                .totals_per_second
                .get("transformed_rows")
                .copied()
                .unwrap_or(0.0),
            report
                .stage_throughput
                .totals_per_second
                .get("buffered_rows")
                .copied()
                .unwrap_or(0.0),
            report
                .stage_throughput
                .totals_per_second
                .get("storage_visible_rows")
                .copied()
                .unwrap_or(0.0)
        );
    }
    if report.loki_progressive_query.available {
        println!(
            "loki_progressive_query ok_delta={} batches_scanned={} files_scanned={} candidate_files={} rows_scanned={} result_rows={} scanned_file_fraction={} plan_avg_ms={} candidate_execute_avg_ms={} total_avg_ms={}",
            report
                .loki_progressive_query
                .requests_ok_delta
                .unwrap_or(0.0),
            fmt_optional(report.loki_progressive_query.final_batches_scanned),
            fmt_optional(report.loki_progressive_query.final_files_scanned),
            fmt_optional(report.loki_progressive_query.final_candidate_files),
            fmt_optional(report.loki_progressive_query.final_rows_scanned),
            fmt_optional(report.loki_progressive_query.final_result_rows),
            fmt_optional(
                report
                    .loki_progressive_query
                    .scanned_file_fraction_of_candidates
            ),
            fmt_optional(
                report
                    .loki_progressive_query
                    .planner_timing
                    .avg_seconds
                    .map(|seconds| seconds * 1000.0)
            ),
            fmt_optional(
                report
                    .loki_progressive_query
                    .candidate_execute_timing
                    .avg_seconds
                    .map(|seconds| seconds * 1000.0)
            ),
            fmt_optional(
                report
                    .loki_progressive_query
                    .total_timing
                    .avg_seconds
                    .map(|seconds| seconds * 1000.0)
            )
        );
    }
    println!("report={}", path.display());
    if !report.failure_reasons.is_empty() {
        println!("failure_reasons={}", report.failure_reasons.join("; "));
    }
    if !report.smell_observations.is_empty() {
        println!(
            "smell_observations={}",
            report.smell_observations.join("; ")
        );
    }
}

#[derive(Clone)]
struct Args {
    base_url: String,
    warmup: Duration,
    duration: Duration,
    target_gb_per_day: f64,
    ingest_concurrency: usize,
    connection_mode: ConnectionMode,
    query_interval: Duration,
    query_concurrency: usize,
    query_pressure: QueryPressure,
    profile: BenchmarkProfile,
    signals: SignalSelection,
    workload: WorkloadProfile,
    timestamp_mode: TimestampMode,
    progress_interval: Duration,
    freshness_sla: Option<Duration>,
    max_runtime: Option<Duration>,
    no_queries_legacy: bool,
    report_dir: Option<PathBuf>,
    server_pid: Option<u32>,
    resource_sample_interval: Duration,
}

impl Args {
    fn parse(args: impl Iterator<Item = String>) -> Result<Self> {
        let mut parsed = Self {
            base_url: DEFAULT_BASE_URL.to_string(),
            warmup: DEFAULT_WARMUP,
            duration: DEFAULT_DURATION,
            target_gb_per_day: DEFAULT_TARGET_GB_PER_DAY,
            ingest_concurrency: 1,
            connection_mode: ConnectionMode::Close,
            query_interval: DEFAULT_QUERY_INTERVAL,
            query_concurrency: DEFAULT_QUERY_CONCURRENCY,
            query_pressure: QueryPressure::Medium,
            profile: BenchmarkProfile::MixedQuery,
            signals: SignalSelection::All,
            workload: WorkloadProfile::default(),
            timestamp_mode: TimestampMode::Fixed,
            progress_interval: DEFAULT_PROGRESS_INTERVAL,
            freshness_sla: None,
            max_runtime: None,
            no_queries_legacy: false,
            report_dir: None,
            server_pid: env::var("CANARDSTACK_BENCHMARK_SERVER_PID")
                .ok()
                .and_then(|pid| pid.parse().ok()),
            resource_sample_interval: Duration::from_secs(5),
        };
        let mut explicit_query_interval = false;
        let mut explicit_query_concurrency = false;
        let mut args = args.peekable();
        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--base-url" => {
                    parsed.base_url = next_arg(&mut args, "--base-url")?;
                }
                "--warmup" => {
                    parsed.warmup = parse_duration(&next_arg(&mut args, "--warmup")?)?;
                }
                "--duration" => {
                    parsed.duration = parse_duration(&next_arg(&mut args, "--duration")?)?;
                }
                "--target-gb-day" => {
                    parsed.target_gb_per_day = parse_next(&mut args, "--target-gb-day")?;
                }
                "--query-interval" => {
                    parsed.query_interval =
                        parse_duration(&next_arg(&mut args, "--query-interval")?)?;
                    explicit_query_interval = true;
                }
                "--ingest-concurrency" => {
                    parsed.ingest_concurrency = parse_next(&mut args, "--ingest-concurrency")?;
                }
                "--connection-mode" => {
                    parsed.connection_mode =
                        ConnectionMode::parse(&next_arg(&mut args, "--connection-mode")?)?;
                }
                "--query-concurrency" => {
                    parsed.query_concurrency = parse_next(&mut args, "--query-concurrency")?;
                    explicit_query_concurrency = true;
                }
                "--query-pressure" => {
                    parsed.query_pressure =
                        QueryPressure::parse(&next_arg(&mut args, "--query-pressure")?)?;
                }
                "--profile" => {
                    parsed.profile = BenchmarkProfile::parse(&next_arg(&mut args, "--profile")?)?;
                }
                "--signals" | "--signal" => {
                    parsed.signals = SignalSelection::parse(&next_arg(&mut args, "--signals")?)?;
                }
                "--services" => {
                    parsed.workload.service_count = parse_next(&mut args, "--services")?;
                }
                "--log-body-bytes" => {
                    parsed.workload.log_body_bytes = parse_next(&mut args, "--log-body-bytes")?;
                }
                "--log-records" => {
                    parsed.workload.log_record_count = parse_next(&mut args, "--log-records")?;
                }
                "--items-per-batch" => {
                    let items_per_batch: usize = parse_next(&mut args, "--items-per-batch")?;
                    if items_per_batch == 0 {
                        bail!("--items-per-batch must be > 0");
                    }
                    parsed.workload.log_record_count = items_per_batch;
                    parsed.workload.trace_span_count = items_per_batch;
                    parsed.workload.metric_series_count = items_per_batch.div_ceil(2);
                }
                "--trace-spans" => {
                    parsed.workload.trace_span_count = parse_next(&mut args, "--trace-spans")?;
                }
                "--trace-attribute-bytes" => {
                    parsed.workload.trace_attribute_bytes =
                        parse_next(&mut args, "--trace-attribute-bytes")?;
                }
                "--metric-series" => {
                    parsed.workload.metric_series_count = parse_next(&mut args, "--metric-series")?;
                }
                "--metric-description-bytes" => {
                    parsed.workload.metric_description_bytes =
                        parse_next(&mut args, "--metric-description-bytes")?;
                }
                "--timestamp-mode" => {
                    parsed.timestamp_mode =
                        TimestampMode::parse(&next_arg(&mut args, "--timestamp-mode")?)?;
                }
                "--progress-interval" => {
                    parsed.progress_interval =
                        parse_duration(&next_arg(&mut args, "--progress-interval")?)?;
                }
                "--freshness-sla" => {
                    parsed.freshness_sla =
                        Some(parse_duration(&next_arg(&mut args, "--freshness-sla")?)?);
                }
                "--max-runtime" => {
                    parsed.max_runtime =
                        Some(parse_duration(&next_arg(&mut args, "--max-runtime")?)?);
                }
                "--no-queries" => {
                    parsed.no_queries_legacy = true;
                    parsed.profile = BenchmarkProfile::IngestOnly;
                }
                "--report-dir" => {
                    parsed.report_dir = Some(PathBuf::from(next_arg(&mut args, "--report-dir")?));
                }
                "--server-pid" => {
                    parsed.server_pid = Some(parse_next(&mut args, "--server-pid")?);
                }
                "--resource-sample-interval" => {
                    parsed.resource_sample_interval =
                        parse_duration(&next_arg(&mut args, "--resource-sample-interval")?)?;
                }
                "--bench" => {}
                "--help" | "-h" => {
                    println!("{USAGE}");
                    std::process::exit(0);
                }
                _ => bail!("unknown argument {arg}; use --help"),
            }
        }
        if parsed.warmup + parsed.duration == Duration::ZERO {
            bail!("warmup and duration cannot both be zero");
        }
        if parsed.target_gb_per_day <= 0.0 {
            bail!("--target-gb-day must be positive");
        }
        if parsed.ingest_concurrency == 0 {
            bail!("--ingest-concurrency must be > 0");
        }
        if !explicit_query_interval {
            parsed.query_interval = parsed.query_pressure.default_interval();
        }
        if !explicit_query_concurrency {
            parsed.query_concurrency = parsed.query_pressure.default_concurrency();
        }
        if parsed.query_interval.is_zero() {
            bail!("--query-interval must be positive");
        }
        if parsed.query_concurrency == 0 {
            bail!("--query-concurrency must be > 0");
        }
        parsed.workload.validate()?;
        if parsed.progress_interval.is_zero() {
            bail!("--progress-interval must be positive");
        }
        if parsed.resource_sample_interval.is_zero() {
            bail!("--resource-sample-interval must be positive");
        }
        if parsed.freshness_sla.is_some_and(|sla| sla.is_zero()) {
            bail!("--freshness-sla must be positive");
        }
        if parsed
            .max_runtime
            .is_some_and(|max_runtime| max_runtime < parsed.warmup + parsed.duration)
        {
            bail!("--max-runtime must be >= warmup + duration");
        }
        Ok(parsed)
    }

    fn target_decoded_bytes_per_sec(&self) -> f64 {
        self.target_gb_per_day * 1_000_000_000.0 / 86_400.0
    }

    fn max_runtime(&self) -> Duration {
        self.max_runtime
            .unwrap_or(self.warmup + self.duration + DEFAULT_MAX_RUNTIME_GRACE)
    }

    fn no_queries(&self) -> bool {
        self.no_queries_legacy
            || matches!(self.profile, BenchmarkProfile::IngestOnly)
            || matches!(self.query_pressure, QueryPressure::Off)
    }
}

#[derive(Clone, Copy)]
enum ConnectionMode {
    Close,
    Persistent,
}

impl ConnectionMode {
    fn parse(raw: &str) -> Result<Self> {
        match raw {
            "close" => Ok(Self::Close),
            "persistent" | "keep-alive" | "keepalive" => Ok(Self::Persistent),
            _ => bail!("--connection-mode must be close or persistent"),
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Close => "close",
            Self::Persistent => "persistent",
        }
    }
}

#[derive(Clone, Copy)]
enum SignalSelection {
    All,
    Logs,
    Spans,
    Metrics,
}

impl SignalSelection {
    fn parse(raw: &str) -> Result<Self> {
        match raw {
            "all" => Ok(Self::All),
            "logs" | "log" => Ok(Self::Logs),
            "spans" | "traces" | "trace" => Ok(Self::Spans),
            "metrics" | "metric" => Ok(Self::Metrics),
            _ => bail!("--signals must be all, logs, spans, or metrics"),
        }
    }

    fn includes(self, signal: &str) -> bool {
        match self {
            Self::All => true,
            Self::Logs => signal == "logs",
            Self::Spans => signal == "spans",
            Self::Metrics => signal == "metrics",
        }
    }

    fn includes_table(self, table: &str) -> bool {
        match self {
            Self::All => true,
            Self::Logs => table == "logs",
            Self::Spans => table == "spans",
            Self::Metrics => table == "metric_gauge" || table == "metric_sum",
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::All => "all",
            Self::Logs => "logs",
            Self::Spans => "spans",
            Self::Metrics => "metrics",
        }
    }
}

#[derive(Clone, Copy)]
enum BenchmarkProfile {
    IngestOnly,
    MixedQuery,
}

impl BenchmarkProfile {
    fn parse(raw: &str) -> Result<Self> {
        match raw {
            "ingest-only" => Ok(Self::IngestOnly),
            "mixed-query" => Ok(Self::MixedQuery),
            _ => bail!("--profile must be ingest-only or mixed-query"),
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::IngestOnly => "ingest-only",
            Self::MixedQuery => "mixed-query",
        }
    }
}

#[derive(Clone, Copy)]
enum QueryPressure {
    Off,
    Low,
    Medium,
    High,
}

impl QueryPressure {
    fn parse(raw: &str) -> Result<Self> {
        match raw {
            "off" => Ok(Self::Off),
            "low" => Ok(Self::Low),
            "medium" => Ok(Self::Medium),
            "high" => Ok(Self::High),
            _ => bail!("--query-pressure must be off, low, medium, or high"),
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Off => "off",
            Self::Low => "low",
            Self::Medium => "medium",
            Self::High => "high",
        }
    }

    fn default_interval(self) -> Duration {
        match self {
            Self::Off => DEFAULT_QUERY_INTERVAL,
            Self::Low => Duration::from_secs(10),
            Self::Medium => DEFAULT_QUERY_INTERVAL,
            Self::High => Duration::from_secs(1),
        }
    }

    fn default_concurrency(self) -> usize {
        match self {
            Self::Off | Self::Low => 1,
            Self::Medium => 2,
            Self::High => 4,
        }
    }
}

#[derive(Clone, Copy)]
enum TimestampMode {
    Fixed,
    Advancing,
}

impl TimestampMode {
    fn parse(raw: &str) -> Result<Self> {
        match raw {
            "fixed" => Ok(Self::Fixed),
            "advancing" | "advance" => Ok(Self::Advancing),
            _ => bail!("--timestamp-mode must be fixed or advancing"),
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Fixed => "fixed",
            Self::Advancing => "advancing",
        }
    }
}

#[derive(Clone, Serialize)]
struct WorkloadProfile {
    service_count: usize,
    log_record_count: usize,
    log_body_bytes: usize,
    trace_span_count: usize,
    trace_attribute_bytes: usize,
    metric_series_count: usize,
    metric_description_bytes: usize,
}

impl Default for WorkloadProfile {
    fn default() -> Self {
        Self {
            service_count: DEFAULT_SERVICE_COUNT,
            log_record_count: DEFAULT_LOG_RECORD_COUNT,
            log_body_bytes: DEFAULT_LOG_BODY_BYTES,
            trace_span_count: DEFAULT_TRACE_SPAN_COUNT,
            trace_attribute_bytes: DEFAULT_TRACE_ATTRIBUTE_BYTES,
            metric_series_count: DEFAULT_METRIC_SERIES_COUNT,
            metric_description_bytes: DEFAULT_METRIC_DESCRIPTION_BYTES,
        }
    }
}

impl WorkloadProfile {
    fn fixture_profile(&self) -> FixtureProfile {
        FixtureProfile {
            service_count: self.service_count,
            log_record_count: self.log_record_count,
            log_body_bytes: self.log_body_bytes,
            trace_span_count: self.trace_span_count,
            trace_attribute_bytes: self.trace_attribute_bytes,
            metric_series_count: self.metric_series_count,
            metric_description_bytes: self.metric_description_bytes,
            scenario_name: SCENARIO_NAME.to_string(),
            deployment_environment: "bench".to_string(),
            scope_name: "throughput_iteration".to_string(),
            scope_version: BENCH_VERSION.to_string(),
            deterministic_seed: DETERMINISTIC_SEED,
            primary_service_name: "bench-checkout".to_string(),
            service_name_prefix: "bench-service".to_string(),
            route: "/bench".to_string(),
            log_message_prefix: "canardstack-v0-iteration".to_string(),
            trace_span_name: "GET /bench".to_string(),
            metric_gauge_name: "canardstack.bench.gauge".to_string(),
            metric_sum_name: "canardstack.bench.sum".to_string(),
        }
    }

    fn validate(&self) -> Result<()> {
        if self.service_count == 0 {
            bail!("--services must be > 0");
        }
        if self.log_body_bytes == 0 {
            bail!("--log-body-bytes must be > 0");
        }
        if self.log_record_count == 0 {
            bail!("--log-records must be > 0");
        }
        if self.trace_span_count == 0 {
            bail!("--trace-spans must be > 0");
        }
        if self.metric_series_count == 0 {
            bail!("--metric-series must be > 0");
        }
        Ok(())
    }
}

fn next_arg(args: &mut impl Iterator<Item = String>, flag: &str) -> Result<String> {
    args.next()
        .with_context(|| format!("{flag} requires a value"))
}

fn parse_next<T>(args: &mut impl Iterator<Item = String>, flag: &str) -> Result<T>
where
    T: std::str::FromStr,
{
    next_arg(args, flag)?
        .parse()
        .map_err(|_| anyhow!("invalid {flag}"))
}

fn parse_duration(raw: &str) -> Result<Duration> {
    let (number, multiplier) = if let Some(value) = raw.strip_suffix("ms") {
        (value, 1)
    } else if let Some(value) = raw.strip_suffix('s') {
        (value, 1_000)
    } else if let Some(value) = raw.strip_suffix('m') {
        (value, 60_000)
    } else if let Some(value) = raw.strip_suffix('h') {
        (value, 3_600_000)
    } else {
        (raw, 1_000)
    };
    let value: u64 = number
        .parse()
        .with_context(|| format!("invalid duration {raw:?}"))?;
    Ok(Duration::from_millis(value.saturating_mul(multiplier)))
}

fn fmt_duration(duration: Duration) -> String {
    if duration.as_secs().is_multiple_of(60) && duration.as_secs() >= 60 {
        format!("{}m", duration.as_secs() / 60)
    } else {
        format!("{}s", duration.as_secs_f64())
    }
}

fn fmt_status_counts(counts: &BTreeMap<u16, u64>) -> String {
    if counts.is_empty() {
        return "{}".to_string();
    }
    let body = counts
        .iter()
        .map(|(status, count)| format!("{status}={count}"))
        .collect::<Vec<_>>()
        .join(",");
    format!("{{{body}}}")
}

fn fetch_storage_config(client: &Client, admin_key: &str) -> Option<Value> {
    let response = client
        .get("/api/admin/health/storage", Some(admin_key))
        .ok()
        .filter(|response| response.status == 200 || response.status == 503)?;
    serde_json::from_str(&response.body).ok()
}

#[derive(Serialize)]
struct ResourceEnvelope {
    configured_cpu_limit: Option<String>,
    configured_memory_limit: Option<String>,
    configured_note: Option<String>,
    os: &'static str,
    arch: &'static str,
    available_parallelism: Option<usize>,
    cgroup_cpu_max: Option<String>,
    cgroup_memory_max: Option<String>,
}

impl ResourceEnvelope {
    fn detect() -> Self {
        Self {
            configured_cpu_limit: env::var("CANARDSTACK_BENCHMARK_CPU_LIMIT").ok(),
            configured_memory_limit: env::var("CANARDSTACK_BENCHMARK_MEMORY_LIMIT").ok(),
            configured_note: env::var("CANARDSTACK_BENCHMARK_RESOURCE_NOTE").ok(),
            os: env::consts::OS,
            arch: env::consts::ARCH,
            available_parallelism: thread::available_parallelism().ok().map(usize::from),
            cgroup_cpu_max: read_trimmed("/sys/fs/cgroup/cpu.max"),
            cgroup_memory_max: read_trimmed("/sys/fs/cgroup/memory.max"),
        }
    }
}

fn read_trimmed(path: &str) -> Option<String> {
    let value = fs::read_to_string(path).ok()?;
    let trimmed = value.trim();
    (!trimmed.is_empty()).then(|| trimmed.to_string())
}

struct ResourceSampler {
    stop: Arc<AtomicBool>,
    samples: Arc<Mutex<Vec<ResourceSample>>>,
    handle: Option<thread::JoinHandle<()>>,
}

impl ResourceSampler {
    fn spawn(server_pid: Option<u32>, interval: Duration, started: Instant) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let samples = Arc::new(Mutex::new(Vec::new()));
        let stop_for_thread = stop.clone();
        let samples_for_thread = samples.clone();
        let benchmark_pid = std::process::id();
        let handle = thread::Builder::new()
            .name("canardstack-bench-resource-sampler".to_string())
            .spawn(move || {
                while !stop_for_thread.load(Ordering::SeqCst) {
                    push_resource_samples(&samples_for_thread, started, benchmark_pid, server_pid);
                    let sleep_until = Instant::now() + interval;
                    while !stop_for_thread.load(Ordering::SeqCst) && Instant::now() < sleep_until {
                        thread::sleep(Duration::from_millis(100));
                    }
                }
                push_resource_samples(&samples_for_thread, started, benchmark_pid, server_pid);
            })
            .expect("spawn benchmark resource sampler");
        Self {
            stop,
            samples,
            handle: Some(handle),
        }
    }

    fn stop(&mut self) -> Vec<ResourceSample> {
        self.stop.store(true, Ordering::SeqCst);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
        self.samples.lock().expect("lock resource samples").clone()
    }
}

fn push_resource_samples(
    samples: &Arc<Mutex<Vec<ResourceSample>>>,
    started: Instant,
    benchmark_pid: u32,
    server_pid: Option<u32>,
) {
    let elapsed = started.elapsed().as_secs_f64();
    let mut next = Vec::new();
    next.push(process_resource_sample("benchmark", benchmark_pid, elapsed));
    if let Some(pid) = server_pid {
        next.push(process_resource_sample("server", pid, elapsed));
    }
    samples.lock().expect("lock resource samples").extend(next);
}

fn process_resource_sample(process: &'static str, pid: u32, elapsed: f64) -> ResourceSample {
    match read_process_stats(pid) {
        Some(stats) => ResourceSample {
            process,
            pid,
            seconds_from_measured_start: elapsed,
            available: true,
            cpu_percent: stats.cpu_percent,
            memory_percent: stats.memory_percent,
            rss_kib: stats.rss_kib,
            error: None,
        },
        None => ResourceSample {
            process,
            pid,
            seconds_from_measured_start: elapsed,
            available: false,
            cpu_percent: None,
            memory_percent: None,
            rss_kib: None,
            error: Some("ps sample unavailable".to_string()),
        },
    }
}

fn read_process_stats(pid: u32) -> Option<ProcessStats> {
    let output = Command::new("ps")
        .args(["-p", &pid.to_string(), "-o", "%cpu=,%mem=,rss="])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let stdout = String::from_utf8_lossy(&output.stdout);
    let mut fields = stdout.split_whitespace();
    Some(ProcessStats {
        cpu_percent: fields.next()?.parse().ok(),
        memory_percent: fields.next()?.parse().ok(),
        rss_kib: fields.next()?.parse().ok(),
    })
}

struct ProcessStats {
    cpu_percent: Option<f64>,
    memory_percent: Option<f64>,
    rss_kib: Option<u64>,
}

#[derive(Clone, Serialize)]
struct ResourceSample {
    process: &'static str,
    pid: u32,
    seconds_from_measured_start: f64,
    available: bool,
    cpu_percent: Option<f64>,
    memory_percent: Option<f64>,
    rss_kib: Option<u64>,
    error: Option<String>,
}

#[derive(Clone)]
struct Workload {
    payloads: Vec<WorkloadPayload>,
    fixture_profile: FixtureProfile,
    timestamp_mode: TimestampMode,
}

impl Workload {
    fn new(
        run_started: chrono::DateTime<Utc>,
        profile: WorkloadProfile,
        signals: SignalSelection,
        timestamp_mode: TimestampMode,
    ) -> Self {
        let base_nanos = run_started
            .timestamp_nanos_opt()
            .unwrap_or(run_started.timestamp_millis() * 1_000_000);
        let fixture_profile = profile.fixture_profile();
        let mut payloads = vec![
            WorkloadPayload::logs(base_nanos, &fixture_profile),
            WorkloadPayload::spans(base_nanos, &fixture_profile),
            WorkloadPayload::metrics(base_nanos, &fixture_profile),
        ]
        .into_iter()
        .filter(|payload| signals.includes(payload.signal))
        .collect::<Vec<_>>();
        let ratio_total = payloads.iter().map(|payload| payload.ratio).sum::<f64>();
        for payload in &mut payloads {
            payload.ratio /= ratio_total;
        }
        Self {
            payloads,
            fixture_profile,
            timestamp_mode,
        }
    }

    fn next_payload_index(
        &self,
        elapsed_seconds: f64,
        target_bytes_per_sec: f64,
        sent_by_signal: &BTreeMap<&'static str, usize>,
    ) -> Option<usize> {
        self.payloads
            .iter()
            .enumerate()
            .map(|payload| {
                let (idx, payload) = payload;
                let desired = elapsed_seconds * target_bytes_per_sec * payload.ratio;
                let sent = sent_by_signal.get(payload.signal).copied().unwrap_or(0) as f64;
                (desired - sent, idx)
            })
            .filter(|(deficit, _)| *deficit > 0.0)
            .max_by(|(left, _), (right, _)| left.total_cmp(right))
            .map(|(_, idx)| idx)
    }

    fn prepare_payload(&self, idx: usize) -> PreparedPayload<'_> {
        self.payloads[idx].prepare(self.timestamp_mode, &self.fixture_profile)
    }
}

#[derive(Clone)]
struct WorkloadPayload {
    kind: PayloadKind,
    signal: &'static str,
    path: &'static str,
    content_type: &'static str,
    ratio: f64,
    body: Vec<u8>,
    decoded_bytes: usize,
    records_per_request: u64,
}

impl WorkloadPayload {
    fn logs(base_nanos: i64, profile: &FixtureProfile) -> Self {
        let body = encode_logs(profile, base_nanos);
        Self {
            kind: PayloadKind::Logs,
            signal: "logs",
            path: "/v1/logs",
            content_type: otlp2records::fixtures::CONTENT_TYPE_PROTOBUF,
            ratio: 0.60,
            decoded_bytes: body.len(),
            records_per_request: (profile.service_count * profile.log_record_count) as u64,
            body,
        }
    }

    fn spans(base_nanos: i64, profile: &FixtureProfile) -> Self {
        let body = encode_traces(profile, base_nanos);
        Self {
            kind: PayloadKind::Spans,
            signal: "spans",
            path: "/v1/traces",
            content_type: otlp2records::fixtures::CONTENT_TYPE_PROTOBUF,
            ratio: 0.25,
            decoded_bytes: body.len(),
            records_per_request: (profile.service_count * profile.trace_span_count) as u64,
            body,
        }
    }

    fn metrics(base_nanos: i64, profile: &FixtureProfile) -> Self {
        let body = encode_metrics(profile, base_nanos);
        Self {
            kind: PayloadKind::Metrics,
            signal: "metrics",
            path: "/v1/metrics",
            content_type: otlp2records::fixtures::CONTENT_TYPE_PROTOBUF,
            ratio: 0.15,
            decoded_bytes: body.len(),
            records_per_request: (profile.service_count * profile.metric_series_count * 2) as u64,
            body,
        }
    }

    fn prepare<'a>(
        &'a self,
        timestamp_mode: TimestampMode,
        profile: &FixtureProfile,
    ) -> PreparedPayload<'a> {
        let body = match timestamp_mode {
            TimestampMode::Fixed => Cow::Borrowed(self.body.as_slice()),
            TimestampMode::Advancing => Cow::Owned(self.kind.encode(current_nanos(), profile)),
        };
        PreparedPayload {
            signal: self.signal,
            path: self.path,
            content_type: self.content_type,
            decoded_bytes: body.len(),
            body,
            records_per_request: self.records_per_request,
        }
    }
}

#[derive(Clone, Copy)]
enum PayloadKind {
    Logs,
    Spans,
    Metrics,
}

impl PayloadKind {
    fn encode(self, base_nanos: i64, profile: &FixtureProfile) -> Vec<u8> {
        match self {
            Self::Logs => encode_logs(profile, base_nanos),
            Self::Spans => encode_traces(profile, base_nanos),
            Self::Metrics => encode_metrics(profile, base_nanos),
        }
    }
}

struct PreparedPayload<'a> {
    signal: &'static str,
    path: &'static str,
    content_type: &'static str,
    body: Cow<'a, [u8]>,
    decoded_bytes: usize,
    records_per_request: u64,
}

fn current_nanos() -> i64 {
    let now = Utc::now();
    now.timestamp_nanos_opt()
        .unwrap_or(now.timestamp_millis() * 1_000_000)
}

struct QueryPlan {
    run_started: chrono::DateTime<Utc>,
    signals: SignalSelection,
    next_idx: usize,
}

impl QueryPlan {
    fn new(run_started: chrono::DateTime<Utc>, signals: SignalSelection) -> Self {
        Self {
            run_started,
            signals,
            next_idx: 0,
        }
    }

    fn next(&mut self) -> String {
        let from = self.run_started - ChronoDuration::minutes(10);
        let to = Utc::now() + ChronoDuration::minutes(1);
        let paths = match self.signals {
            SignalSelection::All => vec![
                loki_query_range_path(from, to),
                prometheus_query_range_path(from, to),
                tempo_search_path(from, to),
            ],
            SignalSelection::Logs => vec![loki_query_range_path(from, to)],
            SignalSelection::Spans => vec![tempo_search_path(from, to)],
            SignalSelection::Metrics => vec![prometheus_query_range_path(from, to)],
        };
        let path = paths[self.next_idx % paths.len()].clone();
        self.next_idx += 1;
        path
    }
}

fn loki_query_range_path(from: chrono::DateTime<Utc>, to: chrono::DateTime<Utc>) -> String {
    format!(
        "/loki/api/v1/query_range?query={}&start={}&end={}&limit=100",
        enc("{service_name=\"bench-checkout\"} |= \"canardstack-v0-iteration\""),
        enc(&from.to_rfc3339()),
        enc(&to.to_rfc3339())
    )
}

fn prometheus_query_range_path(from: chrono::DateTime<Utc>, to: chrono::DateTime<Utc>) -> String {
    format!(
        "/api/v1/query_range?query={}&start={}&end={}&step=60",
        enc("avg(canardstack.bench.gauge{service_name=\"bench-checkout\"})"),
        enc(&from.to_rfc3339()),
        enc(&to.to_rfc3339())
    )
}

fn tempo_search_path(from: chrono::DateTime<Utc>, to: chrono::DateTime<Utc>) -> String {
    format!(
        "/api/search?start={}&end={}&service.name=bench-checkout&limit=10",
        enc(&from.to_rfc3339()),
        enc(&to.to_rfc3339())
    )
}

#[derive(Clone, Default)]
struct RunStats {
    elapsed: Duration,
    accepted_decoded_bytes: u64,
    accepted_request_bytes: u64,
    request_bytes_sent: u64,
    accepted_records_by_signal: BTreeMap<String, u64>,
    status_counts: BTreeMap<u16, u64>,
    ingest_latency_ms: Vec<f64>,
    query_latency_ms: Vec<f64>,
    query_requests: u64,
    query_failures: u64,
    transport_errors: u64,
    pacing_wait_count: u64,
    pacing_wait_seconds: f64,
    errors: Vec<String>,
    guard_exceeded: bool,
}

impl RunStats {
    fn status_counts_inc(&mut self, status: u16) {
        *self.status_counts.entry(status).or_default() += 1;
    }

    fn record_pacing_wait(&mut self, slept: Duration) {
        if slept.is_zero() {
            return;
        }
        self.pacing_wait_count += 1;
        self.pacing_wait_seconds += slept.as_secs_f64();
    }

    fn record_ingest_outcome(
        &mut self,
        payload: &PreparedPayload<'_>,
        outcome: IngestOutcome,
        measured: bool,
    ) {
        self.request_bytes_sent += payload.body.len() as u64;
        if let Some(status) = outcome.status {
            self.status_counts_inc(status);
            if measured {
                self.ingest_latency_ms.push(outcome.elapsed_ms);
            }
            if status == 202 {
                self.accepted_decoded_bytes += payload.decoded_bytes as u64;
                self.accepted_request_bytes += payload.body.len() as u64;
                *self
                    .accepted_records_by_signal
                    .entry(payload.signal.to_string())
                    .or_default() += outcome.records.unwrap_or(payload.records_per_request);
            }
        }
        if let Some(error) = outcome.error {
            self.errors.push(error);
        }
        if outcome.transport_error {
            self.transport_errors += 1;
        }
    }

    fn merge(&mut self, other: RunStats) {
        self.accepted_decoded_bytes += other.accepted_decoded_bytes;
        self.accepted_request_bytes += other.accepted_request_bytes;
        self.request_bytes_sent += other.request_bytes_sent;
        for (signal, records) in other.accepted_records_by_signal {
            *self.accepted_records_by_signal.entry(signal).or_default() += records;
        }
        for (status, count) in other.status_counts {
            *self.status_counts.entry(status).or_default() += count;
        }
        self.ingest_latency_ms.extend(other.ingest_latency_ms);
        self.query_latency_ms.extend(other.query_latency_ms);
        self.query_requests += other.query_requests;
        self.query_failures += other.query_failures;
        self.transport_errors += other.transport_errors;
        self.pacing_wait_count += other.pacing_wait_count;
        self.pacing_wait_seconds += other.pacing_wait_seconds;
        self.errors.extend(other.errors);
        self.guard_exceeded |= other.guard_exceeded;
    }
}

struct Client {
    host: String,
    port: u16,
    connection_mode: ConnectionMode,
    stream: Arc<Mutex<Option<PersistentStream>>>,
}

struct PersistentStream {
    stream: TcpStream,
    last_used: Instant,
}

impl Clone for Client {
    fn clone(&self) -> Self {
        Self {
            host: self.host.clone(),
            port: self.port,
            connection_mode: self.connection_mode,
            stream: Arc::new(Mutex::new(None)),
        }
    }
}

struct Response {
    status: u16,
    body: String,
    connection_close: bool,
}

struct RequestSpec<'a> {
    method: &'a str,
    path: &'a str,
    bearer: Option<&'a str>,
    body: Option<(&'a str, &'a [u8])>,
    keep_alive: bool,
}

impl Client {
    fn new(base_url: &str, connection_mode: ConnectionMode) -> Result<Self> {
        let rest = base_url
            .strip_prefix("http://")
            .ok_or_else(|| anyhow::anyhow!("only http:// base URLs are supported"))?;
        let authority = rest.trim_end_matches('/');
        let (host, port) = authority.split_once(':').unwrap_or((authority, "80"));
        Ok(Self {
            host: host.to_string(),
            port: port.parse().context("parse base URL port")?,
            connection_mode,
            stream: Arc::new(Mutex::new(None)),
        })
    }

    fn get(&self, path: &str, bearer: Option<&str>) -> Result<Response> {
        self.request("GET", path, bearer, None)
    }

    fn post_body(
        &self,
        path: &str,
        bearer: Option<&str>,
        content_type: &str,
        body: &[u8],
    ) -> Result<Response> {
        self.request("POST", path, bearer, Some((content_type, body)))
    }

    fn request(
        &self,
        method: &str,
        path: &str,
        bearer: Option<&str>,
        body: Option<(&str, &[u8])>,
    ) -> Result<Response> {
        match self.connection_mode {
            ConnectionMode::Close => self.request_once(method, path, bearer, body),
            ConnectionMode::Persistent => self.request_persistent(method, path, bearer, body),
        }
    }

    fn request_once(
        &self,
        method: &str,
        path: &str,
        bearer: Option<&str>,
        body: Option<(&str, &[u8])>,
    ) -> Result<Response> {
        let mut stream = self.connect()?;
        let deadline = Instant::now() + CLIENT_REQUEST_TIMEOUT;
        self.write_request(
            &mut stream,
            RequestSpec {
                method,
                path,
                bearer,
                body,
                keep_alive: false,
            },
            deadline,
        )?;
        read_response(&mut stream, deadline)
    }

    fn request_persistent(
        &self,
        method: &str,
        path: &str,
        bearer: Option<&str>,
        body: Option<(&str, &[u8])>,
    ) -> Result<Response> {
        let mut slot = self.stream.lock().expect("lock benchmark client stream");
        let now = Instant::now();
        if slot
            .as_ref()
            .is_some_and(|slot| now.duration_since(slot.last_used) >= PERSISTENT_IDLE_RECONNECT)
        {
            *slot = None;
        }
        if slot.is_none() {
            *slot = Some(PersistentStream {
                stream: self.connect()?,
                last_used: now,
            });
        }
        let slot_stream = slot.as_mut().expect("persistent stream exists");
        let deadline = Instant::now() + CLIENT_REQUEST_TIMEOUT;
        let result = self
            .write_request(
                &mut slot_stream.stream,
                RequestSpec {
                    method,
                    path,
                    bearer,
                    body,
                    keep_alive: true,
                },
                deadline,
            )
            .and_then(|()| read_response(&mut slot_stream.stream, deadline));
        match result {
            Ok(response) => {
                if response.connection_close {
                    *slot = None;
                } else if let Some(slot) = slot.as_mut() {
                    slot.last_used = Instant::now();
                }
                Ok(response)
            }
            Err(err) => {
                *slot = None;
                Err(err)
            }
        }
    }

    fn connect(&self) -> Result<TcpStream> {
        let addr = (self.host.as_str(), self.port)
            .to_socket_addrs()
            .with_context(|| format!("resolve http://{}:{}", self.host, self.port))?
            .next()
            .ok_or_else(|| anyhow::anyhow!("no socket address for {}", self.host))?;
        let stream = TcpStream::connect_timeout(&addr, CLIENT_REQUEST_TIMEOUT)
            .with_context(|| format!("connect to {}", fmt_addr(addr)))?;
        stream.set_read_timeout(Some(CLIENT_REQUEST_TIMEOUT))?;
        stream.set_write_timeout(Some(CLIENT_REQUEST_TIMEOUT))?;
        Ok(stream)
    }

    fn write_request(
        &self,
        stream: &mut TcpStream,
        spec: RequestSpec<'_>,
        deadline: Instant,
    ) -> Result<()> {
        let (content_type, body) = spec.body.unwrap_or(("application/octet-stream", b""));
        let mut head = format!(
            "{} {} HTTP/1.1\r\nhost: {}\r\naccept: application/json\r\ncontent-length: {}\r\nconnection: {}\r\n",
            spec.method,
            spec.path,
            self.host,
            body.len(),
            if spec.keep_alive {
                "keep-alive"
            } else {
                "close"
            }
        );
        if let Some(token) = spec.bearer {
            head.push_str(&format!("authorization: Bearer {token}\r\n"));
        }
        if spec.method == "POST" {
            head.push_str(&format!("content-type: {content_type}\r\n"));
        }
        head.push_str("\r\n");
        write_all_retry(stream, head.as_bytes(), deadline).context("write request headers")?;
        write_all_retry(stream, body, deadline).context("write request body")?;
        Ok(())
    }
}

fn write_all_retry(stream: &mut TcpStream, mut bytes: &[u8], deadline: Instant) -> Result<()> {
    while !bytes.is_empty() {
        match stream.write(bytes) {
            Ok(0) => bail!("socket write returned zero bytes"),
            Ok(written) => bytes = &bytes[written..],
            Err(err) if retry_io(&err, deadline) => continue,
            Err(err) => return Err(err).context("write to socket"),
        }
    }
    Ok(())
}

fn read_response(stream: &mut TcpStream, deadline: Instant) -> Result<Response> {
    let mut bytes = Vec::new();
    let mut buf = [0u8; 8192];
    loop {
        match stream.read(&mut buf) {
            Ok(0) => break,
            Ok(read) => {
                bytes.extend_from_slice(&buf[..read]);
                if response_complete(&bytes)? {
                    break;
                }
            }
            Err(err) if retry_io(&err, deadline) => continue,
            Err(err) => return Err(err).context("read HTTP response"),
        }
    }
    let raw = String::from_utf8_lossy(&bytes);
    let (head, body) = raw
        .split_once("\r\n\r\n")
        .ok_or_else(|| anyhow::anyhow!("malformed HTTP response"))?;
    let status = head
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|raw| raw.parse::<u16>().ok())
        .ok_or_else(|| anyhow::anyhow!("malformed HTTP status line"))?;
    let connection_close = response_connection_close(head);
    Ok(Response {
        status,
        body: body.to_string(),
        connection_close,
    })
}

fn response_connection_close(head: &str) -> bool {
    head.lines().skip(1).any(|line| {
        let Some((name, value)) = line.split_once(':') else {
            return false;
        };
        name.eq_ignore_ascii_case("connection")
            && value
                .split(',')
                .any(|token| token.trim().eq_ignore_ascii_case("close"))
    })
}

fn response_complete(bytes: &[u8]) -> Result<bool> {
    let Some(header_end) = find_header_end(bytes) else {
        return Ok(false);
    };
    let head = String::from_utf8_lossy(&bytes[..header_end]);
    let Some(content_length) = content_length(&head)? else {
        return Ok(false);
    };
    Ok(bytes.len().saturating_sub(header_end + b"\r\n\r\n".len()) >= content_length)
}

fn find_header_end(bytes: &[u8]) -> Option<usize> {
    bytes
        .windows(b"\r\n\r\n".len())
        .position(|window| window == b"\r\n\r\n")
}

fn content_length(head: &str) -> Result<Option<usize>> {
    for line in head.lines().skip(1) {
        let Some((name, value)) = line.split_once(':') else {
            continue;
        };
        if name.eq_ignore_ascii_case("content-length") {
            return Ok(Some(
                value
                    .trim()
                    .parse()
                    .context("parse response content-length")?,
            ));
        }
    }
    Ok(None)
}

fn retry_io(err: &std::io::Error, deadline: Instant) -> bool {
    let retryable = matches!(
        err.kind(),
        ErrorKind::Interrupted | ErrorKind::WouldBlock | ErrorKind::TimedOut
    );
    if retryable && Instant::now() < deadline {
        thread::sleep(Duration::from_millis(5));
        true
    } else {
        false
    }
}

fn fmt_addr(addr: SocketAddr) -> String {
    format!("http://{addr}")
}

#[derive(Clone, Default)]
struct ScrapedMetrics {
    raw_values: BTreeMap<String, f64>,
    freshness_lag_seconds: BTreeMap<String, f64>,
    queue: QueueReport,
    storage: StorageReport,
    server_phase_timing: BTreeMap<String, PhaseTimingReport>,
    transform_counters: BTreeMap<String, u64>,
    ingest_buffer_counters: BTreeMap<String, u64>,
    ingest_buffer_gauges: BTreeMap<String, f64>,
    ducklake_maintenance_timing: BTreeMap<String, PhaseTimingReport>,
}

fn scrape_metrics(text: &str) -> ScrapedMetrics {
    let mut out = ScrapedMetrics::default();
    let mut phase_counts: BTreeMap<String, f64> = BTreeMap::new();
    let mut phase_sums: BTreeMap<String, f64> = BTreeMap::new();
    let mut ducklake_counts: BTreeMap<String, f64> = BTreeMap::new();
    let mut ducklake_sums: BTreeMap<String, f64> = BTreeMap::new();

    for line in text.lines() {
        let Some(metric) = parse_metric_line(line) else {
            continue;
        };
        out.raw_values.insert(
            metric_series_key(&metric.name, &metric.labels),
            metric.value,
        );
        match metric.name.as_str() {
            "canardstack_ingest_to_query_lag_seconds" => {
                if let Some(table) = metric.labels.get("table") {
                    out.freshness_lag_seconds
                        .insert(table.clone(), metric.value);
                }
            }
            "canardstack_ingest_inflight_bytes" | "canardstack_ingest_inflight_bytes_max" => {
                out.queue.max_bytes = Some(out.queue.max_bytes.unwrap_or(0.0).max(metric.value));
            }
            "canardstack_storage_physical_bytes" => {
                out.storage.physical_bytes = Some(metric.value as u64);
            }
            "canardstack_storage_logical_rows" => {
                if let Some(table) = metric.labels.get("table") {
                    out.storage
                        .logical_rows
                        .insert(table.clone(), metric.value as u64);
                }
            }
            "canardstack_ducklake_active_data_files" => {
                if let Some(table) = metric.labels.get("table") {
                    out.storage
                        .ducklake_active_data_files
                        .insert(table.clone(), metric.value as u64);
                }
            }
            "canardstack_ducklake_active_data_file_rows" => {
                if let Some(table) = metric.labels.get("table") {
                    out.storage
                        .ducklake_active_data_file_rows
                        .insert(table.clone(), metric.value as u64);
                }
            }
            "canardstack_phase_duration_seconds_count" => {
                phase_counts.insert(labels_key(&metric.labels), metric.value);
            }
            "canardstack_phase_duration_seconds_sum" => {
                phase_sums.insert(labels_key(&metric.labels), metric.value);
            }
            "canardstack_otlp2records_transform_events_total" => {
                out.transform_counters
                    .insert(labels_key(&metric.labels), metric.value as u64);
            }
            name if name.starts_with("canardstack_ingest_buffered_")
                && name.ends_with("_total") =>
            {
                out.ingest_buffer_counters.insert(
                    format!("{} {}", metric.name, labels_key(&metric.labels)),
                    metric.value as u64,
                );
            }
            name if name.starts_with("canardstack_ingest_buffered_") => {
                out.ingest_buffer_gauges.insert(
                    format!("{} {}", metric.name, labels_key(&metric.labels)),
                    metric.value,
                );
            }
            "canardstack_ducklake_compaction_duration_seconds_count" => {
                ducklake_counts.insert(
                    format!(
                        "{} {}",
                        metric.name.trim_end_matches("_count"),
                        labels_key(&metric.labels)
                    ),
                    metric.value,
                );
            }
            "canardstack_ducklake_compaction_duration_seconds_sum" => {
                ducklake_sums.insert(
                    format!(
                        "{} {}",
                        metric.name.trim_end_matches("_sum"),
                        labels_key(&metric.labels)
                    ),
                    metric.value,
                );
            }
            _ => {}
        }
    }

    for (key, count) in phase_counts {
        let sum_seconds = phase_sums.get(&key).copied().unwrap_or(0.0);
        out.server_phase_timing.insert(
            key,
            PhaseTimingReport {
                count: count as u64,
                sum_seconds,
                avg_seconds: if count > 0.0 {
                    Some(sum_seconds / count)
                } else {
                    None
                },
                seconds_per_mib: None,
                wall_time_share: None,
            },
        );
    }
    for (key, count) in ducklake_counts {
        let sum_seconds = ducklake_sums.get(&key).copied().unwrap_or(0.0);
        out.ducklake_maintenance_timing.insert(
            key,
            PhaseTimingReport {
                count: count as u64,
                sum_seconds,
                avg_seconds: if count > 0.0 {
                    Some(sum_seconds / count)
                } else {
                    None
                },
                seconds_per_mib: None,
                wall_time_share: None,
            },
        );
    }
    out
}

struct ParsedMetric {
    name: String,
    labels: BTreeMap<String, String>,
    value: f64,
}

fn parse_metric_line(line: &str) -> Option<ParsedMetric> {
    if line.trim().is_empty() || line.starts_with('#') {
        return None;
    }
    let (series, raw_value) = line.rsplit_once(' ')?;
    let value = raw_value.parse().ok()?;
    if let Some((name, rest)) = series.split_once('{') {
        let labels = rest.strip_suffix('}')?;
        Some(ParsedMetric {
            name: name.to_string(),
            labels: parse_labels(labels),
            value,
        })
    } else {
        Some(ParsedMetric {
            name: series.to_string(),
            labels: BTreeMap::new(),
            value,
        })
    }
}

fn parse_labels(raw: &str) -> BTreeMap<String, String> {
    raw.split(',')
        .filter_map(|part| {
            let (key, value) = part.split_once('=')?;
            Some((
                key.to_string(),
                value.trim_matches('"').replace("\\\"", "\""),
            ))
        })
        .collect()
}

fn max_freshness_lag_from_samples(
    samples: &[MetricSample],
    signals: SignalSelection,
) -> Option<f64> {
    samples
        .iter()
        .filter_map(|sample| sample.metrics.as_ref())
        .filter_map(|metrics| {
            max_freshness_lag_for_signals(&metrics.freshness_lag_seconds, signals)
        })
        .max_by(f64::total_cmp)
}

fn max_freshness_lag_for_signals(
    freshness_lag_seconds: &BTreeMap<String, f64>,
    signals: SignalSelection,
) -> Option<f64> {
    freshness_lag_seconds
        .iter()
        .filter_map(|(table, lag)| signals.includes_table(table).then_some(*lag))
        .max_by(f64::total_cmp)
}

fn labels_key(labels: &BTreeMap<String, String>) -> String {
    labels
        .iter()
        .map(|(key, value)| format!("{key}={value}"))
        .collect::<Vec<_>>()
        .join(",")
}

fn metric_series_key(name: &str, labels: &BTreeMap<String, String>) -> String {
    if labels.is_empty() {
        return name.to_string();
    }
    format!("{name} {}", labels_key(labels))
}

#[derive(Clone)]
struct MetricSample {
    label: String,
    seconds_from_measured_start: f64,
    metrics: Option<ScrapedMetrics>,
}

impl MetricSample {
    fn capture(label: &str, elapsed: Duration, client: &Client) -> Self {
        let metrics = client
            .get("/metrics", None)
            .ok()
            .filter(|response| response.status == 200)
            .map(|response| scrape_metrics(&response.body));
        Self {
            label: label.to_string(),
            seconds_from_measured_start: elapsed.as_secs_f64(),
            metrics,
        }
    }
}

fn trend_from_samples(
    samples: &[MetricSample],
    value: impl Fn(&ScrapedMetrics) -> Option<f64>,
) -> TrendReport {
    let points = samples
        .iter()
        .filter_map(|sample| {
            let metrics = sample.metrics.as_ref()?;
            let value = value(metrics)?;
            Some(TrendPoint {
                label: sample.label.clone(),
                seconds_from_measured_start: sample.seconds_from_measured_start,
                value,
            })
        })
        .collect::<Vec<_>>();
    let available = points.len() >= 2;
    let clearly_increasing = if available {
        let first = points.first().map(|point| point.value).unwrap_or_default();
        let last = points.last().map(|point| point.value).unwrap_or_default();
        let recovered = points
            .iter()
            .rev()
            .take(2)
            .any(|point| point.value <= first * 1.10 + 1.0);
        last > first * 1.25 + 5.0 && !recovered
    } else {
        false
    };
    TrendReport {
        available,
        clearly_increasing,
        points,
    }
}

fn top_phase_timings(
    phases: &BTreeMap<String, PhaseTimingReport>,
) -> Vec<(&String, &PhaseTimingReport)> {
    let mut timings = phases.iter().collect::<Vec<_>>();
    timings.sort_by(|(_, left), (_, right)| right.sum_seconds.total_cmp(&left.sum_seconds));
    timings
}

fn peak_resource_samples(samples: &[ResourceSample]) -> Vec<(&'static str, &ResourceSample)> {
    let mut peaks = BTreeMap::new();
    for sample in samples.iter().filter(|sample| sample.available) {
        let replace = peaks
            .get(sample.process)
            .is_none_or(|current: &&ResourceSample| {
                sample.cpu_percent.unwrap_or(0.0) > current.cpu_percent.unwrap_or(0.0)
            });
        if replace {
            peaks.insert(sample.process, sample);
        }
    }
    peaks.into_iter().collect()
}

#[derive(Serialize)]
struct Report {
    git_sha: Option<String>,
    benchmark_name: String,
    benchmark_version: String,
    base_url: String,
    resource_envelope: ResourceEnvelope,
    storage_config: Option<Value>,
    generator: GeneratorReport,
    workload_profile: WorkloadProfile,
    query_profile: QueryProfileReport,
    scenario: ScenarioReport,
    warmup_duration_seconds: f64,
    measured_duration_seconds: f64,
    target_decoded_bytes_per_sec: f64,
    actual_decoded_bytes_per_sec: f64,
    request_bytes_per_sec: f64,
    accepted_records_per_sec_by_signal: BTreeMap<String, f64>,
    http_status_counts: BTreeMap<String, u64>,
    ingest_latency_ms: LatencySummary,
    query_latency_ms: LatencySummary,
    freshness_lag_seconds: BTreeMap<String, f64>,
    freshness_sla_seconds: Option<f64>,
    max_measured_freshness_lag_seconds: Option<f64>,
    queue: Option<QueueReport>,
    storage: Option<StorageReport>,
    server_phase_timing: BTreeMap<String, PhaseTimingReport>,
    transform_counters: BTreeMap<String, u64>,
    ingest_buffer_counters: BTreeMap<String, u64>,
    ingest_buffer_gauges: BTreeMap<String, f64>,
    ducklake_maintenance_timing: BTreeMap<String, PhaseTimingReport>,
    resource_samples: Vec<ResourceSample>,
    metric_snapshots: Vec<MetricSnapshotReport>,
    stage_throughput: StageThroughputReport,
    loki_progressive_query: LokiProgressiveQueryReport,
    queue_oldest_age_trend: TrendReport,
    queue_rows_trend: TrendReport,
    queue_bytes_trend: TrendReport,
    freshness_lag_trend: TrendReport,
    pass_fail_criteria: Vec<PassFailCriterionReport>,
    smell_observations: Vec<String>,
    pass: bool,
    failure_reasons: Vec<String>,
}

#[derive(Serialize)]
struct GeneratorReport {
    ingest_concurrency: usize,
    pacing_wait_count: u64,
    pacing_wait_seconds: f64,
    pacing_wait_fraction_of_worker_time: Option<f64>,
    target_utilization: Option<f64>,
    likely_generator_or_schedule_limited: bool,
}

#[derive(Serialize)]
struct QueryProfileReport {
    profile: String,
    pressure: String,
    enabled: bool,
    interval_seconds: f64,
    concurrency: usize,
}

#[derive(Serialize)]
struct PassFailCriterionReport {
    name: String,
    passed: bool,
    detail: String,
}

impl PassFailCriterionReport {
    fn new(name: &str, passed: bool, detail: String) -> Self {
        Self {
            name: name.to_string(),
            passed,
            detail,
        }
    }
}

#[derive(Serialize)]
struct ScenarioReport {
    name: String,
    deterministic_seed: u64,
    target_gb_per_day: f64,
    signals: String,
    timestamp_mode: String,
    connection_mode: String,
    byte_mix: BTreeMap<String, f64>,
    payloads: Vec<PayloadReport>,
}

#[derive(Serialize)]
struct PayloadReport {
    signal: String,
    decoded_bytes: u64,
    records_per_request: u64,
}

#[derive(Clone, Default, Serialize)]
struct QueueReport {
    max_rows: Option<f64>,
    max_bytes: Option<f64>,
    max_oldest_age_seconds: Option<f64>,
}

#[derive(Clone, Default, Serialize)]
struct StorageReport {
    physical_bytes: Option<u64>,
    logical_rows: BTreeMap<String, u64>,
    ducklake_active_data_files: BTreeMap<String, u64>,
    ducklake_active_data_file_rows: BTreeMap<String, u64>,
}

#[derive(Clone, Serialize)]
struct PhaseTimingReport {
    count: u64,
    sum_seconds: f64,
    avg_seconds: Option<f64>,
    seconds_per_mib: Option<f64>,
    wall_time_share: Option<f64>,
}

impl PhaseTimingReport {
    fn zero() -> Self {
        Self {
            count: 0,
            sum_seconds: 0.0,
            avg_seconds: None,
            seconds_per_mib: None,
            wall_time_share: None,
        }
    }
}

#[derive(Serialize)]
struct MetricSnapshotReport {
    label: String,
    seconds_from_measured_start: f64,
    available: bool,
    queue: Option<QueueReport>,
    freshness_lag_seconds: BTreeMap<String, f64>,
}

impl MetricSnapshotReport {
    fn from_sample(sample: &MetricSample) -> Self {
        Self {
            label: sample.label.clone(),
            seconds_from_measured_start: sample.seconds_from_measured_start,
            available: sample.metrics.is_some(),
            queue: sample.metrics.as_ref().map(|metrics| metrics.queue.clone()),
            freshness_lag_seconds: sample
                .metrics
                .as_ref()
                .map(|metrics| metrics.freshness_lag_seconds.clone())
                .unwrap_or_default(),
        }
    }
}

#[derive(Serialize)]
struct LokiProgressiveQueryReport {
    available: bool,
    requests_ok_delta: Option<f64>,
    final_candidate_files: Option<f64>,
    final_candidate_rows: Option<f64>,
    final_candidate_bytes: Option<f64>,
    final_batch_size: Option<f64>,
    final_files_scanned: Option<f64>,
    final_batches_scanned: Option<f64>,
    final_rows_scanned: Option<f64>,
    final_bytes_scanned: Option<f64>,
    final_result_rows: Option<f64>,
    final_total_log_files: Option<f64>,
    final_total_log_rows: Option<f64>,
    scanned_file_fraction_of_candidates: Option<f64>,
    scanned_row_fraction_of_candidates: Option<f64>,
    scanned_byte_fraction_of_candidates: Option<f64>,
    scanned_file_fraction_of_total: Option<f64>,
    scanned_row_fraction_of_total: Option<f64>,
    planner_timing: PhaseTimingReport,
    candidate_execute_timing: PhaseTimingReport,
    total_timing: PhaseTimingReport,
}

impl LokiProgressiveQueryReport {
    fn from_samples(
        samples: &[MetricSample],
        server_phase_timing: &BTreeMap<String, PhaseTimingReport>,
    ) -> Self {
        let total_timing = server_phase_timing
            .get(
                "phase=loki_progressive_query_execute,route_template=/loki/api/v1/query_range,storage_signal=logs",
            )
            .cloned()
            .unwrap_or_else(PhaseTimingReport::zero);
        let planner_timing = server_phase_timing
            .get(
                "phase=loki_progressive_query_candidate_plan,route_template=/loki/api/v1/query_range,storage_signal=logs",
            )
            .cloned()
            .unwrap_or_else(PhaseTimingReport::zero);
        let candidate_execute_timing = server_phase_timing
            .get(
                "phase=loki_progressive_query_candidate_execute,route_template=/loki/api/v1/query_range,storage_signal=logs",
            )
            .cloned()
            .unwrap_or_else(PhaseTimingReport::zero);
        let Some(end) = samples.iter().find(|sample| sample.label == "end") else {
            return Self::unavailable(planner_timing, candidate_execute_timing, total_timing);
        };
        let Some(end_metrics) = end.metrics.as_ref() else {
            return Self::unavailable(planner_timing, candidate_execute_timing, total_timing);
        };
        let start_metrics = samples
            .iter()
            .find(|sample| sample.label == "start")
            .and_then(|sample| sample.metrics.as_ref());

        let final_candidate_files = metric_value(
            end_metrics,
            "canardstack_loki_progressive_query_candidate_files",
        );
        let final_candidate_rows = metric_value(
            end_metrics,
            "canardstack_loki_progressive_query_candidate_rows",
        );
        let final_candidate_bytes = metric_value(
            end_metrics,
            "canardstack_loki_progressive_query_candidate_bytes",
        );
        let final_batch_size =
            metric_value(end_metrics, "canardstack_loki_progressive_query_batch_size");
        let final_files_scanned = metric_value(
            end_metrics,
            "canardstack_loki_progressive_query_files_scanned",
        );
        let final_batches_scanned = metric_value(
            end_metrics,
            "canardstack_loki_progressive_query_batches_scanned",
        );
        let final_rows_scanned = metric_value(
            end_metrics,
            "canardstack_loki_progressive_query_rows_scanned",
        );
        let final_bytes_scanned = metric_value(
            end_metrics,
            "canardstack_loki_progressive_query_bytes_scanned",
        );
        let final_result_rows = metric_value(
            end_metrics,
            "canardstack_loki_progressive_query_result_rows",
        );
        let final_total_log_files = labeled_metric_value(
            end_metrics,
            "canardstack_ducklake_active_data_files",
            "table",
            "logs",
        );
        let final_total_log_rows = labeled_metric_value(
            end_metrics,
            "canardstack_ducklake_active_data_file_rows",
            "table",
            "logs",
        );
        let requests_ok_delta = counter_delta(
            start_metrics,
            end_metrics,
            "canardstack_loki_progressive_query_requests_total",
            &[("status", "ok")],
        );
        let available = final_files_scanned.is_some()
            || requests_ok_delta.unwrap_or(0.0) > 0.0
            || total_timing.count > 0;

        Self {
            available,
            requests_ok_delta,
            final_candidate_files,
            final_candidate_rows,
            final_candidate_bytes,
            final_batch_size,
            final_files_scanned,
            final_batches_scanned,
            final_rows_scanned,
            final_bytes_scanned,
            final_result_rows,
            final_total_log_files,
            final_total_log_rows,
            scanned_file_fraction_of_candidates: ratio(final_files_scanned, final_candidate_files),
            scanned_row_fraction_of_candidates: ratio(final_rows_scanned, final_candidate_rows),
            scanned_byte_fraction_of_candidates: ratio(final_bytes_scanned, final_candidate_bytes),
            scanned_file_fraction_of_total: ratio(final_files_scanned, final_total_log_files),
            scanned_row_fraction_of_total: ratio(final_rows_scanned, final_total_log_rows),
            planner_timing,
            candidate_execute_timing,
            total_timing,
        }
    }

    fn unavailable(
        planner_timing: PhaseTimingReport,
        candidate_execute_timing: PhaseTimingReport,
        total_timing: PhaseTimingReport,
    ) -> Self {
        Self {
            available: total_timing.count > 0,
            requests_ok_delta: None,
            final_candidate_files: None,
            final_candidate_rows: None,
            final_candidate_bytes: None,
            final_batch_size: None,
            final_files_scanned: None,
            final_batches_scanned: None,
            final_rows_scanned: None,
            final_bytes_scanned: None,
            final_result_rows: None,
            final_total_log_files: None,
            final_total_log_rows: None,
            scanned_file_fraction_of_candidates: None,
            scanned_row_fraction_of_candidates: None,
            scanned_byte_fraction_of_candidates: None,
            scanned_file_fraction_of_total: None,
            scanned_row_fraction_of_total: None,
            planner_timing,
            candidate_execute_timing,
            total_timing,
        }
    }
}

#[derive(Serialize)]
struct StageThroughputReport {
    available: bool,
    window_seconds: Option<f64>,
    start_label: Option<String>,
    end_label: Option<String>,
    totals: BTreeMap<String, f64>,
    totals_per_second: BTreeMap<String, f64>,
    by_signal: BTreeMap<String, BTreeMap<String, f64>>,
    by_signal_per_second: BTreeMap<String, BTreeMap<String, f64>>,
}

impl StageThroughputReport {
    fn from_samples(samples: &[MetricSample]) -> Self {
        let Some(start) = samples.iter().find(|sample| sample.label == "start") else {
            return Self::unavailable();
        };
        let Some(end) = samples.iter().find(|sample| sample.label == "end") else {
            return Self::unavailable();
        };
        let Some(start_metrics) = start.metrics.as_ref() else {
            return Self::unavailable();
        };
        let Some(end_metrics) = end.metrics.as_ref() else {
            return Self::unavailable();
        };
        let window_seconds =
            (end.seconds_from_measured_start - start.seconds_from_measured_start).max(0.001);
        let stages = [
            StageMetric {
                stage: "raw_spooled_records",
                metric: "canardstack_raw_spool_records_total",
                label: "request_kind",
                kind: StageMetricKind::Counter,
            },
            StageMetric {
                stage: "raw_spool_replayed_records",
                metric: "canardstack_raw_spool_replayed_records_total",
                label: "request_kind",
                kind: StageMetricKind::Counter,
            },
            StageMetric {
                stage: "accepted_request_bytes",
                metric: "canardstack_ingest_request_bytes_total",
                label: "request_kind",
                kind: StageMetricKind::Counter,
            },
            StageMetric {
                stage: "accepted_decoded_bytes",
                metric: "canardstack_ingest_decoded_bytes_total",
                label: "request_kind",
                kind: StageMetricKind::Counter,
            },
            StageMetric {
                stage: "transformed_rows",
                metric: "canardstack_ingest_transformed_rows_total",
                label: "storage_signal",
                kind: StageMetricKind::Counter,
            },
            StageMetric {
                stage: "buffered_rows",
                metric: "canardstack_ingest_buffered_rows_total",
                label: "storage_signal",
                kind: StageMetricKind::Counter,
            },
            StageMetric {
                stage: "buffered_bytes",
                metric: "canardstack_ingest_buffered_bytes_total",
                label: "storage_signal",
                kind: StageMetricKind::Counter,
            },
            StageMetric {
                stage: "duckdb_arrow_appended_rows",
                metric: "canardstack_duckdb_arrow_appended_rows_total",
                label: "storage_signal",
                kind: StageMetricKind::Counter,
            },
            StageMetric {
                stage: "arrow_flush_rows",
                metric: "canardstack_arrow_flush_rows_total",
                label: "storage_signal",
                kind: StageMetricKind::Counter,
            },
            StageMetric {
                stage: "raw_spool_checkpointed_records",
                metric: "canardstack_raw_spool_checkpointed_records_total",
                label: "request_kind",
                kind: StageMetricKind::Counter,
            },
            StageMetric {
                stage: "storage_visible_rows",
                metric: "canardstack_storage_logical_rows",
                label: "table",
                kind: StageMetricKind::GaugeDelta,
            },
            StageMetric {
                stage: "ducklake_active_data_file_rows",
                metric: "canardstack_ducklake_active_data_file_rows",
                label: "table",
                kind: StageMetricKind::GaugeDelta,
            },
            StageMetric {
                stage: "ducklake_active_data_files",
                metric: "canardstack_ducklake_active_data_files",
                label: "table",
                kind: StageMetricKind::GaugeDelta,
            },
        ];

        let mut totals = BTreeMap::new();
        let mut by_signal = BTreeMap::new();
        for stage in stages {
            let values = stage_delta_by_label(start_metrics, end_metrics, stage);
            if values.is_empty() {
                continue;
            }
            let total = values.values().sum::<f64>();
            totals.insert(stage.stage.to_string(), total);
            by_signal.insert(stage.stage.to_string(), values);
        }
        let totals_per_second = totals
            .iter()
            .map(|(stage, value)| (stage.clone(), value / window_seconds))
            .collect();
        let by_signal_per_second = by_signal
            .iter()
            .map(|(stage, values)| {
                (
                    stage.clone(),
                    values
                        .iter()
                        .map(|(signal, value)| (signal.clone(), value / window_seconds))
                        .collect(),
                )
            })
            .collect();
        Self {
            available: true,
            window_seconds: Some(window_seconds),
            start_label: Some(start.label.clone()),
            end_label: Some(end.label.clone()),
            totals,
            totals_per_second,
            by_signal,
            by_signal_per_second,
        }
    }

    fn unavailable() -> Self {
        Self {
            available: false,
            window_seconds: None,
            start_label: None,
            end_label: None,
            totals: BTreeMap::new(),
            totals_per_second: BTreeMap::new(),
            by_signal: BTreeMap::new(),
            by_signal_per_second: BTreeMap::new(),
        }
    }
}

#[derive(Clone, Copy)]
struct StageMetric {
    stage: &'static str,
    metric: &'static str,
    label: &'static str,
    kind: StageMetricKind,
}

#[derive(Clone, Copy)]
enum StageMetricKind {
    Counter,
    GaugeDelta,
}

fn stage_delta_by_label(
    start: &ScrapedMetrics,
    end: &ScrapedMetrics,
    stage: StageMetric,
) -> BTreeMap<String, f64> {
    let mut values = BTreeMap::new();
    for line in end.raw_values.keys() {
        let (name, labels) = parse_metric_series_key(line);
        if name != stage.metric {
            continue;
        }
        let Some(label_value) = labels.get(stage.label) else {
            continue;
        };
        let end_value = end.raw_values.get(line).copied().unwrap_or(0.0);
        let start_value = start.raw_values.get(line).copied().unwrap_or(0.0);
        let delta = match stage.kind {
            StageMetricKind::Counter => end_value - start_value,
            StageMetricKind::GaugeDelta => (end_value - start_value).max(0.0),
        };
        if delta > 0.0 {
            *values.entry(label_value.clone()).or_default() += delta;
        }
    }
    values
}

fn parse_metric_series_key(key: &str) -> (String, BTreeMap<String, String>) {
    let Some((name, labels)) = key.split_once(' ') else {
        return (key.to_string(), BTreeMap::new());
    };
    let parsed = labels
        .split(',')
        .filter_map(|part| {
            let (key, value) = part.split_once('=')?;
            Some((key.to_string(), value.to_string()))
        })
        .collect();
    (name.to_string(), parsed)
}

fn metric_value(metrics: &ScrapedMetrics, name: &str) -> Option<f64> {
    metrics.raw_values.get(name).copied()
}

fn labeled_metric_value(
    metrics: &ScrapedMetrics,
    name: &str,
    label: &str,
    label_value: &str,
) -> Option<f64> {
    series_value(metrics, name, &[(label, label_value)])
}

fn counter_delta(
    start: Option<&ScrapedMetrics>,
    end: &ScrapedMetrics,
    name: &str,
    labels: &[(&str, &str)],
) -> Option<f64> {
    let end_value = series_value(end, name, labels)?;
    let start_value = start
        .and_then(|metrics| series_value(metrics, name, labels))
        .unwrap_or(0.0);
    Some((end_value - start_value).max(0.0))
}

fn series_value(metrics: &ScrapedMetrics, name: &str, labels: &[(&str, &str)]) -> Option<f64> {
    let labels = labels
        .iter()
        .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
        .collect::<BTreeMap<_, _>>();
    metrics
        .raw_values
        .get(&metric_series_key(name, &labels))
        .copied()
}

fn ratio(numerator: Option<f64>, denominator: Option<f64>) -> Option<f64> {
    let numerator = numerator?;
    let denominator = denominator?;
    (denominator > 0.0).then_some(numerator / denominator)
}

#[derive(Serialize)]
struct TrendReport {
    available: bool,
    clearly_increasing: bool,
    points: Vec<TrendPoint>,
}

#[derive(Serialize)]
struct TrendPoint {
    label: String,
    seconds_from_measured_start: f64,
    value: f64,
}

#[derive(Default, Serialize)]
struct LatencySummary {
    count: usize,
    p50: Option<f64>,
    p95: Option<f64>,
    p99: Option<f64>,
}

impl LatencySummary {
    fn from_samples(mut samples: Vec<f64>) -> Self {
        samples.sort_by(f64::total_cmp);
        Self {
            count: samples.len(),
            p50: percentile(&samples, 0.50),
            p95: percentile(&samples, 0.95),
            p99: percentile(&samples, 0.99),
        }
    }
}

fn percentile(samples: &[f64], q: f64) -> Option<f64> {
    if samples.is_empty() {
        return None;
    }
    let idx = ((samples.len() as f64 * q).ceil() as usize).saturating_sub(1);
    samples.get(idx.min(samples.len() - 1)).copied()
}

fn fmt_optional(value: Option<f64>) -> String {
    value
        .map(|value| format!("{value:.1}"))
        .unwrap_or_else(|| "n/a".to_string())
}

fn git_sha() -> Option<String> {
    let output = Command::new("git")
        .args(["rev-parse", "HEAD"])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    Some(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

fn enc(value: &str) -> String {
    value
        .bytes()
        .flat_map(|b| match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                vec![b as char]
            }
            b' ' => vec!['+'],
            other => format!("%{other:02X}").chars().collect(),
        })
        .collect()
}
