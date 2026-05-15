use anyhow::{bail, Context, Result};
use chrono::{Duration as ChronoDuration, Utc};
use serde::Serialize;
use serde_json::Value;
use std::collections::BTreeMap;
use std::env;
use std::fs;
use std::io::{ErrorKind, Read, Write};
use std::net::{SocketAddr, TcpStream, ToSocketAddrs};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::thread;
use std::time::{Duration, Instant};

const BENCH_NAME: &str = "v0_iteration";
const BENCH_VERSION: &str = "0.1.0";
const SCENARIO_NAME: &str = "v0-local-100gpd";
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
const DEFAULT_SERVICE_COUNT: usize = 1;
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
            eprintln!("v0_iteration failed before completing a reportable run: {err:#}");
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
    let client = Client::new(&args.base_url)?;
    ensure_reachable(&client)?;
    let resource_envelope = ResourceEnvelope::detect();
    let storage_config = fetch_storage_config(&client, &admin_key);

    let run_started = Utc::now();
    let workload = Workload::new(run_started, args.workload.clone());
    let target_bytes_per_sec = args.target_decoded_bytes_per_sec();
    let guard_deadline = Instant::now() + args.max_runtime();

    eprintln!(
        "v0_iteration: warmup={} measured={} target={:.0} decoded B/s base_url={} profile={} query_concurrency={} progress={} max_runtime={}",
        fmt_duration(args.warmup),
        fmt_duration(args.duration),
        target_bytes_per_sec,
        args.base_url,
        args.profile.as_str(),
        args.query_concurrency,
        fmt_duration(args.progress_interval),
        fmt_duration(args.max_runtime())
    );

    let mut query_plan = QueryPlan::new(run_started);
    let _warmup = run_phase(
        &client,
        &api_key,
        &workload,
        &mut query_plan,
        PhaseConfig {
            phase: "warmup",
            duration: args.warmup,
            target_bytes_per_sec,
            query_interval: args.query_interval,
            query_concurrency: args.query_concurrency,
            no_queries: args.no_queries(),
            measured: false,
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
            query_interval: args.query_interval,
            query_concurrency: args.query_concurrency,
            no_queries: args.no_queries(),
            measured: true,
            progress_interval: args.progress_interval,
            guard_deadline,
        },
        Some(&mut midpoint_sample),
    );
    if let Some(sample) = midpoint_sample {
        metric_samples.push(sample);
    }
    metric_samples.push(MetricSample::capture(
        "end",
        measured_start.elapsed(),
        &client,
    ));

    let _ = client.post_body(
        "/api/admin/maintenance/flush",
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

    let report = build_report(
        args.clone(),
        workload,
        measured,
        scraped,
        metric_samples,
        resource_envelope,
        storage_config,
    );
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
            print_progress(config.phase, started, config.duration, &stats, client);
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
        let Some(payload) =
            workload.next_payload(elapsed, config.target_bytes_per_sec, &sent_by_signal)
        else {
            sleep_until_next_tick(deadline);
            continue;
        };
        *sent_by_signal.entry(payload.signal).or_default() += payload.decoded_bytes;
        stats.request_bytes_sent += payload.body.len() as u64;

        let started_request = Instant::now();
        match client.post_body(
            payload.path,
            Some(api_key),
            payload.content_type,
            &payload.body,
        ) {
            Ok(response) => {
                let elapsed_ms = started_request.elapsed().as_secs_f64() * 1000.0;
                stats.status_counts_inc(response.status);
                if config.measured {
                    stats.ingest_latency_ms.push(elapsed_ms);
                }
                if response.status == 202 {
                    stats.accepted_decoded_bytes += payload.decoded_bytes as u64;
                    stats.accepted_request_bytes += payload.body.len() as u64;
                    let records = serde_json::from_str::<Value>(&response.body)
                        .ok()
                        .and_then(|body| body.get("records").and_then(Value::as_u64))
                        .unwrap_or(payload.records_per_request);
                    *stats
                        .accepted_records_by_signal
                        .entry(payload.signal.to_string())
                        .or_default() += records;
                }
            }
            Err(err) => {
                stats.transport_errors += 1;
                let elapsed_ms = started_request.elapsed().as_secs_f64() * 1000.0;
                let detail = format!(
                    "ingest transport error signal={} path={} decoded_bytes={} request_bytes={} elapsed_ms={elapsed_ms:.1}: {}",
                    payload.signal,
                    payload.path,
                    payload.decoded_bytes,
                    payload.body.len(),
                    format_error_chain(&err)
                );
                eprintln!("v0_iteration {detail}");
                if config.measured {
                    stats.errors.push(detail);
                }
                thread::sleep(Duration::from_millis(100));
            }
        }
    }

    stats.elapsed = started.elapsed();
    print_progress(config.phase, started, config.duration, &stats, client);
    stats
}

fn print_progress(
    phase: &str,
    started: Instant,
    duration: Duration,
    stats: &RunStats,
    client: &Client,
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
        .and_then(|metrics| max_map_value(&metrics.freshness_lag_seconds))
        .map(|lag| format!(" freshness_lag={lag:.1}s"))
        .unwrap_or_default();
    eprintln!(
        "v0_iteration progress phase={} elapsed={}/{} accepted={:.0}B/s status_counts={} queries={}/{} transport_errors={}{}{}",
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
                error: (response.status != 200)
                    .then(|| format!("query returned HTTP {}", response.status)),
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

fn sleep_until_next_tick(deadline: Instant) {
    let remaining = deadline.saturating_duration_since(Instant::now());
    if remaining.is_zero() {
        return;
    }
    thread::sleep(remaining.min(Duration::from_millis(10)));
}

#[derive(Clone, Copy)]
struct PhaseConfig {
    phase: &'static str,
    duration: Duration,
    target_bytes_per_sec: f64,
    query_interval: Duration,
    query_concurrency: usize,
    no_queries: bool,
    measured: bool,
    progress_interval: Duration,
    guard_deadline: Instant,
}

fn build_report(
    args: Args,
    workload: Workload,
    stats: RunStats,
    scraped: Option<ScrapedMetrics>,
    metric_samples: Vec<MetricSample>,
    resource_envelope: ResourceEnvelope,
    storage_config: Option<Value>,
) -> Report {
    let measured_seconds = stats.elapsed.as_secs_f64().max(0.001);
    let target_decoded_bytes_per_sec = args.target_decoded_bytes_per_sec();
    let actual_decoded_bytes_per_sec = stats.accepted_decoded_bytes as f64 / measured_seconds;
    let request_bytes_per_sec = stats.request_bytes_sent as f64 / measured_seconds;
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
        max_map_value(&metrics.freshness_lag_seconds)
    });
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

    let metric_snapshots = metric_samples
        .iter()
        .map(MetricSnapshotReport::from_sample)
        .collect::<Vec<_>>();

    let accepted_mib = stats.accepted_decoded_bytes as f64 / (1024.0 * 1024.0);
    let (
        freshness_lag_seconds,
        queue,
        storage,
        mut server_phase_timing,
        ducklake_maintenance_timing,
    ) = match scraped {
        Some(scraped) => (
            scraped.freshness_lag_seconds,
            Some(scraped.queue),
            Some(scraped.storage),
            scraped.server_phase_timing,
            scraped.ducklake_maintenance_timing,
        ),
        None => (
            BTreeMap::new(),
            None,
            None,
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

    let pass_fail_criteria = vec![
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

    let pass = failure_reasons.is_empty();

    Report {
        git_sha: git_sha(),
        benchmark_name: BENCH_NAME.to_string(),
        benchmark_version: BENCH_VERSION.to_string(),
        base_url: args.base_url.clone(),
        resource_envelope,
        storage_config,
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
            byte_mix: BTreeMap::from([
                ("logs".to_string(), 0.60),
                ("spans".to_string(), 0.25),
                ("metrics".to_string(), 0.15),
            ]),
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
        queue,
        storage,
        server_phase_timing,
        ducklake_maintenance_timing,
        metric_snapshots,
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
        "v0_iteration scenario={} pass={} actual={:.0}B/s target={:.0}B/s profile={} query_concurrency={}",
        report.scenario.name,
        report.pass,
        report.actual_decoded_bytes_per_sec,
        report.target_decoded_bytes_per_sec,
        report.query_profile.profile,
        report.query_profile.concurrency
    );
    println!(
        "workload services={} log_body_bytes={} trace_spans={} metric_series={} metric_description_bytes={}",
        report.workload_profile.service_count,
        report.workload_profile.log_body_bytes,
        report.workload_profile.trace_span_count,
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
        if !storage.ducklake_parquet_files.is_empty() || !storage.ducklake_inlined_rows.is_empty() {
            println!(
                "ducklake_layout parquet_files={:?} parquet_rows={:?} inlined_rows={:?}",
                storage.ducklake_parquet_files,
                storage.ducklake_parquet_rows,
                storage.ducklake_inlined_rows
            );
        }
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
    query_interval: Duration,
    query_concurrency: usize,
    query_pressure: QueryPressure,
    profile: BenchmarkProfile,
    workload: WorkloadProfile,
    progress_interval: Duration,
    max_runtime: Option<Duration>,
    no_queries_legacy: bool,
    report_dir: Option<PathBuf>,
}

impl Args {
    fn parse(args: impl Iterator<Item = String>) -> Result<Self> {
        let mut parsed = Self {
            base_url: DEFAULT_BASE_URL.to_string(),
            warmup: DEFAULT_WARMUP,
            duration: DEFAULT_DURATION,
            target_gb_per_day: DEFAULT_TARGET_GB_PER_DAY,
            query_interval: DEFAULT_QUERY_INTERVAL,
            query_concurrency: DEFAULT_QUERY_CONCURRENCY,
            query_pressure: QueryPressure::Medium,
            profile: BenchmarkProfile::MixedQuery,
            workload: WorkloadProfile::default(),
            progress_interval: DEFAULT_PROGRESS_INTERVAL,
            max_runtime: None,
            no_queries_legacy: false,
            report_dir: None,
        };
        let mut explicit_query_interval = false;
        let mut explicit_query_concurrency = false;
        let mut args = args.peekable();
        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--base-url" => {
                    parsed.base_url = args.next().context("--base-url requires a value")?;
                }
                "--warmup" => {
                    parsed.warmup =
                        parse_duration(&args.next().context("--warmup requires a value")?)?;
                }
                "--duration" => {
                    parsed.duration =
                        parse_duration(&args.next().context("--duration requires a value")?)?;
                }
                "--target-gb-day" => {
                    parsed.target_gb_per_day = args
                        .next()
                        .context("--target-gb-day requires a value")?
                        .parse()
                        .context("invalid --target-gb-day")?;
                }
                "--query-interval" => {
                    parsed.query_interval =
                        parse_duration(&args.next().context("--query-interval requires a value")?)?;
                    explicit_query_interval = true;
                }
                "--query-concurrency" => {
                    parsed.query_concurrency = args
                        .next()
                        .context("--query-concurrency requires a value")?
                        .parse()
                        .context("invalid --query-concurrency")?;
                    explicit_query_concurrency = true;
                }
                "--query-pressure" => {
                    parsed.query_pressure = QueryPressure::parse(
                        &args.next().context("--query-pressure requires a value")?,
                    )?;
                }
                "--profile" => {
                    parsed.profile = BenchmarkProfile::parse(
                        &args.next().context("--profile requires a value")?,
                    )?;
                }
                "--services" => {
                    parsed.workload.service_count = args
                        .next()
                        .context("--services requires a value")?
                        .parse()
                        .context("invalid --services")?;
                }
                "--log-body-bytes" => {
                    parsed.workload.log_body_bytes = args
                        .next()
                        .context("--log-body-bytes requires a value")?
                        .parse()
                        .context("invalid --log-body-bytes")?;
                }
                "--trace-spans" => {
                    parsed.workload.trace_span_count = args
                        .next()
                        .context("--trace-spans requires a value")?
                        .parse()
                        .context("invalid --trace-spans")?;
                }
                "--metric-series" => {
                    parsed.workload.metric_series_count = args
                        .next()
                        .context("--metric-series requires a value")?
                        .parse()
                        .context("invalid --metric-series")?;
                }
                "--metric-description-bytes" => {
                    parsed.workload.metric_description_bytes = args
                        .next()
                        .context("--metric-description-bytes requires a value")?
                        .parse()
                        .context("invalid --metric-description-bytes")?;
                }
                "--progress-interval" => {
                    parsed.progress_interval = parse_duration(
                        &args
                            .next()
                            .context("--progress-interval requires a value")?,
                    )?;
                }
                "--max-runtime" => {
                    parsed.max_runtime = Some(parse_duration(
                        &args.next().context("--max-runtime requires a value")?,
                    )?);
                }
                "--no-queries" => {
                    parsed.no_queries_legacy = true;
                    parsed.profile = BenchmarkProfile::IngestOnly;
                }
                "--report-dir" => {
                    parsed.report_dir = Some(PathBuf::from(
                        args.next().context("--report-dir requires a value")?,
                    ));
                }
                "--bench" => {}
                "--help" | "-h" => {
                    println!(
                        "cargo bench --bench v0_iteration -- [--base-url URL] [--warmup 2m] [--duration 20m] [--target-gb-day 100] [--profile ingest-only|mixed-query] [--query-pressure off|low|medium|high] [--query-interval 5s] [--query-concurrency 1] [--services 1] [--log-body-bytes 120000] [--trace-spans 16] [--metric-series 40] [--metric-description-bytes 192] [--progress-interval 30s] [--max-runtime 27m] [--no-queries] [--report-dir DIR]"
                    );
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

#[derive(Clone, Serialize)]
struct WorkloadProfile {
    service_count: usize,
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
            log_body_bytes: DEFAULT_LOG_BODY_BYTES,
            trace_span_count: DEFAULT_TRACE_SPAN_COUNT,
            trace_attribute_bytes: DEFAULT_TRACE_ATTRIBUTE_BYTES,
            metric_series_count: DEFAULT_METRIC_SERIES_COUNT,
            metric_description_bytes: DEFAULT_METRIC_DESCRIPTION_BYTES,
        }
    }
}

impl WorkloadProfile {
    fn validate(&self) -> Result<()> {
        if self.service_count == 0 {
            bail!("--services must be > 0");
        }
        if self.log_body_bytes == 0 {
            bail!("--log-body-bytes must be > 0");
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

#[derive(Clone)]
struct Workload {
    payloads: Vec<WorkloadPayload>,
}

impl Workload {
    fn new(run_started: chrono::DateTime<Utc>, profile: WorkloadProfile) -> Self {
        let base_nanos = run_started
            .timestamp_nanos_opt()
            .unwrap_or(run_started.timestamp_millis() * 1_000_000);
        Self {
            payloads: vec![
                WorkloadPayload::logs(base_nanos, &profile),
                WorkloadPayload::spans(base_nanos, &profile),
                WorkloadPayload::metrics(base_nanos, &profile),
            ],
        }
    }

    fn next_payload(
        &self,
        elapsed_seconds: f64,
        target_bytes_per_sec: f64,
        sent_by_signal: &BTreeMap<&'static str, usize>,
    ) -> Option<&WorkloadPayload> {
        self.payloads
            .iter()
            .map(|payload| {
                let desired = elapsed_seconds * target_bytes_per_sec * payload.ratio;
                let sent = sent_by_signal.get(payload.signal).copied().unwrap_or(0) as f64;
                (desired - sent, payload)
            })
            .filter(|(deficit, _)| *deficit > 0.0)
            .max_by(|(left, _), (right, _)| left.total_cmp(right))
            .map(|(_, payload)| payload)
    }
}

#[derive(Clone)]
struct WorkloadPayload {
    signal: &'static str,
    path: &'static str,
    content_type: &'static str,
    ratio: f64,
    body: Vec<u8>,
    decoded_bytes: usize,
    records_per_request: u64,
}

impl WorkloadPayload {
    fn logs(base_nanos: i64, profile: &WorkloadProfile) -> Self {
        let body = otlp_fixture::encode_logs(base_nanos, profile);
        Self {
            signal: "logs",
            path: "/v1/logs",
            content_type: otlp_fixture::CONTENT_TYPE,
            ratio: 0.60,
            decoded_bytes: body.len(),
            records_per_request: (profile.service_count * 8) as u64,
            body,
        }
    }

    fn spans(base_nanos: i64, profile: &WorkloadProfile) -> Self {
        let body = otlp_fixture::encode_traces(base_nanos, profile);
        Self {
            signal: "spans",
            path: "/v1/traces",
            content_type: otlp_fixture::CONTENT_TYPE,
            ratio: 0.25,
            decoded_bytes: body.len(),
            records_per_request: (profile.service_count * profile.trace_span_count) as u64,
            body,
        }
    }

    fn metrics(base_nanos: i64, profile: &WorkloadProfile) -> Self {
        let body = otlp_fixture::encode_metrics(base_nanos, profile);
        Self {
            signal: "metrics",
            path: "/v1/metrics",
            content_type: otlp_fixture::CONTENT_TYPE,
            ratio: 0.15,
            decoded_bytes: body.len(),
            records_per_request: (profile.service_count * profile.metric_series_count * 2) as u64,
            body,
        }
    }
}

fn deterministic_ascii(seed: u64, len: usize) -> String {
    let mut state = DETERMINISTIC_SEED ^ seed;
    let mut out = String::with_capacity(len);
    for _ in 0..len {
        state ^= state << 7;
        state ^= state >> 9;
        state ^= state << 8;
        let ch = b'a' + (state % 26) as u8;
        out.push(ch as char);
    }
    out
}

fn deterministic_bytes(seed: u64, len: usize) -> Vec<u8> {
    let mut state = DETERMINISTIC_SEED ^ seed;
    let mut out = Vec::with_capacity(len);
    for _ in 0..len {
        state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
        out.push((state & 0xff) as u8);
    }
    out
}

mod otlp_fixture {
    use super::{
        deterministic_ascii, deterministic_bytes, WorkloadProfile, BENCH_VERSION, SCENARIO_NAME,
    };
    use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
    use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
    use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
    use opentelemetry_proto::tonic::common::v1::{
        any_value, AnyValue, InstrumentationScope, KeyValue,
    };
    use opentelemetry_proto::tonic::logs::v1::{
        LogRecord, ResourceLogs, ScopeLogs, SeverityNumber,
    };
    use opentelemetry_proto::tonic::metrics::v1::{
        metric, number_data_point, AggregationTemporality, Gauge, Metric, NumberDataPoint,
        ResourceMetrics, ScopeMetrics, Sum,
    };
    use opentelemetry_proto::tonic::resource::v1::Resource;
    use opentelemetry_proto::tonic::trace::v1::{
        span, status, ResourceSpans, ScopeSpans, Span, Status,
    };
    use prost::Message;

    pub const CONTENT_TYPE: &str = "application/x-protobuf";

    pub fn encode_logs(base_nanos: i64, profile: &WorkloadProfile) -> Vec<u8> {
        ExportLogsServiceRequest {
            resource_logs: (0..profile.service_count)
                .map(|service_idx| ResourceLogs {
                    resource: Some(resource(service_idx)),
                    scope_logs: vec![ScopeLogs {
                        scope: Some(scope()),
                        log_records: (0..8)
                            .map(|idx| {
                                let logical_idx = service_idx as i64 * 1_000 + idx;
                                let nanos = nanos(base_nanos, logical_idx);
                                LogRecord {
                                    time_unix_nano: nanos,
                                    observed_time_unix_nano: nanos,
                                    severity_number: SeverityNumber::Info as i32,
                                    severity_text: "INFO".to_string(),
                                    body: Some(any_str(format!(
                                        "canardstack-v0-iteration log event {} {}",
                                        logical_idx,
                                        deterministic_ascii(
                                            logical_idx as u64,
                                            profile.log_body_bytes
                                        )
                                    ))),
                                    attributes: vec![
                                        kv_str("http.route", "/bench"),
                                        kv_str("workload.id", SCENARIO_NAME),
                                    ],
                                    trace_id: deterministic_bytes(logical_idx as u64, 16),
                                    span_id: deterministic_bytes(logical_idx as u64 + 10_000, 8),
                                    ..Default::default()
                                }
                            })
                            .collect(),
                        schema_url: String::new(),
                    }],
                    schema_url: String::new(),
                })
                .collect(),
        }
        .encode_to_vec()
    }

    pub fn encode_traces(base_nanos: i64, profile: &WorkloadProfile) -> Vec<u8> {
        ExportTraceServiceRequest {
            resource_spans: (0..profile.service_count)
                .map(|service_idx| ResourceSpans {
                    resource: Some(resource(service_idx)),
                    scope_spans: vec![ScopeSpans {
                        scope: Some(scope()),
                        spans: (0..profile.trace_span_count as i64)
                            .map(|idx| {
                                let logical_idx = service_idx as i64 * 10_000 + idx;
                                let start = nanos(base_nanos, logical_idx);
                                Span {
                                    trace_id: deterministic_bytes(service_idx as u64, 16),
                                    span_id: deterministic_bytes(logical_idx as u64 + 20_000, 8),
                                    name: "GET /bench".to_string(),
                                    kind: span::SpanKind::Server as i32,
                                    start_time_unix_nano: start,
                                    end_time_unix_nano: start
                                        + 12_000_000
                                        + (idx % 17) as u64 * 1_000_000,
                                    attributes: vec![
                                        kv_str("http.request.method", "GET"),
                                        kv_i64("http.response.status_code", 200),
                                        kv_str("http.route", "/bench"),
                                        kv_str("workload.bucket", format!("bucket-{}", idx % 16)),
                                        kv_str(
                                            "payload.sample",
                                            deterministic_ascii(
                                                logical_idx as u64 + 1_000,
                                                profile.trace_attribute_bytes,
                                            ),
                                        ),
                                    ],
                                    status: Some(Status {
                                        code: status::StatusCode::Ok as i32,
                                        message: String::new(),
                                    }),
                                    ..Default::default()
                                }
                            })
                            .collect(),
                        schema_url: String::new(),
                    }],
                    schema_url: String::new(),
                })
                .collect(),
        }
        .encode_to_vec()
    }

    pub fn encode_metrics(base_nanos: i64, profile: &WorkloadProfile) -> Vec<u8> {
        ExportMetricsServiceRequest {
            resource_metrics: (0..profile.service_count)
                .map(|service_idx| ResourceMetrics {
                    resource: Some(resource(service_idx)),
                    scope_metrics: vec![ScopeMetrics {
                        scope: Some(scope()),
                        metrics: vec![
                            Metric {
                                name: "canardstack.bench.gauge".to_string(),
                                description: deterministic_ascii(
                                    30_000 + service_idx as u64,
                                    profile.metric_description_bytes,
                                ),
                                unit: "1".to_string(),
                                data: Some(metric::Data::Gauge(Gauge {
                                    data_points: (0..profile.metric_series_count as i64)
                                        .map(|idx| {
                                            number_point(
                                                base_nanos,
                                                service_idx,
                                                idx,
                                                "gauge",
                                                NumberValue::Double(100.0 + (idx % 23) as f64),
                                            )
                                        })
                                        .collect(),
                                })),
                                metadata: vec![],
                            },
                            Metric {
                                name: "canardstack.bench.sum".to_string(),
                                description: deterministic_ascii(
                                    40_000 + service_idx as u64,
                                    profile.metric_description_bytes,
                                ),
                                unit: "1".to_string(),
                                data: Some(metric::Data::Sum(Sum {
                                    aggregation_temporality: AggregationTemporality::Cumulative
                                        as i32,
                                    is_monotonic: true,
                                    data_points: (0..profile.metric_series_count as i64)
                                        .map(|idx| {
                                            number_point(
                                                base_nanos,
                                                service_idx,
                                                idx,
                                                "sum",
                                                NumberValue::Int(10_000 + idx),
                                            )
                                        })
                                        .collect(),
                                })),
                                metadata: vec![],
                            },
                        ],
                        schema_url: String::new(),
                    }],
                    schema_url: String::new(),
                })
                .collect(),
        }
        .encode_to_vec()
    }

    pub fn kv_str(key: impl Into<String>, value: impl Into<String>) -> KeyValue {
        KeyValue {
            key: key.into(),
            value: Some(any_str(value)),
        }
    }

    pub fn kv_i64(key: impl Into<String>, value: i64) -> KeyValue {
        KeyValue {
            key: key.into(),
            value: Some(AnyValue {
                value: Some(any_value::Value::IntValue(value)),
            }),
        }
    }

    pub fn resource(service_idx: usize) -> Resource {
        Resource {
            attributes: vec![
                kv_str("service.name", service_name(service_idx)),
                kv_str("deployment.environment", "bench"),
                kv_str("benchmark.scenario", SCENARIO_NAME),
            ],
            dropped_attributes_count: 0,
            entity_refs: vec![],
        }
    }

    pub fn scope() -> InstrumentationScope {
        InstrumentationScope {
            name: "v0_iteration".to_string(),
            version: BENCH_VERSION.to_string(),
            attributes: vec![],
            dropped_attributes_count: 0,
        }
    }

    fn any_str(value: impl Into<String>) -> AnyValue {
        AnyValue {
            value: Some(any_value::Value::StringValue(value.into())),
        }
    }

    enum NumberValue {
        Double(f64),
        Int(i64),
    }

    fn number_point(
        base_nanos: i64,
        service_idx: usize,
        idx: i64,
        series_prefix: &str,
        value: NumberValue,
    ) -> NumberDataPoint {
        NumberDataPoint {
            attributes: vec![
                kv_str("route", "/bench"),
                kv_str("series", format!("{series_prefix}-{service_idx}-{idx}")),
            ],
            start_time_unix_nano: nanos(base_nanos, 0),
            time_unix_nano: nanos(base_nanos, service_idx as i64 * 10_000 + idx),
            exemplars: vec![],
            flags: 0,
            value: Some(match value {
                NumberValue::Double(value) => number_data_point::Value::AsDouble(value),
                NumberValue::Int(value) => number_data_point::Value::AsInt(value),
            }),
        }
    }

    fn nanos(base_nanos: i64, idx: i64) -> u64 {
        (base_nanos + idx * 1_000_000) as u64
    }

    fn service_name(service_idx: usize) -> String {
        if service_idx == 0 {
            "bench-checkout".to_string()
        } else {
            format!("bench-service-{service_idx}")
        }
    }
}

struct QueryPlan {
    run_started: chrono::DateTime<Utc>,
    next_idx: usize,
}

impl QueryPlan {
    fn new(run_started: chrono::DateTime<Utc>) -> Self {
        Self {
            run_started,
            next_idx: 0,
        }
    }

    fn next(&mut self) -> String {
        let from = self.run_started - ChronoDuration::minutes(10);
        let to = Utc::now() + ChronoDuration::minutes(1);
        let path = match self.next_idx % 3 {
            0 => format!(
                "/loki/api/v1/query_range?query={}&start={}&end={}&limit=100",
                enc("{service_name=\"bench-checkout\"} |= \"canardstack-v0-iteration\""),
                enc(&from.to_rfc3339()),
                enc(&to.to_rfc3339())
            ),
            1 => format!(
                "/api/v1/query_range?query={}&start={}&end={}&step=60",
                enc("avg(canardstack.bench.gauge{service_name=\"bench-checkout\"})"),
                enc(&from.to_rfc3339()),
                enc(&to.to_rfc3339())
            ),
            _ => format!(
                "/api/search?start={}&end={}&service.name=bench-checkout&limit=10",
                enc(&from.to_rfc3339()),
                enc(&to.to_rfc3339())
            ),
        };
        self.next_idx += 1;
        path
    }
}

#[derive(Default)]
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
    errors: Vec<String>,
    guard_exceeded: bool,
}

impl RunStats {
    fn status_counts_inc(&mut self, status: u16) {
        *self.status_counts.entry(status).or_default() += 1;
    }
}

#[derive(Clone)]
struct Client {
    host: String,
    port: u16,
}

struct Response {
    status: u16,
    body: String,
}

impl Client {
    fn new(base_url: &str) -> Result<Self> {
        let rest = base_url
            .strip_prefix("http://")
            .ok_or_else(|| anyhow::anyhow!("only http:// base URLs are supported"))?;
        let authority = rest.trim_end_matches('/');
        let (host, port) = authority.split_once(':').unwrap_or((authority, "80"));
        Ok(Self {
            host: host.to_string(),
            port: port.parse().context("parse base URL port")?,
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
        let addr = (self.host.as_str(), self.port)
            .to_socket_addrs()
            .with_context(|| format!("resolve http://{}:{}", self.host, self.port))?
            .next()
            .ok_or_else(|| anyhow::anyhow!("no socket address for {}", self.host))?;
        let mut stream = TcpStream::connect_timeout(&addr, CLIENT_REQUEST_TIMEOUT)
            .with_context(|| format!("connect to {}", fmt_addr(addr)))?;
        stream.set_read_timeout(Some(CLIENT_REQUEST_TIMEOUT))?;
        stream.set_write_timeout(Some(CLIENT_REQUEST_TIMEOUT))?;
        let deadline = Instant::now() + CLIENT_REQUEST_TIMEOUT;
        let (content_type, body) = body.unwrap_or(("application/octet-stream", b""));
        let mut head = format!(
            "{method} {path} HTTP/1.1\r\nhost: {}\r\naccept: application/json\r\ncontent-length: {}\r\nconnection: close\r\n",
            self.host,
            body.len()
        );
        if let Some(token) = bearer {
            head.push_str(&format!("authorization: Bearer {token}\r\n"));
        }
        if method == "POST" {
            head.push_str(&format!("content-type: {content_type}\r\n"));
        }
        head.push_str("\r\n");
        write_all_retry(&mut stream, head.as_bytes(), deadline).context("write request headers")?;
        write_all_retry(&mut stream, body, deadline).context("write request body")?;
        read_response(stream, deadline)
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

fn read_response(mut stream: TcpStream, deadline: Instant) -> Result<Response> {
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
    Ok(Response {
        status,
        body: body.to_string(),
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
    freshness_lag_seconds: BTreeMap<String, f64>,
    queue: QueueReport,
    storage: StorageReport,
    server_phase_timing: BTreeMap<String, PhaseTimingReport>,
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
        match metric.name.as_str() {
            "canardstack_ingest_to_query_lag_seconds" => {
                if let Some(table) = metric.labels.get("table") {
                    out.freshness_lag_seconds
                        .insert(table.clone(), metric.value);
                }
            }
            "canardstack_ingest_queue_rows" => {
                out.queue.max_rows = Some(out.queue.max_rows.unwrap_or(0.0).max(metric.value));
            }
            "canardstack_ingest_queue_bytes" => {
                out.queue.max_bytes = Some(out.queue.max_bytes.unwrap_or(0.0).max(metric.value));
            }
            "canardstack_ingest_queue_oldest_age_seconds" => {
                out.queue.max_oldest_age_seconds = Some(
                    out.queue
                        .max_oldest_age_seconds
                        .unwrap_or(0.0)
                        .max(metric.value),
                );
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
            "canardstack_ducklake_parquet_files" => {
                if let Some(table) = metric.labels.get("table") {
                    out.storage
                        .ducklake_parquet_files
                        .insert(table.clone(), metric.value as u64);
                }
            }
            "canardstack_ducklake_parquet_rows" => {
                if let Some(table) = metric.labels.get("table") {
                    out.storage
                        .ducklake_parquet_rows
                        .insert(table.clone(), metric.value as u64);
                }
            }
            "canardstack_ducklake_inlined_rows" => {
                if let Some(table) = metric.labels.get("table") {
                    out.storage
                        .ducklake_inlined_rows
                        .insert(table.clone(), metric.value as u64);
                }
            }
            "canardstack_phase_duration_seconds_count" => {
                phase_counts.insert(labels_key(&metric.labels), metric.value);
            }
            "canardstack_phase_duration_seconds_sum" => {
                phase_sums.insert(labels_key(&metric.labels), metric.value);
            }
            "canardstack_ducklake_flush_inlined_duration_seconds_count"
            | "canardstack_ducklake_compaction_duration_seconds_count" => {
                ducklake_counts.insert(
                    format!(
                        "{} {}",
                        metric.name.trim_end_matches("_count"),
                        labels_key(&metric.labels)
                    ),
                    metric.value,
                );
            }
            "canardstack_ducklake_flush_inlined_duration_seconds_sum"
            | "canardstack_ducklake_compaction_duration_seconds_sum" => {
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

fn labels_key(labels: &BTreeMap<String, String>) -> String {
    labels
        .iter()
        .map(|(key, value)| format!("{key}={value}"))
        .collect::<Vec<_>>()
        .join(",")
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

fn max_map_value(values: &BTreeMap<String, f64>) -> Option<f64> {
    values.values().copied().reduce(f64::max)
}

fn top_phase_timings(
    phases: &BTreeMap<String, PhaseTimingReport>,
) -> Vec<(&String, &PhaseTimingReport)> {
    let mut timings = phases.iter().collect::<Vec<_>>();
    timings.sort_by(|(_, left), (_, right)| right.sum_seconds.total_cmp(&left.sum_seconds));
    timings
}

#[derive(Serialize)]
struct Report {
    git_sha: Option<String>,
    benchmark_name: String,
    benchmark_version: String,
    base_url: String,
    resource_envelope: ResourceEnvelope,
    storage_config: Option<Value>,
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
    queue: Option<QueueReport>,
    storage: Option<StorageReport>,
    server_phase_timing: BTreeMap<String, PhaseTimingReport>,
    ducklake_maintenance_timing: BTreeMap<String, PhaseTimingReport>,
    metric_snapshots: Vec<MetricSnapshotReport>,
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
    ducklake_parquet_files: BTreeMap<String, u64>,
    ducklake_parquet_rows: BTreeMap<String, u64>,
    ducklake_inlined_rows: BTreeMap<String, u64>,
}

#[derive(Clone, Serialize)]
struct PhaseTimingReport {
    count: u64,
    sum_seconds: f64,
    avg_seconds: Option<f64>,
    seconds_per_mib: Option<f64>,
    wall_time_share: Option<f64>,
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
