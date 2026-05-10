use anyhow::{bail, Context, Result};
use chrono::{Duration as ChronoDuration, Utc};
use serde::Serialize;
use serde_json::Value;
use std::collections::BTreeMap;
use std::env;
use std::fs;
use std::io::{ErrorKind, Read, Write};
use std::net::TcpStream;
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
const DEFAULT_PROGRESS_INTERVAL: Duration = Duration::from_secs(30);
const DEFAULT_MAX_RUNTIME_GRACE: Duration = Duration::from_secs(5 * 60);
const NEAR_TIMEOUT_MS: f64 = 25_000.0;
const CLIENT_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

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

    let run_started = Utc::now();
    let workload = Workload::new(run_started);
    let target_bytes_per_sec = args.target_decoded_bytes_per_sec();
    let guard_deadline = Instant::now() + args.max_runtime();

    eprintln!(
        "v0_iteration: warmup={} measured={} target={:.0} decoded B/s base_url={} progress={} max_runtime={}",
        fmt_duration(args.warmup),
        fmt_duration(args.duration),
        target_bytes_per_sec,
        args.base_url,
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
            no_queries: args.no_queries,
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
            no_queries: args.no_queries,
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

    let report = build_report(args.clone(), workload, measured, scraped, metric_samples);
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
            run_query(client, api_key, query_plan, &mut stats);
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

fn run_query(client: &Client, api_key: &str, query_plan: &mut QueryPlan, stats: &mut RunStats) {
    let path = query_plan.next();
    let started = Instant::now();
    match client.get(&path, Some(api_key)) {
        Ok(response) => {
            stats.status_counts_inc(response.status);
            stats.query_requests += 1;
            stats
                .query_latency_ms
                .push(started.elapsed().as_secs_f64() * 1000.0);
            if response.status != 200 {
                stats.query_failures += 1;
                stats
                    .errors
                    .push(format!("query returned HTTP {}", response.status));
            }
        }
        Err(err) => {
            stats.query_requests += 1;
            stats.query_failures += 1;
            stats.transport_errors += 1;
            stats.errors.push(format!(
                "query transport error path={path} elapsed_ms={:.1}: {}",
                started.elapsed().as_secs_f64() * 1000.0,
                format_error_chain(&err)
            ));
            thread::sleep(Duration::from_millis(100));
        }
    }
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
    if actual_decoded_bytes_per_sec < target_decoded_bytes_per_sec * 0.90 {
        failure_reasons.push(format!(
            "accepted decoded throughput {:.0} B/s below 90% of target {:.0} B/s",
            actual_decoded_bytes_per_sec, target_decoded_bytes_per_sec
        ));
        smell_observations.push("throughput collapsed below the modest v0 target".to_string());
    }
    if args.no_queries {
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
    if !args.no_queries
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
    let (freshness_lag_seconds, queue, storage, mut server_phase_timing) = match scraped {
        Some(scraped) => (
            scraped.freshness_lag_seconds,
            Some(scraped.queue),
            Some(scraped.storage),
            scraped.server_phase_timing,
        ),
        None => (BTreeMap::new(), None, None, BTreeMap::new()),
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

    let pass = failure_reasons.is_empty();

    Report {
        git_sha: git_sha(),
        benchmark_name: BENCH_NAME.to_string(),
        benchmark_version: BENCH_VERSION.to_string(),
        base_url: args.base_url,
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
        metric_snapshots,
        queue_oldest_age_trend,
        queue_rows_trend,
        queue_bytes_trend,
        freshness_lag_trend,
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
        "v0_iteration scenario={} pass={} actual={:.0}B/s target={:.0}B/s",
        report.scenario.name,
        report.pass,
        report.actual_decoded_bytes_per_sec,
        report.target_decoded_bytes_per_sec
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
    progress_interval: Duration,
    max_runtime: Option<Duration>,
    no_queries: bool,
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
            progress_interval: DEFAULT_PROGRESS_INTERVAL,
            max_runtime: None,
            no_queries: false,
            report_dir: None,
        };
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
                    parsed.no_queries = true;
                }
                "--report-dir" => {
                    parsed.report_dir = Some(PathBuf::from(
                        args.next().context("--report-dir requires a value")?,
                    ));
                }
                "--bench" => {}
                "--help" | "-h" => {
                    println!(
                        "cargo bench --bench v0_iteration -- [--base-url URL] [--warmup 2m] [--duration 20m] [--target-gb-day 100] [--query-interval 5s] [--progress-interval 30s] [--max-runtime 27m] [--no-queries] [--report-dir DIR]"
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
        if parsed.query_interval.is_zero() {
            bail!("--query-interval must be positive");
        }
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

#[derive(Clone)]
struct Workload {
    payloads: Vec<WorkloadPayload>,
}

impl Workload {
    fn new(run_started: chrono::DateTime<Utc>) -> Self {
        let base_nanos = run_started
            .timestamp_nanos_opt()
            .unwrap_or(run_started.timestamp_millis() * 1_000_000);
        Self {
            payloads: vec![
                WorkloadPayload::logs(base_nanos),
                WorkloadPayload::spans(base_nanos),
                WorkloadPayload::metrics(base_nanos),
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
    fn logs(base_nanos: i64) -> Self {
        let body = otlp_fixture::encode_logs(base_nanos);
        Self {
            signal: "logs",
            path: "/v1/logs",
            content_type: otlp_fixture::CONTENT_TYPE,
            ratio: 0.60,
            decoded_bytes: body.len(),
            records_per_request: 8,
            body,
        }
    }

    fn spans(base_nanos: i64) -> Self {
        let body = otlp_fixture::encode_traces(base_nanos);
        Self {
            signal: "spans",
            path: "/v1/traces",
            content_type: otlp_fixture::CONTENT_TYPE,
            ratio: 0.25,
            decoded_bytes: body.len(),
            records_per_request: 16,
            body,
        }
    }

    fn metrics(base_nanos: i64) -> Self {
        let body = otlp_fixture::encode_metrics(base_nanos);
        Self {
            signal: "metrics",
            path: "/v1/metrics",
            content_type: otlp_fixture::CONTENT_TYPE,
            ratio: 0.15,
            decoded_bytes: body.len(),
            records_per_request: 80,
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
    use super::{deterministic_ascii, deterministic_bytes, BENCH_VERSION, SCENARIO_NAME};
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

    pub fn encode_logs(base_nanos: i64) -> Vec<u8> {
        ExportLogsServiceRequest {
            resource_logs: vec![ResourceLogs {
                resource: Some(resource()),
                scope_logs: vec![ScopeLogs {
                    scope: Some(scope()),
                    log_records: (0..8)
                        .map(|idx| {
                            let nanos = nanos(base_nanos, idx);
                            LogRecord {
                                time_unix_nano: nanos,
                                observed_time_unix_nano: nanos,
                                severity_number: SeverityNumber::Info as i32,
                                severity_text: "INFO".to_string(),
                                body: Some(any_str(format!(
                                    "canardstack-v0-iteration log event {} {}",
                                    idx,
                                    deterministic_ascii(idx as u64, 120_000)
                                ))),
                                attributes: vec![
                                    kv_str("http.route", "/bench"),
                                    kv_str("workload.id", SCENARIO_NAME),
                                ],
                                trace_id: deterministic_bytes(idx as u64, 16),
                                span_id: deterministic_bytes(idx as u64 + 10_000, 8),
                                ..Default::default()
                            }
                        })
                        .collect(),
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        }
        .encode_to_vec()
    }

    pub fn encode_traces(base_nanos: i64) -> Vec<u8> {
        ExportTraceServiceRequest {
            resource_spans: vec![ResourceSpans {
                resource: Some(resource()),
                scope_spans: vec![ScopeSpans {
                    scope: Some(scope()),
                    spans: (0..16)
                        .map(|idx| {
                            let start = nanos(base_nanos, idx);
                            Span {
                                trace_id: deterministic_bytes(idx as u64, 16),
                                span_id: deterministic_bytes(idx as u64 + 20_000, 8),
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
                                        deterministic_ascii(idx as u64 + 1_000, 48_000),
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
            }],
        }
        .encode_to_vec()
    }

    pub fn encode_metrics(base_nanos: i64) -> Vec<u8> {
        ExportMetricsServiceRequest {
            resource_metrics: vec![ResourceMetrics {
                resource: Some(resource()),
                scope_metrics: vec![ScopeMetrics {
                    scope: Some(scope()),
                    metrics: vec![
                        Metric {
                            name: "canardstack.bench.gauge".to_string(),
                            description: deterministic_ascii(30_000, 280_000),
                            unit: "1".to_string(),
                            data: Some(metric::Data::Gauge(Gauge {
                                data_points: (0..40)
                                    .map(|idx| {
                                        number_point(
                                            base_nanos,
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
                            description: deterministic_ascii(40_000, 280_000),
                            unit: "1".to_string(),
                            data: Some(metric::Data::Sum(Sum {
                                aggregation_temporality: AggregationTemporality::Cumulative as i32,
                                is_monotonic: true,
                                data_points: (0..40)
                                    .map(|idx| {
                                        number_point(
                                            base_nanos,
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
            }],
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

    pub fn resource() -> Resource {
        Resource {
            attributes: vec![
                kv_str("service.name", "bench-checkout"),
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
        idx: i64,
        series_prefix: &str,
        value: NumberValue,
    ) -> NumberDataPoint {
        NumberDataPoint {
            attributes: vec![
                kv_str("route", "/bench"),
                kv_str("series", format!("{series_prefix}-{}", idx % 20)),
            ],
            start_time_unix_nano: nanos(base_nanos, 0),
            time_unix_nano: nanos(base_nanos, idx),
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
        let mut stream = TcpStream::connect((self.host.as_str(), self.port))
            .with_context(|| format!("connect to http://{}:{}", self.host, self.port))?;
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
            Ok(read) => bytes.extend_from_slice(&buf[..read]),
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

#[derive(Clone, Default)]
struct ScrapedMetrics {
    freshness_lag_seconds: BTreeMap<String, f64>,
    queue: QueueReport,
    storage: StorageReport,
    server_phase_timing: BTreeMap<String, PhaseTimingReport>,
}

fn scrape_metrics(text: &str) -> ScrapedMetrics {
    let mut out = ScrapedMetrics::default();
    let mut phase_counts: BTreeMap<String, f64> = BTreeMap::new();
    let mut phase_sums: BTreeMap<String, f64> = BTreeMap::new();

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
            "canardstack_phase_duration_seconds_count" => {
                phase_counts.insert(labels_key(&metric.labels), metric.value);
            }
            "canardstack_phase_duration_seconds_sum" => {
                phase_sums.insert(labels_key(&metric.labels), metric.value);
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
    metric_snapshots: Vec<MetricSnapshotReport>,
    queue_oldest_age_trend: TrendReport,
    queue_rows_trend: TrendReport,
    queue_bytes_trend: TrendReport,
    freshness_lag_trend: TrendReport,
    smell_observations: Vec<String>,
    pass: bool,
    failure_reasons: Vec<String>,
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
