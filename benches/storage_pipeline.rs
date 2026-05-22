use anyhow::{bail, Context, Result};
use arrow58::array::{
    BooleanArray, Float64Array, Int32Array, Int64Array, StringArray, TimestampMicrosecondArray,
};
use arrow58::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow58::record_batch::RecordBatch;
use canardstack::config::Config;
use canardstack::ingest::Signal;
use canardstack::storage::{ArrowBatchBuffer, ArrowBatchBufferTiming, Storage};
use chrono::Utc;
use std::collections::BTreeMap;
use std::env;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tempfile::TempDir;

const BENCH_NAME: &str = "storage_pipeline";
const DEFAULT_ROWS: usize = 50_000;
const DEFAULT_ITERATIONS: usize = 4;
const DEFAULT_LOG_BODY_BYTES: usize = 512;
const DEFAULT_TRACE_ATTRIBUTE_BYTES: usize = 512;
const DEFAULT_METRIC_DESCRIPTION_BYTES: usize = 256;

fn main() {
    if let Err(err) = run() {
        eprintln!("{BENCH_NAME} failed: {err:#}");
        std::process::exit(1);
    }
}

fn run() -> Result<()> {
    let args = Args::parse(env::args().skip(1))?;
    let (config, _tempdir) = storage_config(&args)?;
    let storage = Storage::open(&config)?;
    let mut phase_stats = PhaseStats::default();
    let mut total_rows = 0usize;
    let mut total_arrow_bytes = 0usize;
    let mut sealed_rows = 0usize;
    let mut sealed_files = 0usize;

    eprintln!(
        "{BENCH_NAME}: signals={} rows_per_batch={} iterations={} data_dir={}",
        args.signal,
        args.rows,
        args.iterations,
        config
            .duckdb_path
            .parent()
            .unwrap_or_else(|| std::path::Path::new("."))
            .display()
    );

    let mut storage_elapsed = Duration::ZERO;
    for iteration in 0..args.iterations {
        let mut generated = Vec::new();
        for signal in args.signals() {
            let batch = make_batch(signal, args.rows, iteration, &args)?;
            total_rows += batch.num_rows();
            total_arrow_bytes += batch.get_array_memory_size().max(batch.num_rows());
            generated.push((signal, batch));
        }

        let started = Instant::now();
        for (signal, batch) in generated {
            let result = storage.buffer_arrow_batches(&[ArrowBatchBuffer {
                table: signal,
                batch: &batch,
                source_format: "storage_pipeline_bench",
            }])?;
            phase_stats.record_timings(result.timings);
        }
        let flush = storage.flush_immutable_segments(true)?;
        sealed_rows += flush.sealed_rows;
        sealed_files += flush.sealed_files;
        phase_stats.record_timings(flush.timings);
        storage_elapsed += started.elapsed();
    }

    print_summary(
        &args,
        storage_elapsed,
        total_rows,
        total_arrow_bytes,
        sealed_rows,
        sealed_files,
    );
    phase_stats.print_csv();
    Ok(())
}

fn storage_config(args: &Args) -> Result<(Config, Option<TempDir>)> {
    if let Some(data_dir) = &args.data_dir {
        std::fs::create_dir_all(data_dir)
            .with_context(|| format!("create data dir {}", data_dir.display()))?;
        let mut config = Config::test(data_dir.join("canardstack.duckdb"));
        config.local_storage_dir = data_dir.join("storage");
        config.immutable_segment_target_bytes = args.segment_target_bytes;
        config.immutable_segment_max_age = Duration::from_secs(3_600);
        return Ok((config, None));
    }

    let tempdir = tempfile::tempdir().context("create temporary storage benchmark dir")?;
    let mut config = Config::test(tempdir.path().join("canardstack.duckdb"));
    config.immutable_segment_target_bytes = args.segment_target_bytes;
    config.immutable_segment_max_age = Duration::from_secs(3_600);
    Ok((config, Some(tempdir)))
}

fn print_summary(
    args: &Args,
    elapsed: Duration,
    total_rows: usize,
    total_arrow_bytes: usize,
    sealed_rows: usize,
    sealed_files: usize,
) {
    let seconds = elapsed.as_secs_f64();
    let arrow_mib = total_arrow_bytes as f64 / 1024.0 / 1024.0;
    println!(
        "summary,name,signals,rows_per_batch,iterations,total_rows,sealed_rows,sealed_files,total_arrow_mib,total_seconds,rows_per_sec,arrow_mib_per_sec"
    );
    println!(
        "summary,{BENCH_NAME},{},{},{},{},{},{},{:.3},{:.6},{:.0},{:.3}",
        args.signal,
        args.rows,
        args.iterations,
        total_rows,
        sealed_rows,
        sealed_files,
        arrow_mib,
        seconds,
        total_rows as f64 / seconds,
        arrow_mib / seconds,
    );
}

#[derive(Default)]
struct PhaseStats {
    by_phase: BTreeMap<(String, String), PhaseStat>,
}

#[derive(Default)]
struct PhaseStat {
    count: usize,
    rows: usize,
    seconds: f64,
}

impl PhaseStats {
    fn record_timings(&mut self, timings: Vec<ArrowBatchBufferTiming>) {
        for timing in timings {
            let key = (timing.table.as_str().to_string(), timing.phase.to_string());
            let stat = self.by_phase.entry(key).or_default();
            stat.count += 1;
            stat.rows += timing.rows;
            stat.seconds += timing.seconds;
        }
    }

    fn print_csv(&self) {
        println!("phase,signal,phase,count,rows,total_ms,avg_ms,rows_per_sec");
        for ((signal, phase), stat) in &self.by_phase {
            let avg_ms = if stat.count == 0 {
                0.0
            } else {
                stat.seconds * 1000.0 / stat.count as f64
            };
            let rows_per_sec = if stat.seconds <= 0.0 {
                0.0
            } else {
                stat.rows as f64 / stat.seconds
            };
            println!(
                "phase,{signal},{phase},{},{},{:.3},{:.3},{:.0}",
                stat.count,
                stat.rows,
                stat.seconds * 1000.0,
                avg_ms,
                rows_per_sec
            );
        }
    }
}

#[derive(Clone, Debug)]
struct Args {
    rows: usize,
    iterations: usize,
    signal: SignalSelection,
    data_dir: Option<PathBuf>,
    segment_target_bytes: usize,
    log_body_bytes: usize,
    trace_attribute_bytes: usize,
    metric_description_bytes: usize,
}

impl Args {
    fn parse(mut args: impl Iterator<Item = String>) -> Result<Self> {
        let mut parsed = Self {
            rows: DEFAULT_ROWS,
            iterations: DEFAULT_ITERATIONS,
            signal: SignalSelection::All,
            data_dir: None,
            segment_target_bytes: 64 * 1024 * 1024,
            log_body_bytes: DEFAULT_LOG_BODY_BYTES,
            trace_attribute_bytes: DEFAULT_TRACE_ATTRIBUTE_BYTES,
            metric_description_bytes: DEFAULT_METRIC_DESCRIPTION_BYTES,
        };

        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--rows" => parsed.rows = parse_next(&mut args, "--rows")?,
                "--iterations" => parsed.iterations = parse_next(&mut args, "--iterations")?,
                "--signal" => {
                    parsed.signal = args
                        .next()
                        .context("--signal requires logs|spans|metric-gauge|metric-sum|all")?
                        .parse()?;
                }
                "--data-dir" => {
                    parsed.data_dir = Some(PathBuf::from(
                        args.next().context("--data-dir requires a path")?,
                    ));
                }
                "--segment-target-bytes" => {
                    parsed.segment_target_bytes = parse_next(&mut args, "--segment-target-bytes")?;
                }
                "--log-body-bytes" => {
                    parsed.log_body_bytes = parse_next(&mut args, "--log-body-bytes")?;
                }
                "--trace-attribute-bytes" => {
                    parsed.trace_attribute_bytes =
                        parse_next(&mut args, "--trace-attribute-bytes")?;
                }
                "--metric-description-bytes" => {
                    parsed.metric_description_bytes =
                        parse_next(&mut args, "--metric-description-bytes")?;
                }
                "--help" | "-h" => {
                    print_help();
                    std::process::exit(0);
                }
                "--bench" => {}
                other => bail!("unknown argument {other}; pass --help for usage"),
            }
        }

        if parsed.rows == 0 || parsed.iterations == 0 {
            bail!("--rows and --iterations must be > 0");
        }
        Ok(parsed)
    }

    fn signals(&self) -> Vec<Signal> {
        match self.signal {
            SignalSelection::Logs => vec![Signal::Logs],
            SignalSelection::Spans => vec![Signal::Spans],
            SignalSelection::MetricGauge => vec![Signal::MetricGauge],
            SignalSelection::MetricSum => vec![Signal::MetricSum],
            SignalSelection::All => vec![
                Signal::Logs,
                Signal::Spans,
                Signal::MetricGauge,
                Signal::MetricSum,
            ],
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum SignalSelection {
    Logs,
    Spans,
    MetricGauge,
    MetricSum,
    All,
}

impl std::fmt::Display for SignalSelection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Logs => "logs",
            Self::Spans => "spans",
            Self::MetricGauge => "metric-gauge",
            Self::MetricSum => "metric-sum",
            Self::All => "all",
        })
    }
}

impl std::str::FromStr for SignalSelection {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self> {
        match s {
            "logs" => Ok(Self::Logs),
            "spans" => Ok(Self::Spans),
            "metric-gauge" => Ok(Self::MetricGauge),
            "metric-sum" => Ok(Self::MetricSum),
            "all" => Ok(Self::All),
            _ => bail!("unknown signal {s}; expected logs|spans|metric-gauge|metric-sum|all"),
        }
    }
}

fn print_help() {
    println!(
        "cargo bench --bench storage_pipeline -- [--rows 50000] [--iterations 4] [--signal logs|spans|metric-gauge|metric-sum|all] [--data-dir DIR] [--segment-target-bytes BYTES]"
    );
}

fn parse_next<T>(args: &mut impl Iterator<Item = String>, name: &str) -> Result<T>
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    let raw = args
        .next()
        .with_context(|| format!("{name} requires a value"))?;
    raw.parse()
        .map_err(|err| anyhow::anyhow!("invalid {name} value {raw}: {err}"))
}

fn make_batch(signal: Signal, rows: usize, iteration: usize, args: &Args) -> Result<RecordBatch> {
    match signal {
        Signal::Logs => logs_batch(rows, iteration, args.log_body_bytes),
        Signal::Spans => spans_batch(rows, iteration, args.trace_attribute_bytes),
        Signal::MetricGauge => metric_batch(signal, rows, iteration, args.metric_description_bytes),
        Signal::MetricSum => metric_batch(signal, rows, iteration, args.metric_description_bytes),
    }
}

fn logs_batch(rows: usize, iteration: usize, body_bytes: usize) -> Result<RecordBatch> {
    let schema = Arc::new(Schema::new(vec![
        ts_field("timestamp"),
        str_field("trace_id"),
        str_field("span_id"),
        str_field("service_name"),
        str_field("service_namespace"),
        str_field("service_instance_id"),
        int_field("severity_number"),
        str_field("severity_text"),
        str_field("body"),
        str_field("resource_attributes"),
        str_field("scope_name"),
        str_field("scope_version"),
        str_field("scope_attributes"),
        str_field("log_attributes"),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(timestamps(rows, iteration)),
            Arc::new(strings(rows, |idx| format!("{:032x}", idx + iteration * rows))),
            Arc::new(strings(rows, |idx| format!("{:016x}", idx))),
            Arc::new(service_names(rows)),
            Arc::new(repeated(rows, "bench")),
            Arc::new(strings(rows, |idx| format!("instance-{}", idx % 32))),
            Arc::new(Int32Array::from(vec![9; rows])),
            Arc::new(repeated(rows, "INFO")),
            Arc::new(repeated(rows, &payload("storage-pipeline-log", body_bytes))),
            Arc::new(repeated(rows, resource_attributes())),
            Arc::new(repeated(rows, "bench-scope")),
            Arc::new(repeated(rows, "1.0.0")),
            Arc::new(repeated(rows, "{}")),
            Arc::new(repeated(
                rows,
                r#"{"http.request.method":"GET","http.response.status_code":200,"http.route":"/bench"}"#,
            )),
        ],
    )
    .context("build logs batch")
}

fn spans_batch(rows: usize, iteration: usize, attr_bytes: usize) -> Result<RecordBatch> {
    let schema = Arc::new(Schema::new(vec![
        ts_field("timestamp"),
        int64_field("end_timestamp"),
        int64_field("duration"),
        str_field("trace_id"),
        str_field("span_id"),
        str_field("parent_span_id"),
        str_field("trace_state"),
        str_field("span_name"),
        int_field("span_kind"),
        int_field("status_code"),
        str_field("status_message"),
        str_field("service_name"),
        str_field("service_namespace"),
        str_field("service_instance_id"),
        str_field("scope_name"),
        str_field("scope_version"),
        str_field("scope_attributes"),
        str_field("span_attributes"),
        str_field("resource_attributes"),
        str_field("events_json"),
        str_field("links_json"),
        int_field("dropped_attributes_count"),
        int_field("dropped_events_count"),
        int_field("dropped_links_count"),
        int_field("flags"),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(timestamps(rows, iteration)),
            Arc::new(Int64Array::from(
                (0..rows)
                    .map(|idx| timestamp_micros(iteration, idx) + 25_000)
                    .collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(vec![25_000; rows])),
            Arc::new(strings(rows, |idx| format!("{:032x}", idx + iteration * rows))),
            Arc::new(strings(rows, |idx| format!("{:016x}", idx))),
            Arc::new(strings(rows, |idx| format!("{:016x}", idx.saturating_sub(1)))),
            Arc::new(repeated(rows, "")),
            Arc::new(repeated(rows, "GET /bench")),
            Arc::new(Int32Array::from(vec![2; rows])),
            Arc::new(Int32Array::from(vec![1; rows])),
            Arc::new(repeated(rows, "")),
            Arc::new(service_names(rows)),
            Arc::new(repeated(rows, "bench")),
            Arc::new(strings(rows, |idx| format!("instance-{}", idx % 32))),
            Arc::new(repeated(rows, "bench-scope")),
            Arc::new(repeated(rows, "1.0.0")),
            Arc::new(repeated(rows, "{}")),
            Arc::new(repeated(
                rows,
                &payload(
                    r#"{"http.request.method":"GET","http.response.status_code":200,"http.route":"/bench","bench.payload":""#,
                    attr_bytes,
                ),
            )),
            Arc::new(repeated(rows, resource_attributes())),
            Arc::new(repeated(rows, "[]")),
            Arc::new(repeated(rows, "[]")),
            Arc::new(Int32Array::from(vec![0; rows])),
            Arc::new(Int32Array::from(vec![0; rows])),
            Arc::new(Int32Array::from(vec![0; rows])),
            Arc::new(Int32Array::from(vec![1; rows])),
        ],
    )
    .context("build spans batch")
}

fn metric_batch(
    signal: Signal,
    rows: usize,
    iteration: usize,
    description_bytes: usize,
) -> Result<RecordBatch> {
    let mut fields = vec![
        ts_field("timestamp"),
        int64_field("start_timestamp"),
        str_field("metric_name"),
        str_field("metric_description"),
        str_field("metric_unit"),
        Field::new("value", DataType::Float64, true),
        str_field("service_name"),
        str_field("service_namespace"),
        str_field("service_instance_id"),
        str_field("resource_attributes"),
        str_field("scope_name"),
        str_field("scope_version"),
        str_field("scope_attributes"),
        str_field("metric_attributes"),
        int_field("flags"),
        str_field("exemplars_json"),
    ];
    if signal == Signal::MetricSum {
        fields.push(int_field("aggregation_temporality"));
        fields.push(Field::new("is_monotonic", DataType::Boolean, true));
    }
    let schema = Arc::new(Schema::new(fields));

    let metric_name = match signal {
        Signal::MetricGauge => "storage.pipeline.gauge",
        Signal::MetricSum => "storage.pipeline.sum",
        _ => unreachable!("metric_batch only supports metric signals"),
    };
    let mut arrays: Vec<Arc<dyn arrow58::array::Array>> = vec![
        Arc::new(timestamps(rows, iteration)),
        Arc::new(Int64Array::from(
            (0..rows)
                .map(|idx| timestamp_micros(iteration, idx) - 1_000_000)
                .collect::<Vec<_>>(),
        )),
        Arc::new(repeated(rows, metric_name)),
        Arc::new(repeated(
            rows,
            &payload("storage pipeline metric description", description_bytes),
        )),
        Arc::new(repeated(rows, "1")),
        Arc::new(Float64Array::from(
            (0..rows).map(|idx| idx as f64).collect::<Vec<_>>(),
        )),
        Arc::new(service_names(rows)),
        Arc::new(repeated(rows, "bench")),
        Arc::new(strings(rows, |idx| format!("instance-{}", idx % 32))),
        Arc::new(repeated(rows, resource_attributes())),
        Arc::new(repeated(rows, "bench-scope")),
        Arc::new(repeated(rows, "1.0.0")),
        Arc::new(repeated(rows, "{}")),
        Arc::new(strings(rows, |idx| {
            format!(r#"{{"series":"{}"}}"#, idx % 512)
        })),
        Arc::new(Int32Array::from(vec![0; rows])),
        Arc::new(repeated(rows, "[]")),
    ];
    if signal == Signal::MetricSum {
        arrays.push(Arc::new(Int32Array::from(vec![2; rows])));
        arrays.push(Arc::new(BooleanArray::from(vec![true; rows])));
    }
    RecordBatch::try_new(schema, arrays).context("build metric batch")
}

fn ts_field(name: &str) -> Field {
    Field::new(name, DataType::Timestamp(TimeUnit::Microsecond, None), true)
}

fn str_field(name: &str) -> Field {
    Field::new(name, DataType::Utf8, true)
}

fn int_field(name: &str) -> Field {
    Field::new(name, DataType::Int32, true)
}

fn int64_field(name: &str) -> Field {
    Field::new(name, DataType::Int64, true)
}

fn timestamps(rows: usize, iteration: usize) -> TimestampMicrosecondArray {
    TimestampMicrosecondArray::from(
        (0..rows)
            .map(|idx| timestamp_micros(iteration, idx))
            .collect::<Vec<_>>(),
    )
}

fn timestamp_micros(iteration: usize, row: usize) -> i64 {
    Utc::now().timestamp_micros() + (iteration as i64 * 1_000_000) + row as i64
}

fn strings(rows: usize, f: impl Fn(usize) -> String) -> StringArray {
    StringArray::from((0..rows).map(f).collect::<Vec<_>>())
}

fn repeated(rows: usize, value: &str) -> StringArray {
    StringArray::from(vec![value.to_string(); rows])
}

fn service_names(rows: usize) -> StringArray {
    strings(rows, |idx| format!("bench-service-{}", idx % 16))
}

fn resource_attributes() -> &'static str {
    r#"{"deployment.environment":"bench"}"#
}

fn payload(prefix: &str, bytes: usize) -> String {
    if prefix.len() >= bytes {
        return prefix[..bytes].to_string();
    }
    let mut out = String::with_capacity(bytes);
    out.push_str(prefix);
    while out.len() < bytes {
        out.push('x');
    }
    out
}
