# Benchmark Plan

## Goal

Prove or disprove the v0 path:

```text
OTLP/HTTP -> otlp2records -> DuckLake insert -> DuckDB product query
```

The benchmark must measure throughput, freshness, DuckLake inlining behavior, Parquet conversion, maintenance behavior, and query safety under realistic pressure.

## Test Matrix

### Signals

- Logs.
- Spans.
- Gauge metrics.
- Sum metrics.
- Mixed workload matching expected production ratios.

Initial mixed ratio:

- 60% logs by bytes.
- 25% spans by bytes.
- 15% metrics by bytes.

### Volumes

- 25 GB/day equivalent.
- 100 GB/day equivalent.
- 250 GB/day equivalent.
- 500 GB/day equivalent.
- 1 TB/day stretch, only after 500 GB/day is stable.

### Storage Modes

- Local filesystem.
- S3-compatible object storage.
- Degraded object storage with injected 5xx and latency.

### Batch Modes

- 50k rows / 32 MiB.
- 200k rows / 128 MiB.
- 500k rows / 256 MiB.
- Flush age 2s, 10s, 30s.

### Query Mix

- Log search over 15m, 1h, 24h.
- Trace lookup by id.
- Span search sorted by timestamp.
- Span search sorted by duration.
- Prometheus-compatible metric range query 1h, 24h, 7d.
- Concurrent Prometheus/Loki/Tempo investigation query mix at configured concurrency.

## Metrics To Capture

### Ingest

- Requests/sec.
- Bytes/sec compressed and decoded.
- Records/sec by signal.
- Decode and transform latency.
- Arrow batch rows and bytes.
- Queue memory by signal.
- Accepted, `400`, `429`, `503`.
- Process RSS.

### DuckLake

- Insert latency.
- Commit latency.
- Inlined rows and bytes.
- Oldest inlined data age.
- Parquet files created.
- Average file size.
- Snapshot count.
- Catalog table sizes in Postgres.
- Flush latency and failures.

### Query

- Query latency P50/P95/P99.
- Rows scanned if available.
- Bytes scanned if available.
- Memory high-water mark.
- Timeout count.
- OOM count.
- Concurrency rejection count.

### Maintenance

- Flush runtime.
- Snapshot expiration runtime.
- Cleanup runtime.
- Compaction runtime.
- Retention runtime.
- Maintenance lag by table.
- Files removed.
- Bytes reclaimed.

## Workload Generator

Use real OTLP fixture corpora plus synthetic cardinality controls.

Required dimensions:

- Number of services: 10, 100, 1,000.
- Attribute cardinality: low, medium, pathological.
- Trace size: 5, 50, 500 spans.
- Log body size: 100 B, 1 KiB, 10 KiB.
- Metrics series cardinality: 1k, 100k, 1M active series.

## Success Criteria For 100-500 GB/day Claim

At 500 GB/day equivalent for a 24-hour run:

- No process OOM.
- No unbounded Postgres catalog growth.
- P95 accepted ingest-to-query freshness under 2 minutes after warmup.
- Oldest inlined data under 10 minutes in steady state.
- `429` rate under 1% during healthy storage.
- No `503` during healthy storage.
- Retention dry run and actual cleanup complete within the maintenance window.
- Query P95 remains under caps without starving ingest.
- Compatibility query concurrency cannot crash the process.

## Failure Injection

Run each at 100 GB/day and 500 GB/day equivalent:

- Kill process after `2xx` but before DuckLake commit.
- Restart Postgres for 30 seconds.
- Inject object storage 5xx for 10 minutes.
- Slow object storage writes by 10x.
- Start pathological broad queries.
- Pause maintenance for 2 hours, then resume.
- Fill disk or bucket quota in a controlled environment.

## Benchmark Outputs

Each benchmark run must produce:

- Configuration.
- Hardware profile.
- Dataset profile.
- Time series metrics.
- Query latency table.
- Failure events.
- Postgres catalog growth chart.
- DuckLake inlining and Parquet transition chart.
- Storage physical/logical byte chart.
- Recommendation: pass, fail, or retry with changed config.

## v0 Iteration Benchmark

The first Rust-native iteration benchmark is intentionally smaller than this
matrix. It targets an already-running local canardstack and is meant to catch
release-blocking hot-path smells before v0: throughput collapse, growing queue
or freshness lag, query interference with ingest, unstable tail latency, and
dominant server phases that correlate with failed gates.

```sh
cargo bench --bench v0_iteration -- --duration 30s --warmup 5s
```

Useful local knobs are `--target-gb-day`, `--query-interval`, `--no-queries`,
and `--report-dir`.
