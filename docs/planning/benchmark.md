# Benchmark And Ingest Proof Gates

This is the canonical benchmark planning and evidence file for Canardstack.

Use this file as the first context document for future benchmark or ingest
architecture work. It is structured for coding agents: current facts first,
then commands, proof gates, evidence, and next actions.

## Agent Scan Guide

OpenAI GPT-5 coding guidance emphasizes explicit role/workflow instructions,
testing requirements, concrete tool examples, clean Markdown, and structured
progress tracking. Apply that here:

- Treat benchmark work as proof-gated engineering, not marketing.
- Preserve Canardstack's product constraints unless the user explicitly changes
  them.
- Before claiming throughput, record command, machine/environment, duration,
  concurrency, payload shape, and result.
- Distinguish decoded MiB/sec, request/compressed MiB/sec, rows/sec, item/sec,
  and durable storage throughput.
- Prefer narrow changes with focused verification.
- Keep this file updated with short factual entries; put large raw output in
  benchmark report JSON files, not in prose.

OpenAI references used for this document shape:

- <https://developers.openai.com/api/docs/guides/prompt-engineering#coding>
- <https://developers.openai.com/cookbook/examples/gpt-5/gpt-5_prompting_guide>

## Current Product Constraints

Canardstack remains:

- one Rust binary
- synchronous std-library HTTP server
- OTLP/HTTP only
- no async runtime
- no gRPC endpoint
- no Kafka or second service
- DuckLake/DuckDB analytical storage
- best-effort ingest acknowledgement: `2xx` means accepted into bounded process
  memory, not durably committed
- stable compatibility API behavior and Prometheus/Loki error envelope shape

Do not expose arbitrary SQL through compatibility APIs. Preserve bounded query
behavior.

## Current Implementation Facts

- `Cargo.toml` uses crates.io `otlp2records = { version = "0.6.0", features = ["parquet"] }`.
- `src/otlp.rs::transform` calls non-partitioned `transform_logs`,
  `transform_traces`, and `transform_metrics`.
- `otlp2records` `0.6.0` exposes `transform_logs_partitioned`,
  `transform_traces_partitioned`, and `transform_metrics_partitioned`.
- Crates.io source inspection showed those partitioned APIs call the existing
  non-partitioned transform first, then split batches by `service_name` with
  `group_batch_by_service`. They are useful for deterministic lane ownership
  after transform, but they do not provide parallel decode or transform.
- Do not use `*_decoded_for_bench` APIs in production unless upstream promotes
  them intentionally.

## Current Serial Bottlenecks

- `src/ingest/mod.rs::Ingestor::ingest` runs decompress, `otlp2records`
  transform, timestamp validation, memory reservation, and enqueue on the HTTP
  request thread. Multiple HTTP connection threads can run concurrently, but
  each accepted request owns full decoded payload and Arrow output until enqueue
  completes.
- `src/otlp.rs::transform` treats one request as one transform unit per signal;
  metrics produce gauge and sum batches from one call.
- `src/ingest/mod.rs::Ingestor` stores every queue in one
  `Arc<Mutex<queue::QueueMap>>`. `src/ingest/queue.rs` partitions only by
  signal and source encoding.
- `src/ingest/flush.rs` uses one `flush_lock`, so only one flush path drains,
  coalesces, and hands batches to storage at a time.
- `src/storage/immutable_write.rs::Storage::insert_arrow_batches` prepares
  batches and appends them to one `immutable_buffers` mutex.
- `src/storage/immutable_write.rs::Storage::seal_immutable_buffers` seals due
  table buffers in one scheduler job, then holds the single DuckDB writer
  connection while registering files and committing.
- `src/maintenance.rs::scheduler_loop` runs watchdog, forced flush, metadata
  refresh, metrics snapshot, compaction, and retention on one scheduler thread.

## Target Architecture Direction

This is a proof-gated direction, not a product claim. A credible single-binary
shape for high decoded ingest is a fixed set of synchronous OS-thread lanes:

1. HTTP request threads validate auth, content type, compression, size, runtime
   memory admission, and dependency health.
2. Decode/transform lanes own bounded input slots. Each lane performs
   decompress, OTLP decode, Arrow transform, timestamp validation, and emits
   lane-owned Arrow batches.
3. Queue lanes own disjoint shards such as `(signal, partition)`. Admission
   accounting remains global, but enqueue/drain locks are lane-local.
4. Storage handoff lanes convert drained Arrow batches into immutable segment
   buffers and sealed Parquet files without holding the DuckDB writer
   connection.
5. DuckLake registration remains a single-writer lane unless evidence proves
   safe concurrent registration.
6. Metrics distinguish compressed request bytes/sec, decoded bytes/sec,
   rows/sec, item/sec, queue age, transform latency, queue handoff latency,
   Parquet encode latency, file write latency, fsync latency, DuckLake
   registration latency, commit latency, and query-visible freshness.

Backpressure semantics stay unchanged:

- `2xx`: accepted into bounded process memory.
- `429`: request, queue, process-memory, or runtime-memory pressure.
- `503`: unhealthy storage dependencies or forced dependency unhealthiness.

## Implemented Benchmark Instrumentation

Storage phase metrics now separate:

- `storage_partition_split`
- `storage_parquet_encode`
- `storage_file_write`
- `storage_file_fsync`
- `storage_file_rename`
- `storage_ducklake_register`
- `storage_ducklake_commit`

`storage_insert` remains emitted for compatibility with current flush
accounting and parsing.

`benches/storage_pipeline.rs` measures Arrow-to-storage work only. It does not
include HTTP parsing, decompression, `otlp2records`, query load, or freshness.

`benches/throughput_iteration.rs` now supports:

- `--items-per-batch`
- `--log-records`
- `--trace-attribute-bytes`
- `--signals all|logs|spans|metrics`
- `--server-pid`
- `--resource-sample-interval`
- generator pacing fields in report JSON
- benchmark and server process resource samples

## Required Proof Gates

Before claiming 25-100 GB/day:

- Docker/local quickstart starts in local DuckLake mode with one documented
  command.
- Docker/local smoke ingests representative OTLP fixtures through port `4318`
  and verifies Prometheus/Loki/Tempo compatibility results.
- Capped Docker Desktop sentinel benchmark records CPU/memory envelope and
  passes at the claimed GB/day volume.
- A real Linux VM 2-hour benchmark records instance/disk/object-store details
  and passes at the claimed volume, with and without query interference.
- Process memory admission control reliably returns `429` before OOM.
- DuckLake insert path survives process restart with expected best-effort loss
  window.
- Compatibility endpoints enforce time range, limit, timeout, memory, and
  concurrency.
- Retention reclaims physical storage in local filesystem mode.
- Existing OpenTelemetry Collector can export to all v0 ingest endpoints.

Before claiming 100-500 GB/day:

- 2-hour and overnight mixed workload benchmarks at 100, 250, and 500 GB/day
  equivalent.
- Ingest-only and ingest-plus-query-interference evidence for each step.
- Object storage benchmark, not local-only.
- DuckLake inlined data stays bounded and reliably becomes Parquet.
- Postgres catalog growth is measured and bounded where relevant.
- Maintenance catches up after a controlled pause.
- Retention physically reclaims storage after snapshot expiration and cleanup.
- Query OOM cannot take down ingest, or query role isolation is implemented.
- Compatibility query load cannot starve ingest.

Before claiming 500 MiB/sec decoded ingest:

- Architecture proof explains which stages are parallel and which remain
  single-writer.
- Transform-only benchmarks record decoded MiB/sec by payload shape and
  concurrency. These are transform evidence only.
- Storage-only benchmarks record Parquet encode, file write, fsync, DuckLake
  register, and DuckLake commit throughput separately.
- End-to-end OTLP/HTTP benchmarks record request/compressed MiB/sec, decoded
  MiB/sec, rows/sec, item/sec, queue age, query-visible freshness, and durable
  storage throughput.
- Mixed query-interference benchmarks prove compatibility API load does not
  starve ingest or identify the saturated boundary.
- Runs report whether generator or receiver saturated first, including CPU,
  memory, network, queue/backlog, request batch shape, lane/shard counts, and
  query pressure.
- Local macOS evidence remains directional unless Linux/cloud/object-store
  runs prove the claim.

Explicit non-claims:

- durable acknowledgement after `2xx`
- multi-tenant isolation
- arbitrary SQL safety for users
- unlimited cardinality
- unlimited retention
- sub-second freshness
- ClickHouse-class ingest ceilings
- 500 MiB/sec decoded ingest support

## Benchmark Commands

Storage-only proof gate:

```sh
cargo bench --bench storage_pipeline -- \
  --rows 50000 \
  --iterations 4 \
  --signal all
```

End-to-end mixed local scout:

```sh
cargo bench --bench throughput_iteration -- \
  --base-url http://127.0.0.1:4336 \
  --warmup 15s \
  --duration 60s \
  --target-gb-day 500 \
  --profile mixed-query \
  --query-pressure low \
  --ingest-concurrency 12 \
  --items-per-batch 256 \
  --log-body-bytes 512 \
  --trace-attribute-bytes 256 \
  --metric-description-bytes 64 \
  --progress-interval 15s \
  --max-runtime 90s \
  --server-pid 92204 \
  --resource-sample-interval 5s \
  --report-dir target/canardstack-bench/e2e-500gpd-release-items256-conc12
```

Relative OTel-style logs-only scout:

```sh
cargo bench --bench throughput_iteration -- \
  --base-url http://127.0.0.1:4337 \
  --warmup 10s \
  --duration 30s \
  --target-gb-day 580.746 \
  --profile ingest-only \
  --query-pressure off \
  --ingest-concurrency 12 \
  --signals logs \
  --items-per-batch 256 \
  --log-body-bytes 512 \
  --trace-attribute-bytes 256 \
  --metric-description-bytes 64 \
  --progress-interval 10s \
  --max-runtime 50s \
  --server-pid 93933 \
  --resource-sample-interval 5s \
  --report-dir target/canardstack-bench/relative-otel-log10kdps-items256
```

Relative OTel-style spans-only scout:

```sh
cargo bench --bench throughput_iteration -- \
  --base-url http://127.0.0.1:4338 \
  --warmup 10s \
  --duration 30s \
  --target-gb-day 404.322 \
  --profile ingest-only \
  --query-pressure off \
  --ingest-concurrency 12 \
  --signals spans \
  --items-per-batch 256 \
  --log-body-bytes 512 \
  --trace-attribute-bytes 256 \
  --metric-description-bytes 64 \
  --progress-interval 10s \
  --max-runtime 50s \
  --server-pid 94257 \
  --resource-sample-interval 5s \
  --report-dir target/canardstack-bench/relative-otel-trace10ksps-items256
```

Focused verification after benchmark harness changes:

```sh
cargo fmt --all -- --check
cargo check
cargo check --benches
cargo test immutable_segment_write_reports_detailed_timings
cargo clippy --all-targets --all-features --locked -- -D warnings
git diff --check
```

## Current Benchmark Matrix

Keep the matrix small and proof-oriented. Add dimensions only when they answer
a current product question.

Signals:

- logs
- spans
- metric gauge
- metric sum
- mixed workload by decoded bytes

Run modes:

- transform-only, for `otlp2records` evidence
- storage-only, for Arrow-to-Parquet and DuckLake registration evidence
- end-to-end ingest-only
- end-to-end mixed ingest plus compatibility-query pressure
- isolated signal runs for external comparison
- max-load stress, only after a shaped target run passes

Storage envelopes:

- local filesystem DuckLake
- object storage DuckLake before any cloud claim
- degraded object storage with injected 5xx or latency before resilience
  claims

Workload dimensions:

- services
- log records per request and body bytes
- spans per request and attribute bytes
- metric points per request and description bytes
- item/sec target
- decoded byte/sec target
- request concurrency
- query pressure

Every benchmark entry should record:

- configuration
- hardware and OS profile
- storage profile
- dataset and payload profile
- time-series metrics
- query latency table when queries are enabled
- failure events
- DuckLake inlined rows and Parquet transition
- physical/logical storage bytes
- recommendation: pass, fail, or retry with changed config

## Current Evidence Summary

All local macOS evidence is directional. Report JSON paths are the source of
truth for full details.

| Date | Gate | Shape | Result | Report |
| --- | --- | --- | --- | --- |
| 2026-05-17 | Max-load logs ceiling scout | logs-only, 256 records/request, 30s, ingest-only, concurrency 12 | best passing: `29,034 logs/sec`, `18.75 MiB/s`; next target plateaued at `29,511 logs/sec`, `19.06 MiB/s`, no `429/503` | `target/canardstack-bench/maxload-logs-items256-target1700/20260517T005254Z/report.json` |
| 2026-05-17 | Max-load spans ceiling scout | spans-only, 256 spans/request, 30s, ingest-only, concurrency 12 | best passing: `29,475 spans/sec`, `13.15 MiB/s`; higher target plateaued at `29,457 spans/sec`, `13.15 MiB/s`, no `429/503` | `target/canardstack-bench/maxload-spans-items256-target1200/20260517T005555Z/report.json` |
| 2026-05-17 | Relative OTel logs | logs-only, 256 records/request, 30s, ingest-only | `9971 logs/sec`, `6.39 MiB/s`, no `429/503` | `target/canardstack-bench/relative-otel-log10kdps-items256/20260517T002041Z/report.json` |
| 2026-05-17 | Relative OTel spans | spans-only, 256 spans/request, 30s, ingest-only | `9977 spans/sec`, `4.45 MiB/s`, no `429/503` | `target/canardstack-bench/relative-otel-trace10ksps-items256/20260517T002200Z/report.json` |
| 2026-05-17 | OTel-style batch mixed scout | mixed query, low pressure, 256 items/batch, 60s | `5,785,578 decoded B/s`, pass, no `429/503` | `target/canardstack-bench/e2e-500gpd-release-items256-conc12/20260517T000808Z/report.json` |
| 2026-05-16 | 500 GB/day resource scout | mixed query, low pressure, 120s, concurrency 12 | `5,232,269 decoded B/s`, pass threshold, no `429/503` | `target/canardstack-bench/e2e-500gpd-release-resource-conc12/20260516T231256Z/report.json` |
| 2026-05-16 | 500 GB/day release scout | mixed query, low pressure, 120s, concurrency 12 | `5,246,666 decoded B/s`, pass threshold, no `429/503` | `target/canardstack-bench/e2e-500gpd-release-mixed-low-conc12/20260516T222355Z/report.json` |
| 2026-05-16 | 250 GB/day release scout | mixed query, low pressure, 120s, concurrency 6 | `2,608,446 decoded B/s`, no `429/503` | `target/canardstack-bench/e2e-250gpd-release-mixed-low-conc6/20260516T221202Z/report.json` |
| 2026-05-16 | Storage-only local proof | Arrow batches to local DuckLake, 800k rows | `528.112 Arrow MiB/s`, `697,029 rows/sec` | benchmark stdout |
| 2026-05-16 | Storage harness smoke | metric-gauge, 1k rows | `15.690 Arrow MiB/s`, pass | benchmark stdout |
| 2026-05-16 | `otlp2records` transform-only | local release example, protobuf payloads | logs `280.3`, traces `263.1`, metrics `254-279` decoded MiB/sec | `../otlp2records` bench stdout |
| 2026-05-16 | Immutable max-age validation | focused unit/integration validation | passed | test output |
| 2026-05-16 | 100 GB/day immutable DuckLake | mixed query, concurrency 3 | passed local scout | `target/canardstack-bench/100gpd-immutable-ducklake-conc3/20260516T173412Z/report.json` |
| 2026-05-16 | 25 GB/day comparison | mixed query, older `otlp2records` | comparison evidence only | old report paths in git history |
| 2026-05-15 | 10 GB/day realistic metrics | mixed query, realistic descriptions | passed local scout | old report paths in git history |
| 2026-05-15 | 10 GB/day Docker network | 2h, low query pressure, container network | passed, claim-grade for narrow Docker envelope | `target/canardstack-bench/10gpd-mixed-query-low-2h-container-net-attempt7/20260515T083247Z/report.json` |

## Latest Relative Benchmark Findings

Current OpenTelemetry Collector benchmark anchor downloaded from:

```text
https://open-telemetry.github.io/opentelemetry-collector-contrib/benchmarks/loadtests/data.js
```

Latest OTel data entry at time of capture:

- commit `b58b76dc34b5`
- timestamp `2026-05-16T00:57:32Z`
- `Log10kDPS/OTLP-HTTP`: CPU avg `17.60%`, CPU max `20.66%`, RAM avg
  `72 MiB`, RAM max `102 MiB`, dropped count `0`
- `Trace10kSPS/OTLP-HTTP`: CPU avg `12.80%`, CPU max `16.33%`, RAM avg
  `71 MiB`, RAM max `100 MiB`, dropped count `0`

Canardstack local relative scouts:

- logs-only 10k records/sec: `9971 logs/sec`, `6.39 MiB/s`, server sampled
  CPU avg/max `3.9% / 8.9%`, no `429/503`
- spans-only 10k spans/sec: `9977 spans/sec`, `4.45 MiB/s`, server sampled
  CPU avg/max `4.1% / 5.4%`, no `429/503`

Canardstack local max-load scouts, same 256-item payload shape:

- Environment: local macOS `26.3.1 (a)`, Darwin `25.3.0`, `arm64`,
  benchmark `available_parallelism=12`; local filesystem DuckLake under fresh
  `/private/tmp` data directories. This remains directional Mac evidence.
- Logs, best passing point: target `1700 GB/day`, actual `19,661,071 B/s`
  (`18.75 MiB/s`), `29,034 logs/sec`, `113.4 requests/sec`, `0` HTTP
  `429/503`, no transport errors observed in stdout/report, server sampled
  peak CPU/RSS `18.9% / 290 MiB`. Queue oldest age stayed near `0.31s`; final
  logs freshness lag after forced flush was `0.12s`. Top measured phases:
  `storage_prepare` `8.70s`, `otlp_transform` `6.20s`,
  `storage_parquet_write` `1.80s`.
- Logs, first higher failed target: target `2000 GB/day`, actual
  `19,984,219 B/s` (`19.06 MiB/s`), `29,511 logs/sec`,
  `115.3 requests/sec`, `0` HTTP `429/503`. The run failed the target gate
  because accepted decoded throughput was below 90% of the requested target,
  while queue age stayed low. Likely local limiter: receiver/server pipeline
  throughput around `115 requests/sec` for this payload, with storage prepare
  and transform as the largest measured phase buckets.
- Spans, best passing point: target `1200 GB/day`, actual `13,793,606 B/s`
  (`13.15 MiB/s`), `29,475 spans/sec`, `115.1 requests/sec`, `0` HTTP
  `429/503`, no transport errors observed in stdout/report, server sampled
  peak CPU/RSS `21.5% / 294 MiB`. Queue oldest age stayed near `0.20s`; final
  spans freshness lag after forced flush was `0.13s`. Top measured phases:
  `otlp_transform` `10.07s`, `storage_prepare` `10.01s`,
  `storage_parquet_write` `1.94s`.
- Spans, first higher failed target: target `2000 GB/day`, actual
  `13,785,274 B/s` (`13.15 MiB/s`), `29,457 spans/sec`,
  `115.1 requests/sec`, `0` HTTP `429/503`. The higher target produced the
  same request-rate plateau instead of scaling. Likely local limiter:
  receiver/server pipeline throughput around `115 requests/sec`; transform and
  storage preparation split the dominant measured phase time.

Exact max-load commands retained:

```sh
env CANARDSTACK_BIND=127.0.0.1:4341 CANARDSTACK_API_KEY=dev-canardstack-key CANARDSTACK_ADMIN_API_KEY=dev-canardstack-admin-key CANARDSTACK_DATA_DIR=/private/tmp/canardstack-bench-logs-max-20260517 CANARDSTACK_DUCKDB_PATH=/private/tmp/canardstack-bench-logs-max-20260517/canardstack.duckdb CANARDSTACK_STORAGE_DIR=/private/tmp/canardstack-bench-logs-max-20260517/storage CANARDSTACK_BENCHMARK_RESOURCE_NOTE=local-macos-directional cargo run --release -- serve

env CANARDSTACK_BENCHMARK_RESOURCE_NOTE=local-macos-directional cargo bench --bench throughput_iteration -- --base-url http://127.0.0.1:4341 --warmup 10s --duration 30s --target-gb-day 1000 --profile ingest-only --query-pressure off --ingest-concurrency 12 --signals logs --items-per-batch 256 --log-body-bytes 512 --trace-attribute-bytes 256 --metric-description-bytes 64 --progress-interval 10s --max-runtime 50s --server-pid 98287 --resource-sample-interval 5s --report-dir target/canardstack-bench/maxload-logs-items256-target1000

env CANARDSTACK_BENCHMARK_RESOURCE_NOTE=local-macos-directional cargo bench --bench throughput_iteration -- --base-url http://127.0.0.1:4341 --warmup 10s --duration 30s --target-gb-day 1700 --profile ingest-only --query-pressure off --ingest-concurrency 12 --signals logs --items-per-batch 256 --log-body-bytes 512 --trace-attribute-bytes 256 --metric-description-bytes 64 --progress-interval 10s --max-runtime 50s --server-pid 98287 --resource-sample-interval 5s --report-dir target/canardstack-bench/maxload-logs-items256-target1700

env CANARDSTACK_BENCHMARK_RESOURCE_NOTE=local-macos-directional cargo bench --bench throughput_iteration -- --base-url http://127.0.0.1:4341 --warmup 10s --duration 30s --target-gb-day 2000 --profile ingest-only --query-pressure off --ingest-concurrency 12 --signals logs --items-per-batch 256 --log-body-bytes 512 --trace-attribute-bytes 256 --metric-description-bytes 64 --progress-interval 10s --max-runtime 50s --server-pid 98287 --resource-sample-interval 5s --report-dir target/canardstack-bench/maxload-logs-items256-target2000

env CANARDSTACK_BIND=127.0.0.1:4342 CANARDSTACK_API_KEY=dev-canardstack-key CANARDSTACK_ADMIN_API_KEY=dev-canardstack-admin-key CANARDSTACK_DATA_DIR=/private/tmp/canardstack-bench-spans-max-20260517 CANARDSTACK_DUCKDB_PATH=/private/tmp/canardstack-bench-spans-max-20260517/canardstack.duckdb CANARDSTACK_STORAGE_DIR=/private/tmp/canardstack-bench-spans-max-20260517/storage CANARDSTACK_BENCHMARK_RESOURCE_NOTE=local-macos-directional cargo run --release -- serve

env CANARDSTACK_BENCHMARK_RESOURCE_NOTE=local-macos-directional cargo bench --bench throughput_iteration -- --base-url http://127.0.0.1:4342 --warmup 10s --duration 30s --target-gb-day 1000 --profile ingest-only --query-pressure off --ingest-concurrency 12 --signals spans --items-per-batch 256 --log-body-bytes 512 --trace-attribute-bytes 256 --metric-description-bytes 64 --progress-interval 10s --max-runtime 50s --server-pid 2240 --resource-sample-interval 5s --report-dir target/canardstack-bench/maxload-spans-items256-target1000

env CANARDSTACK_BENCHMARK_RESOURCE_NOTE=local-macos-directional cargo bench --bench throughput_iteration -- --base-url http://127.0.0.1:4342 --warmup 10s --duration 30s --target-gb-day 1200 --profile ingest-only --query-pressure off --ingest-concurrency 12 --signals spans --items-per-batch 256 --log-body-bytes 512 --trace-attribute-bytes 256 --metric-description-bytes 64 --progress-interval 10s --max-runtime 50s --server-pid 2240 --resource-sample-interval 5s --report-dir target/canardstack-bench/maxload-spans-items256-target1200

env CANARDSTACK_BENCHMARK_RESOURCE_NOTE=local-macos-directional cargo bench --bench throughput_iteration -- --base-url http://127.0.0.1:4342 --warmup 10s --duration 30s --target-gb-day 2000 --profile ingest-only --query-pressure off --ingest-concurrency 12 --signals spans --items-per-batch 256 --log-body-bytes 512 --trace-attribute-bytes 256 --metric-description-bytes 64 --progress-interval 10s --max-runtime 50s --server-pid 2240 --resource-sample-interval 5s --report-dir target/canardstack-bench/maxload-spans-items256-target2000
```

Interpretation:

- At the OTel 10k item/sec scale, Canardstack is not obviously behind on item
  rate in local directional evidence.
- These runs also normalized into Arrow and registered local DuckLake Parquet
  files.
- This does not prove Canardstack is faster than Collector. Hardware, runtime,
  duration, payload schema, protocol details, and downstream sink semantics
  differ.
- The max-load scouts put local item-rate plateaus near `29k logs/sec` and
  `29k spans/sec` for this exact payload shape. That is roughly `2.9x` the
  OTel 10k item-rate anchors, but it is not an equivalent Collector comparison
  because sink semantics and hardware differ.

Vector sizing anchor:

- Vector docs list conservative planning estimates of about `10 MiB/s/vCPU`
  for unstructured logs and `25 MiB/s/vCPU` for structured logs, metric events,
  and trace span events.
- Canardstack's 10k item/sec local scouts were below that if compared naively
  by decoded byte rate: logs `6.39 MiB/s`, spans `4.45 MiB/s`.
- The max-load local scouts improved byte-rate evidence to logs `18.75 MiB/s`
  best passing / `19.06 MiB/s` plateau and spans `13.15 MiB/s` best passing /
  plateau. Logs exceed the rough `10 MiB/s/vCPU` unstructured-log planning
  anchor on this local run; spans remain below the rough `25 MiB/s/vCPU`
  structured trace-span planning anchor. These are not Vector-equivalent
  benchmarks.
- In these runs, request bytes/sec equals the harness's decoded bytes/sec
  because the benchmark sends uncompressed protobuf payloads and uses body
  length as the decoded-byte target proxy.

Sources:

- <https://open-telemetry.github.io/opentelemetry-collector-contrib/benchmarks/loadtests/>
- <https://opentelemetry.io/docs/collector/benchmarks/>
- <https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/main/testbed>
- <https://opentelemetry.io/docs/specs/otel/performance-benchmark/>
- <https://prometheus.io/docs/practices/remote_write/>
- <https://vector.dev/docs/setup/going-to-prod/sizing/>

## Next Recommended Proof Gates

1. Repeat the strongest local ceiling on Linux with documented CPU, memory,
   disk, and filesystem details.
2. Add a benchmark mode or fixture variant with advancing per-request event
   timestamps so freshness lag can be interpreted as backlog rather than fixed
   payload timestamp age.
3. Run a longer 5-10 minute ceiling confirmation at the best local passing
   logs and spans points, with query pressure still off, to check thermal,
   scheduler, and storage-file accumulation effects.
4. Add transform-lane and queue-shard implementation only after max-load data
   shows which current stage saturates first.
5. Gate any `otlp2records` partitioned API adoption on measurement showing that
   downstream ownership gains offset post-transform grouping cost.

## External Evidence Rules

For every future benchmark entry, include:

- exact command
- machine/environment
- duration
- concurrency
- payload shape
- decoded MiB/sec
- request/compressed MiB/sec when applicable
- rows/sec and item/sec
- durable storage throughput when applicable
- queue age and freshness
- generator/server CPU and RSS
- whether generator or receiver saturated first
- report path
- caveats and explicit non-claims

Do not claim 500 MiB/sec support until an end-to-end benchmark actually proves
500 MiB/sec decoded ingest under a documented hardware and storage envelope.
