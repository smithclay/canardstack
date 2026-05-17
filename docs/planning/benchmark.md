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
- The non-default `transform-split-instrumentation` Cargo feature is allowed
  for benchmark probes that need to split protobuf decode from `otlp2records`
  Arrow construction. Do not enable it in normal production builds.

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
- `--timestamp-mode fixed|advancing`
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
| 2026-05-17 | Linux VM transform split spans attr256 | `galaxy-disk`, spans-only ingest, no queries, 10m, `1500 GB/day`, 256 spans/request, `trace_attribute_bytes=256`, advancing timestamps, concurrency 32 | failed on `429`: `33,971 spans/sec`, `15.16 MiB/s`, `132.7 req/sec`, `7,319` HTTP `429`; split phases: `otlp_transform` `334.37s`, protobuf decode `138.51s` (`1.52ms/request`), `otlp2records_arrow_build` `173.51s` (`1.90ms/request`) | `galaxy-disk:/home/exedev/canardstack-linux-proof/target/canardstack-bench/vm-linux-transform-split-20260517/spans-attr256-10m/20260517T155053Z/report.json` |
| 2026-05-17 | Linux VM transform split spans attr0 same GB/day | `galaxy-disk`, spans-only ingest, no queries, 10m, `1500 GB/day`, 256 spans/request, `trace_attribute_bytes=0`, advancing timestamps, concurrency 32 | failed harder because item/request rate nearly doubled: `63,244 spans/sec`, `12.60 MiB/s`, `247.0 req/sec`, `41,902` HTTP `429`; protobuf decode `313.80s`, `otlp2records_arrow_build` `322.92s` | `galaxy-disk:/home/exedev/canardstack-linux-proof/target/canardstack-bench/vm-linux-transform-split-20260517/spans-attr0-10m/20260517T160205Z/report.json` |
| 2026-05-17 | Linux VM transform split spans attr0 item-rate matched | `galaxy-disk`, spans-only ingest, no queries, 10m, `613 GB/day`, 256 spans/request, `trace_attribute_bytes=0`, advancing timestamps, concurrency 32 | pass at matched item/request rate: `33,948 spans/sec`, `6.77 MiB/s`, `132.6 req/sec`, no `429/503`; `otlp_transform` `231.33s`, protobuf decode `106.32s` (`1.27ms/request`), `otlp2records_arrow_build` `110.15s` (`1.32ms/request`) | `galaxy-disk:/home/exedev/canardstack-linux-proof/target/canardstack-bench/vm-linux-transform-split-20260517/spans-attr0-itemmatch-10m/20260517T161449Z/report.json` |
| 2026-05-17 | Linux VM pure-schema prototype spans ingest-only control | `galaxy-disk`, spans-only ingest, no queries, 256 spans/request, advancing timestamps, concurrency 32, no promoted telemetry columns | failed earlier than baseline: first observed `429` by 5m, final `21,369 spans/sec`, `9.54 MiB/s`, `110,588` HTTP `429`; `storage_prepare` fell to `10.96s`, but `otlp_transform` remained `872.41s` and queue oldest age reached `53.83s` | `galaxy-disk:/home/exedev/canardstack-linux-proof/target/canardstack-bench/vm-linux-prototype-ingestonly-30m-20260517/spans-target1500-30m/20260517T142747Z/report.json` |
| 2026-05-17 | Linux VM pure-schema prototype logs ingest-only control | `galaxy-disk`, logs-only ingest, no queries, 256 records/request, advancing timestamps, concurrency 32, no promoted telemetry columns | failed earlier than baseline: first observed `429` by 5m, final `14,702 logs/sec`, `9.49 MiB/s`, `136,973` HTTP `429`; `storage_prepare` fell to `7.64s`, but queue oldest age reached `116.76s` | `galaxy-disk:/home/exedev/canardstack-linux-proof/target/canardstack-bench/vm-linux-prototype-ingestonly-30m-20260517/logs-target2000-30m/20260517T145925Z/report.json` |
| 2026-05-17 | Linux VM 30m logs low-query step-down | `galaxy-disk`, logs-only ingest, low mixed-query pressure, 256 records/request, advancing timestamps, concurrency 32 | pass at `25,636 logs/sec`, `16.56 MiB/s`, `100.1 req/sec`, `180/180` queries succeeded, query p95 `1.04s`, no `429/503` | `galaxy-disk:/home/exedev/canardstack-linux-proof/target/canardstack-bench/vm-linux-query-low-30m-20260517-next/logs-target1500-30m/20260517T052209Z/report.json` |
| 2026-05-17 | Linux VM 30m spans low-query step-down | `galaxy-disk`, spans-only ingest, low mixed-query pressure, 256 spans/request, advancing timestamps, concurrency 32 | pass at `29,678 spans/sec`, `13.25 MiB/s`, `115.9 req/sec`, `180/180` queries succeeded, query p95 `2.14s`, no `429/503`; `otlp_transform` consumed `51%` wall-time share | `galaxy-disk:/home/exedev/canardstack-linux-proof/target/canardstack-bench/vm-linux-query-low-30m-20260517-next/spans-target1200-30m/20260517T055356Z/report.json` |
| 2026-05-17 | Linux VM 30m spans ingest-only control | `galaxy-disk`, spans-only ingest, no queries, 256 spans/request, advancing timestamps, concurrency 32 | failed: `429` appeared by 20m without query traffic; final `36,204 spans/sec`, `16.16 MiB/s`, `6,276` HTTP `429`; `otlp_transform` `64%` wall-time share | `galaxy-disk:/home/exedev/canardstack-linux-proof/target/canardstack-bench/vm-linux-ingestonly-30m-20260517/spans-target1500-30m/20260517T062930Z/report.json` |
| 2026-05-17 | Linux VM 30m logs ingest-only control | `galaxy-disk`, logs-only ingest, no queries, 256 records/request, advancing timestamps, concurrency 32 | failed: `429` appeared by 20m without query traffic; final `32,873 logs/sec`, `21.23 MiB/s`, `9,209` HTTP `429`; `otlp_transform` `35%` wall-time share | `galaxy-disk:/home/exedev/canardstack-linux-proof/target/canardstack-bench/vm-linux-ingestonly-30m-20260517/logs-target2000-30m/20260517T070054Z/report.json` |
| 2026-05-17 | Linux VM spans attribute-size control | `galaxy-disk`, spans-only ingest, no queries, 256 spans/request, advancing timestamps, concurrency 32, `trace_attribute_bytes=0` | pass for 10m at `35,994 spans/sec`, `7.17 MiB/s`, `140.6 req/sec`, no `429/503`; same span/request rate as 256-byte attribute run cut transform avg from `4.34ms` to `2.68ms`/request | `galaxy-disk:/home/exedev/canardstack-linux-proof/target/canardstack-bench/vm-linux-hotspot-20260517/spans-attr0-10m/20260517T071314Z/report.json` |
| 2026-05-17 | Linux VM 30m logs low-query | `galaxy-disk`, logs-only ingest, low mixed-query pressure, 256 records/request, advancing timestamps, concurrency 32 | failed: `429` appeared after 15-20m; final `33,089 logs/sec`, `21.37 MiB/s`, `180/180` queries succeeded, query p95 `1.34s`, `7,688` HTTP `429` | `galaxy-disk:/home/exedev/canardstack-linux-proof/target/canardstack-bench/vm-linux-query-low-30m-20260517/logs-target2000-30m/20260517T040758Z/report.json` |
| 2026-05-17 | Linux VM 30m spans low-query | `galaxy-disk`, spans-only ingest, low mixed-query pressure, 256 spans/request, advancing timestamps, concurrency 32 | failed: `429` appeared after 15-20m; final `36,225 spans/sec`, `16.17 MiB/s`, `180/180` queries succeeded, query p95 `2.69s`, `6,124` HTTP `429` | `galaxy-disk:/home/exedev/canardstack-linux-proof/target/canardstack-bench/vm-linux-query-low-30m-20260517/spans-target1500-30m/20260517T043832Z/report.json` |
| 2026-05-17 | Linux VM 5m logs low-query | `galaxy-disk`, logs-only ingest, low mixed-query pressure, 256 records/request, advancing timestamps, concurrency 32 | pass at `34,173 logs/sec`, `22.07 MiB/s`, `133.5 req/sec`, `30/30` queries succeeded, query p95 `340ms`, no `429/503` | `galaxy-disk:/home/exedev/canardstack-linux-proof/target/canardstack-bench/vm-linux-query-low-20260517/logs-target2000-5m/20260517T032324Z/report.json` |
| 2026-05-17 | Linux VM 5m spans low-query | `galaxy-disk`, spans-only ingest, low mixed-query pressure, 256 spans/request, advancing timestamps, concurrency 32 | pass at `37,087 spans/sec`, `16.55 MiB/s`, `144.9 req/sec`, `30/30` queries succeeded, query p95 `603ms`, no `429/503` | `galaxy-disk:/home/exedev/canardstack-linux-proof/target/canardstack-bench/vm-linux-query-low-20260517/spans-target1500-5m/20260517T032841Z/report.json` |
| 2026-05-17 | Linux VM 5m logs confirmation | `galaxy-disk`, logs-only, 256 records/request, advancing timestamps, concurrency 32 | pass at `34,180 logs/sec`, `22.07 MiB/s`, `133.5 req/sec`, no `429/503`; `2500 GB/day` and `3000 GB/day` failed with `429` | `galaxy-disk:/home/exedev/canardstack-linux-proof/target/canardstack-bench/vm-linux-confirm-20260517-advancing/logs-target2000-5m/20260517T030243Z/report.json` |
| 2026-05-17 | Linux VM 5m spans confirmation | `galaxy-disk`, spans-only, 256 spans/request, advancing timestamps, concurrency 32 | pass at `37,097 spans/sec`, `16.56 MiB/s`, `144.9 req/sec`, no `429/503`; `2000 GB/day` failed with `429` | `galaxy-disk:/home/exedev/canardstack-linux-proof/target/canardstack-bench/vm-linux-confirm-20260517-advancing/spans-target1500-5m/20260517T025702Z/report.json` |
| 2026-05-17 | Linux VM short max-load logs bracket | `galaxy-disk`, logs-only, 256 records/request, fixed timestamps, 20s, concurrency 32 | best short pass `51,121 logs/sec`, `33.01 MiB/s`; next target `3500 GB/day` failed with `429` | `galaxy-disk:/home/exedev/canardstack-linux-proof/target/canardstack-bench/vm-linux-maxload-20260517-conc32/logs-target3000/20260517T022953Z/report.json` |
| 2026-05-17 | Linux VM short max-load spans bracket | `galaxy-disk`, spans-only, 256 spans/request, fixed timestamps, 20s, concurrency 32 | best short pass `49,291 spans/sec`, `22.00 MiB/s`; next target `2500 GB/day` failed with `429` | `galaxy-disk:/home/exedev/canardstack-linux-proof/target/canardstack-bench/vm-linux-maxload-20260517-conc32/spans-target2000/20260517T023048Z/report.json` |
| 2026-05-17 | Linux VM 10k logs scout | `galaxy-disk`, logs-only, 256 records/request, 20s, ingest-only, concurrency 4 | `9,890 logs/sec`, `6.39 MiB/s`, no `429/503` | `galaxy-disk:/home/exedev/canardstack-linux-proof/target/canardstack-bench/vm-linux-log10kdps-items256-conc4/20260517T021423Z/report.json` |
| 2026-05-17 | Linux VM 10k spans scout | `galaxy-disk`, spans-only, 256 spans/request, 20s, ingest-only, concurrency 4 | `9,938 spans/sec`, `4.44 MiB/s`, no `429/503` | `galaxy-disk:/home/exedev/canardstack-linux-proof/target/canardstack-bench/vm-linux-trace10ksps-items256-conc4/20260517T021623Z/report.json` |
| 2026-05-17 | Linux VM release smoke | `galaxy-disk`, release binary, local DuckLake | OTLP logs/traces/metrics `202`, DuckLake healthy, Loki/Prometheus/Tempo checks returned data | smoke stdout |
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

## Latest Linux VM Findings

`galaxy-disk.exe.xyz` is a limited directional VM, not claim-grade hardware:

- Ubuntu `24.04.4 LTS`, Linux `6.12.87`, `x86_64`
- KVM guest, `2` vCPU, host model reported as AMD EPYC 9554P
- `7.8 GiB` RAM, no swap
- single `25 GiB` root disk, about `20 GiB` free before setup
- Docker available, but the benchmark path used the native release binary
- Rust stable `1.95.0`, `build-essential`, `pkg-config`, `cmake`, and
  `libssl-dev` were installed for the VM build

Setup notes:

- A sanitized working tree was copied to
  `/home/exedev/canardstack-linux-proof`, excluding `.env`, `.git`, `target`,
  `.canardstack`, and common secret/key filename patterns.
- `cargo check --benches` passed on the VM, but first-time bundled DuckDB
  compilation took `12m08s`.
- `cargo run -- smoke` in debug mode passed, but consumed about `13 GiB` of
  build artifacts; `cargo clean` reclaimed that before the release build.
- Native release build of `canardstack` and `throughput_iteration` passed in
  `12m25s`.
- Local Apple Silicon cross-build to Linux x86_64 with Zig succeeded, but local
  policy blocked copying workspace-derived binaries to the VM. The VM-native
  release binary was used for evidence.

Release smoke:

```sh
cd /home/exedev/canardstack-linux-proof
rm -rf /tmp/canardstack-vm-release-smoke
env CANARDSTACK_DATA_DIR=/tmp/canardstack-vm-release-smoke \
  CANARDSTACK_DUCKDB_PATH=/tmp/canardstack-vm-release-smoke/canardstack.duckdb \
  CANARDSTACK_STORAGE_DIR=/tmp/canardstack-vm-release-smoke/storage \
  target/release/canardstack smoke
```

Result: logs, traces, and metrics ingest returned `202`; local DuckLake was
healthy; Loki, Prometheus, and Tempo compatibility checks returned data.

Short Linux 10k item/sec scouts:

- Logs: target `6,721,597 B/s`, actual `6,697,004 B/s` (`6.39 MiB/s`),
  `9,890 logs/sec`, `38.6 requests/sec`, `0` HTTP `429/503`, queue oldest age
  stayed below `0.4s`, final logs freshness lag after forced flush was `0.11s`,
  server sampled peak CPU/RSS `2.5% / 153 MiB`. Top phases:
  `storage_prepare` `0.97s`, `otlp_transform` `0.78s`,
  `storage_parquet_write` `0.16s`.
- Spans: target `4,679,653 B/s`, actual `4,650,606 B/s` (`4.44 MiB/s`),
  `9,938 spans/sec`, `38.8 requests/sec`, `0` HTTP `429/503`, queue oldest age
  stayed below `0.4s`, final spans freshness lag after forced flush was
  `0.11s`, server sampled peak CPU/RSS `5.1% / 159 MiB`. Top phases:
  `otlp_transform` `1.57s`, `storage_prepare` `1.45s`,
  `storage_parquet_write` `0.15s`.

Max-load and 5-minute confirmation results:

- Harness change: `throughput_iteration` `0.3.1` added
  `--timestamp-mode fixed|advancing`. The default remains `fixed` to preserve
  existing benchmark behavior. The 5-minute confirmation runs used
  `--timestamp-mode advancing` so event timestamps progress per request and
  freshness lag is more interpretable.
- Four-worker max-load attempts repeated the 10k scout ceiling because request
  latency around `104ms` limited the generator to about `39 req/sec`; those
  runs are useful only as evidence that concurrency 4 was not max-load.
- With concurrency 16, logs passed through `2500 GB/day` for 20s at
  `26.41 MiB/s`, `40,894 logs/sec`; spans passed `1500 GB/day` at
  `16.51 MiB/s`, `37,021 spans/sec`, while spans `2000 GB/day` failed below
  90% target without `429`.
- With concurrency 32, logs passed `3000 GB/day` for 20s at `34,618,436 B/s`
  (`33.01 MiB/s`), `51,121 logs/sec`, `199.7 req/sec`, no `429/503`. The
  next logs target, `3500 GB/day`, failed with `692` HTTP `429` and
  `32.80 MiB/s` accepted. Likely short-run limiter: transform-dominated
  receiver pressure leading to admission rejection; `otlp_transform` accounted
  for `75%` wall-time share at the passing point and `103%` at the failed
  point across request threads.
- With concurrency 32, spans passed `2000 GB/day` for 20s at `23,067,260 B/s`
  (`22.00 MiB/s`), `49,291 spans/sec`, `192.5 req/sec`, no `429/503`. The
  next spans target, `2500 GB/day`, failed with `770` HTTP `429` and
  `23.09 MiB/s` accepted. Likely short-run limiter: `otlp_transform`
  saturation and queue/admission pressure; transform wall-time share was
  `124%` at the passing point and `177%` at the failed point across request
  threads.
- The strongest 20s points did not survive the 5-minute advancing-timestamp
  gate. Logs `3000 GB/day` failed with `11,721` HTTP `429`, actual
  `26.65 MiB/s`, `41,263 logs/sec`; logs `2500 GB/day` also failed with
  `3,461` HTTP `429`, actual `25.68 MiB/s`, `39,770 logs/sec`.
- The durable logs proof point found in this pass is `2000 GB/day`, 5 minutes,
  concurrency 32, advancing timestamps: actual `23,146,314 B/s`
  (`22.07 MiB/s`), `34,180 logs/sec`, `133.5 req/sec`, `0` HTTP `429/503`, no
  transport errors, server sampled peak CPU/RSS `44.6% / 275 MiB`, benchmark
  peak CPU/RSS `18.9% / 15 MiB`, final queue rows/bytes/oldest-age `0 / 0 /
  0s`, final logs freshness `0.19s`. Top phases:
  `otlp_transform` `100.07s`, `storage_prepare` `51.81s`,
  `storage_parquet_write` `8.42s`. Report:
  `galaxy-disk:/home/exedev/canardstack-linux-proof/target/canardstack-bench/vm-linux-confirm-20260517-advancing/logs-target2000-5m/20260517T030243Z/report.json`.
- The durable spans proof point found in this pass is `1500 GB/day`, 5
  minutes, concurrency 32, advancing timestamps: actual `17,360,607 B/s`
  (`16.56 MiB/s`), `37,097 spans/sec`, `144.9 req/sec`, `0` HTTP `429/503`,
  no transport errors, server sampled peak CPU/RSS `51.4% / 265 MiB`,
  benchmark peak CPU/RSS `15.8% / 14 MiB`, final queue rows/bytes/oldest-age
  `2,089 / 4,462,242 / 0.69s`, final spans freshness `0.20s`. Top phases:
  `otlp_transform` `187.54s`, `storage_prepare` `71.19s`,
  `storage_parquet_write` `9.37s`. Report:
  `galaxy-disk:/home/exedev/canardstack-linux-proof/target/canardstack-bench/vm-linux-confirm-20260517-advancing/spans-target1500-5m/20260517T025702Z/report.json`.
- Advancing timestamp runs expose periodic freshness samples around `15-46s`
  for metric tables from the harness's `/metrics` snapshot. For isolated
  logs/spans, final signal freshness after forced flush was about `0.19-0.20s`.
  Treat mid-run freshness trend as a backlog signal, not as a user-facing
  query SLA.
- Low query pressure at the durable 5-minute points also passed. The harness
  ran `--profile mixed-query --query-pressure low`, which issues one query
  every `10s` with query concurrency `1`.
- Logs low-query result: target `2000 GB/day`, actual `23,141,368 B/s`
  (`22.07 MiB/s`), `34,173 logs/sec`, `133.5 req/sec`, `30/30` queries
  succeeded, no `429/503`, no transport errors, query latency p50/p95/p99
  `183 / 340 / 434ms`, server sampled peak CPU/RSS `46.6% / 265 MiB`,
  benchmark peak CPU/RSS `18.9% / 15 MiB`, final queue rows/bytes/oldest-age
  `0 / 0 / 0s`, final logs freshness `0.19s`. Top phases:
  `otlp_transform` `106.61s`, `storage_prepare` `52.02s`,
  `storage_parquet_write` `8.42s`. Report:
  `galaxy-disk:/home/exedev/canardstack-linux-proof/target/canardstack-bench/vm-linux-query-low-20260517/logs-target2000-5m/20260517T032324Z/report.json`.
- Spans low-query result: target `1500 GB/day`, actual `17,356,162 B/s`
  (`16.55 MiB/s`), `37,087 spans/sec`, `144.9 req/sec`, `30/30` queries
  succeeded, no `429/503`, no transport errors, query latency p50/p95/p99
  `183 / 603 / 604ms`, server sampled peak CPU/RSS `53.3% / 249 MiB`,
  benchmark peak CPU/RSS `15.7% / 15 MiB`, final queue rows/bytes/oldest-age
  `0 / 0 / 0s`, final spans freshness `0.21s`. Top phases:
  `otlp_transform` `195.70s`, `storage_prepare` `69.13s`,
  `storage_parquet_write` `9.18s`. Report:
  `galaxy-disk:/home/exedev/canardstack-linux-proof/target/canardstack-bench/vm-linux-query-low-20260517/spans-target1500-5m/20260517T032841Z/report.json`.
- The same low-query points did not pass a 30-minute confirmation on
  `galaxy-disk`. Both runs were clean for the early window and then began
  returning `429` between roughly `15m` and `20m`.
- Logs 30-minute low-query failure: target `2000 GB/day`, actual
  `22,407,331 B/s` (`21.37 MiB/s`), `33,089 logs/sec`, `129.3 req/sec`,
  `180/180` queries succeeded, `7,688` HTTP `429`, no transport errors, query
  latency p50/p95/p99 `497 / 1341 / 1586ms`, server sampled peak CPU/RSS
  `57.5% / 691 MiB`, final queue rows/bytes/oldest-age
  `121,245 / 296,385,770 / 4.90s`, final logs freshness `0.57s`. Top phases:
  `otlp_transform` `704.58s`, `storage_prepare` `289.54s`,
  `query_execute` for Loki query_range `48.64s`, `storage_parquet_write`
  `47.62s`. Report:
  `galaxy-disk:/home/exedev/canardstack-linux-proof/target/canardstack-bench/vm-linux-query-low-30m-20260517/logs-target2000-30m/20260517T040758Z/report.json`.
- Spans 30-minute low-query failure: target `1500 GB/day`, actual
  `16,952,903 B/s` (`16.17 MiB/s`), `36,225 spans/sec`, `141.5 req/sec`,
  `180/180` queries succeeded, `6,124` HTTP `429`, no transport errors, query
  latency p50/p95/p99 `552 / 2690 / 3088ms`, server sampled peak CPU/RSS
  `65.8% / 669 MiB`, final queue rows/bytes/oldest-age
  `153,922 / 328,787,613 / 5.64s`, final spans freshness `0.61s`. Top
  phases: `otlp_transform` `1343.46s`, `storage_prepare` `384.37s`,
  `query_execute` for Tempo search `96.99s`, `storage_parquet_write`
  `53.68s`. Report:
  `galaxy-disk:/home/exedev/canardstack-linux-proof/target/canardstack-bench/vm-linux-query-low-30m-20260517/spans-target1500-30m/20260517T043832Z/report.json`.
- The lower 30-minute low-query brackets passed on the same VM with fresh
  `/tmp` data directories and no stale server/benchmark process observed before
  either run. No medium-query run was started in this pass.
- Logs 30-minute low-query pass: target `1500 GB/day`, actual
  `17,360,381 B/s` (`16.56 MiB/s`), `25,636 logs/sec`, `100.1 req/sec`,
  `180/180` queries succeeded, `0` HTTP `429/503`, no transport errors, query
  latency p50/p95/p99 `391 / 1043 / 1241ms`, ingest latency p50/p95/p99
  `65 / 110 / 116ms`, generator sampled peak CPU/RSS `14.8% / 19.1 MiB`,
  server sampled peak CPU/RSS `45.2% / 481.8 MiB`, final queue
  rows/bytes/oldest-age `0 / 0 / 0s`, final logs freshness `4.06s`. Start/end
  queue trend samples were `1515 / 3,703,447 / 0.20s` and
  `1422 / 3,476,106 / 0.70s`, so the report did not mark queue age or queue
  bytes as clearly increasing. Top phases: `otlp_transform` `420.49s`
  (`23%` wall-time share), `storage_prepare` `221.14s` (`12%`),
  `query_execute` for Loki query_range `37.14s` (`2%`),
  `storage_parquet_write` `36.58s` (`2%`), `storage_parquet_encode`
  `24.06s` (`1%`). Report:
  `galaxy-disk:/home/exedev/canardstack-linux-proof/target/canardstack-bench/vm-linux-query-low-30m-20260517-next/logs-target1500-30m/20260517T052209Z/report.json`.
- Spans 30-minute low-query pass: target `1200 GB/day`, actual
  `13,888,633 B/s` (`13.25 MiB/s`), `29,678 spans/sec`, `115.9 req/sec`,
  `180/180` queries succeeded, `0` HTTP `429/503`, no transport errors, query
  latency p50/p95/p99 `442 / 2141 / 2396ms`, ingest latency p50/p95/p99
  `67 / 110 / 118ms`, generator sampled peak CPU/RSS `13.5% / 18.0 MiB`,
  server sampled peak CPU/RSS `54.7% / 477.9 MiB`, final queue
  rows/bytes/oldest-age `0 / 0 / 0s`, final spans freshness `0.50s`.
  Start/end queue trend samples were `1699 / 3,629,176 / 0.29s` and
  `1310 / 2,798,246 / 0.69s`, so the report did not mark queue age or queue
  bytes as clearly increasing. Top phases: `otlp_transform` `920.11s`
  (`51%` wall-time share), `storage_prepare` `326.36s` (`18%`),
  `query_execute` for Tempo search `75.05s` (`4%`),
  `storage_parquet_write` `42.31s` (`2%`), `storage_parquet_encode`
  `29.49s` (`2%`). Report:
  `galaxy-disk:/home/exedev/canardstack-linux-proof/target/canardstack-bench/vm-linux-query-low-30m-20260517-next/spans-target1200-30m/20260517T055356Z/report.json`.
- Interpretation: the 5-minute pass points are not invalidated as short proof,
  but they are invalidated as 30-minute low-query proof at those higher targets.
  The best current 30-minute low-query proof points are logs `1500 GB/day` and
  spans `1200 GB/day` on this VM and payload shape. The higher failed targets
  show `429` and queue growth after sustained runtime, while the lower targets
  keep queue trend bounded. The next engineering investigation should target
  transform cost first, especially spans. Queue/admission pressure is the
  visible failure mode at the higher points, but current evidence makes it look
  downstream of sustained transform plus storage-prepare work rather than an
  isolated queue-lock issue. Query interference is material for latency and
  dataset growth, especially Tempo search, but low-query work did not dominate
  phase time at the passing points. The harness does not look schedule-limited:
  target utilization was effectively `1.00`, and the generator reported
  `likely_generator_or_schedule_limited=false`.
- Ingest-only controls at the prior 30-minute failure targets also failed, so
  low-query traffic is not required to trigger the sustained queue/admission
  failure. Spans `1500 GB/day`, no queries: actual `16,942,919 B/s`
  (`16.16 MiB/s`), `36,204 spans/sec`, `141.4 req/sec`, `6,276` HTTP `429`,
  no transport errors, server sampled peak CPU/RSS `57.4% / 754.1 MiB`, final
  queue rows/bytes/oldest-age `152,307 / 325,337,866 / 9.65s`. Top phases:
  `otlp_transform` `1149.67s` (`64%` wall-time share), `storage_prepare`
  `384.78s` (`21%`), `storage_parquet_write` `51.35s` (`3%`),
  `queue_admission` `42.08s` (`2%`). Report:
  `galaxy-disk:/home/exedev/canardstack-linux-proof/target/canardstack-bench/vm-linux-ingestonly-30m-20260517/spans-target1500-30m/20260517T062930Z/report.json`.
- Logs `2000 GB/day`, no queries: actual `22,260,645 B/s` (`21.23 MiB/s`),
  `32,873 logs/sec`, `128.4 req/sec`, `9,209` HTTP `429`, no transport
  errors, server sampled peak CPU/RSS `52.1% / 661.8 MiB`, final queue
  rows/bytes/oldest-age `129,751 / 317,178,853 / 9.93s`. Top phases:
  `otlp_transform` `626.27s` (`35%` wall-time share), `storage_prepare`
  `283.69s` (`16%`), `storage_parquet_write` `47.38s` (`3%`),
  `storage_parquet_encode` `31.58s` (`2%`), `queue_admission` `29.96s`
  (`2%`). Report:
  `galaxy-disk:/home/exedev/canardstack-linux-proof/target/canardstack-bench/vm-linux-ingestonly-30m-20260517/logs-target2000-30m/20260517T070054Z/report.json`.
- A trace-attribute-size control points at row-wise attribute string/JSON work
  as a concrete hot spot inside the transform/storage-prep path. With
  `trace_attribute_bytes=0`, a 10-minute spans ingest-only run at roughly the
  same request and item rate passed: actual `7,522,227 B/s` (`7.17 MiB/s`),
  `35,994 spans/sec`, `140.6 req/sec`, no `429/503`, server sampled peak
  CPU/RSS `40.6% / 253.2 MiB`, final queue rows/bytes/oldest-age
  `42,175 / 49,126,131 / 2.11s`. At roughly the same request rate, average
  `otlp_transform` fell from `4.34ms/request` in the 256-byte attribute run to
  `2.68ms/request`, and transform wall-time share fell from `64%` to `39%`.
  Report:
  `galaxy-disk:/home/exedev/canardstack-linux-proof/target/canardstack-bench/vm-linux-hotspot-20260517/spans-attr0-10m/20260517T071314Z/report.json`.
- Concrete engineering insight: the hot path is not arbitrary query work and
  not just DuckLake registration. It includes row-wise OTLP attribute
  materialization and re-materialization across `src/otlp.rs`, `otlp2records`
  trace/log batch builders, and `src/storage/arrow.rs`. `otlp2records`
  serializes resource, scope, span, and log attributes into JSON strings for
  Arrow columns; then `storage_duckdb_batch` reparses those JSON strings
  row-by-row to populate Canardstack-added promoted columns such as
  `http_method`, `http_status_code`, `http_route`, `exception_type`, and
  `deployment_environment`. If the storage contract is the pure
  `otlp2records` schema, those promoted DuckDB columns are the wrong boundary.
  A meaningful optimization and correctness fix is to remove the promoted
  columns from storage, append the `otlp2records` RecordBatch schema directly
  plus the accepted operational `ingested_at` and `source_format` columns, and
  move compatibility label extraction to bounded query/metadata paths.
- Prototype proof gate result: removing storage-time promoted-column reparsing
  was directionally correct for storage prep, but not sufficient for sustained
  throughput. In the pure-schema prototype, spans `1500 GB/day` failed by the
  first 5-minute checkpoint and finished at `9.54 MiB/s`, `21,369 spans/sec`,
  `110,588` HTTP `429`, and max queue oldest age `53.83s`. Compared with the
  previous spans ingest-only control, `storage_prepare` fell from `384.78s` to
  `10.96s`, but `otlp_transform` was still `872.41s` and admission rejections
  rose from `6,276` to `110,588`.
- Logs showed the same negative gate. The pure-schema prototype logs
  `2000 GB/day` run failed by the first 5-minute checkpoint and finished at
  `9.49 MiB/s`, `14,702 logs/sec`, `136,973` HTTP `429`, and max queue oldest
  age `116.76s`. Compared with the previous logs ingest-only control,
  `storage_prepare` fell from `283.69s` to `7.64s`, but accepted throughput
  collapsed and queue/admission pressure dominated.
- Interpretation: storage-time promoted JSON reparsing was real waste, but it
  was not the sustained limiter. The next meaningful code investigation should
  target the transform/admission lane: the synchronous request workers still
  spend the dominant measured time in `otlp_transform`, and once the server
  saturates, admission rejects before the 30-minute target can be sustained.
  The prototype evidence does not support a throughput-tier claim.
- Transform split gate: protobuf decode and `otlp2records` Arrow construction
  are both material parts of the request-thread hot path. With
  `trace_attribute_bytes=256` at spans `1500 GB/day`, a 10-minute run failed
  with `7,319` HTTP `429` at `33,971 spans/sec` and `132.7 req/sec`;
  `otlp_transform` consumed `334.37s`, split into protobuf decode `138.51s`
  (`1.52ms/request`) and `otlp2records_arrow_build` `173.51s`
  (`1.90ms/request`). At the same item/request rate, `trace_attribute_bytes=0`
  passed with no `429`, `otlp_transform` fell to `231.33s`, decode fell to
  `106.32s` (`1.27ms/request`), and Arrow build fell to `110.15s`
  (`1.32ms/request`). This isolates roughly `0.58ms/request` and `63.36s`
  cumulative over 10 minutes to attribute-heavy Arrow/JSON build work, with
  another `0.24ms/request` and `32.20s` showing up in protobuf decode for the
  larger payload.
- A same-GB/day `trace_attribute_bytes=0` run is not a valid attribute-isolation
  control because it nearly doubles item/request rate: it drove
  `63,244 spans/sec`, `247.0 req/sec`, and `41,902` HTTP `429`. Use it only as
  evidence that fixed per-request/per-span transform and admission costs now
  dominate once payload bytes shrink.
- Concrete next code target: optimize or bypass attribute JSON materialization
  in the `otlp2records` Arrow build path for traces. The obvious hot functions
  are in `otlp2records`'s trace batch builder, especially repeated
  `append_attrs_json` work for `resource_attributes`, `scope_attributes`, and
  `span_attributes`, plus the protobuf decode cost of carrying those attribute
  payloads through every request. Canardstack-side storage prep is no longer
  the best next target.

Exact VM benchmark commands:

```sh
cd /home/exedev/canardstack-linux-proof

# Transform split runs require the benchmark-only instrumentation feature.
cargo build --release --features transform-split-instrumentation \
  --bin canardstack \
  --bench throughput_iteration

# Transform split spans attr256.
ps -eo comm=,args= | awk '$1 == "canardstack" || $1 ~ /^throughput_/ {print; found=1} END {exit found ? 0 : 1}'
rm -rf /tmp/canardstack-vm-transform-split-spans-attr256-10m-20260517
env CANARDSTACK_BIND=127.0.0.1:4354 \
  CANARDSTACK_DATA_DIR=/tmp/canardstack-vm-transform-split-spans-attr256-10m-20260517 \
  CANARDSTACK_DUCKDB_PATH=/tmp/canardstack-vm-transform-split-spans-attr256-10m-20260517/canardstack.duckdb \
  CANARDSTACK_STORAGE_DIR=/tmp/canardstack-vm-transform-split-spans-attr256-10m-20260517/storage \
  CANARDSTACK_API_KEY=dev-canardstack-key \
  CANARDSTACK_ADMIN_API_KEY=dev-canardstack-admin-key \
  CANARDSTACK_BENCHMARK_CPU_LIMIT="2 vCPU" \
  CANARDSTACK_BENCHMARK_MEMORY_LIMIT="7.8 GiB RAM" \
  CANARDSTACK_BENCHMARK_RESOURCE_NOTE="galaxy-disk Ubuntu 24.04 x86_64 KVM; transform split instrumentation; pure schema prototype" \
  target/release/canardstack serve

env CANARDSTACK_BENCHMARK_CPU_LIMIT="2 vCPU" \
  CANARDSTACK_BENCHMARK_MEMORY_LIMIT="7.8 GiB RAM" \
  CANARDSTACK_BENCHMARK_RESOURCE_NOTE="galaxy-disk Ubuntu 24.04 x86_64 KVM; transform split instrumentation; pure schema prototype" \
  target/release/deps/throughput_iteration-c454b6542cb49121 \
  --base-url http://127.0.0.1:4354 \
  --warmup 30s \
  --duration 10m \
  --target-gb-day 1500 \
  --profile ingest-only \
  --query-pressure off \
  --ingest-concurrency 32 \
  --signals spans \
  --items-per-batch 256 \
  --log-body-bytes 512 \
  --trace-attribute-bytes 256 \
  --metric-description-bytes 64 \
  --timestamp-mode advancing \
  --progress-interval 2m \
  --max-runtime 13m \
  --server-pid 2455586 \
  --resource-sample-interval 15s \
  --report-dir target/canardstack-bench/vm-linux-transform-split-20260517/spans-attr256-10m

# Transform split spans attr0, same GB/day.
ps -eo comm=,args= | awk '$1 == "canardstack" || $1 ~ /^throughput_/ {print; found=1} END {exit found ? 0 : 1}'
rm -rf /tmp/canardstack-vm-transform-split-spans-attr0-10m-20260517
env CANARDSTACK_BIND=127.0.0.1:4355 \
  CANARDSTACK_DATA_DIR=/tmp/canardstack-vm-transform-split-spans-attr0-10m-20260517 \
  CANARDSTACK_DUCKDB_PATH=/tmp/canardstack-vm-transform-split-spans-attr0-10m-20260517/canardstack.duckdb \
  CANARDSTACK_STORAGE_DIR=/tmp/canardstack-vm-transform-split-spans-attr0-10m-20260517/storage \
  CANARDSTACK_API_KEY=dev-canardstack-key \
  CANARDSTACK_ADMIN_API_KEY=dev-canardstack-admin-key \
  CANARDSTACK_BENCHMARK_CPU_LIMIT="2 vCPU" \
  CANARDSTACK_BENCHMARK_MEMORY_LIMIT="7.8 GiB RAM" \
  CANARDSTACK_BENCHMARK_RESOURCE_NOTE="galaxy-disk Ubuntu 24.04 x86_64 KVM; transform split instrumentation; pure schema prototype" \
  target/release/canardstack serve

env CANARDSTACK_BENCHMARK_CPU_LIMIT="2 vCPU" \
  CANARDSTACK_BENCHMARK_MEMORY_LIMIT="7.8 GiB RAM" \
  CANARDSTACK_BENCHMARK_RESOURCE_NOTE="galaxy-disk Ubuntu 24.04 x86_64 KVM; transform split instrumentation; pure schema prototype" \
  target/release/deps/throughput_iteration-c454b6542cb49121 \
  --base-url http://127.0.0.1:4355 \
  --warmup 30s \
  --duration 10m \
  --target-gb-day 1500 \
  --profile ingest-only \
  --query-pressure off \
  --ingest-concurrency 32 \
  --signals spans \
  --items-per-batch 256 \
  --log-body-bytes 512 \
  --trace-attribute-bytes 0 \
  --metric-description-bytes 64 \
  --timestamp-mode advancing \
  --progress-interval 2m \
  --max-runtime 13m \
  --server-pid 2547156 \
  --resource-sample-interval 15s \
  --report-dir target/canardstack-bench/vm-linux-transform-split-20260517/spans-attr0-10m

# Transform split spans attr0, item-rate matched to attr256.
ps -eo comm=,args= | awk '$1 == "canardstack" || $1 ~ /^throughput_/ {print; found=1} END {exit found ? 0 : 1}'
rm -rf /tmp/canardstack-vm-transform-split-spans-attr0-itemmatch-10m-20260517
env CANARDSTACK_BIND=127.0.0.1:4356 \
  CANARDSTACK_DATA_DIR=/tmp/canardstack-vm-transform-split-spans-attr0-itemmatch-10m-20260517 \
  CANARDSTACK_DUCKDB_PATH=/tmp/canardstack-vm-transform-split-spans-attr0-itemmatch-10m-20260517/canardstack.duckdb \
  CANARDSTACK_STORAGE_DIR=/tmp/canardstack-vm-transform-split-spans-attr0-itemmatch-10m-20260517/storage \
  CANARDSTACK_API_KEY=dev-canardstack-key \
  CANARDSTACK_ADMIN_API_KEY=dev-canardstack-admin-key \
  CANARDSTACK_BENCHMARK_CPU_LIMIT="2 vCPU" \
  CANARDSTACK_BENCHMARK_MEMORY_LIMIT="7.8 GiB RAM" \
  CANARDSTACK_BENCHMARK_RESOURCE_NOTE="galaxy-disk Ubuntu 24.04 x86_64 KVM; transform split instrumentation; attr0 item-rate matched control" \
  target/release/canardstack serve

env CANARDSTACK_BENCHMARK_CPU_LIMIT="2 vCPU" \
  CANARDSTACK_BENCHMARK_MEMORY_LIMIT="7.8 GiB RAM" \
  CANARDSTACK_BENCHMARK_RESOURCE_NOTE="galaxy-disk Ubuntu 24.04 x86_64 KVM; transform split instrumentation; attr0 item-rate matched control" \
  target/release/deps/throughput_iteration-c454b6542cb49121 \
  --base-url http://127.0.0.1:4356 \
  --warmup 30s \
  --duration 10m \
  --target-gb-day 613 \
  --profile ingest-only \
  --query-pressure off \
  --ingest-concurrency 32 \
  --signals spans \
  --items-per-batch 256 \
  --log-body-bytes 512 \
  --trace-attribute-bytes 0 \
  --metric-description-bytes 64 \
  --timestamp-mode advancing \
  --progress-interval 2m \
  --max-runtime 13m \
  --server-pid 2747130 \
  --resource-sample-interval 15s \
  --report-dir target/canardstack-bench/vm-linux-transform-split-20260517/spans-attr0-itemmatch-10m

# Pure-schema prototype spans ingest-only proof gate.
ps -eo comm=,args= | awk '$1 == "canardstack" || $1 ~ /^throughput_/ {print; found=1} END {exit found ? 0 : 1}'
rm -rf /tmp/canardstack-vm-prototype-spans-target1500-30m-20260517
env CANARDSTACK_BIND=127.0.0.1:4352 \
  CANARDSTACK_DATA_DIR=/tmp/canardstack-vm-prototype-spans-target1500-30m-20260517 \
  CANARDSTACK_DUCKDB_PATH=/tmp/canardstack-vm-prototype-spans-target1500-30m-20260517/canardstack.duckdb \
  CANARDSTACK_STORAGE_DIR=/tmp/canardstack-vm-prototype-spans-target1500-30m-20260517/storage \
  CANARDSTACK_API_KEY=dev-canardstack-key \
  CANARDSTACK_ADMIN_API_KEY=dev-canardstack-admin-key \
  CANARDSTACK_BENCHMARK_CPU_LIMIT="2 vCPU" \
  CANARDSTACK_BENCHMARK_MEMORY_LIMIT="7.8 GiB RAM" \
  CANARDSTACK_BENCHMARK_RESOURCE_NOTE="galaxy-disk Ubuntu 24.04 x86_64 KVM; 25G root disk; prototype no promoted telemetry columns" \
  target/release/canardstack serve

env CANARDSTACK_BENCHMARK_CPU_LIMIT="2 vCPU" \
  CANARDSTACK_BENCHMARK_MEMORY_LIMIT="7.8 GiB RAM" \
  CANARDSTACK_BENCHMARK_RESOURCE_NOTE="galaxy-disk Ubuntu 24.04 x86_64 KVM; 25G root disk; prototype no promoted telemetry columns" \
  target/release/deps/throughput_iteration-c454b6542cb49121 \
  --base-url http://127.0.0.1:4352 \
  --warmup 30s \
  --duration 30m \
  --target-gb-day 1500 \
  --profile ingest-only \
  --query-pressure off \
  --ingest-concurrency 32 \
  --signals spans \
  --items-per-batch 256 \
  --log-body-bytes 512 \
  --trace-attribute-bytes 256 \
  --metric-description-bytes 64 \
  --timestamp-mode advancing \
  --progress-interval 5m \
  --max-runtime 35m \
  --server-pid 1945121 \
  --resource-sample-interval 30s \
  --report-dir target/canardstack-bench/vm-linux-prototype-ingestonly-30m-20260517/spans-target1500-30m

# Pure-schema prototype logs ingest-only proof gate.
ps -eo comm=,args= | awk '$1 == "canardstack" || $1 ~ /^throughput_/ {print; found=1} END {exit found ? 0 : 1}'
rm -rf /tmp/canardstack-vm-prototype-logs-target2000-30m-20260517
env CANARDSTACK_BIND=127.0.0.1:4353 \
  CANARDSTACK_DATA_DIR=/tmp/canardstack-vm-prototype-logs-target2000-30m-20260517 \
  CANARDSTACK_DUCKDB_PATH=/tmp/canardstack-vm-prototype-logs-target2000-30m-20260517/canardstack.duckdb \
  CANARDSTACK_STORAGE_DIR=/tmp/canardstack-vm-prototype-logs-target2000-30m-20260517/storage \
  CANARDSTACK_API_KEY=dev-canardstack-key \
  CANARDSTACK_ADMIN_API_KEY=dev-canardstack-admin-key \
  CANARDSTACK_BENCHMARK_CPU_LIMIT="2 vCPU" \
  CANARDSTACK_BENCHMARK_MEMORY_LIMIT="7.8 GiB RAM" \
  CANARDSTACK_BENCHMARK_RESOURCE_NOTE="galaxy-disk Ubuntu 24.04 x86_64 KVM; 25G root disk; prototype no promoted telemetry columns" \
  target/release/canardstack serve

env CANARDSTACK_BENCHMARK_CPU_LIMIT="2 vCPU" \
  CANARDSTACK_BENCHMARK_MEMORY_LIMIT="7.8 GiB RAM" \
  CANARDSTACK_BENCHMARK_RESOURCE_NOTE="galaxy-disk Ubuntu 24.04 x86_64 KVM; 25G root disk; prototype no promoted telemetry columns" \
  target/release/deps/throughput_iteration-c454b6542cb49121 \
  --base-url http://127.0.0.1:4353 \
  --warmup 30s \
  --duration 30m \
  --target-gb-day 2000 \
  --profile ingest-only \
  --query-pressure off \
  --ingest-concurrency 32 \
  --signals logs \
  --items-per-batch 256 \
  --log-body-bytes 512 \
  --trace-attribute-bytes 256 \
  --metric-description-bytes 64 \
  --timestamp-mode advancing \
  --progress-interval 5m \
  --max-runtime 35m \
  --server-pid 2210670 \
  --resource-sample-interval 30s \
  --report-dir target/canardstack-bench/vm-linux-prototype-ingestonly-30m-20260517/logs-target2000-30m

target/release/deps/throughput_iteration-c454b6542cb49121 \
  --base-url http://127.0.0.1:4319 \
  --warmup 5s \
  --duration 20s \
  --target-gb-day 580.746 \
  --profile ingest-only \
  --query-pressure off \
  --ingest-concurrency 4 \
  --signals logs \
  --items-per-batch 256 \
  --log-body-bytes 512 \
  --trace-attribute-bytes 256 \
  --metric-description-bytes 64 \
  --progress-interval 5s \
  --max-runtime 35s \
  --server-pid 14911 \
  --resource-sample-interval 5s \
  --report-dir target/canardstack-bench/vm-linux-log10kdps-items256-conc4

target/release/deps/throughput_iteration-c454b6542cb49121 \
  --base-url http://127.0.0.1:4320 \
  --warmup 5s \
  --duration 20s \
  --target-gb-day 404.322 \
  --profile ingest-only \
  --query-pressure off \
  --ingest-concurrency 4 \
  --signals spans \
  --items-per-batch 256 \
  --log-body-bytes 512 \
  --trace-attribute-bytes 256 \
  --metric-description-bytes 64 \
  --progress-interval 5s \
  --max-runtime 35s \
  --server-pid 16005 \
  --resource-sample-interval 5s \
  --report-dir target/canardstack-bench/vm-linux-trace10ksps-items256-conc4
```

Exact VM max-load and confirmation benchmark commands used the same release
server pattern with fresh `/tmp` data directories per run:

```sh
env CANARDSTACK_BIND=127.0.0.1:<port> \
  CANARDSTACK_DATA_DIR=/tmp/canardstack-vm-<case> \
  CANARDSTACK_DUCKDB_PATH=/tmp/canardstack-vm-<case>/canardstack.duckdb \
  CANARDSTACK_STORAGE_DIR=/tmp/canardstack-vm-<case>/storage \
  CANARDSTACK_API_KEY=dev-canardstack-key \
  CANARDSTACK_ADMIN_API_KEY=dev-canardstack-admin-key \
  target/release/canardstack serve

target/release/deps/throughput_iteration-c454b6542cb49121 \
  --base-url http://127.0.0.1:4335 \
  --warmup 5s \
  --duration 20s \
  --target-gb-day 3000 \
  --profile ingest-only \
  --query-pressure off \
  --ingest-concurrency 32 \
  --signals logs \
  --items-per-batch 256 \
  --log-body-bytes 512 \
  --trace-attribute-bytes 256 \
  --metric-description-bytes 64 \
  --progress-interval 5s \
  --max-runtime 40s \
  --server-pid 49009 \
  --resource-sample-interval 5s \
  --report-dir target/canardstack-bench/vm-linux-maxload-20260517-conc32/logs-target3000

target/release/deps/throughput_iteration-c454b6542cb49121 \
  --base-url http://127.0.0.1:4335 \
  --warmup 5s \
  --duration 20s \
  --target-gb-day 3500 \
  --profile ingest-only \
  --query-pressure off \
  --ingest-concurrency 32 \
  --signals logs \
  --items-per-batch 256 \
  --log-body-bytes 512 \
  --trace-attribute-bytes 256 \
  --metric-description-bytes 64 \
  --progress-interval 5s \
  --max-runtime 40s \
  --server-pid 54165 \
  --resource-sample-interval 5s \
  --report-dir target/canardstack-bench/vm-linux-maxload-20260517-conc32/logs-target3500

target/release/deps/throughput_iteration-c454b6542cb49121 \
  --base-url http://127.0.0.1:4336 \
  --warmup 5s \
  --duration 20s \
  --target-gb-day 2000 \
  --profile ingest-only \
  --query-pressure off \
  --ingest-concurrency 32 \
  --signals spans \
  --items-per-batch 256 \
  --log-body-bytes 512 \
  --trace-attribute-bytes 256 \
  --metric-description-bytes 64 \
  --progress-interval 5s \
  --max-runtime 40s \
  --server-pid 60157 \
  --resource-sample-interval 5s \
  --report-dir target/canardstack-bench/vm-linux-maxload-20260517-conc32/spans-target2000

target/release/deps/throughput_iteration-c454b6542cb49121 \
  --base-url http://127.0.0.1:4336 \
  --warmup 5s \
  --duration 20s \
  --target-gb-day 2500 \
  --profile ingest-only \
  --query-pressure off \
  --ingest-concurrency 32 \
  --signals spans \
  --items-per-batch 256 \
  --log-body-bytes 512 \
  --trace-attribute-bytes 256 \
  --metric-description-bytes 64 \
  --progress-interval 5s \
  --max-runtime 40s \
  --server-pid 65137 \
  --resource-sample-interval 5s \
  --report-dir target/canardstack-bench/vm-linux-maxload-20260517-conc32/spans-target2500

env CANARDSTACK_BENCHMARK_CPU_LIMIT="2 vCPU" \
  CANARDSTACK_BENCHMARK_MEMORY_LIMIT="7.8 GiB RAM" \
  CANARDSTACK_BENCHMARK_RESOURCE_NOTE="galaxy-disk Ubuntu 24.04 x86_64 KVM; 25G root disk" \
  target/release/deps/throughput_iteration-c454b6542cb49121 \
  --base-url http://127.0.0.1:4343 \
  --warmup 15s \
  --duration 5m \
  --target-gb-day 2000 \
  --profile ingest-only \
  --query-pressure off \
  --ingest-concurrency 32 \
  --signals logs \
  --items-per-batch 256 \
  --log-body-bytes 512 \
  --trace-attribute-bytes 256 \
  --metric-description-bytes 64 \
  --timestamp-mode advancing \
  --progress-interval 30s \
  --max-runtime 7m \
  --server-pid 306772 \
  --resource-sample-interval 10s \
  --report-dir target/canardstack-bench/vm-linux-confirm-20260517-advancing/logs-target2000-5m

env CANARDSTACK_BENCHMARK_CPU_LIMIT="2 vCPU" \
  CANARDSTACK_BENCHMARK_MEMORY_LIMIT="7.8 GiB RAM" \
  CANARDSTACK_BENCHMARK_RESOURCE_NOTE="galaxy-disk Ubuntu 24.04 x86_64 KVM; 25G root disk" \
  target/release/deps/throughput_iteration-c454b6542cb49121 \
  --base-url http://127.0.0.1:4342 \
  --warmup 15s \
  --duration 5m \
  --target-gb-day 1500 \
  --profile ingest-only \
  --query-pressure off \
  --ingest-concurrency 32 \
  --signals spans \
  --items-per-batch 256 \
  --log-body-bytes 512 \
  --trace-attribute-bytes 256 \
  --metric-description-bytes 64 \
  --timestamp-mode advancing \
  --progress-interval 30s \
  --max-runtime 7m \
  --server-pid 260906 \
  --resource-sample-interval 10s \
  --report-dir target/canardstack-bench/vm-linux-confirm-20260517-advancing/spans-target1500-5m

env CANARDSTACK_BENCHMARK_CPU_LIMIT="2 vCPU" \
  CANARDSTACK_BENCHMARK_MEMORY_LIMIT="7.8 GiB RAM" \
  CANARDSTACK_BENCHMARK_RESOURCE_NOTE="galaxy-disk Ubuntu 24.04 x86_64 KVM; 25G root disk" \
  target/release/deps/throughput_iteration-c454b6542cb49121 \
  --base-url http://127.0.0.1:4344 \
  --warmup 15s \
  --duration 5m \
  --target-gb-day 2000 \
  --profile mixed-query \
  --query-pressure low \
  --ingest-concurrency 32 \
  --signals logs \
  --items-per-batch 256 \
  --log-body-bytes 512 \
  --trace-attribute-bytes 256 \
  --metric-description-bytes 64 \
  --timestamp-mode advancing \
  --progress-interval 30s \
  --max-runtime 7m \
  --server-pid 349085 \
  --resource-sample-interval 10s \
  --report-dir target/canardstack-bench/vm-linux-query-low-20260517/logs-target2000-5m

env CANARDSTACK_BENCHMARK_CPU_LIMIT="2 vCPU" \
  CANARDSTACK_BENCHMARK_MEMORY_LIMIT="7.8 GiB RAM" \
  CANARDSTACK_BENCHMARK_RESOURCE_NOTE="galaxy-disk Ubuntu 24.04 x86_64 KVM; 25G root disk" \
  target/release/deps/throughput_iteration-c454b6542cb49121 \
  --base-url http://127.0.0.1:4345 \
  --warmup 15s \
  --duration 5m \
  --target-gb-day 1500 \
  --profile mixed-query \
  --query-pressure low \
  --ingest-concurrency 32 \
  --signals spans \
  --items-per-batch 256 \
  --log-body-bytes 512 \
  --trace-attribute-bytes 256 \
  --metric-description-bytes 64 \
  --timestamp-mode advancing \
  --progress-interval 30s \
  --max-runtime 7m \
  --server-pid 391451 \
  --resource-sample-interval 10s \
  --report-dir target/canardstack-bench/vm-linux-query-low-20260517/spans-target1500-5m

env CANARDSTACK_BENCHMARK_CPU_LIMIT="2 vCPU" \
  CANARDSTACK_BENCHMARK_MEMORY_LIMIT="7.8 GiB RAM" \
  CANARDSTACK_BENCHMARK_RESOURCE_NOTE="galaxy-disk Ubuntu 24.04 x86_64 KVM; 25G root disk" \
  target/release/deps/throughput_iteration-c454b6542cb49121 \
  --base-url http://127.0.0.1:4346 \
  --warmup 30s \
  --duration 30m \
  --target-gb-day 2000 \
  --profile mixed-query \
  --query-pressure low \
  --ingest-concurrency 32 \
  --signals logs \
  --items-per-batch 256 \
  --log-body-bytes 512 \
  --trace-attribute-bytes 256 \
  --metric-description-bytes 64 \
  --timestamp-mode advancing \
  --progress-interval 5m \
  --max-runtime 35m \
  --server-pid 437430 \
  --resource-sample-interval 30s \
  --report-dir target/canardstack-bench/vm-linux-query-low-30m-20260517/logs-target2000-30m

env CANARDSTACK_BENCHMARK_CPU_LIMIT="2 vCPU" \
  CANARDSTACK_BENCHMARK_MEMORY_LIMIT="7.8 GiB RAM" \
  CANARDSTACK_BENCHMARK_RESOURCE_NOTE="galaxy-disk Ubuntu 24.04 x86_64 KVM; 25G root disk" \
  target/release/deps/throughput_iteration-c454b6542cb49121 \
  --base-url http://127.0.0.1:4347 \
  --warmup 30s \
  --duration 30m \
  --target-gb-day 1500 \
  --profile mixed-query \
  --query-pressure low \
  --ingest-concurrency 32 \
  --signals spans \
  --items-per-batch 256 \
  --log-body-bytes 512 \
  --trace-attribute-bytes 256 \
  --metric-description-bytes 64 \
  --timestamp-mode advancing \
  --progress-interval 5m \
  --max-runtime 35m \
  --server-pid 682612 \
  --resource-sample-interval 30s \
  --report-dir target/canardstack-bench/vm-linux-query-low-30m-20260517/spans-target1500-30m

pgrep -af "target/release/(canardstack|deps/throughput_iteration)" || true

rm -rf /tmp/canardstack-vm-logs-target1500-30m-20260517-next

env CANARDSTACK_BIND=127.0.0.1:4348 \
  CANARDSTACK_DATA_DIR=/tmp/canardstack-vm-logs-target1500-30m-20260517-next \
  CANARDSTACK_DUCKDB_PATH=/tmp/canardstack-vm-logs-target1500-30m-20260517-next/canardstack.duckdb \
  CANARDSTACK_STORAGE_DIR=/tmp/canardstack-vm-logs-target1500-30m-20260517-next/storage \
  CANARDSTACK_API_KEY=dev-canardstack-key \
  CANARDSTACK_ADMIN_API_KEY=dev-canardstack-admin-key \
  CANARDSTACK_BENCHMARK_CPU_LIMIT="2 vCPU" \
  CANARDSTACK_BENCHMARK_MEMORY_LIMIT="7.8 GiB RAM" \
  CANARDSTACK_BENCHMARK_RESOURCE_NOTE="galaxy-disk Ubuntu 24.04 x86_64 KVM; 25G root disk" \
  target/release/canardstack serve

env CANARDSTACK_BENCHMARK_CPU_LIMIT="2 vCPU" \
  CANARDSTACK_BENCHMARK_MEMORY_LIMIT="7.8 GiB RAM" \
  CANARDSTACK_BENCHMARK_RESOURCE_NOTE="galaxy-disk Ubuntu 24.04 x86_64 KVM; 25G root disk" \
  target/release/deps/throughput_iteration-c454b6542cb49121 \
  --base-url http://127.0.0.1:4348 \
  --warmup 30s \
  --duration 30m \
  --target-gb-day 1500 \
  --profile mixed-query \
  --query-pressure low \
  --ingest-concurrency 32 \
  --signals logs \
  --items-per-batch 256 \
  --log-body-bytes 512 \
  --trace-attribute-bytes 256 \
  --metric-description-bytes 64 \
  --timestamp-mode advancing \
  --progress-interval 5m \
  --max-runtime 35m \
  --server-pid 948661 \
  --resource-sample-interval 30s \
  --report-dir target/canardstack-bench/vm-linux-query-low-30m-20260517-next/logs-target1500-30m

pgrep -af "target/release/(canardstack|deps/throughput_iteration)" || true

rm -rf /tmp/canardstack-vm-spans-target1200-30m-20260517-next

env CANARDSTACK_BIND=127.0.0.1:4349 \
  CANARDSTACK_DATA_DIR=/tmp/canardstack-vm-spans-target1200-30m-20260517-next \
  CANARDSTACK_DUCKDB_PATH=/tmp/canardstack-vm-spans-target1200-30m-20260517-next/canardstack.duckdb \
  CANARDSTACK_STORAGE_DIR=/tmp/canardstack-vm-spans-target1200-30m-20260517-next/storage \
  CANARDSTACK_API_KEY=dev-canardstack-key \
  CANARDSTACK_ADMIN_API_KEY=dev-canardstack-admin-key \
  CANARDSTACK_BENCHMARK_CPU_LIMIT="2 vCPU" \
  CANARDSTACK_BENCHMARK_MEMORY_LIMIT="7.8 GiB RAM" \
  CANARDSTACK_BENCHMARK_RESOURCE_NOTE="galaxy-disk Ubuntu 24.04 x86_64 KVM; 25G root disk" \
  target/release/canardstack serve

env CANARDSTACK_BENCHMARK_CPU_LIMIT="2 vCPU" \
  CANARDSTACK_BENCHMARK_MEMORY_LIMIT="7.8 GiB RAM" \
  CANARDSTACK_BENCHMARK_RESOURCE_NOTE="galaxy-disk Ubuntu 24.04 x86_64 KVM; 25G root disk" \
  target/release/deps/throughput_iteration-c454b6542cb49121 \
  --base-url http://127.0.0.1:4349 \
  --warmup 30s \
  --duration 30m \
  --target-gb-day 1200 \
  --profile mixed-query \
  --query-pressure low \
  --ingest-concurrency 32 \
  --signals spans \
  --items-per-batch 256 \
  --log-body-bytes 512 \
  --trace-attribute-bytes 256 \
  --metric-description-bytes 64 \
  --timestamp-mode advancing \
  --progress-interval 5m \
  --max-runtime 35m \
  --server-pid 1132779 \
  --resource-sample-interval 30s \
  --report-dir target/canardstack-bench/vm-linux-query-low-30m-20260517-next/spans-target1200-30m

env CANARDSTACK_BIND=127.0.0.1:4350 \
  CANARDSTACK_DATA_DIR=/tmp/canardstack-vm-spans-target1500-30m-ingestonly-20260517 \
  CANARDSTACK_DUCKDB_PATH=/tmp/canardstack-vm-spans-target1500-30m-ingestonly-20260517/canardstack.duckdb \
  CANARDSTACK_STORAGE_DIR=/tmp/canardstack-vm-spans-target1500-30m-ingestonly-20260517/storage \
  CANARDSTACK_API_KEY=dev-canardstack-key \
  CANARDSTACK_ADMIN_API_KEY=dev-canardstack-admin-key \
  CANARDSTACK_BENCHMARK_CPU_LIMIT="2 vCPU" \
  CANARDSTACK_BENCHMARK_MEMORY_LIMIT="7.8 GiB RAM" \
  CANARDSTACK_BENCHMARK_RESOURCE_NOTE="galaxy-disk Ubuntu 24.04 x86_64 KVM; 25G root disk" \
  target/release/canardstack serve

env CANARDSTACK_BENCHMARK_CPU_LIMIT="2 vCPU" \
  CANARDSTACK_BENCHMARK_MEMORY_LIMIT="7.8 GiB RAM" \
  CANARDSTACK_BENCHMARK_RESOURCE_NOTE="galaxy-disk Ubuntu 24.04 x86_64 KVM; 25G root disk" \
  target/release/deps/throughput_iteration-c454b6542cb49121 \
  --base-url http://127.0.0.1:4350 \
  --warmup 30s \
  --duration 30m \
  --target-gb-day 1500 \
  --profile ingest-only \
  --query-pressure off \
  --ingest-concurrency 32 \
  --signals spans \
  --items-per-batch 256 \
  --log-body-bytes 512 \
  --trace-attribute-bytes 256 \
  --metric-description-bytes 64 \
  --timestamp-mode advancing \
  --progress-interval 5m \
  --max-runtime 35m \
  --server-pid 1345788 \
  --resource-sample-interval 30s \
  --report-dir target/canardstack-bench/vm-linux-ingestonly-30m-20260517/spans-target1500-30m

env CANARDSTACK_BIND=127.0.0.1:4351 \
  CANARDSTACK_DATA_DIR=/tmp/canardstack-vm-logs-target2000-30m-ingestonly-20260517 \
  CANARDSTACK_DUCKDB_PATH=/tmp/canardstack-vm-logs-target2000-30m-ingestonly-20260517/canardstack.duckdb \
  CANARDSTACK_STORAGE_DIR=/tmp/canardstack-vm-logs-target2000-30m-ingestonly-20260517/storage \
  CANARDSTACK_API_KEY=dev-canardstack-key \
  CANARDSTACK_ADMIN_API_KEY=dev-canardstack-admin-key \
  CANARDSTACK_BENCHMARK_CPU_LIMIT="2 vCPU" \
  CANARDSTACK_BENCHMARK_MEMORY_LIMIT="7.8 GiB RAM" \
  CANARDSTACK_BENCHMARK_RESOURCE_NOTE="galaxy-disk Ubuntu 24.04 x86_64 KVM; 25G root disk" \
  target/release/canardstack serve

env CANARDSTACK_BENCHMARK_CPU_LIMIT="2 vCPU" \
  CANARDSTACK_BENCHMARK_MEMORY_LIMIT="7.8 GiB RAM" \
  CANARDSTACK_BENCHMARK_RESOURCE_NOTE="galaxy-disk Ubuntu 24.04 x86_64 KVM; 25G root disk" \
  target/release/deps/throughput_iteration-c454b6542cb49121 \
  --base-url http://127.0.0.1:4351 \
  --warmup 30s \
  --duration 30m \
  --target-gb-day 2000 \
  --profile ingest-only \
  --query-pressure off \
  --ingest-concurrency 32 \
  --signals logs \
  --items-per-batch 256 \
  --log-body-bytes 512 \
  --trace-attribute-bytes 256 \
  --metric-description-bytes 64 \
  --timestamp-mode advancing \
  --progress-interval 5m \
  --max-runtime 35m \
  --server-pid 1611281 \
  --resource-sample-interval 30s \
  --report-dir target/canardstack-bench/vm-linux-ingestonly-30m-20260517/logs-target2000-30m

env CANARDSTACK_BIND=127.0.0.1:4352 \
  CANARDSTACK_DATA_DIR=/tmp/canardstack-vm-spans-attr0-10m-ingestonly-20260517 \
  CANARDSTACK_DUCKDB_PATH=/tmp/canardstack-vm-spans-attr0-10m-ingestonly-20260517/canardstack.duckdb \
  CANARDSTACK_STORAGE_DIR=/tmp/canardstack-vm-spans-attr0-10m-ingestonly-20260517/storage \
  CANARDSTACK_API_KEY=dev-canardstack-key \
  CANARDSTACK_ADMIN_API_KEY=dev-canardstack-admin-key \
  CANARDSTACK_BENCHMARK_CPU_LIMIT="2 vCPU" \
  CANARDSTACK_BENCHMARK_MEMORY_LIMIT="7.8 GiB RAM" \
  CANARDSTACK_BENCHMARK_RESOURCE_NOTE="galaxy-disk Ubuntu 24.04 x86_64 KVM; 25G root disk" \
  target/release/canardstack serve

env CANARDSTACK_BENCHMARK_CPU_LIMIT="2 vCPU" \
  CANARDSTACK_BENCHMARK_MEMORY_LIMIT="7.8 GiB RAM" \
  CANARDSTACK_BENCHMARK_RESOURCE_NOTE="galaxy-disk Ubuntu 24.04 x86_64 KVM; 25G root disk" \
  target/release/deps/throughput_iteration-c454b6542cb49121 \
  --base-url http://127.0.0.1:4352 \
  --warmup 30s \
  --duration 10m \
  --target-gb-day 650 \
  --profile ingest-only \
  --query-pressure off \
  --ingest-concurrency 32 \
  --signals spans \
  --items-per-batch 256 \
  --log-body-bytes 512 \
  --trace-attribute-bytes 0 \
  --metric-description-bytes 64 \
  --timestamp-mode advancing \
  --progress-interval 2m \
  --max-runtime 13m \
  --server-pid 1855943 \
  --resource-sample-interval 30s \
  --report-dir target/canardstack-bench/vm-linux-hotspot-20260517/spans-attr0-10m
```

Caveats:

- `galaxy-disk` is a small 2-vCPU VM with one root disk. These results are
  stronger than local Mac scouts for Linux behavior, but still not
  claim-grade cloud/object-store evidence.
- Short 20-second max-load passes are useful for bracketing the first limiter,
  but the 5-minute confirmations are the stronger proof points.
- The latest 30-minute runs used low query pressure: one query every `10s`,
  query concurrency `1`. They do not prove medium/high query interference or
  longer 2-hour stability.
- The harness still uses uncompressed protobuf request body length as the
  decoded-byte proxy.

## Next Recommended Proof Gates

1. Do not treat pure `otlp2records` storage as a throughput win yet. It remains
   the cleaner schema boundary, but the prototype failed both 30-minute
   ingest-only controls and worsened 429 onset. If kept for schema correctness,
   gate it separately at the last known passing 30-minute low-query points:
   logs `1500 GB/day` and spans `1200 GB/day`.
2. Split and profile `otlp_transform` before changing storage again. Add
   one more level inside `otlp2records` trace Arrow build, or vendor/patch
   `otlp2records` with internal timings around `ResourceContext::from_attrs`,
   `ScopeContext::new`, and `append_attrs_json` for `span_attributes`. The
   Canardstack wrapper split proves the hot lane is protobuf decode plus Arrow
   build, but cannot yet isolate resource/scope/span attribute JSON shares
   inside the library call.
3. Prototype the smallest meaningful trace build optimization before running
   another 30-minute ceiling gate: avoid repeated serialization of identical
   resource/scope attributes per span, and benchmark whether dictionary-like
   reuse or cached JSON strings reduces `otlp2records_arrow_build` at the
   item-rate matched shape.
4. Add one queue/admission control after transform timing exists: same spans
   shape, but record accepted/rejected request counters by reason and queue
   wait/age at the moment of rejection. The current evidence shows
   `queue_admission` rising while queue age grows late in the run, but not
   enough to distinguish worker saturation from a too-conservative pressure
   threshold.
5. Run one medium-query 5-minute scout only after explicitly choosing
   query-saturation exploration over ingest-ceiling bracketing.
6. Gate any `otlp2records` partitioned API adoption on measurement showing that
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
