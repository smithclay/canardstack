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

- `Cargo.toml` uses crates.io `otlp2records` `0.7.0` with the `parquet`
  feature. That release includes the observer-based transform instrumentation
  used by the benchmark harness.
- `src/otlp.rs::transform` calls non-partitioned `transform_logs`,
  `transform_traces`, and `transform_metrics`.
- Crates.io source inspection showed the partitioned `otlp2records` APIs call the existing
  non-partitioned transform first, then split batches by `service_name` with
  `group_batch_by_service`. They are useful for deterministic lane ownership
  after transform, but they do not provide parallel decode or transform.
- The non-default `otlp2records-observer` Cargo feature uses the supported
  `transform_*_with_observer` APIs to report internal `otlp2records` phase
  timings and transform counters. Do not enable it in normal production builds.
- The Canardstack observer sink aggregates `otlp2records` phase timings and
  counters per request before touching the global `Metrics` mutex. Earlier
  observer-enabled benchmark runs that emitted one global metrics update per
  per-span/per-log phase event overstated transform cost and could create
  artificial queue pressure.

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

2026-05-18 stage-visibility audit:

- `throughput_iteration` still treats HTTP `202` as the primary pass/fail
  throughput signal. That is accepted decoded bytes/sec, not query-visible or
  DuckLake-committed bytes/sec.
- A `202` means accepted into bounded process memory. It does not mean the rows
  were transformed, enqueued, drained, sealed into Parquet, registered with
  DuckLake, or visible to compatibility queries.
- Recent OrbStack reports lacked server CPU/RSS samples because Mac-side
  benchmark drivers use local `ps`, which cannot sample a server PID inside the
  Linux VM. Running the driver inside the VM can make `--server-pid` work, but
  it competes for VM CPU/cache/RSS and can depress receiver throughput compared
  with Mac-side driver behavior.
- The null-sink/control matrix separated accepted, transformed, enqueued,
  flushed, sealed, and query-visible progress. Do not prototype a new staged
  architecture unless a later report shows the specific stage that is lagging.

Current stage counters and gauges:

- Accepted request bytes: `canardstack_ingest_request_bytes_total`.
- Accepted decoded bytes: `canardstack_ingest_decoded_bytes_total`.
- Transform output rows: `canardstack_ingest_transformed_rows_total`.
- Enqueued rows/bytes: `canardstack_ingest_enqueued_rows_total` and
  `canardstack_ingest_enqueued_bytes_total`.
- Queue rows/bytes/oldest age/pressure:
  `canardstack_ingest_queue_*`.
- Flush drain/coalesce/buffer rows and bytes:
  `canardstack_ingest_flush_drained_*`,
  `canardstack_ingest_flush_coalesced_*`, and
  `canardstack_ingest_flush_buffered_*`.
- Immutable segment buffers/seals:
  `canardstack_immutable_buffer_*` and
  `canardstack_immutable_segments_sealed_*`.
- Storage/query-visible gauges:
  `canardstack_storage_logical_rows`,
  `canardstack_ducklake_parquet_rows`, and
  `canardstack_ducklake_parquet_files`.
- Freshness lag: `canardstack_ingest_to_query_lag_seconds`.
- Writer/storage phase timings:
  `canardstack_phase_duration_seconds{phase="storage_*"}`.

`throughput_iteration` report version `0.3.3` added
`stage_throughput`, computed from measured-window start/end `/metrics`
samples. Use that section for fast-fail decisions; an accepted-throughput
increase without matching transformed/enqueued/flushed/visible progress is a
negative result.

`throughput_iteration` report versions `0.3.5` through `0.3.8` added historical
Loki candidate-scout and shadow metrics. Those were proof scaffolding for the
progressive query path and are no longer active runtime controls.

`throughput_iteration` report version `0.3.7` added shadow batch fields:
`final_batch_size` and `final_batches_scanned`. These were originally used to
distinguish one-file progressive reads from batched raw-Parquet candidate
reads; raw-Parquet execution is now a retired scout.

`throughput_iteration` report version `0.3.8` added historical shadow mode and
timing split fields. They separated DuckLake metadata planning from
logical-window execution cost while the path was still a scout.

`throughput_iteration` report version `0.3.9` added
`loki_progressive_query`, the authoritative Loki progressive query report. Use
it to compare candidate scope, scanned rows/files, and planning/execution
timing.

`throughput_iteration` report version `0.3.10` retires idle persistent client
sockets before the server read timeout. This keeps low per-worker request-rate
runs from counting expected idle keep-alive timeout closures as ingest
transport resets.

`throughput_iteration` report version `0.3.11` removes retired Loki
candidate-scout and shadow report sections. Backward Loki `query_range`
progressive metrics now live in `loki_progressive_query`.

Benchmark-only reversible controls:

- `CANARDSTACK_BENCH_INGEST_CONTROL=validation-only` validates auth, content
  type, compressed size, dependency health, and runtime memory admission, then
  returns `202` without decompression, transform, timestamp validation, enqueue,
  flush, or storage. This estimates the synchronous HTTP + cheap validation
  ceiling. It is not a production acknowledgement mode.
- `CANARDSTACK_BENCH_INGEST_CONTROL=transform-only` runs decompression,
  `otlp2records`, timestamp validation, and peak runtime-memory admission, then
  returns `202` without enqueue or storage. It emits
  `canardstack_ingest_control_dropped_*` counters for transformed-but-dropped
  rows/bytes.
- `CANARDSTACK_BENCH_STORAGE_CONTROL=null-sink` leaves ingest and queue
  admission intact, and flush still drains and coalesces batches, but the flush
  path drops coalesced batches instead of appending immutable buffers or
  registering DuckLake files. It emits `canardstack_ingest_null_sink_*`
  counters. Query-visible rows are expected not to advance.
- Defaults are `full` for both controls. Do not enable these outside controlled
  benchmark runs.
- Backward Loki `query_range` now always uses the newest-first DuckLake
  candidate-window logical query path. There is no experimental flag or shadow
  mode for this path.

HTTP keep-alive scout control:

- `CANARDSTACK_BENCH_HTTP_KEEPALIVE=true` enables a benchmark-only HTTP/1.1
  keep-alive loop in the existing synchronous std-library server. Default server
  behavior still sends `connection: close`.
- `throughput_iteration` report version `0.3.4` adds
  `--connection-mode close|persistent`. `close` is the default. In
  `persistent` mode each ingest worker owns one reusable TCP connection; worker
  connections are not shared or serialized.
- This control isolates TCP accept, socket setup, and per-connection thread
  churn. It is not a writer-lane experiment and does not change ingest
  acknowledgement semantics.

2026-05-18 control-matrix scout:

- Environment: OrbStack Linux VMs, VM-local benchmark driver against
  `127.0.0.1`, `--features otlp2records-observer`, 32 ingest workers,
  `items-per-batch=256`, advancing timestamps. VM-local driver gives server
  CPU/RSS samples but competes with the server, so treat throughput ceilings as
  directional.
- Logs target was 5000 GB/day for 60s measured / 10s warmup. Accepted decoded
  throughput stayed in one band across controls: validation-only 53.3 MB/s,
  transform-only 52.6 MB/s, null-sink 52.5 MB/s, full storage 53.2 MB/s.
- Spans target was 4000 GB/day for 45s measured / 10s warmup. Accepted decoded
  throughput again stayed in one band: validation-only 36.5 MB/s,
  transform-only 36.1 MB/s, null-sink 36.1 MB/s, full storage 36.6 MB/s.
- Null-sink rows tracked transformed/enqueued rows, and full-storage
  storage-visible rows tracked transformed/enqueued rows closely over the short
  window. Removing durable storage did not improve accepted throughput.
- Interpretation: at this shape, the next bottleneck is not DuckLake
  registration, Parquet sealing, or queue drain. The current ceiling is at or
  before HTTP request handling plus payload generation/routing, with transform
  CPU visible but not decisive for accepted throughput. Do not start a
  writer-lane rewrite from this evidence. The next experiment should isolate the
  benchmark driver/routing and HTTP request path before changing storage
  architecture.

2026-05-18 Loki progressive shadow scout:

- Shape: local release server, `CANARDSTACK_BENCH_HTTP_KEEPALIVE=true`, logs
  only, persistent ingest connections, 32 ingest workers, mixed-query profile,
  medium query pressure, advancing timestamps, 500 GB/day target, 10s warmup,
  120s measured. Reports:
  `target/canardstack-bench/loki-progressive-shadow/off/20260518T223446Z/report.json`
  and
  `target/canardstack-bench/loki-progressive-shadow/on/20260518T223207Z/report.json`.
- Both runs hit one ingest transport reset, so treat pass/fail as inconclusive
  and compare directional metrics only.
- Baseline: 5.784 MB/s accepted decoded, query p50/p95/p99
  `83.8/124.7/127.8 ms`, storage-visible logs `49.8 rows/s`, final logs
  `5979` rows in `12` files.
- Shadow enabled: 5.785 MB/s accepted decoded, query p50/p95/p99
  `83.2/140.7/162.2 ms`, storage-visible logs `45.6 rows/s`, final logs
  `5961` rows in `12` candidate files. Shadow executed `16` measured matches,
  `0` mismatches, scanned `1/12` files and `498/5961` rows in the final sample,
  and recorded ~`895 ms` average shadow execution timing.
- Interpretation: newest-first DuckLake planning is semantically promising
  because one recent file satisfied the 100-row Loki limit with exact shadow
  matches. The direct `read_parquet` shadow executor is not yet a performance
  win under pressure; it added material sidecar work even while accepted
  throughput remained unchanged. Treat this as historical evidence only: active
  progressive work now stays on logical DuckLake table queries.

2026-05-18 Loki progressive shadow batch scout:

- Shape: same as the shadow scout above, with batch size `4`. Report:
  `target/canardstack-bench/loki-progressive-shadow/batch4/20260518T224645Z/report.json`.
- The run hit two ingest transport resets, so treat pass/fail as inconclusive
  and compare directional metrics only.
- Batch-4 shadow: 5.777 MB/s accepted decoded, query p50/p95/p99
  `74.0/110.2/145.5 ms`, storage-visible logs `47.8 rows/s`, final logs
  `5823` rows in `12` files. Shadow executed `16` measured matches,
  `0` mismatches, scanned one batch containing `4/12` files and `1988/5823`
  rows in the final sample, and recorded ~`894 ms` average shadow execution
  timing.
- Interpretation: batching candidate files did not reduce shadow execution
  timing compared with the one-file scout (~`895 ms`) and scanned more rows to
  satisfy the same 100-row limit. This closed the raw-Parquet scout path and
  moved the proof to candidate-limited logical DuckLake queries.

2026-05-18 Loki logical-window shadow scout:

- Shape: same mixed logs workload, using logical-window shadow mode and batch
  size `1`. Report:
  `target/canardstack-bench/loki-progressive-shadow/logical-window/20260518T225741Z/report.json`.
- The run hit three ingest transport resets, so treat pass/fail as
  inconclusive and compare directional metrics only.
- Logical-window shadow: 5.769 MB/s accepted decoded, query p50/p95/p99
  `74.8/146.0/148.5 ms`, storage-visible logs `47.5 rows/s`, final logs
  `5783` rows in `12` files. Shadow executed `16` measured matches,
  `0` mismatches, scoped `1/12` candidate files and `490/5783` rows in the
  final sample.
- Timing split: candidate planning averaged ~`3.8 ms`; candidate execution
  averaged ~`891.6 ms`; total shadow timing averaged ~`896.2 ms`.
- Interpretation: DuckLake metadata planning is not the shadow cost. The cost
  is the extra query execution itself. The next proof was to avoid double
  execution by running the candidate-limited logical path as the sole response
  source behind a stricter experimental gate.

2026-05-18 Loki authoritative progressive query scout:

- Shape: same mixed logs workload, with authoritative progressive query enabled
  behind the historical experimental flag and batch size `1`. Report:
  `target/canardstack-bench/loki-progressive-authoritative/on/20260518T230828Z/report.json`.
- The run hit two ingest transport resets, so treat pass/fail as inconclusive
  and compare directional metrics only.
- Authoritative progressive query: 5.777 MB/s accepted decoded, query
  p50/p95/p99 `62.9/117.6/144.2 ms`, storage-visible logs `48.8 rows/s`,
  final logs `5945` rows in `12` files. Progressive query served `16`
  measured Loki requests, with `0` fallbacks, scoped `1/12` candidate files and
  `500/5945` rows in the final sample.
- Timing split: candidate planning averaged ~`7.8 ms`; candidate execution
  averaged ~`899.7 ms`; total progressive timing averaged ~`907 ms`.
- Interpretation: removing the shadow double-query tax improved observed Loki
  query p50/p95 versus the same logical-window shadow run, but the
  candidate-limited DuckDB execution itself still costs roughly the same as
  before. This validates the cleaner vertical slice and keeps compatibility
  stable, but it is not yet a large performance win.

2026-05-18 DuckDB/DuckLake logical plan proof:

- Added `GET /api/admin/query/loki-progressive-explain`, which generates two
  bounded Loki `query_range` SQL shapes and asks DuckDB to `EXPLAIN` or
  `EXPLAIN ANALYZE` them. The generated SQL targets the DuckLake logical table
  (`main.logs` in the response display, attached as `canardlake.logs` inside
  DuckDB), not `read_parquet`.
- On the authoritative benchmark data, `analyze=true` with a matching `{}` Loki
  selector showed the full logical query reading `13` files / `6256` rows with
  DuckDB total time ~`65.7 ms`.
- The progressive logical-window query added only a timestamp lower bound from
  DuckLake metadata and DuckDB read `1` file / `311` rows with total time
  ~`4.0 ms`.
- Interpretation: the architecture should rely on DuckDB/DuckLake for physical
  file planning and reads. Canardstack should not execute Loki queries by raw
  Parquet file path. The remaining mixed-pressure latency is not because
  DuckDB cannot prune the logical query; it is likely query contention,
  connection/setup cost, or the benchmark pressure shape around the query
  engine.

2026-05-18 current direction and timer-fix proof:

- The Brooksian end-to-end direction is now one coherent vertical slice:
  DuckLake metadata may bound a logical Loki query, but DuckDB/DuckLake owns
  physical file planning and reads. Raw-Parquet shadow execution is retired
  from active code paths.
- `with_query_conn` now checks the completion flag before waiting, so a fast
  query that finishes before the timeout thread starts waiting no longer
  reports a timeout-sized phase delay.
- Timer-fix rerun report:
  `target/canardstack-bench/loki-progressive-authoritative/timerfix/20260518T232731Z/report.json`.
  The run was not a clean pass because it had five ingest socket resets, but
  the progressive query proof result is decisive: `0` fallbacks,
  `1/12` files scanned in the final sample, `490` rows scanned, candidate
  planning averaged ~`7.8 ms`, candidate execution averaged ~`15.3 ms`, and
  total progressive timing averaged ~`22.6 ms`.
- Interpretation: the previous ~`900 ms` mixed-pressure candidate execution
  timing was primarily timer/reporting behavior, not DuckDB failing to prune
  the logical DuckLake query. Keep the architecture direction. The next proof
  should move to reliability and freshness: explain the persistent ingest
  transport resets under mixed pressure, then revisit durable raw spool
  semantics and query-visible freshness under backlog.
- A follow-up no-peek server run reduced transport resets from five to one but
  still logged macOS socket timeout errors on idle keep-alive reads. The likely
  remaining reset source is the benchmark client reusing a persistent socket
  after the server's 30s read timeout. Report:
  `target/canardstack-bench/loki-progressive-authoritative/nopeek/20260518T233732Z/report.json`.
- Idle-reconnect rerun report:
  `target/canardstack-bench/loki-progressive-authoritative/idle-reconnect/20260518T234116Z/report.json`.
  This passed with `0` transport errors, query p50/p95/p99
  `75.1/134.0/148.6 ms`, progressive `0` fallbacks, `1/12` files scanned,
  candidate execution ~`16.6 ms`, and total progressive timing ~`23.6 ms`.
  Queue oldest age stayed ~`0.4s`. Mid-run freshness lag rose and fell with the
  30s scheduler flush cadence, ending around `20.9s`; this is flush cadence
  visibility, not queue backlog. The next freshness proof should test shorter
  flush cadence or explicit freshness SLA controls under sustained pressure.
- Committed default-path rerun report:
  `target/canardstack-bench/loki-progressive-committed/default/20260518T235455Z/report.json`.
  This ran without the historical experimental Loki flag and passed with `0`
  transport errors, query p50/p95/p99 `67.6/128.3/142.5 ms`, progressive `16`
  ok requests, `1/12` files scanned, candidate execution ~`14.5 ms`, and total
  progressive timing ~`21.6 ms`. Queue oldest age stayed ~`0.4s`; freshness
  lag again tracked flush visibility cadence rather than ingest backlog.
- Post-review regression report:
  `target/canardstack-bench/loki-progressive-committed/review/20260519T000414Z/report.json`.
  This includes the safer full bounded candidate-file set for selective
  filters and still passed with `0` transport errors, query p50/p95/p99
  `72.0/135.5/150.4 ms`, progressive `16` ok requests, `1/12` files scanned,
  candidate execution ~`16.2 ms`, and total progressive timing ~`23.3 ms`.

2026-05-18 HTTP keep-alive scout:

- Environment: OrbStack Linux VMs, VM-local benchmark driver against
  `127.0.0.1`, `--features otlp2records-observer`, 32 ingest workers,
  advancing timestamps, 45s measured / 10s warmup, query pressure off.
- Logs used `target=5000 GB/day`, `items-per-batch=256`,
  `log-body-bytes=512`. Validation-only close mode accepted 52.8 MB/s with
  server CPU peaking at 60.5% and p50 ingest latency 107.9 ms. Persistent mode
  accepted 57.8 MB/s with server CPU 22.8% and p50 41.1 ms.
- Logs transform-only close mode transformed 80.2k rows/s at 54.3 MB/s decoded.
  Persistent mode transformed 85.1k rows/s at 57.6 MB/s decoded. Rows were
  intentionally dropped by the control.
- Logs full-storage close mode made 81.7k rows/s storage-visible at
  54.4 MB/s decoded; persistent mode made 85.1k rows/s storage-visible at
  57.6 MB/s decoded. Queue oldest age remained around 1.2s and no `429`s were
  observed.
- Spans full-storage close mode made 77.3k rows/s storage-visible at
  36.9 MB/s decoded and failed the high target. Persistent mode made
  97.9k rows/s storage-visible at 46.0 MB/s decoded and passed the same target.
  Queue oldest age fell from 2.0s to 1.0s.
- Interpretation: the one-request-per-TCP benchmark path was materially
  depressing accepted, transformed, enqueued, flushed, and storage-visible
  progress. Keep the reversible keep-alive control and use persistent
  connections for the next fast-fail gates. This evidence argues against a
  writer-lane rewrite as the next experiment; the next production-facing
  question is whether bounded keep-alive semantics should become a real server
  capability, or whether benchmark targets should assume exporters reuse
  connections.

2026-05-18 SmithDB-lite planning note:

- `docs/planning/smithdb-lite-lsm-experiment.md` records the single-binary
  LSM-shaped experiment path and durable-spool state machine.
- The first proof gate is an admin-only DuckLake metadata probe for registered
  data-file planning facts. This tests whether DuckLake can replace a custom
  segment manifest for file membership before any progressive query rewrite.
- The first proof gate was a benchmark-only Loki `query_range` candidate scout:
  `GET /api/admin/query/loki-candidates` lists newest intersecting DuckLake log
  files. The sidecar scout flag has since been retired because backward Loki
  `query_range` now uses the candidate-window path directly.

2026-05-18 local Loki candidate scout comparison:

- Environment: local macOS directional run, release server, logs-only mixed
  workload, `target=500 GB/day`, 12 ingest workers, persistent connections,
  medium query pressure, `items-per-batch=256`, `log-body-bytes=512`,
  advancing timestamps, 10s warmup / 60s measured. Server CPU/RSS sampling was
  unavailable, so treat this as a scout, not a durable ceiling.
- Scout off passed at 5.787 MB/s decoded with `24/24` queries successful,
  query p50/p95/p99 `82.3/172.2/172.5 ms`, storage-visible logs
  `7,545.6 rows/s`, and final DuckLake logs `598,272 rows` in `8` files.
- Scout on passed at 5.787 MB/s decoded with `24/24` queries successful, query
  p50/p95/p99 `93.8/169.9/205.1 ms`, storage-visible logs `7,533.0 rows/s`,
  and final DuckLake logs `598,272 rows` in `8` files.
- The scout sidecar executed for `10` Loki query-range requests and spent
  `0.066s` total (`6.63 ms/query`) in DuckLake candidate planning. Stored
  operator metrics sampled the candidate set at `5` files, `377,501` rows, and
  `5,737,635` bytes.
- Interpretation: the sidecar planner overhead was small in this local scout
  and did not materially change accepted throughput or storage-visible
  progress. Query tail latency moved within a small-sample band, not enough to
  claim improvement or regression. The next gate should run the same scout in a
  longer mixed workload and then decide whether to execute Loki progressively
  over those candidates.

Report paths:

- `target/canardstack-bench/loki-candidate-scout/off/20260518T213502Z/report.json`
- `target/canardstack-bench/loki-candidate-scout/on/20260518T213645Z/report.json`

2026-05-18 local Loki candidate scout 10-minute comparison:

- Environment: local macOS directional run, release server, logs-only mixed
  workload, `target=500 GB/day`, 12 ingest workers, persistent connections,
  medium query pressure, `items-per-batch=256`, `log-body-bytes=512`,
  advancing timestamps, 15s warmup / 10m measured. Server CPU/RSS sampling was
  unavailable.
- Scout off passed at 5.787 MB/s decoded with `240/240` queries successful,
  query p50/p95/p99 `94.3/332.8/413.2 ms`, storage-visible logs
  `8,543 rows/s`, and final DuckLake logs `5,255,936 rows` in `70` files.
- Scout on passed at 5.787 MB/s decoded with `240/240` queries successful,
  query p50/p95/p99 `94.4/360.4/410.5 ms`, storage-visible logs
  `8,549 rows/s`, and final DuckLake logs `5,255,936 rows` in `71` files.
- The enriched report recorded `80` measured-window successful scout requests
  and `82` total planner timings including warmup/end effects. Candidate
  planning took `1.529s` total, `18.6 ms/query` average, and `0.25%` of
  measured wall time.
- End-window candidate set was `69` files, `5,131,430` rows, and `78,165,755`
  bytes versus `70` total log files and `5,206,890` total report-window log
  rows. Candidate fractions were `98.6%` of files and `98.6%` of rows.
- Interpretation: the sidecar planner remained cheap enough not to affect
  throughput or storage-visible progress, but the default one-hour Loki range
  covers almost all files in a 10-minute advancing-timestamp run. Time-only
  candidate filtering does not prove a query-work reduction for this shape.
  The next useful proof is a shadow progressive executor that walks newest
  candidate files and records how many files/rows are needed before satisfying
  the Loki limit.

Report paths:

- `target/canardstack-bench/loki-candidate-scout-long/off/20260518T215906Z/report.json`
- `target/canardstack-bench/loki-candidate-scout-long/on/20260518T220959Z/report.json`

Report paths:

- `target/canardstack-bench/http-keepalive/logs-validation-close/20260518T182249Z/report.json`
- `target/canardstack-bench/http-keepalive/logs-validation-persistent/20260518T182411Z/report.json`
- `target/canardstack-bench/http-keepalive/logs-transform-close/20260518T182525Z/report.json`
- `target/canardstack-bench/http-keepalive/logs-transform-persistent/20260518T182636Z/report.json`
- `target/canardstack-bench/http-keepalive/logs-full-close/20260518T182754Z/report.json`
- `target/canardstack-bench/http-keepalive/logs-full-persistent/20260518T182906Z/report.json`
- `target/canardstack-bench/http-keepalive/spans-full-close/20260518T183050Z/report.json`
- `target/canardstack-bench/http-keepalive/spans-full-persistent/20260518T183202Z/report.json`

2026-05-18 persistent-connection fast-fail gate:

- Environment: same OrbStack Linux VMs, VM-local benchmark driver against
  `127.0.0.1`, `--features otlp2records-observer`, 32 ingest workers,
  advancing timestamps, 3m measured / 15s warmup, `items-per-batch=256`,
  `log-body-bytes=512`, `trace-attribute-bytes=256`.
- Ingest-only target runs used the original gate targets: logs `2000 GB/day`,
  spans `1500 GB/day`, query pressure off. Close mode and persistent mode both
  passed, so at these paced targets this is a latency/CPU/backlog result rather
  than a max-throughput result.
- Logs ingest-only close mode: accepted 23.11 MB/s decoded, transformed and
  enqueued 34.1k rows/s, storage-visible 34.3k rows/s, peak server CPU 29.6%,
  ingest p50/p99 55.0/104.2 ms, queue oldest 3.0s. Persistent mode: accepted
  23.12 MB/s decoded, transformed and enqueued 34.1k rows/s, storage-visible
  34.3k rows/s, peak server CPU 19.1%, ingest p50/p99 0.9/42.7 ms, queue
  oldest 3.2s.
- Spans ingest-only close mode: accepted 17.32 MB/s decoded, transformed and
  enqueued 37.0k rows/s, storage-visible 36.8k rows/s, peak server CPU 30.8%,
  ingest p50/p99 55.0/104.2 ms, queue oldest 2.5s. Persistent mode: accepted
  17.34 MB/s decoded, transformed and enqueued 37.1k rows/s, storage-visible
  36.9k rows/s, peak server CPU 19.2%, ingest p50/p99 0.9/42.5 ms, queue
  oldest 2.6s.
- Low-query validation used persistent mode, `--profile mixed-query`, and
  `--query-pressure low`. Logs passed at 23.11 MB/s accepted decoded,
  storage-visible 34.3k rows/s, query p95 292 ms. Spans passed at
  17.34 MB/s accepted decoded, storage-visible 36.9k rows/s, query p95 188 ms.
  Both runs returned only `200` query responses and `202` ingest responses.
- Interpretation: persistent connections are worth keeping as a benchmark
  control and likely worth turning into bounded production behavior, because
  they cut request latency and server CPU without reducing transformed,
  enqueued, flushed, or storage-visible throughput. This still does not justify
  a writer-lane rewrite. The next architecture experiment should first make
  keep-alive production-safe under bounded connection/thread semantics, then
  re-run a max-throughput gate to see whether the next visible limiter is
  transform or storage.

Report paths:

- `target/canardstack-bench/http-keepalive-gate/logs-full-close/20260518T184605Z/report.json`
- `target/canardstack-bench/http-keepalive-gate/logs-full-persistent/20260518T184950Z/report.json`
- `target/canardstack-bench/http-keepalive-gate/spans-full-close/20260518T184606Z/report.json`
- `target/canardstack-bench/http-keepalive-gate/spans-full-persistent/20260518T184950Z/report.json`
- `target/canardstack-bench/http-keepalive-gate/logs-lowquery-persistent/20260518T185342Z/report.json`
- `target/canardstack-bench/http-keepalive-gate/spans-lowquery-persistent/20260518T185343Z/report.json`

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
| 2026-05-17 | OrbStack Linux observer-buffered logs low-query with fresh DuckLake catalog | OrbStack VM `canardstack-bench-logs`, Ubuntu Linux `aarch64`, 30m, low query pressure, `2000 GB/day`, 256 records/request, 512-byte log bodies, advancing timestamps, concurrency 32, `otlp2records-observer`, request-local observer aggregation; reset removed `data/`, `storage/`, `canardstack.duckdb`, and `canardstack.ducklake` | failed on sustained admission: `25,689 logs/sec`, `16.59 MiB/s`, `133.5 req/sec`, `180/180` queries succeeded, no `503`/transport errors; first observed `429` around minute 7, final `59,727` HTTP `429`; query p50/p95/p99 `274/1521/1635ms`; server peak CPU/RSS `66.7% / 753 MiB`; max queue `219,617` rows / `536.86 MiB` / `23.22s`; top phases `otlp_transform` `153.11s`, Loki query execute `56.33s`, protobuf decode `47.20s`, resource/scope/log build `32.93/32.31/23.03s`, queue admission `19.49s`, flush lock hold `17.17s`, Parquet write/encode `22.27/14.04s` | `target/canardstack-bench/orbstack-linux-lowquery-30m-fresh-ducklake-20260517/logs-target2000-30m/20260517T232830Z/report.json` |
| 2026-05-17 | OrbStack Linux observer-buffered spans low-query with fresh DuckLake catalog | OrbStack VM `canardstack-bench-spans`, Ubuntu Linux `aarch64`, 30m, low query pressure, `1500 GB/day`, 256 spans/request, `trace_attribute_bytes=256`, advancing timestamps, concurrency 32, `otlp2records-observer`, request-local observer aggregation; reset removed `data/`, `storage/`, `canardstack.duckdb`, and `canardstack.ducklake` | failed on sustained admission: `29,898 spans/sec`, `13.34 MiB/s`, `144.9 req/sec`, `180/180` queries succeeded, no `503`/transport errors; first observed `429` around minute 9, final `50,611` HTTP `429`; query p50/p95/p99 `295/1203/1273ms`; server peak CPU/RSS `63.3% / 746 MiB`; max queue `251,330` rows / `536.86 MiB` / `21.17s`; top phases `otlp_transform` `254.26s`, resource/scope/span build `87.40/86.75/62.81s`, protobuf decode `81.96s`, Tempo query execute `43.83s`, span attributes JSON `37.03s`, flush lock hold `21.83s`, Parquet write/encode `26.38/16.56s` | `target/canardstack-bench/orbstack-linux-lowquery-30m-fresh-ducklake-20260517/spans-target1500-30m/20260517T232836Z/report.json` |
| 2026-05-17 | OrbStack Linux observer-buffered logs low-query with stale DuckLake catalog | same low-query logs shape, but reset removed `storage/` and `canardstack.duckdb` without removing local `canardstack.ducklake` | failed on query `503`, not ingest: `34,182 logs/sec`, `22.07 MiB/s`, `240,351` HTTP `202`, no `429`, max queue `6,676` rows / `16.32 MiB` / `0.40s`; `120/180` queries succeeded, all `60` failures were Loki `/loki/api/v1/query_range` with `query_storage_unavailable` reading an old DuckLake-registered logs Parquet path that had been deleted by the harness reset; retained as harness-cleanup evidence, not a product query-lifecycle bug | `target/canardstack-bench/orbstack-linux-lowquery-30m-observer-buffered-20260517/logs-target2000-30m/20260517T222915Z/report.json` |
| 2026-05-17 | OrbStack Linux observer-buffered spans low-query with stale DuckLake catalog | same low-query spans shape, but reset removed `storage/` and `canardstack.duckdb` without removing local `canardstack.ducklake` | failed on query `503`, not ingest: `37,096 spans/sec`, `16.56 MiB/s`, `260,843` HTTP `202`, no `429`, max queue `9,824` rows / `20.98 MiB` / `0.30s`; `120/180` queries succeeded, all `60` failures were Tempo `/api/search` with `query_storage_unavailable` reading an old DuckLake-registered spans Parquet path that had been deleted by the harness reset; retained as harness-cleanup evidence, not a product query-lifecycle bug | `target/canardstack-bench/orbstack-linux-lowquery-30m-observer-buffered-20260517/spans-target1500-30m/20260517T222934Z/report.json` |
| 2026-05-17 | OrbStack Linux observer-buffered logs ingest-only | OrbStack VM `canardstack-bench-logs`, Ubuntu Linux `aarch64`, 30m, no queries, `2000 GB/day`, 256 records/request, 512-byte log bodies, advancing timestamps, concurrency 32, `otlp2records-observer`, request-local observer aggregation | pass: `34,183 logs/sec`, `22.07 MiB/s`, `133.5 req/sec`, `240,351` HTTP `202`, no `429/503`, ingest p50/p95/p99 `55.9/101.2/106.3ms`, server peak CPU/RSS `30.8% / 218 MiB`, max queue `5,757` rows / `14.07 MiB` / `0.40s`; top phases `otlp_transform` `193.43s`, protobuf decode `54.32s`, resource/scope/log build `47.67/46.63/33.03s`; storage write/encode `34.04/21.13s` | `target/canardstack-bench/orbstack-linux-ingestonly-30m-observer-buffered-20260517/logs-target2000-30m/20260517T214949Z/report.json` |
| 2026-05-17 | OrbStack Linux observer-buffered spans ingest-only | OrbStack VM `canardstack-bench-spans`, Ubuntu Linux `aarch64`, 30m, no queries, `1500 GB/day`, 256 spans/request, `trace_attribute_bytes=256`, advancing timestamps, concurrency 32, `otlp2records-observer`, request-local observer aggregation | pass: `37,096 spans/sec`, `16.56 MiB/s`, `144.9 req/sec`, `260,843` HTTP `202`, no `429/503`, ingest p50/p95/p99 `56.2/101.2/106.3ms`, server peak CPU/RSS `33.2% / 224 MiB`, max queue `6,943` rows / `14.83 MiB` / `0.32s`; top phases `otlp_transform` `328.08s`, resource/scope/span build `129.30/128.19/94.93s`, protobuf decode `92.33s`, span attributes JSON `54.56s`; storage write/encode `34.42/21.86s` | `target/canardstack-bench/orbstack-linux-ingestonly-30m-observer-buffered-20260517/spans-target1500-30m/20260517T214958Z/report.json` |
| 2026-05-17 | OrbStack Linux unbuffered observer logs ingest-only | same OrbStack logs VM shape as above before request-local observer aggregation | failed: `429` first appeared at minute 7; final `25,361 logs/sec`, `16.38 MiB/s`, `62,032` HTTP `429`, max queue `536.87 MiB` / `23.45s`; `otlp_transform` `2050.65s`, `otlp2records_log_record_build` `1525.13s`; this run is retained as evidence of observer-sink overhead, not a product ingest ceiling | `target/canardstack-bench/orbstack-linux-ingestonly-30m-20260517/logs-target2000-30m/20260517T211552Z/report.json` |
| 2026-05-17 | OrbStack Linux unbuffered observer spans ingest-only | same OrbStack spans VM shape as above before request-local observer aggregation | failed: `429` first appeared at minute 8; final `29,497 spans/sec`, `13.16 MiB/s`, `53,435` HTTP `429`, max queue `536.85 MiB` / `21.53s`; `otlp_transform` `3799.19s`, `otlp2records_span_build` `3063.84s`; the broad span timer included tens of millions of global metrics updates and is not reliable evidence of pure transform cost | `target/canardstack-bench/orbstack-linux-ingestonly-30m-20260517/spans-target1500-30m/20260517T211557Z/report.json` |
| 2026-05-17 | Local macOS observer drain instrumentation spans | local macOS directional, spans-only ingest, no queries, 10m, `1500 GB/day`, 256 spans/request, `trace_attribute_bytes=256`, advancing timestamps, concurrency 32, default flush caps | failed late on `429`: actual `36,235 spans/sec`, `16.17 MiB/s`, `2,011` HTTP `429`; high-water queue reached `251,303` rows / `536,799,896` bytes / `10.05s` oldest age; `otlp_transform` `6222.47s`, `otlp2records_span_build` `5660.20s`; flush lock hold only `8.64s`, storage prepare `2.82s`, Parquet encode/write `5.41s`/`9.42s`; drain path attempted `22,815,994` rows across `11,623` flush attempts | `target/canardstack-bench/local-observer-span10m-flushmetrics2-20260517/spans-target1500-10m/20260517T195144Z/report.json` |
| 2026-05-17 | Local macOS high-flush drain experiment spans | local macOS directional, spans-only ingest, no queries, 10m, `1500 GB/day`, 256 spans/request, `trace_attribute_bytes=256`, advancing timestamps, concurrency 32, `CANARDSTACK_MAX_ROWS_PER_FLUSH=20000`, `CANARDSTACK_MAX_BYTES_PER_FLUSH=16777216` | failed on `429` and did not improve baseline: actual `35,677 spans/sec`, `15.92 MiB/s`, `3,317` HTTP `429`; storage prepare calls fell to `2,839`, but `otlp_transform` remained `5936.07s` and `otlp2records_span_build` `5241.92s`; larger flush chunks alone are not the fix | `target/canardstack-bench/local-observer-span10m-flush16m-20260517/spans-target1500-10m/20260517T190242Z/report.json` |
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

## Latest OrbStack Observer Findings

The new `scripts/orbstack-bench-vms.sh` path creates one VM per signal. These
runs used `canardstack-bench-logs` and `canardstack-bench-spans`; both report
Linux `aarch64`, unrestricted cgroup CPU/memory (`max`), and
`available_parallelism=14`. Treat this as directional Linux evidence rather
than the earlier `galaxy-disk` `x86_64` VM envelope.

Concrete insight from this pass: the observer-enabled benchmark itself had a
hot path. The unbuffered Canardstack `TransformObserver` updated the global
`Metrics` mutex for every per-span/per-log phase event. On the failed spans
run, that meant about `68.2M` `SpanBuild` observations and `136.3M`
`ArrowAppend` observations. The broad `SpanBuild` timer included nested
observer-update overhead, so it overstated pure `otlp2records` transform cost
and created artificial queue/admission pressure.

The fix was to aggregate observer phase counts/sums and counters in the
request-local observer, then flush one metrics update per phase/counter after
the transform call. After this change, the exact same 30-minute ingest-only
controls passed:

- Logs `2000 GB/day`: `34,183 logs/sec`, `22.07 MiB/s`, `240,351` HTTP `202`,
  no `429/503`, max queue `14.07 MiB` and `0.40s`, server peak CPU/RSS
  `30.8% / 218 MiB`.
- Spans `1500 GB/day`: `37,096 spans/sec`, `16.56 MiB/s`, `260,843` HTTP
  `202`, no `429/503`, max queue `14.83 MiB` and `0.32s`, server peak CPU/RSS
  `33.2% / 224 MiB`.

The drain path is not the sustained limiter for these passing OrbStack
ingest-only controls. For spans, storage write/encode/insert plus flush lock
hold are material but small compared with transform, and queue age stays flat.
The remaining likely limiter for the next higher or query-interference gates is
still request-thread transform work, especially protobuf decode plus
`otlp2records` trace `ResourceSpansBuild` / `ScopeSpansBuild` / `SpanBuild` /
`SpanAttributesJson`. That is now a real post-fix target rather than an
observer-lock artifact.

The first low-query rerun at the same target rates failed on
`503 query_storage_unavailable`, but that was a benchmark-harness freshness bug:
the reset command removed `storage/` and `canardstack.duckdb` without removing
the local DuckLake catalog at `canardstack.ducklake`. The stale catalog still
referenced Parquet files from a prior run. `scripts/orbstack-bench-vms.sh` now
has `reset-data <profile>` so a fresh run clears `data/`, `storage/`,
`canardstack.duckdb`, `canardstack.ducklake`, and `tmp/` while preserving the
Cargo target cache.

- Logs run: Loki `/loki/api/v1/query_range` failed `60/60` attempts with a
  missing old `storage/main/logs/year=2026/month=5/day=17/...parquet` file.
  After `reset-data`, `180/180` low-query requests succeeded with no `503`.
- Spans run: Tempo `/api/search` failed `60/60` attempts with a missing
  old `storage/main/spans/year=2026/month=5/day=17/...parquet` file. After
  `reset-data`, `180/180` low-query requests succeeded with no `503`.

With a genuinely fresh DuckLake catalog, the same 30-minute low-query targets
returned to the real sustained limiter: queue/admission pressure under query
interference. They both failed with `429`, falling below 90% of target accepted
decoded throughput:

- Logs `2000 GB/day`: `25,689 logs/sec`, `16.59 MiB/s`, `180/180` queries
  succeeded, first observed `429` around minute 7, final `59,727` HTTP `429`,
  query p50/p95/p99 `274/1521/1635ms`, server peak CPU/RSS `66.7% / 753 MiB`,
  max queue `219,617` rows / `536.86 MiB` / `23.22s`.
- Spans `1500 GB/day`: `29,898 spans/sec`, `13.34 MiB/s`, `180/180` queries
  succeeded, first observed `429` around minute 9, final `50,611` HTTP `429`,
  query p50/p95/p99 `295/1203/1273ms`, server peak CPU/RSS `63.3% / 746 MiB`,
  max queue `251,330` rows / `536.86 MiB` / `21.17s`.

The likely sustained limiter is still synchronous request-thread transform plus
bounded queue/admission under accumulated low query load, not DuckLake
registration correctness. Query execution is now a meaningful interference
term, especially Loki query-range in the logs run and Tempo search in the spans
run, but storage encode/write/register remains too small to explain the queue
cap by itself.

Caveats:

- The ingest-only (`--no-queries`) runs prove only ingest/admission at these
  targets. The fresh-catalog low-query rerun proves the stale catalog cleanup
  fix, but it does not prove these target rates for 30 minutes under low query
  pressure.
- OrbStack VMs are Linux `aarch64` with many reported scheduling threads; they
  are not equivalent to the earlier `galaxy-disk` `x86_64`, 2-vCPU VM.
- The fresh-catalog low-query failures are not a reason to lower product
  targets by themselves; they identify the next optimization proof gate. The
  latest proven `galaxy-disk` 30-minute low-query points remain logs
  `1500 GB/day` and spans `1200 GB/day`.

Exact setup and benchmark commands:

```sh
scripts/orbstack-bench-vms.sh up
scripts/orbstack-bench-vms.sh run logs -- \
  cargo build --release --features otlp2records-observer \
    --bin canardstack --bench throughput_iteration
scripts/orbstack-bench-vms.sh run spans -- \
  env CARGO_BUILD_JOBS=1 cargo build --release \
    --features otlp2records-observer --bin canardstack \
    --bench throughput_iteration

scripts/orbstack-bench-vms.sh reset-data logs
scripts/orbstack-bench-vms.sh reset-data spans

scripts/orbstack-bench-vms.sh run logs -- \
  bash -lc 'exec "$CARGO_TARGET_DIR/release/canardstack" serve'
scripts/orbstack-bench-vms.sh run spans -- \
  bash -lc 'exec "$CARGO_TARGET_DIR/release/canardstack" serve'

scripts/orbstack-bench-vms.sh run logs -- \
  cargo bench --bench throughput_iteration \
    --features otlp2records-observer -- \
    --base-url http://127.0.0.1:4318 \
    --warmup 30s \
    --duration 30m \
    --target-gb-day 2000 \
    --profile ingest-only \
    --signals logs \
    --query-pressure off \
    --ingest-concurrency 32 \
    --items-per-batch 256 \
    --log-body-bytes 512 \
    --trace-attribute-bytes 256 \
    --timestamp-mode advancing \
    --progress-interval 60s \
    --max-runtime 35m \
    --no-queries \
    --server-pid 253509 \
    --resource-sample-interval 5s \
    --report-dir target/canardstack-bench/orbstack-linux-ingestonly-30m-observer-buffered-20260517/logs-target2000-30m

scripts/orbstack-bench-vms.sh run spans -- \
  cargo bench --bench throughput_iteration \
    --features otlp2records-observer -- \
    --base-url http://127.0.0.1:4318 \
    --warmup 30s \
    --duration 30m \
    --target-gb-day 1500 \
    --profile ingest-only \
    --signals spans \
    --query-pressure off \
    --ingest-concurrency 32 \
    --items-per-batch 256 \
    --log-body-bytes 512 \
    --trace-attribute-bytes 256 \
    --timestamp-mode advancing \
    --progress-interval 60s \
    --max-runtime 35m \
    --no-queries \
    --server-pid 275170 \
    --resource-sample-interval 5s \
    --report-dir target/canardstack-bench/orbstack-linux-ingestonly-30m-observer-buffered-20260517/spans-target1500-30m

scripts/orbstack-bench-vms.sh run logs -- \
  cargo bench --bench throughput_iteration \
    --features otlp2records-observer -- \
    --base-url http://127.0.0.1:4318 \
    --warmup 30s \
    --duration 30m \
    --target-gb-day 2000 \
    --profile mixed-query \
    --signals logs \
    --query-pressure low \
    --ingest-concurrency 32 \
    --items-per-batch 256 \
    --log-body-bytes 512 \
    --trace-attribute-bytes 256 \
    --timestamp-mode advancing \
    --progress-interval 60s \
    --max-runtime 35m \
    --server-pid 498789 \
    --resource-sample-interval 5s \
    --report-dir target/canardstack-bench/orbstack-linux-lowquery-30m-observer-buffered-20260517/logs-target2000-30m

scripts/orbstack-bench-vms.sh run spans -- \
  cargo bench --bench throughput_iteration \
    --features otlp2records-observer -- \
    --base-url http://127.0.0.1:4318 \
    --warmup 30s \
    --duration 30m \
    --target-gb-day 1500 \
    --profile mixed-query \
    --signals spans \
    --query-pressure low \
    --ingest-concurrency 32 \
    --items-per-batch 256 \
    --log-body-bytes 512 \
    --trace-attribute-bytes 256 \
    --timestamp-mode advancing \
    --progress-interval 60s \
    --max-runtime 35m \
    --server-pid 541284 \
    --resource-sample-interval 5s \
    --report-dir target/canardstack-bench/orbstack-linux-lowquery-30m-observer-buffered-20260517/spans-target1500-30m

scripts/orbstack-bench-vms.sh reset-data logs
scripts/orbstack-bench-vms.sh reset-data spans

scripts/orbstack-bench-vms.sh run logs -- \
  cargo bench --bench throughput_iteration \
    --features otlp2records-observer -- \
    --base-url http://127.0.0.1:4318 \
    --warmup 30s \
    --duration 30m \
    --target-gb-day 2000 \
    --profile mixed-query \
    --signals logs \
    --query-pressure low \
    --ingest-concurrency 32 \
    --items-per-batch 256 \
    --log-body-bytes 512 \
    --trace-attribute-bytes 256 \
    --timestamp-mode advancing \
    --progress-interval 60s \
    --max-runtime 35m \
    --server-pid 744760 \
    --resource-sample-interval 5s \
    --report-dir target/canardstack-bench/orbstack-linux-lowquery-30m-fresh-ducklake-20260517/logs-target2000-30m

scripts/orbstack-bench-vms.sh run spans -- \
  cargo bench --bench throughput_iteration \
    --features otlp2records-observer -- \
    --base-url http://127.0.0.1:4318 \
    --warmup 30s \
    --duration 30m \
    --target-gb-day 1500 \
    --profile mixed-query \
    --signals spans \
    --query-pressure low \
    --ingest-concurrency 32 \
    --items-per-batch 256 \
    --log-body-bytes 512 \
    --trace-attribute-bytes 256 \
    --timestamp-mode advancing \
    --progress-interval 60s \
    --max-runtime 35m \
    --server-pid 808187 \
    --resource-sample-interval 5s \
    --report-dir target/canardstack-bench/orbstack-linux-lowquery-30m-fresh-ducklake-20260517/spans-target1500-30m
```

## Latest Local Drain Findings

Local macOS evidence remains directional, and the unbuffered observer runs
below are now known to include observer-sink overhead. They remain useful for
the drain-path counters and flush-size comparison, but not as clean
`otlp2records` transform ceilings:

- Raising flush caps from the defaults (`5000` rows / `4 MiB`) to `20000` rows
  / `16 MiB` reduced `storage_prepare` call count but did not reduce `429`s.
  This argues against a simple default flush-size increase.
- The default-flush, high-water-instrumented run still filled the spans queue to
  the `512 MiB` cap and started `429`s around 6.5 minutes, even though
  `flush_lock_hold`, `storage_prepare`, Parquet encode/write, DuckLake
  register, and DuckLake commit were all small compared with transform time.
- After request-local observer aggregation, the remaining meaningful transform
  target is still `otlp2records` trace build work, especially the nested
  `resource_spans_build` / `scope_spans_build` / `span_build` path and span
  attribute JSON. Queue/admission is the symptom boundary at failed higher
  targets, but the measured drain path is not consuming enough time to be the
  primary sustained limiter.
- The report now persists `flush_counters`, `flush_gauges`, and queue high-water
  gauges so later runs can distinguish instantaneous final queue state from
  peak pressure.

Exact local commands:

```sh
env CANARDSTACK_DATA_DIR=/private/tmp/canardstack-local-span10m-flush16m-20260517 \
  CANARDSTACK_DUCKDB_PATH=/private/tmp/canardstack-local-span10m-flush16m-20260517/canardstack.duckdb \
  CANARDSTACK_STORAGE_DIR=/private/tmp/canardstack-local-span10m-flush16m-20260517/storage \
  CANARDSTACK_BIND=127.0.0.1:4318 \
  CANARDSTACK_MAX_ROWS_PER_FLUSH=20000 \
  CANARDSTACK_MAX_BYTES_PER_FLUSH=16777216 \
  CANARDSTACK_BENCHMARK_RESOURCE_NOTE=local-macos-directional \
  target/release/canardstack serve

env CANARDSTACK_BENCHMARK_RESOURCE_NOTE=local-macos-directional \
  cargo bench --bench throughput_iteration --features otlp2records-observer -- \
  --base-url http://127.0.0.1:4318 \
  --warmup 30s --duration 10m --target-gb-day 1500 \
  --profile ingest-only --signals spans --query-pressure off \
  --ingest-concurrency 32 --items-per-batch 256 \
  --trace-attribute-bytes 256 --timestamp-mode advancing \
  --progress-interval 30s --max-runtime 12m --no-queries \
  --server-pid 49801 --resource-sample-interval 5s \
  --report-dir target/canardstack-bench/local-observer-span10m-flush16m-20260517/spans-target1500-10m

env CANARDSTACK_DATA_DIR=/private/tmp/canardstack-local-span10m-flushmetrics2-20260517 \
  CANARDSTACK_DUCKDB_PATH=/private/tmp/canardstack-local-span10m-flushmetrics2-20260517/canardstack.duckdb \
  CANARDSTACK_STORAGE_DIR=/private/tmp/canardstack-local-span10m-flushmetrics2-20260517/storage \
  CANARDSTACK_BIND=127.0.0.1:4318 \
  CANARDSTACK_BENCHMARK_RESOURCE_NOTE=local-macos-directional \
  target/release/canardstack serve

env CANARDSTACK_BENCHMARK_RESOURCE_NOTE=local-macos-directional \
  cargo bench --bench throughput_iteration --features otlp2records-observer -- \
  --base-url http://127.0.0.1:4318 \
  --warmup 30s --duration 10m --target-gb-day 1500 \
  --profile ingest-only --signals spans --query-pressure off \
  --ingest-concurrency 32 --items-per-batch 256 \
  --trace-attribute-bytes 256 --timestamp-mode advancing \
  --progress-interval 30s --max-runtime 12m --no-queries \
  --server-pid 66910 --resource-sample-interval 5s \
  --report-dir target/canardstack-bench/local-observer-span10m-flushmetrics2-20260517/spans-target1500-10m
```

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
- Follow-up implementation: Canardstack now uses crates.io `otlp2records`
  `0.7.0`, and the benchmark feature uses observer-based instrumentation from
  the production transform APIs. New benchmark reports can expose finer phases
  such as `otlp2records_resource_context_build`,
  `otlp2records_resource_attributes_json`,
  `otlp2records_span_attributes_json`, and `otlp2records_arrow_finalize`, plus
  duplicate-context and row-copy counters under
  `canardstack_otlp2records_transform_events_total`.
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

# Observer runs require the benchmark-only instrumentation feature.
cargo build --release --features otlp2records-observer \
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

1. Treat `scripts/orbstack-bench-vms.sh reset-data` as mandatory for OrbStack
   runs that need a fresh local DuckLake catalog. The earlier missing-file
   query failure reproduced as stale `canardstack.ducklake` metadata after a
   partial harness reset; the fresh-catalog rerun cleared the `503`s.
2. Use the same low-query shape after the next transform/query-interference
   optimization: logs `2000 GB/day` and spans `1500 GB/day`, low query
   pressure, advancing timestamps, concurrency 32, `reset-data` first. Do not
   lower the target based on the stale-catalog run; the fresh-catalog run shows
   the actual failure is sustained `429` after queue growth.
3. The next code investigation should prioritize the measured request-thread
   hot path and query interference:
   protobuf decode plus `otlp2records` resource/scope/item build for both logs
   and spans; Loki query-range and Tempo search should be treated as
   interference terms because they add tens of seconds of query execution under
   low pressure.
4. Re-run the same observer-buffered ingest-only controls on the original
   `galaxy-disk` `x86_64`, 2-vCPU VM if that environment is still the canonical
   low-query proof host. The OrbStack pass is useful but not hardware-equivalent
   to the earlier failing VM.
5. If either observer-buffered ingest-only control fails on a comparable VM,
   target the real
   post-fix transform phases: spans `ProtobufDecode`,
   `ResourceSpansBuild`/`ScopeSpansBuild`/`SpanBuild`, and
   `SpanAttributesJson`; logs `ProtobufDecode`,
   `ResourceLogsBuild`/`ScopeLogsBuild`/`LogRecordBuild`, and body/attribute
   append. Avoid changing queue or flush defaults unless queue age grows while
   transform headroom remains.
6. Do not treat pure `otlp2records` storage as a throughput win yet. It remains
   the cleaner schema boundary, but the prototype failed both 30-minute
   ingest-only controls before the observer-sink issue was understood. If kept
   for schema correctness, gate it separately at the best passing low-query
   points before claiming a perf win.
7. Run one medium-query 5-minute scout only after explicitly choosing
   query-saturation exploration over ingest-ceiling bracketing.
8. Gate any `otlp2records` partitioned API adoption on measurement showing that
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
