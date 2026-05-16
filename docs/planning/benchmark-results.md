# Benchmark Results Log

This is a running factual log of benchmark evidence. Keep entries narrow: record
the exact claim tested, environment, command shape, report path, result, and
known caveats. Do not generalize one run into a broader product claim.

## 2026-05-16 - 100 GB/day Immutable DuckLake, Ingest Concurrency 3

### Claim Tested

Check whether the immutable DuckLake segment path can reach the new
100 GB/day equivalent target on a single local node when the benchmark generator
uses concurrent OTLP ingest requests.

This uses the prototype path:

- `CANARDSTACK_EXPERIMENTAL_IMMUTABLE_SEGMENTS=true`
- `CANARDSTACK_IMMUTABLE_SEGMENT_TARGET_BYTES=67108864`
- `CANARDSTACK_IMMUTABLE_SEGMENT_MAX_AGE_SECS=10`
- benchmark `--ingest-concurrency 3`

### Result

Pass for the current 30-minute local target after the next iteration.

100 GB/day ingest is reachable with concurrency 3. 100 GB/day with low
mixed-query pressure passed. A first medium mixed-query run failed with DuckDB
database invalidation after a fatal `Attempted to dereference shared_ptr that is
NULL!` error. The next iteration moved immutable segment sealing out of the
request path and disables managed DuckLake compaction for immutable segments.
After that change, two consecutive 90s medium mixed-query runs passed on the
same warm local catalog, then the same shape passed a 30-minute medium
mixed-query run.

Passing reports:

```text
target/canardstack-bench/immutable-buffer-64m-10s-100gpd-ingest-conc3/20260516T043230Z/report.json
target/canardstack-bench/immutable-buffer-64m-10s-100gpd-mixed-low-conc3/20260516T043658Z/report.json
```

Failing medium-query report:

```text
target/canardstack-bench/immutable-buffer-64m-10s-100gpd-mixed-conc3/20260516T043426Z/report.json
```

Diagnostic medium-query report with response bodies:

```text
target/canardstack-bench/medium-debug-90s-warm-state/20260516T044721Z/report.json
```

Passing post-iteration medium-query reports:

```text
target/canardstack-bench/medium-no-gate-90s-cold/20260516T050629Z/report.json
target/canardstack-bench/medium-no-gate-90s-warm/20260516T050823Z/report.json
target/canardstack-bench/medium-no-gate-30m-100gpd/20260516T141458Z/report.json
```

### Environment

- Host: local macOS Apple Silicon development machine.
- Server process: local `cargo run -- serve`, not Docker.
- Benchmark process: local `cargo bench --bench v0_iteration`.
- Network path: local loopback, `127.0.0.1`.
- Storage mode: local DuckLake with DuckDB catalog.
- Caveat: this is local prototype evidence for throughput direction and
  bottleneck discovery, not cloud-equivalent Linux VM proof.

### Command Shape

Server:

```sh
env \
  CANARDSTACK_BIND=127.0.0.1:4330 \
  CANARDSTACK_DATA_DIR=/private/tmp/canardstack-bench-immutable-buffer-64m-10s-100gpd-mixed-low-conc3 \
  CANARDSTACK_DUCKDB_PATH=/private/tmp/canardstack-bench-immutable-buffer-64m-10s-100gpd-mixed-low-conc3/canardstack.duckdb \
  CANARDSTACK_STORAGE_DIR=/private/tmp/canardstack-bench-immutable-buffer-64m-10s-100gpd-mixed-low-conc3/storage \
  CANARDSTACK_SCHEDULER_FLUSH_SECS=30 \
  CANARDSTACK_SCHEDULER_METADATA_SECS=1 \
  CANARDSTACK_EXPERIMENTAL_IMMUTABLE_SEGMENTS=true \
  CANARDSTACK_IMMUTABLE_SEGMENT_TARGET_BYTES=67108864 \
  CANARDSTACK_IMMUTABLE_SEGMENT_MAX_AGE_SECS=10 \
  cargo run -- serve
```

Passing low-query benchmark:

```sh
cargo bench --bench v0_iteration -- \
  --base-url http://127.0.0.1:4330 \
  --warmup 15s \
  --duration 90s \
  --target-gb-day 100 \
  --profile mixed-query \
  --query-pressure low \
  --ingest-concurrency 3 \
  --progress-interval 15s \
  --report-dir target/canardstack-bench/immutable-buffer-64m-10s-100gpd-mixed-low-conc3
```

### Key Numbers

Ingest-only, concurrency 3:

- Pass: `true`.
- Target throughput: `1157407 decoded B/s`.
- Actual throughput: `1159467 decoded B/s`.
- HTTP status counts: `202=2595`.
- Ingest latency: p50 `85.3ms`, p95 `107.3ms`, p99 `114.2ms`.
- Queue/freshness progress samples remained bounded; final progress sample
  showed queue oldest age `3.5s` and freshness lag `12.3s`.
- Top server phase: `otlp_transform metric_gauge`, `41.101s`, `46%` wall time.
- DuckLake Parquet files at end: logs `21`, spans `17`, metric_gauge `19`,
  metric_sum `20`, metadata_summary `4`.

Mixed-query low pressure, concurrency 3:

- Pass: `true`.
- Target throughput: `1157407 decoded B/s`.
- Actual throughput: `1159012 decoded B/s`.
- Query profile: mixed-query, low pressure, query concurrency 1.
- HTTP status counts: `202=2595`, `200=9`.
- Ingest latency: p50 `89.6ms`, p95 `107.7ms`, p99 `114.5ms`.
- Query latency: p50 `131.1ms`, p95 `366.8ms`, p99 `366.8ms`.
- Final progress sample showed queue oldest age `6.9s` and freshness lag
  `10.1s`.
- Top server phase: `otlp_transform metric_gauge`, `22.154s`, `25%` wall time.
- DuckLake Parquet files at end: logs `9`, spans `8`, metric_gauge `9`,
  metric_sum `9`, metadata_summary `4`.

Initial mixed-query medium pressure, concurrency 3:

- Pass: `false`.
- Actual throughput: `1158585 decoded B/s`, so ingest reached target.
- HTTP status counts: `202=2595`, `200=4`, `503=32`.
- Query failures: `32` of `36` query requests returned 503.
- Queue oldest age grew to `81.2s` by the final progress sample.
- Top server phase: `otlp_transform metric_gauge`, `53.217s`, `59%` wall time.

Diagnostic warm-state medium pressure, before the fix:

- Pass: `false`.
- Actual throughput: `1158942 decoded B/s`.
- HTTP status counts: `202=2595`, `200=34`, `503=2`.
- Query failure body reported DuckDB database invalidation after
  `Attempted to dereference shared_ptr that is NULL!`.
- Top server phase: `otlp_transform metric_gauge`, `47.838s`, `53%` wall time.

Post-iteration cold medium pressure:

- Pass: `true`.
- Actual throughput: `1155096 decoded B/s`.
- HTTP status counts: `202=2544`, `200=36`.
- Ingest latency: p50 `106.5ms`, p95 `114.1ms`, p99 `134.4ms`.
- Query latency: p50 `121.0ms`, p95 `395.9ms`, p99 `409.6ms`.
- Final progress sample showed queue oldest age `5.1s` and freshness lag
  `11.6s`.
- DuckLake Parquet files at end: logs `8`, spans `7`, metric_gauge `9`,
  metric_sum `9`, metadata_summary `4`.

Post-iteration warm medium pressure:

- Pass: `true`.
- Actual throughput: `1155400 decoded B/s`.
- HTTP status counts: `202=2544`, `200=36`.
- Ingest latency: p50 `106.5ms`, p95 `114.1ms`, p99 `131.7ms`.
- Query latency: p50 `155.1ms`, p95 `417.2ms`, p99 `422.1ms`.
- Final progress sample showed queue oldest age `3.6s` and freshness lag
  `16.3s`.
- DuckLake Parquet files at end: logs `16`, spans `14`, metric_gauge `17`,
  metric_sum `17`, metadata_summary `4`.

Post-iteration 30-minute medium pressure:

- Pass: `true`.
- Target throughput: `1157407 decoded B/s`.
- Actual throughput: `1148498 decoded B/s`.
- HTTP status counts: `202=50973`, `200=720`.
- Ingest latency: p50 `104.5ms`, p95 `113.0ms`, p99 `123.4ms`.
- Query latency: p50 `398.3ms`, p95 `772.3ms`, p99 `873.5ms`.
- Queue oldest-age trend was not increasing; start `8.2s`, end `1.9s`.
- Freshness lag trend was not increasing; start `15.3s`, end `18.4s`, final
  scrape max `2.8s`.
- Top server phase: `otlp_transform metric_gauge`, `361.605s`, `20%` wall
  time.
- Top query phase: `/api/v1/query_range`, `112.998s`, `6%` wall time.
- DuckLake Parquet files at end: logs `159`, spans `127`, metric_gauge `150`,
  metric_sum `150`, metadata_summary `4`.

### What This Supports

This supports the narrow statement:

```text
The immutable DuckLake segment path can accept 100 GB/day equivalent ingest on
one local process when OTLP ingest has at least three concurrent in-flight
requests. It can also pass a 30-minute medium mixed-query run at that ingest
rate on the local DuckDB-catalog DuckLake setup.
```

It also supports this bottleneck statement:

```text
At 100 GB/day with medium mixed-query pressure, request-path sealing and
managed compaction are the wrong shape for immutable telemetry. Keep request
handling to transform plus in-memory buffering, let the watchdog seal detached
buffers, and avoid DuckLake compaction while segment files are already sized.
```

### What This Does Not Support

- It does not prove 100 GB/day with a single in-flight OTLP client request.
- It does not prove 100 GB/day for a multi-hour run yet.
- It does not prove object-store DuckLake behavior.
- It does not prove real Linux VM or cloud instance behavior.

### Next Target

The next target is:

```text
100 GB/day, immutable segments 64MiB/10s, ingest concurrency 3, mixed-query
medium pressure, 2-hour duration, zero 503s, bounded queue oldest age, p95
freshness under 30s.
```

The likely next optimization area is still not segment sizing. Promote this to
a 2-hour run, then focus on metric transform cost or query latency only if the
longer run shows p95 freshness, query latency, or file-count growth.

## 2026-05-16 - 25 GB/day Mixed Query, otlp2records 0.5.0 Comparison

### Claim Tested

Compare `otlp2records` 0.5.0 against the prior 0.4.0 result using the same
25 GB/day equivalent mixed ingest workload, low compatibility-query pressure,
and realistic 192-byte metric descriptions.

### Result

Pass.

Report:

```text
target/canardstack-bench/25gpd-mixed-query-low-30m-otlp050-descfix/20260516T002304Z/report.json
```

Baseline report:

```text
target/canardstack-bench/25gpd-mixed-query-low-30m-descfix/20260515T230832Z/report.json
```

### Environment

- Host: macOS Docker Desktop on Apple Silicon.
- Server container: `canardstack:local`, Compose project
  `canardstack-otlp050-25-30m`.
- Network path: local benchmark process to Docker-published
  `127.0.0.1:4319`, not Docker container-to-container networking.
- Server cap: Compose benchmark defaults, 2 CPU / 4 GB memory.
- Storage mode: local DuckLake with DuckDB catalog.
- Caveat: this is a local regression comparison for the transform dependency,
  not cloud-equivalent Linux VM proof.

### Command Shape

```sh
env \
  CANARDSTACK_BENCHMARK_CPU_LIMIT=2.0 \
  CANARDSTACK_BENCHMARK_MEMORY_LIMIT=4g \
  CANARDSTACK_BENCHMARK_RESOURCE_NOTE='Docker Desktop VM capped to 2 CPU / 4 GB memory; otlp2records 0.5.0 comparison, 25 GB/day 30m fixed metric descriptions' \
  cargo bench --bench v0_iteration -- \
    --base-url http://127.0.0.1:4319 \
    --warmup 2m \
    --duration 30m \
    --max-runtime 35m \
    --target-gb-day 25 \
    --profile mixed-query \
    --query-pressure low \
    --query-concurrency 1 \
    --report-dir target/canardstack-bench/25gpd-mixed-query-low-30m-otlp050-descfix
```

### Key Numbers

- Pass: `true`.
- Measured duration: `1800.094885958s`.
- Target throughput: `289351.85185185185 decoded B/s`.
- Actual throughput: `289960.30713248655 decoded B/s`.
- Query profile: mixed-query, low pressure, concurrency 1, interval 10s.
- Workload metric description bytes: `192`.
- HTTP status counts: `202=12974`, `200=180`.
- Ingest latency: p50 `64.2ms`, p95 `106.1ms`, p99 `114.4ms`.
- Query latency: p50 `138.5ms`, p95 `228.6ms`, p99 `249.5ms`.
- Top server phase: `otlp_transform metric_gauge`, `25.589s` total,
  `0.001922s` average, `0.051407s/MiB`.
- Baseline top server phase with `otlp2records` 0.4.0:
  `otlp_transform metric_gauge`, `38.959s` total, `0.002927s` average,
  `0.078266s/MiB`.
- Metric-gauge transform improved by about `34%` by total time, average time,
  and seconds per MiB.
- End-to-end throughput was effectively unchanged because the benchmark is
  target-throttled at 25 GB/day.
- DuckLake inlined rows at end: `0` for logs, spans, gauge metrics, sum metrics,
  and metadata summary.
- DuckLake Parquet files at end: logs `23`, spans `16`, metric_gauge `28`,
  metric_sum `28`, metadata_summary `31`.
- Physical storage bytes: `494741159`.

### What This Supports

This supports the narrow statement:

```text
With realistic 192-byte metric descriptions, otlp2records 0.5.0 materially
reduces the metric-gauge transform hot phase in the local 25 GB/day, 30-minute
mixed-query benchmark, while preserving the same pass result and stable queue
and freshness trends.
```

### What This Does Not Support

- It does not prove additional end-to-end throughput headroom, because this run
  was target-throttled to 25 GB/day.
- It does not support medium or high query-pressure claims.
- It does not prove object-store DuckLake behavior.
- It does not prove real Linux VM or cloud instance behavior.
- It is not directly comparable to container-to-container runs on network path
  alone, because this run used the Docker-published host port.

## 2026-05-15 - 10 GB/day Mixed Query, Realistic Metric Descriptions

### Claim Tested

One Canardstack instance can sustain the same 10 GB/day equivalent mixed ingest
workload for one tenant for 2 hours with low compatibility-query interference
after reducing the benchmark's metric-description fixture from pathological
280 KB descriptions to realistic 192-byte descriptions.

### Result

Pass.

Report:

```text
target/canardstack-bench/10gpd-mixed-query-low-2h-descfix/20260515T172702Z/report.json
```

### Environment

- Host: macOS Docker Desktop on Apple Silicon.
- Server container: `canardstack:local`, Compose project
  `canardstack-descfix`.
- Network path: local benchmark process to Docker-published
  `127.0.0.1:4319`, not Docker container-to-container networking.
- Server cap: Compose benchmark defaults, 2 CPU / 4 GB memory.
- Storage mode: local DuckLake with DuckDB catalog.
- Caveat: this is useful local regression evidence for the fixture correction,
  not cloud-equivalent Linux VM proof.

### Command Shape

```sh
env \
  CANARDSTACK_BENCHMARK_CPU_LIMIT=2.0 \
  CANARDSTACK_BENCHMARK_MEMORY_LIMIT=4g \
  CANARDSTACK_BENCHMARK_RESOURCE_NOTE='Docker Desktop VM capped to 2 CPU / 4 GB memory; benchmark server uses fresh canardstack-descfix Compose volume' \
  cargo bench --bench v0_iteration -- \
    --base-url http://127.0.0.1:4319 \
    --warmup 5m \
    --duration 2h \
    --max-runtime 130m \
    --target-gb-day 10 \
    --profile mixed-query \
    --query-pressure low \
    --query-concurrency 1 \
    --report-dir target/canardstack-bench/10gpd-mixed-query-low-2h-descfix
```

### Key Numbers

- Pass: `true`.
- Measured duration: `7200.003722458s`.
- Target throughput: `115740.74074074074 decoded B/s`.
- Actual throughput: `115973.13934650828 decoded B/s`.
- Query profile: mixed-query, low pressure, concurrency 1, interval 10s.
- Workload metric description bytes: `192`.
- HTTP status counts: `202=20757`, `200=720`.
- Ingest latency: p50 `60.5ms`, p95 `106.7ms`, p99 `114.1ms`.
- Query latency: p50 `132.0ms`, p95 `212.1ms`, p99 `221.3ms`.
- Top server phase: `otlp_transform metric_gauge`, `66.633s` total,
  `0.003204s` average, `0.083675s/MiB`.
- Storage insert: `metric_gauge` `7.084s`, `metric_sum` `4.785s`.
- DuckLake inlined rows at end: `0` for logs, spans, gauge metrics, and sum
  metrics.
- DuckLake Parquet files at end: logs `5`, spans `3`, metric_gauge `6`,
  metric_sum `6`.
- Physical storage bytes: `807674644`.

### What This Supports

This supports the narrow statement:

```text
With realistic 192-byte metric descriptions, this local Docker Desktop run
sustained 10 GB/day equivalent mixed ingest with low query interference for
2 hours without 429/503s, query failures, transport errors, or DuckLake
inlined-row buildup.
```

### What This Does Not Support

- It does not support a 25 GB/day claim under the same envelope.
- It does not support medium or high query-pressure claims.
- It does not prove object-store DuckLake behavior.
- It does not prove real Linux VM or cloud instance behavior.
- It is not directly comparable to the earlier container-to-container run on
  network path alone, because this run used the Docker-published host port.

## 2026-05-15 - 10 GB/day Mixed Query, Docker Container Network

### Claim Tested

One Canardstack instance can sustain a 10 GB/day equivalent mixed ingest workload
for one tenant for 2 hours with low compatibility-query interference under a
known Docker resource envelope.

### Result

Pass.

Report:

```text
target/canardstack-bench/10gpd-mixed-query-low-2h-container-net-attempt7/20260515T083247Z/report.json
```

### Environment

- Host: macOS Docker Desktop on Apple Silicon.
- Server container: `canardstack:local`.
- Benchmark generator container: `canardstack:bench-builder`.
- Network path: Docker container-to-container network, not macOS host port
  forwarding.
- Server cap: configured as 2 CPU / 4 GB memory.
- Generator cap: configured as 1 CPU / 2 GB memory.
- Storage mode: local DuckLake with DuckDB catalog.
- Caveat: this is useful local regression evidence, not cloud-equivalent Linux VM
  proof.

The report captured the generator cgroup values:

```text
cgroup_cpu_max=100000 100000
cgroup_memory_max=2147483648
```

The server cap was supplied through the run setup and report note:

```text
Docker-container-network-SUT-capped-to-2-CPU-4GB-generator-capped-to-1-CPU-2GB
```

### Command Shape

The benchmark was run from the builder container against the server container:

```sh
cargo bench --bench v0_iteration -- \
  --base-url http://canardstack-proof7:4318 \
  --warmup 5m \
  --duration 2h \
  --max-runtime 130m \
  --target-gb-day 10 \
  --profile mixed-query \
  --query-pressure low \
  --query-concurrency 1 \
  --report-dir /reports/10gpd-mixed-query-low-2h-container-net-attempt7
```

### Key Numbers

- Pass: `true`.
- Measured duration: `7200.00278853s`.
- Target throughput: `115740.74074074074 decoded B/s`.
- Actual throughput: `115981.38744200302 decoded B/s`.
- Query profile: mixed-query, low pressure, concurrency 1, interval 10s.
- HTTP status counts: `202=1013`, `200=720`.
- Ingest latency: p50 `67.986292ms`, p95 `118.579541ms`, p99 `134.821125ms`.
- Query latency: p50 `86.926167ms`, p95 `153.067501ms`, p99 `166.336459ms`.
- Queue oldest-age trend: not clearly increasing.
- Freshness lag trend: not clearly increasing.
- DuckLake inlined rows at end: `0` for logs, spans, gauge metrics, and sum
  metrics.
- DuckLake Parquet files at end: `1` per table.
- Physical storage bytes: `2380356430`.

### What This Supports

This supports the narrow statement:

```text
On Docker Desktop using container-to-container networking, with the Canardstack
server capped to 2 CPU / 4 GB and the benchmark generator capped separately,
this build sustained 10 GB/day equivalent mixed ingest with low query
interference for 2 hours without 429/503s, query failures, queue growth,
freshness trend growth, or DuckLake inlined-row buildup.
```

### What This Does Not Support

- It does not support a 25 GB/day claim under the same envelope.
- It does not support medium or high query-pressure claims.
- It does not prove object-store DuckLake behavior.
- It does not prove real Linux VM or cloud instance behavior.
- It does not prove overnight stability.

### Negative Evidence From Same Session

Earlier runs in the same session did not produce claim-grade proof:

- 25 GB/day mixed-query medium pressure failed during warmup with transport
  errors and throughput collapse.
- 25 GB/day mixed-query low pressure failed around 20 minutes measured with a
  transport timeout.
- 25 GB/day ingest-only failed around 28 minutes measured with repeated
  transport timeouts, throughput below target, and freshness lag growth.
- 10 GB/day mixed-query low pressure over macOS host-published ports failed
  around 22 minutes measured with transport timeouts.

The passing 10 GB/day run avoided macOS host-published port forwarding by using a
Docker container network between the generator and server.
