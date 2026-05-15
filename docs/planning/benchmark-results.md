# Benchmark Results Log

This is a running factual log of benchmark evidence. Keep entries narrow: record
the exact claim tested, environment, command shape, report path, result, and
known caveats. Do not generalize one run into a broader product claim.

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
