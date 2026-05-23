# Benchmarking

This document contains reusable local benchmark recipes for canardstack. Keep
entries concrete: include the command, full report path, target, result, and the
pass/fail criteria that future agents should compare against.

## 10 Minute Mixed-Query Smoke

Use this as the reusable performance smoke for ingest-topology or scheduler
changes when the machine can afford a 10 minute run.

Current Arrow append baseline, latest clean run:
[report.json](/private/tmp/canardstack-arrow-cutover-400-10m-20260523/20260523T171602Z/report.json)

- Target: `400 GB/day`, `logs`, `mixed-query`, persistent connections.
- Write path: Arrow write buffer -> DuckDB Arrow append -> DuckLake commit.
- Actual: `4,631,120 B/s` against target `4,629,630 B/s`.
- Statuses: `200:240`, `202:2890`, no `429`.
- Queries: `240/240`, transport errors `0`.
- Ingest p50/p95/p99: `10.6 / 14.4 / 17.2 ms`.
- Query p50/p95/p99: `82.1 / 97.8 / 102.0 ms`.
- Freshness lag final: `0.9s`; measured trend data was unavailable, but
  progress logs oscillated between `0.1s` and `1.0s` without sustained growth.

Run the server in one shell with a fresh data directory and a free local port:

```bash
env CANARDSTACK_DATA_DIR=/private/tmp/canardstack-arch-cutover-data-400-rerunN \
  CANARDSTACK_BIND=127.0.0.1:4330 \
  cargo run --release -- serve
```

Run the benchmark in another shell:

```bash
cargo bench --bench throughput_iteration -- \
  --base-url http://127.0.0.1:4330 \
  --warmup 5s \
  --duration 600s \
  --target-gb-day 400 \
  --profile mixed-query \
  --signals logs \
  --ingest-concurrency 16 \
  --connection-mode persistent \
  --query-pressure medium \
  --query-concurrency 2 \
  --query-interval 5s \
  --progress-interval 30s \
  --max-runtime 660s \
  --report-dir /private/tmp/canardstack-arch-cutover-400-10m-rerunN
```

Pass criteria:

- `pass=true`.
- No `429` unless the change intentionally tightens freshness/admission limits.
- Query success count equals attempted query count.
- No transport errors.
- Freshness lag must oscillate or recover; sustained monotonic growth is a
  regression.
- Treat ingest p95 above `50 ms` or query p95 above `150 ms` as requiring
  investigation.
- Stop the release server after the run and report the full `report.json` path.
