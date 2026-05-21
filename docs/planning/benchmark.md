# Benchmark Gates

This document describes the current benchmark and proof gates for canardstack.
It intentionally does not keep historical experiment notes; use git history for
old scout runs.

## Current Architecture Under Test

The current MVP architecture is:

```text
OTLP/HTTP -> local raw spool write, periodic append sync -> inline transform/admission
  -> freshness-budget admission -> bounded in-process queues -> protected flush lane
  -> immutable Parquet segments
  -> DuckLake registration -> logical DuckLake SQL compatibility APIs
```

Current product constraints:

- One Rust binary.
- Synchronous standard-library HTTP server.
- OTLP/HTTP only.
- DuckDB/DuckLake for storage and query execution.
- No async runtime, gRPC, Kafka, DataFusion, Vortex, Postgres requirement,
  second service, or arbitrary SQL over compatibility APIs.
- `202` means the raw request was written to the local raw spool and accepted
  for processing, pending periodic or byte-threshold append sync. It does not
  mean the append is fsynced, rows are committed, or rows are query-visible.
- QueryEngine and compatibility APIs read registered logical DuckLake tables,
  not raw Parquet file paths.
- Logical lane admission reserves flush capacity before query capacity. Cheap
  discovery/probe/instant-ish queries retain a protected lane; heavy range/search
  queries degrade or reject first under freshness debt.
- Metrics performance is TBD. The current MVP envelope is for logs and traces.

## What Must Stay True

The MVP gates prove these behaviors:

- **Shutdown/restart:** SIGTERM/SIGINT stops accepts, drains active request
  threads inside the shutdown window, and preserves every already-returned
  `202` through raw-spool replay.
- **Replay backlog:** restart can replay `10,000` pending raw records, or at
  least `128 MiB` pending raw spool data if the fixture reaches that first.
- **Operator surface:** `/metrics`, `/healthz`, and
  `/api/admin/health/ingest` distinguish accepted, raw-spooled, pending replay,
  queued, flushed/sealed, DuckLake-visible, checkpointed, spool full, and
  storage unavailable states.
- **Query path:** backward Loki `query_range` and Tempo search/lookup use
  bounded logical DuckLake queries. There is no active raw-Parquet shadow mode
  or custom manifest path.
- **Cutover cleanliness:** old memory-only acceptance paths, null-sink storage,
  validation-only/transform-only ingest controls, and raw-spool crash hooks are
  not serving configuration.

## Main Commands

Use the narrowest proof that covers the changed path:

```bash
# Static checks
cargo fmt --all -- --check
cargo check
cargo check --benches
cargo clippy --all-targets --all-features --locked -- -D warnings

# Focused tests
cargo test raw_spool -- --nocapture
cargo test raw_spool_replays_accepted_unflushed_request_after_restart -- --nocapture
cargo test admin_ingest_health_includes_raw_spool_backlog -- --nocapture
cargo test loki_query_range_backward_uses_standard_log_query -- --nocapture

# End-to-end MVP gate
scripts/raw-spool-promotion-gates.sh
```

The promotion script builds the release binary and runs:

1. SIGTERM during sustained log ingest with scheduler disabled.
2. Replay backlog seed, restart, flush, and pending-count drain.
3. Mixed log ingest plus backward Loki `query_range` pressure.
4. Mixed trace ingest plus Tempo search pressure.

The script writes artifacts under:

```text
/private/tmp/canardstack-raw-spool-gates-<timestamp>/
```

## Promotion Script Knobs

Useful environment overrides:

```bash
CANARDSTACK_RAW_SPOOL_GATE_ROOT=/private/tmp/my-gate
CANARDSTACK_RAW_SPOOL_GATE_PORT=4319
CANARDSTACK_RAW_SPOOL_GATE_WARMUP=10s
CANARDSTACK_RAW_SPOOL_GATE_DURATION=60s
CANARDSTACK_RAW_SPOOL_GATE_TARGET_GB_DAY=500
CANARDSTACK_RAW_SPOOL_GATE_TRACE_TARGET_GB_DAY=500
CANARDSTACK_RAW_SPOOL_GATE_FRESHNESS_SLA=15s
CANARDSTACK_RAW_SPOOL_GATE_BACKLOG_RECORDS=10000
CANARDSTACK_RAW_SPOOL_GATE_BACKLOG_BYTES=134217728
CANARDSTACK_RAW_SPOOL_GATE_MAX_RUNTIME=3m
CANARDSTACK_RAW_SPOOL_GROUP_COMMIT_MS=1
```

If the local machine cannot afford the default backlog or benchmark duration,
lower the knobs and record the exact gap in the handoff. Do not call the MVP
done from a reduced gate.

The group-commit settings are part of the ingest capacity envelope. Record any
non-default values next to benchmark latency and throughput results because a
larger batch/delay can improve throughput while directly raising `202`
acknowledgement latency.

## Benchmark Harness

The benchmark harness is:

```bash
cargo bench --bench throughput_iteration -- [options]
```

Common options:

- `--base-url http://127.0.0.1:4318`
- `--warmup 10s`
- `--duration 60s`
- `--target-gb-day 500`
- `--profile ingest-only|mixed-query`
- `--signals logs|spans|metrics|all`
- `--query-pressure off|low|medium|high`
- `--ingest-concurrency 16`
- `--connection-mode close|persistent`
- `--timestamp-mode fixed|advancing`
- `--freshness-sla 15s`
- `--report-dir /private/tmp/canardstack-bench`

Signal-specific mixed-query pressure maps to the current compatibility surface:

- `logs`: Loki `GET /loki/api/v1/query_range`.
- `spans`: Tempo `GET /api/search`.
- `metrics`: Prometheus `GET /api/v1/query_range`.
- `all`: cycles all three.

Reports include:

- accepted decoded throughput
- accepted request counts and HTTP status counts
- ingest and query latency p50/p95/p99
- freshness lag
- queue trends
- raw-spool, transform, enqueue, flush, seal, checkpoint, and storage-visible
  stage throughput
- lane capacity/in-use, projected flush seconds, projected visibility seconds,
  freshness-budget rejections, and heavy-query lane reductions when emitted
- Loki query latency for log runs
- process CPU/RSS samples when `--server-pid` can be sampled

## Current MVP Envelope

Latest full gate:

```text
/private/tmp/canardstack-raw-spool-gates-20260519T141930Z
```

Replay and shutdown:

- SIGTERM gate: `100` successful `202`s before/while shutdown drained;
  restart plus flush produced `logical_rows.logs=100`.
- Backlog gate: `10,000` pending raw records, `6.33 MB` pending spool bytes,
  seed time `1061s`, restart/replay rounded to `1s`, post-flush
  `logical_rows.logs=10000`, pending records returned to zero.
- The fixture hit the `10,000`-record target before the `128 MiB` byte target.

Logs:

- Gate: mixed log ingest plus backward Loki `query_range`.
- Pass: `true`.
- Accepted decoded throughput: `5.78 MB/s`.
- Successful ingest/query requests: `2003` `202`s, `24` `200` queries.
- Ingest latency p50/p95/p99: `64.9/113.2/238.4 ms`.
- Query latency p50/p95/p99: `76.5/125.8/126.0 ms`.
- Max measured freshness lag: `0.693s`.
- Measured-window storage-visible rows: `8455 rows/s`.
- Final logical log rows: `598,272`.
- Final Loki query returned `100` rows.

Traces:

- Gate: mixed trace ingest plus Tempo search.
- Pass: `true`.
- Accepted decoded throughput: `5.79 MB/s`.
- Successful ingest/query requests: `2899` `202`s, `24` `200` queries.
- Ingest latency p50/p95/p99: `64.7/113.1/228.8 ms`.
- Query latency p50/p95/p99: `103.3/136.3/136.4 ms`.
- Max measured freshness lag: `1.047s`.
- Measured-window storage-visible rows: `12221 rows/s`.
- Final logical span rows: `866,048`.

Metrics:

- Sustained mixed-query metric performance remains exploratory. Keep the latest
  comparable report path with any lane or query-admission change.
- The closest pre-lane 10-minute metrics mixed-query baseline for the
  freshness-first lane work is
  `/private/tmp/canardstack-fireforget-baseline-current/report/20260521T030914Z/report.json`.
  It targeted `4000 GB/day`, `metrics`, `mixed-query`,
  `ingest_concurrency=64`, persistent connections, `30s` warmup, and `601s`
  measured duration. It accepted `10.58 MB/s`, returned `1,013,925` `202`s,
  `1,474,970` `429`s, `14` `503`s, had `14/144` query failures, max queue
  bytes `509,995,784`, max queue oldest age `16.91s`, and max measured
  freshness lag `4.53s`.

Recent single-node throughput scout, May 20 2026: the highest clean 10-minute
mixed-signal run was `2000 GB/day` at ingest concurrency `64`, with
`23.1 MB/s` accepted decoded throughput, `111.4/120.8 ms` ingest p95/p99, no
`429`/`503` responses, no query failures, max queue oldest age `8.7s`, and max
measured freshness lag `1.6s`
(`/private/tmp/canardstack-single-node-confirm-2000-c64-default-10m/report/20260520T033432Z/report.json`).
Higher targets are not currently sustainable under the guardrails: `2500 GB/day`
failed the 10-minute run with `429` queue-pressure responses after roughly
eight minutes, while `3000 GB/day` passed only a 4-minute screen with visible
backlog drift. The dominant measured cost was raw-spool append time, so the
current performance limit is ingest durability/backlog handling rather than
DuckLake registration or query visibility.

## Evidence Rules

Every future benchmark entry should record:

- command
- git SHA or dirty-worktree note
- machine/environment
- duration and warmup
- target GB/day
- signals and payload shape
- concurrency and connection mode
- pass/fail
- report path
- accepted throughput
- storage-visible progress
- ingest/query latency p50/p95/p99
- freshness lag
- status counts and transport errors
- lane projected visibility, freshness-budget rejections, and query lane reductions/rejections when relevant

Use `15s` as the quick validation freshness SLA and `30s` as the sustained
multi-hour freshness SLA. Treat a clearly increasing freshness trend as a
failure even when the maximum stays under the configured SLA.

Do not treat compile success, shadow measurements, or accepted throughput alone
as architecture proof. The current architecture proof requires replay,
checkpoint drain, and logical DuckLake query visibility.
