# Developer Guide

This guide covers contributor setup, local development workflows, and the
implementation details that are useful when changing canardstack itself.

For a practitioner-focused overview, start with the [README](../README.md).

## Architecture

```text
OTLP/HTTP JSON or protobuf -> otlp2records -> bounded process queues -> ducklake/duckdb
```

canardstack is currently shaped as:

- One Rust binary, `canardstack`.
- Synchronous standard-library HTTP server on `CANARDSTACK_BIND`.
- `otlp2records` for OTLP logs, traces, gauge metrics, and sum metrics.
- Bounded per-signal in-memory queues with row, byte, age, and pressure checks.
- DuckDB through `duckdb-rs`.
- DuckLake through DuckDB's official `ducklake` extension SQL surface when
  `CANARDSTACK_USE_DUCKLAKE=true`. Set `CANARDSTACK_USE_DUCKLAKE=false` for a
  local-DuckDB-only mode (dev / smoke / single-host demos).
- Prometheus, Loki, and Tempo compatibility adapters over bounded query helpers.
- HTTP routes for ingest, smoke checks, and compatibility queries.
- Query execution with time range, limit, timeout, memory, and concurrency
  enforcement.
- Grafana is the only bundled UI; canardstack itself does not serve a custom
  browser interface.
- Whole-day retention execution for telemetry tables, followed by DuckLake
  snapshot expiration and cleanup hooks when DuckLake is attached.
- Storage health with freshness watermarks, logical row counts, and local
  physical bytes.
- Prometheus-style operator metrics at `/metrics`, also snapshotted into the
  metric store for Grafana dashboards.

## Local DuckLake Mode

Compose stores local metadata and files in the `canardstack-data` named volume
mounted at `/var/lib/canardstack`.

The default Compose environment is explicit:

```text
CANARDSTACK_BIND=0.0.0.0:4318
CANARDSTACK_DATA_DIR=/var/lib/canardstack
CANARDSTACK_DUCKDB_PATH=/var/lib/canardstack/canardstack.duckdb
CANARDSTACK_STORAGE_DIR=/var/lib/canardstack/storage
CANARDSTACK_DUCKDB_EXTENSION_DIR=/usr/local/lib/duckdb/extensions
CANARDSTACK_USE_DUCKLAKE=true
```

With no `CANARDSTACK_POSTGRES_DSN`, DuckLake uses a local DuckDB-backed catalog
file beside `CANARDSTACK_DUCKDB_PATH` and local file storage under
`CANARDSTACK_STORAGE_DIR`. Postgres catalogs and object storage are later
deployment modes, not required for the Docker quickstart.

Default v0 writes use DuckLake direct data-file writes
(`CANARDSTACK_DUCKLAKE_DATA_INLINING_ROW_LIMIT=0`) and scheduled
`ducklake_merge_adjacent_files` compaction
(`CANARDSTACK_DUCKLAKE_COMPACTION_ENABLED=true`). This keeps ingest latency low
without leaving many active small files. Immediate cleanup of compacted files is
off by default (`CANARDSTACK_DUCKLAKE_COMPACTION_CLEANUP_FILES=false`) because
operators should only remove files scheduled for deletion when long-running
readers are understood. Local single-process runs can enable it to reclaim disk
space aggressively.

The Docker image build runs `canardstack install-ducklake-extension` and stores
the DuckDB DuckLake extension under `/usr/local/lib/duckdb/extensions`. That
build step needs network access to the DuckDB extension repository the first
time the image is built. At runtime, startup first attempts to load the packaged
extension. If it is missing, startup fails loudly with guidance to either fix
the extension path or set `CANARDSTACK_USE_DUCKLAKE=false` for a local-only
mode.

`develop.watch` is intentionally not enabled. The Docker quickstart evaluates
the packaged binary path; Rust source edits should rebuild the image or use the
host workflow below.

## MotherDuck-Hosted DuckLake

For a read-write DuckLake hosted in MotherDuck, configure canardstack with a
MotherDuck attach URI instead of the local or Postgres-catalog DuckLake path:

```bash
export MOTHERDUCK_TOKEN='<your-motherduck-token>'
export CANARDSTACK_DUCKLAKE_ATTACH_URI='md:test-ducklake'
docker compose up --build
```

The attach URI must be the URI only, not a full SQL statement. For example, use
`md:test-ducklake`, not `ATTACH 'md:test-ducklake';`.

Keep `CANARDSTACK_POSTGRES_DSN` unset when
`CANARDSTACK_DUCKLAKE_ATTACH_URI` is set. At startup, canardstack loads the
MotherDuck extension, runs:

```sql
ATTACH 'md:test-ducklake' AS canardlake;
USE canardlake;
```

and then creates or reuses the standard telemetry tables in that remote
database. The local `CANARDSTACK_DUCKDB_PATH` still exists as the client-side
DuckDB file used to load extensions and establish query connections.

Run the live MotherDuck smoke test with your token loaded:

```bash
set -a
. ./.env
set +a
cargo test remote_motherduck_ducklake_smoke -- --ignored --nocapture
```

The normal test suite does not contact MotherDuck. It verifies the attach plan
offline; the ignored smoke verifies startup, ingest, flush, and compatibility
query visibility against the configured remote database.

## Host Workflow

Host Rust is useful for contributors, but is not required for Docker evaluation.
The default dependency on `otlp2records` is the published crates.io package, not
a sibling checkout.

Start from the checked-in example:

```bash
cp config/example.env .env
```

Load it in your shell, or export the variables directly:

```bash
set -a
. ./.env
set +a
```

For a PostgreSQL DuckLake metadata catalog:

```bash
createdb ducklake_catalog
export CANARDSTACK_POSTGRES_DSN='dbname=ducklake_catalog host=localhost user=postgres password=postgres'
```

If `CANARDSTACK_POSTGRES_DSN` is unset, DuckLake uses a local DuckDB metadata
catalog file beside `CANARDSTACK_DUCKDB_PATH`.

When `CANARDSTACK_USE_DUCKLAKE=true`, startup fails fast if DuckLake cannot
attach — no silent fallback. For an explicit local-only run (dev, smoke, CI),
set `CANARDSTACK_USE_DUCKLAKE=false`.

Build and run:

```bash
cargo check
cargo test
cargo run -- serve
```

Then open:

```text
http://127.0.0.1:4318/
```

Run the in-process smoke command:

```bash
cargo run -- smoke
```

The smoke command starts the app in-process, ingests representative OTLP JSON
logs, traces, gauge metrics, and sum metrics, flushes process queues, calls
representative compatibility query endpoints, and prints health.

## Query Surface

The primary v0 query surface is a set of compatibility adapters. They use the
same bounded query engine as smoke tests and Grafana, with time ranges,
limits, server-owned timeouts, DuckDB memory limits, and query concurrency
guards.

Prometheus-compatible metrics routes:

- `GET/POST /api/v1/query`
- `GET/POST /api/v1/query_range`
- `GET /api/v1/labels`
- `GET /api/v1/label/{name}/values`
- `GET /api/v1/series`
- `GET /api/v1/metadata`

Loki-compatible logs routes:

- `GET /loki/api/v1/query_range`
- `GET /loki/api/v1/query`
- `GET /loki/api/v1/labels`
- `GET /loki/api/v1/label/{name}/values`
- `GET /loki/api/v1/series`

Tempo-compatible trace routes:

- `GET /api/v2/traces/{traceID}`
- `GET /api/traces/{traceID}`
- `GET /api/search`
- `GET /api/search/tags`
- `GET /api/search/tag/{tag}/values`

These are subset adapters, not full protocol implementations. Prometheus and
Loki errors use `{"status":"error","errorType":"...","error":"..."}`. The
normal HTTP API does not expose arbitrary SQL.

Supported ingest response behavior:

- `400` for invalid payload, content type, compression, payload size, or
  timestamp skew.
- `401` for missing API key.
- `403` for bad API key.
- `429` for retryable queue or process ingest pressure.
- `503` when storage dependencies are unhealthy.

## Pre-commit Hooks

Local checks are wired up through [prek](https://github.com/j178/prek), a Rust
reimplementation of the `pre-commit` framework. The repo's
`.pre-commit-config.yaml` runs `cargo fmt --all -- --check` and the same clippy
invocation as CI (`cargo clippy --all-targets --all-features --locked --
-D warnings`) before each commit.

Install once per checkout:

```bash
brew install prek   # or: cargo install --locked prek
prek install
```

Run all hooks against the working tree without committing:

```bash
prek run --all-files
```

## Tests And Proofs

Run the Rust test suite:

```bash
cargo test
```

Coverage currently includes auth, invalid payloads, timestamp skew,
dependency-unhealthy mode, unauthenticated `/healthz`, queue pressure, query
limit validation, compatibility auth/error envelopes, ingest-to-query visibility
through Prometheus/Loki/Tempo subsets, crash semantics, the scheduled queue
watchdog, removed dashboard/alert routes, and the retention executor.

Docker-local checks are intentionally outside normal `cargo test`:

```bash
docker compose config
docker compose build
scripts/smoke-docker-local.sh
```

Those checks validate image build, Compose config, healthy container startup,
OTLP fixture ingest through port `4318`, compatibility query responses from the
running container, named-volume persistence across restart, and data removal
after the documented reset.

The CI-shaped Docker proof script builds the image, starts the service, runs
the smoke, restarts the container to prove named-volume persistence, then
removes the volume and verifies the fixture trace is gone:

```bash
scripts/smoke-docker-local.sh
```

## Background Scheduler

The `serve` command spawns one background thread that closes the maintenance
loop without operator action:

- A queue watchdog/flush worker drains due queue partitions when row, byte, or
  age thresholds fire. Ingest request threads enqueue and signal this worker;
  they do not perform DuckDB/DuckLake writes inline.
- A periodic flush drains process queues to DuckLake and triggers DuckLake's
  inlined-data flush.
- A separate compaction pass runs DuckLake adjacent-file compaction only when
  active Parquet file fanout reaches the configured threshold.
- A retention pass enforces the configured retention days, expires DuckLake
  snapshots, and cleans old files.

All four respect `POST /api/admin/maintenance/pause`. Cadences are configurable
via `CANARDSTACK_SCHEDULER_*` env vars. Set
`CANARDSTACK_SCHEDULER_ENABLED=false` to fall back to operator-triggered
maintenance only. The scheduler shuts down cleanly when `serve` exits.

## V0 Gaps

- The standard-library HTTP server is intentionally minimal.
- OTLP/gRPC is intentionally not implemented.
- Histograms and exponential histograms are decoded as unsupported for v0 and
  not stored.
- Custom dashboard and alert APIs have been removed from the v0 path; Grafana
  provisioning is the supported dashboard surface.
- Canardstack no longer presents a bespoke query protocol as a v0 product/API
  surface. Use the Grafana-compatible Prometheus, Loki, and Tempo subsets for
  HTTP workflows, or external DuckDB/MotherDuck/SQL clients for direct SQL.
- Arrow IPC artifacts and embedded Perspective are not implemented.
- Grafana discovery endpoints use daily metadata summaries over promoted
  columns; full raw-attribute introspection remains outside the v0 HTTP API.
- Retention is row-level `DELETE` against single tables as a documented v0
  fallback; the day-tables-behind-views migration is the next storage-layout
  proof gate.
- The maintenance singleton lease is in-process; a Postgres-backed lease is
  needed before splitting maintenance into its own role.
- Benchmarks for 25-100 GB/day and above are not implemented; no throughput
  claim is made.

## Related Docs

- [V0 architecture](architecture/v0-architecture.md)
- [Storage schema](architecture/storage-schema.md)
- [Query API](architecture/query-api.md)
- [Operator metrics](architecture/operator-metrics.md)
- [Benchmark plan](planning/benchmark-plan.md)
- [Proof gates](planning/proof-gates.md)
- [Failure runbooks](runbooks/failure-runbooks.md)
