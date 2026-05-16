# Repository Instructions

This file is durable guidance for coding agents working in this repository. Keep it short, concrete, and updated when repeated mistakes show up.

## Project Context

canardstack is a single-binary, experimental observability backend written in Rust. It accepts OpenTelemetry logs, traces, gauge metrics, and sum metrics over OTLP/HTTP, normalizes them through `otlp2records` into Arrow `RecordBatch`es, stores them in DuckLake-managed DuckDB tables, and exposes bounded Prometheus/Loki/Tempo compatibility APIs for Grafana-style clients.

There is exactly one binary (`canardstack`), one synchronous std-library HTTP server, and one DuckDB process. There is no async runtime, no OTLP/gRPC endpoint, no Kafka, and no separate hot store.

## Commands

Prefer the narrowest command that proves the change.

```bash
# Build / typecheck
cargo check
cargo build --all-targets --locked

# Tests (offline; does not touch MotherDuck)
cargo test
cargo test <test_name>
cargo test remote_ducklake_attach_uri_smoke -- --ignored --nocapture  # live remote DuckLake smoke; requires credentials

# Lint and formatting (CI treats warnings as errors)
cargo fmt --all -- --check
cargo clippy --all-targets --all-features --locked -- -D warnings

# Pre-commit, matching CI shape
prek install
prek run --all-files

# Local server workflow
cp config/example.env .env && set -a && . ./.env && set +a
cargo run -- serve              # serves on CANARDSTACK_BIND, default 127.0.0.1:4318
cargo run -- smoke              # in-process smoke: starts app, ingests fixtures, queries, prints health
cargo run -- smoke-http <url>   # smoke against an already-running server
cargo run -- healthcheck <url>  # used as the Docker healthcheck

# Docker quickstart
docker compose up --build
docker compose run --rm smoke
scripts/smoke-docker-local.sh

# Bench
cargo bench --bench v0_iteration
```

DuckLake is the only ingest storage mode and requires the DuckLake DuckDB extension; startup should fail loudly if it is not loadable. With no remote catalog configuration, canardstack uses a local DuckLake catalog and local data files.

## Architecture

Data flow:

```text
OTLP/HTTP (JSON or protobuf, optional gzip)
  -> validation (auth, content type, size, timestamp skew)
  -> otlp2records -> Arrow RecordBatch grouped by Signal
  -> bounded per-signal in-memory queue
  -> immutable Parquet segment files registered with DuckLake
  -> bounded compat query adapters for Prometheus / Loki / Tempo subsets
```

Signals are `Logs`, `Spans`, `MetricGauge`, and `MetricSum`. Histograms and exponential histograms are intentionally rejected in v0.

## Source Map

The crate is intentionally flat; each module maps to a pipeline stage or boundary.

- `src/main.rs` - argv dispatch for `serve`, `smoke`, `smoke-http`, `healthcheck`, and `install-ducklake-extension`; installs SIGINT/SIGTERM handlers.
- `src/lib.rs` - re-exports `AppState`, `Config`, and `Scheduler`; defines `log_event` and `LockExt::lock_or_poisoned`.
- `src/app.rs` - wires long-lived components into shared `Arc<AppState>`.
- `src/config.rs` - reads `CANARDSTACK_*` env vars into one `Config`; startup calls `Config::validate()`.
- `src/http.rs` - hand-rolled std-library HTTP/1.1 server with bounded per-connection threads and non-blocking accept shutdown.
- `src/validation.rs` - auth, content-type, size, compression, timestamp-skew checks, `ApiError`, and error envelopes.
- `src/otlp.rs` - OTLP JSON/protobuf decode and `Transformed` payload construction.
- `src/ingest/` - request flow, admission, queue accounting, flush orchestration, and `PartialFlushError`.
- `src/storage.rs` - DuckDB lifecycle, DuckLake `ATTACH`, extension install, immutable segment writes, `StorageProbe`, retention, and maintenance SQL.
- `src/query.rs` - bounded query helpers with interactive and background lanes.
- `src/compat.rs` - Prometheus/Loki/Tempo route adapters and the v0 public query surface.
- `src/metadata.rs` - bounded discovery-metadata adapters over `metadata_summary`, with a generation-keyed in-process cache.
- `src/maintenance.rs` - `Scheduler` background thread for flush, metadata refresh, operator-metrics snapshot, compaction, retention, and maintenance pause.
- `src/metrics.rs` - Prometheus-style operator metrics at `/metrics`.
- `src/sql.rs` - shared SQL fragment helpers used by `storage` and `compat`.
- `src/cli/` - subcommand implementations.

## Load-Bearing Constraints

- Keep the code synchronous. Do not add `tokio`, `async fn`, gRPC, Kafka, a second binary, or another long-running service unless the task explicitly changes the architecture.
- Use OS threads plus `Arc<Mutex<_>>`. Prefer `LockExt::lock_or_poisoned()` over `.lock().unwrap()` for shared state.
- Treat ingest as best-effort: a 2xx response means "accepted into a bounded in-memory queue," not "durably committed." There is no WAL.
- Preserve pressure behavior: queues return 429 under pressure, and storage/dependency failures surface as 503 where appropriate.
- Keep query routes bounded by time range, row limit, timeout, DuckDB memory limit, and concurrency caps through `QueryEngine`.
- Do not expose arbitrary SQL through the compatibility APIs. Direct SQL is intentionally an external DuckDB CLI / MotherDuck path.
- Preserve the Prometheus/Loki error envelope shape: `{"status":"error","errorType":"...","error":"..."}`.
- Assume one in-process scheduler and single writer. There is no Postgres-backed maintenance lease yet.

## Testing Expectations

- Add or update tests for behavior changes when practical.
- End-to-end tests usually belong in `tests/v0.rs` with shared fixtures under `tests/common/`; do not create a new test crate without a clear reason.
- For storage-mode changes, cover local DuckLake plus relevant `CANARDSTACK_POSTGRES_DSN` and `CANARDSTACK_DUCKLAKE_ATTACH_URI` combinations, or explain why a live smoke is required.
- For compatibility API changes, verify both success payloads and protocol-specific error envelopes.
- If a check cannot be run locally, say exactly which command was skipped and why.

## Done When

Before handing work back, confirm the requested behavior is implemented and the smallest relevant verification has run:

- Formatting is clean, or the change did not touch formatted code.
- Typecheck, targeted tests, or smoke checks cover the edited path.
- Clippy is run for nontrivial Rust changes, or the reason for skipping it is explicit.
- The final diff is reviewed for regressions, unbounded query paths, accidental architecture expansion, and unrelated churn.

## Conventions

- Commits must use Conventional Commits: `type(optional-scope): imperative summary`.
- Common commit types are `feat`, `fix`, `docs`, `test`, `refactor`, `perf`, `build`, and `chore`.
- Keep the commit subject concise and lowercase after the type/scope unless it names a proper noun or API.
- CI treats clippy warnings as errors with `-D warnings`; match that locally before pushing.
- Prefer existing module patterns and helper APIs over new abstractions.
- Keep edits scoped. Do not refactor unrelated code while fixing a narrow issue.

Examples:

```text
feat(ingest): queue Arrow batches for metrics
perf(storage): append RecordBatches through DuckDB
docs: document commit message convention
```

## Further Reading

The `docs/` tree is the canonical longer-form reference. Start with `docs/developer.md`; deeper material is under `docs/architecture/`, `docs/planning/`, and `docs/runbooks/`.
