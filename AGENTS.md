# Repository Instructions

This file is durable guidance for coding agents working in this repository. Keep it short, concrete, and update it when repeated mistakes show up.

## Project Context

canardstack is a single-binary, experimental observability query server written in Rust. It attaches DuckDB to DuckLake tables populated by an external writer, usually `duckdb-otlp`, and exposes bounded Prometheus, Loki, and Tempo-compatible HTTP APIs for Grafana-style clients.

There is exactly one binary (`canardstack`), one synchronous std-library HTTP server, and one DuckDB process inside canardstack. There is no async runtime, no OTLP ingest endpoint, no OTLP/gRPC endpoint, no Kafka, no bundled catalog service, and no separate hot store. The optional `tls` cargo feature (deps `rustls` + `rcgen`, off by default, enabled in the Docker image) adds a synchronous in-binary TLS terminator for the public `serve` endpoint.

## Commands

Prefer the narrowest command that proves the change.

```bash
# Build / typecheck
cargo check
cargo build --all-targets --locked

# Tests
cargo test
cargo test <test_name>

# Lint and formatting
cargo fmt --all -- --check
cargo clippy --all-targets --all-features --locked -- -D warnings

# Site
cd site && ASTRO_TELEMETRY_DISABLED=1 npm run build
cd site && ASTRO_TELEMETRY_DISABLED=1 npm run check

# Local server workflow
cp config/example.env .env && set -a && . ./.env && set +a
cargo run -- serve
cargo run -- serve --listen 127.0.0.1:4320
cargo run -- healthcheck --endpoint http://127.0.0.1:9090/healthz

# Local duckdb-otlp integration smoke
scripts/e2e-duckdb-otlp-local.py

# Docker
docker compose up
```

DuckLake is the only storage target canardstack queries. Startup should fail loudly if DuckDB cannot attach the configured catalog or load required extensions. With no remote catalog configuration, canardstack uses a local DuckLake catalog and local data files.

## Architecture

Data flow:

```text
OpenTelemetry producers
  -> external DuckDB writer with duckdb-otlp
  -> DuckLake otlp_* tables
  -> canardstack QueryEngine
  -> bounded Prometheus / Loki / Tempo compatibility APIs
```

Storage signals are `Logs`, `Spans`, `MetricGauge`, and `MetricSum`. Histograms and exponential histograms are not part of the v0 query surface.

## Source Map

- `src/main.rs` - argv dispatch for `serve`, `healthcheck`, and `--version`; installs SIGINT/SIGTERM handlers.
- `src/lib.rs` - re-exports `AppState` and `Config`; defines `log_event` and `LockExt::lock_or_poisoned`.
- `src/app.rs` - wires long-lived components into shared `Arc<AppState>`.
- `src/config.rs` - reads `CANARDSTACK_*` env vars into one `Config`; startup calls `Config::validate()`.
- `src/http.rs` and `src/http/` - hand-rolled std-library HTTP/1.1 server, routing, auth, parsing, and responses.
- `src/validation.rs` - shared auth, request bounds, `ApiError`, and error envelopes.
- `src/signal.rs` - shared `StorageSignal` physical signal/table vocabulary.
- `src/admission_control.rs` - cheap/heavy query admission.
- `src/storage/` - DuckDB/DuckLake attach, schema fencing, health probes, metadata helpers, and scoped query connections.
- `src/query/` - bounded query helpers, shared query plans, and Prometheus/Loki/Tempo selector parsing.
- `src/compat/` - Prometheus/Loki/Tempo route adapters and the v0 public query surface.
- `src/metadata.rs` - bounded discovery-metadata adapters over visible DuckLake rows, with an in-process cache.
- `src/metrics.rs` - Prometheus-style operator metrics at `/metrics`.
- `src/db/sql.rs` - shared SQL fragment helpers used by `storage`, `query`, and `compat`.
- `src/runtime/memory.rs` - runtime memory-pressure probing.
- `src/cli/` - subcommand implementations.

## Load-Bearing Constraints

- Keep the code synchronous. Do not add `tokio`, `async fn`, gRPC, Kafka, a second binary, or another long-running service unless the task explicitly changes the architecture.
- Use OS threads plus `Arc<Mutex<_>>`. Prefer `LockExt::lock_or_poisoned()` over `.lock().unwrap()` for shared state.
- Keep query routes bounded by time range, row limit, timeout, DuckDB memory limit, and concurrency caps through `QueryEngine`.
- Do not expose arbitrary SQL through the compatibility APIs. Direct SQL is intentionally an external DuckDB CLI / MotherDuck path.
- Preserve the Prometheus/Loki error envelope shape: `{"status":"error","errorType":"...","error":"..."}`.
- canardstack does not own ingestion durability or visibility timing. It queries whatever rows are visible in the attached DuckLake catalog.
- The storage schema is static and version-fenced. The DuckLake catalog carries a `schema_version` in `canardstack_meta`; `Storage::open` fails boot when it is outside `[MIN_COMPATIBLE_SCHEMA_VERSION, SCHEMA_VERSION]` (`src/storage/schema.rs`). Changing a `*_COLUMNS` set means bumping `SCHEMA_VERSION`; additive/expand-contract keeps `MIN_COMPATIBLE` low, while a breaking change raises both.

## Testing Expectations

- Add or update tests for behavior changes when practical.
- End-to-end query tests usually belong in `tests/integration.rs`; do not create a new test crate without a clear reason.
- For storage attach changes, cover local DuckLake plus relevant `CANARDSTACK_DUCKLAKE_ATTACH_URI` combinations, or explain why a live smoke is required.
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
feat(query): add bounded metric discovery
fix(storage): reject unsupported schema versions
docs: document duckdb-otlp smoke
```

## Further Reading

The `docs/` tree is the canonical longer-form reference. Start with `docs/developer.md`; architecture details are under `docs/architecture/`, and the local duckdb-otlp integration smoke is documented in `docs/e2e-duckdb-otlp.md`.
