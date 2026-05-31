# Developer Notes

canardstack is now a query-only Rust binary. It attaches DuckDB to a DuckLake
catalog and serves bounded Prometheus, Loki, and Tempo-compatible HTTP routes.

## Local Checks

```bash
cargo check --all-targets
cargo test
cargo clippy --all-targets --all-features --locked -- -D warnings
cargo fmt --all -- --check
```

## Run Locally

```bash
cp config/example.env .env
set -a && . ./.env && set +a
cargo run -- serve
cargo run -- serve --listen 127.0.0.1:4320
cargo run -- healthcheck --endpoint http://127.0.0.1:4320/healthz
```

Telemetry writes are handled outside this binary. Use
[duckdb-otlp](https://github.com/smithclay/duckdb-otlp) in a DuckDB process to
write the DuckLake tables, then configure canardstack with the same catalog and
data path.

To validate the local extension checkout end to end:

```bash
scripts/e2e-duckdb-otlp-local.py
```

See [`docs/e2e-duckdb-otlp.md`](e2e-duckdb-otlp.md) for the exact shape and the
local file-lock caveat.

## Source Map

- `src/main.rs` dispatches `serve`, `healthcheck`, and `--version`.
- `src/http/` contains the synchronous std-library HTTP server and route table.
- `src/compat/` adapts Prometheus, Loki, and Tempo-style HTTP requests.
- `src/query/` builds bounded SQL over the DuckLake tables.
- `src/storage/` owns DuckDB/DuckLake attach, health probes, schema fencing, and
  per-query connection cloning.
- `src/metadata.rs` serves discovery routes from `metadata_summary`, with a
  bounded raw-table fallback for Prometheus metric discovery.
- `src/metrics.rs` renders the process-local `/metrics` surface.

## Constraints

- Keep the server synchronous; do not add an async runtime.
- Do not add OTLP ingest, gRPC, Kafka, a bundled catalog service, or a second
  long-running process to this binary.
- Do not expose arbitrary SQL through the compatibility APIs.
- Keep query paths bounded by time range, row limit, timeout, DuckDB memory
  limit, and admission caps.
