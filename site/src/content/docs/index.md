---
title: canardstack
description: Query OpenTelemetry-shaped logs, traces, and metrics stored in DuckLake.
hero:
  tagline: Query OpenTelemetry-shaped logs, traces, and metrics stored in DuckLake.
  image:
    file: ../../assets/canardstack.png
  actions:
    - text: Get Started
      link: '#start-locally'
      icon: right-arrow
    - text: View on GitHub
      link: https://github.com/smithclay/canardstack
      icon: external
      variant: secondary
---

canardstack is an experimental query server for observability data stored in
[DuckLake](https://ducklake.select/). It exposes bounded Prometheus, Loki, and
Tempo-compatible HTTP APIs for Grafana-style clients.

Telemetry writes are handled outside canardstack. Use
[`duckdb-otlp`](https://github.com/smithclay/duckdb-otlp) in a DuckDB process to
write OTLP logs, traces, gauge metrics, and sum metrics into DuckLake tables,
then point canardstack at that catalog for query serving.

## Why canardstack?

canardstack is for exploring observability data as lakehouse data: DuckLake
tables underneath, Grafana-compatible APIs on top, and direct DuckDB SQL when
you want to inspect the files yourself.

The project is intentionally narrow. It does not own ingest, a raw spool, a
remote catalog service, or a full Prometheus/Loki/Tempo implementation. Query
paths are bounded by time range, row limit, timeout, memory limit, and
concurrency caps.

## Start Locally

First create or point at a DuckLake catalog populated by `duckdb-otlp`. For a
local end-to-end smoke against a sibling checkout, use the repository harness:

```bash
scripts/e2e-duckdb-otlp-local.py
```

Then run canardstack against the resulting DuckLake catalog:

```bash
CANARDSTACK_DUCKLAKE_ATTACH_URI=ducklake:/path/to/catalog.ducklake \
CANARDSTACK_DUCKLAKE_DATA_PATH=/path/to/ducklake-data \
CANARDSTACK_API_KEY=dev-canardstack-key \
canardstack serve
```

The HTTP server listens on `127.0.0.1:4318` by default. Use `--listen` or
`CANARDSTACK_BIND` to change it.

```bash
canardstack serve --listen 127.0.0.1:4320
canardstack healthcheck --endpoint http://127.0.0.1:4320/healthz
```

## Schema

canardstack queries the DuckLake tables created by `duckdb-otlp`:

| Table | Signal |
| --- | --- |
| `otlp_logs` | OTLP log records |
| `otlp_traces` | OTLP spans |
| `otlp_metrics_gauge` | OTLP gauge datapoints |
| `otlp_metrics_sum` | OTLP sum datapoints |

Arbitrary OpenTelemetry resource, scope, log, span, and metric attributes are
stored as JSON strings in attribute columns. Grafana-facing labels such as
`deployment_environment`, `http_method`, and `http_route` are derived from those
canonical columns in bounded query paths instead of being stored as separate
raw-table columns.

:::caution[Schema stability]
The current DuckLake schema is experimental and may evolve with breaking
changes before canardstack is ready for stable production use.
:::

## Query Data

Use the [Grafana datasource guide](/query-data/grafana/) to connect metrics,
logs, and traces. Use the [DuckDB SQL guide](/query-data/duckdb/) to query the
DuckLake tables directly.

## Build From Source

For host development:

```bash
cargo check --all-targets
cargo test
cargo run -- serve
```
