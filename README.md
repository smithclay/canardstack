<h1>
  <img src="canardstack.png" alt="canardstack logo" height="48" align="left" />
  canardstack
</h1>

[![Crates.io](https://img.shields.io/crates/v/canardstack)](https://crates.io/crates/canardstack)
[![CI](https://github.com/smithclay/canardstack/actions/workflows/ci.yml/badge.svg)](https://github.com/smithclay/canardstack/actions/workflows/ci.yml)
[![License: Apache 2.0](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![DuckLake](https://img.shields.io/badge/storage-DuckLake-fff000.svg?logo=duckdb&logoColor=black)](https://ducklake.select/)

> Query OpenTelemetry-shaped logs, traces, and metrics stored in DuckLake.

canardstack is an experimental query server for observability data stored in [DuckLake](https://ducklake.select/). It exposes bounded Prometheus, Loki, and Tempo-compatible HTTP APIs for Grafana-style clients.

Write telemetry with [duckdb-otlp](https://github.com/smithclay/duckdb-otlp) in a DuckDB process, then point canardstack at the resulting DuckLake catalog for query serving.

## Quick Start

```bash
cargo install --locked canardstack

CANARDSTACK_DUCKLAKE_ATTACH_URI=ducklake:/path/to/catalog.ducklake \
CANARDSTACK_DUCKLAKE_DATA_PATH=/path/to/ducklake-data \
canardstack serve
```

The HTTP server listens on `127.0.0.1:4318` by default. Use `--listen` or
`CANARDSTACK_BIND` to change it.

```bash
canardstack serve --listen 127.0.0.1:4320
canardstack healthcheck --endpoint http://127.0.0.1:4320/healthz
```

## Write Telemetry

Use `duckdb-otlp` for ingestion:

```sql
INSTALL ducklake;
LOAD ducklake;
INSTALL otlp FROM community;
LOAD otlp;

ATTACH 'ducklake:/path/to/catalog.ducklake' AS canardlake
  (DATA_PATH '/path/to/ducklake-data');
USE canardlake;

-- See duckdb-otlp for OTLP server and table setup details.
```

For a local end-to-end smoke against a sibling `../duckdb-otlp` checkout, see
[`docs/e2e-duckdb-otlp.md`](docs/e2e-duckdb-otlp.md).

## Query

canardstack serves:

- Prometheus-compatible query and discovery routes under `/api/v1/...`
- Loki-compatible routes under `/loki/api/v1/...`
- Tempo-compatible search and trace routes under `/api/...`
- Operational endpoints at `/healthz`, `/metrics`, and `/api/admin/health/...`

Set `CANARDSTACK_API_KEY` for query routes and
`CANARDSTACK_ADMIN_API_KEY` for admin health routes.

## Status

This project is experimental. The compatibility APIs are intentionally bounded:
there is no arbitrary SQL endpoint, no full PromQL/LogQL/TraceQL
implementation, and query paths are constrained by time range, row limit,
timeout, memory limit, and admission caps.
