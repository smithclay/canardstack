---
title: Operations
description: Operator notes for the query-only canardstack binary.
---

canardstack is experimental and not production-ready. The operator surface is
small and intentionally explicit.

| Area | Behavior |
| --- | --- |
| Process model | One synchronous Rust binary, one embedded DuckDB client process, no async runtime. |
| Ingest | Not served by canardstack. Write telemetry with `duckdb-otlp` or another DuckLake writer. |
| Storage | DuckLake-backed DuckDB tables created by the writer process. |
| Query | Compatibility subsets with server-side time range, row limit, timeout, memory, and concurrency guards. |
| UI | Grafana only. canardstack does not serve a custom browser UI. |
| Maintenance | Catalog/file maintenance is owned by DuckLake and the writer/catalog deployment. |

## Configuration

Configuration is available through `config.toml` and `CANARDSTACK_*`
environment variables. Start from `config/example.toml` for structured config
or `config/example.env` for host development. Environment variables override
the file. Set `CANARDSTACK_CONFIG=/path/to/config.toml` to load a different
config file.

At minimum, point canardstack at a DuckLake catalog:

```bash
CANARDSTACK_DUCKLAKE_ATTACH_URI=ducklake:/path/to/catalog.ducklake
CANARDSTACK_DUCKLAKE_DATA_PATH=/path/to/ducklake-data
CANARDSTACK_API_KEY=dev-canardstack-key
```

Diagnostics are logfmt-style structured events on stderr. Set
`CANARDSTACK_LOG=debug` or use `RUST_LOG` to adjust verbosity.

## Health

The basic health endpoint checks that canardstack can attach to DuckLake and
prepare a read query against the expected telemetry tables:

```bash
canardstack healthcheck --endpoint http://127.0.0.1:4318/healthz
```

Admin health endpoints expose storage capabilities, row counts, freshness
watermarks, and query configuration. Query routes and admin routes can use
separate bearer tokens through `CANARDSTACK_API_KEY` and
`CANARDSTACK_ADMIN_API_KEY`.

## Query Incidents

When query routes fail, first check:

- the DuckLake catalog URI and data path match the writer
- the writer created the expected `otlp_*` tables
- the requested time range contains visible data
- the query limit, timeout, memory limit, and concurrency settings are not too low
- direct DuckDB SQL can read the same rows from the same catalog

Use the [DuckDB SQL guide](/query-data/duckdb/) for direct catalog inspection.
