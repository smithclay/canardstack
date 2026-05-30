---
title: Get Started
description: Run the local duckdb-otlp to canardstack query flow.
---

This tutorial validates the query-only shape on your machine:

```text
duckdb-otlp OTLP writer -> DuckLake tables -> canardstack query APIs
```

## Prerequisites

- Rust and Cargo
- Node is only needed for editing this docs site
- A sibling `../duckdb-otlp` checkout with release artifacts:

```text
../duckdb-otlp/build/release/duckdb
../duckdb-otlp/build/release/extension/otlp/otlp.duckdb_extension
```

## Run the E2E Smoke

From the canardstack repository root:

```bash
scripts/e2e-duckdb-otlp-local.py
```

The smoke:

1. Starts DuckDB with the local `duckdb-otlp` extension.
2. Creates a temporary DuckLake catalog.
3. Posts sample OTLP logs to the extension's `/v1/logs` endpoint.
4. Flushes the writer.
5. Starts canardstack against the same catalog.
6. Queries the rows through Loki `query_range`.

Expected output:

```text
ok: duckdb-otlp wrote 3 OTLP log rows to DuckLake; canardstack queried them through Loki query_range
```

Use `--keep-temp` to inspect the generated catalog and logs after the run.

## Next

- [Serve DuckLake](/quickstart/serve/) shows the smallest server command.
- [Query with Grafana](/guides/query-with-grafana/) connects Grafana datasources.
- [Schema Reference](/reference/schema/) lists the table contract.
