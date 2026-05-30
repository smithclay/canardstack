---
title: Serve DuckLake
description: Start canardstack against an existing DuckLake catalog.
---

Use this when a DuckDB writer has already populated DuckLake tables with
`duckdb-otlp`.

## Start the Query Server

```bash
CANARDSTACK_DUCKLAKE_ATTACH_URI=ducklake:/path/to/catalog.ducklake \
CANARDSTACK_DUCKLAKE_DATA_PATH=/path/to/ducklake-data \
CANARDSTACK_API_KEY=dev-canardstack-key \
canardstack serve --listen 127.0.0.1:4318
```

## Check Health

```bash
canardstack healthcheck --endpoint http://127.0.0.1:4318/healthz
```

The health check must be able to attach DuckLake and prepare a read query
against `otlp_logs`.

## Query a Route

```bash
curl -sS -H 'Authorization: Bearer dev-canardstack-key' \
  http://127.0.0.1:4318/loki/api/v1/labels
```

For exact route contracts, see [API Reference](/reference/api/).
