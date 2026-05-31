---
title: Serve an Existing DuckLake Catalog
description: Start canardstack against an existing DuckLake catalog.
---

This guide shows you how to start canardstack against DuckLake tables that a
DuckDB writer has already populated with `duckdb-otlp`.

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

For exact route contracts, see [HTTP API reference](/reference/http-api/).
