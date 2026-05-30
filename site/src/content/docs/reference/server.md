---
title: Server Contract
description: Runtime contract for the canardstack query server.
---

canardstack is a query-only process.

## Process

- one Rust binary: `canardstack`
- synchronous standard-library HTTP server
- no async runtime
- no OTLP ingest endpoint
- no gRPC endpoint
- no bundled DuckLake catalog service

## Required Storage

Set a DuckLake attach target:

```bash
CANARDSTACK_DUCKLAKE_ATTACH_URI=ducklake:/path/to/catalog.ducklake
CANARDSTACK_DUCKLAKE_DATA_PATH=/path/to/ducklake-data
```

The catalog must contain the `otlp_*` tables in the
[schema reference](/reference/schema/).

## Auth

- Query routes use `CANARDSTACK_API_KEY`.
- Admin health routes use `CANARDSTACK_ADMIN_API_KEY`.
- If a key is unset, that route group is unauthenticated.

## Errors

Prometheus and Loki-compatible routes return their protocol error envelope:

```json
{"status":"error","errorType":"...","error":"..."}
```
