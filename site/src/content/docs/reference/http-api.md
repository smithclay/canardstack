---
title: HTTP API Reference
description: HTTP routes served by canardstack.
---

All query routes use `Authorization: Bearer ${CANARDSTACK_API_KEY}` when an API
key is configured.

## Operator

| Method | Path | Purpose |
| --- | --- | --- |
| `GET` | `/healthz` | Basic storage readiness. |
| `GET` | `/metrics` | Process-local Prometheus metrics. |
| `GET` | `/api/status/buildinfo` | Build/version metadata. |
| `GET` | `/api/admin/health/storage` | Storage details. |
| `GET` | `/api/admin/health/queries` | Query limits and admission state. |

Admin routes use `CANARDSTACK_ADMIN_API_KEY` when configured.

## Prometheus-Compatible

| Method | Path |
| --- | --- |
| `GET`, `POST` | `/api/v1/query` |
| `GET`, `POST` | `/api/v1/query_range` |
| `GET` | `/api/v1/labels` |
| `GET` | `/api/v1/label/{name}/values` |
| `GET` | `/api/v1/series` |
| `GET` | `/api/v1/metadata` |

## Loki-Compatible

| Method | Path |
| --- | --- |
| `GET` | `/loki/api/v1/query` |
| `GET` | `/loki/api/v1/query_range` |
| `GET` | `/loki/api/v1/labels` |
| `GET` | `/loki/api/v1/label/{name}/values` |
| `GET` | `/loki/api/v1/series` |

## Tempo-Compatible

| Method | Path |
| --- | --- |
| `GET` | `/api/search` |
| `GET` | `/api/search/tags` |
| `GET` | `/api/search/tag/{tag}/values` |
| `GET` | `/api/traces/{traceID}` |
| `GET` | `/api/v2/traces/{traceID}` |

## Not Served

canardstack does not serve OTLP ingest routes. `/v1/logs`, `/v1/traces`, and
`/v1/metrics` should be handled by `duckdb-otlp` or another writer.
