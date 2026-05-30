---
title: Query with Grafana
description: Connect Grafana to canardstack metrics, logs, and traces.
---

canardstack speaks enough Prometheus, Loki, and Tempo for Grafana to inspect
stored telemetry.

## Add Datasources

Use these settings for each datasource:

| Signal | Datasource type | URL |
| --- | --- | --- |
| Metrics | Prometheus | `http://localhost:4318` |
| Logs | Loki | `http://localhost:4318` |
| Traces | Tempo | `http://localhost:4318` |

Each datasource must send:

```text
Authorization: Bearer dev-canardstack-key
```

For Tempo trace-to-logs, point Tempo at the canardstack Loki datasource and
filter by trace ID.

## Smoke Checks

After data is visible in DuckLake, these should return successful JSON
responses:

```bash
curl -sS -H 'Authorization: Bearer dev-canardstack-key' \
  http://127.0.0.1:4318/api/v1/labels

curl -sS -H 'Authorization: Bearer dev-canardstack-key' \
  http://127.0.0.1:4318/loki/api/v1/label/service_name/values

curl -sS -H 'Authorization: Bearer dev-canardstack-key' \
  http://127.0.0.1:4318/api/search/tags
```

## Keep Panels Simple

The query APIs are compatibility subsets. Use explicit selectors, short time
ranges, and panel limits. canardstack is not a full PromQL, LogQL, TraceQL,
Prometheus, Loki, or Tempo implementation.
