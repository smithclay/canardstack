---
title: Connect Grafana
description: Connect Grafana to canardstack metrics, logs, and traces.
---

This guide shows you how to connect Grafana datasources to canardstack for
stored metrics, logs, and traces.

## Add Datasources

Use these settings for each datasource:

| Signal | Datasource type | URL |
| --- | --- | --- |
| Metrics | Prometheus | `http://localhost:9090` |
| Logs | Loki | `http://localhost:9090` |
| Traces | Tempo | `http://localhost:9090` |

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
  http://127.0.0.1:9090/api/v1/labels

curl -sS -H 'Authorization: Bearer dev-canardstack-key' \
  http://127.0.0.1:9090/loki/api/v1/label/service_name/values

curl -sS -H 'Authorization: Bearer dev-canardstack-key' \
  http://127.0.0.1:9090/api/search/tags
```

## Keep Panels Simple

The query APIs are compatibility subsets. Use explicit selectors, short time
ranges, and panel limits. canardstack is not a full PromQL, LogQL, TraceQL,
Prometheus, Loki, or Tempo implementation.
