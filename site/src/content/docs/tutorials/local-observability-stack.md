---
title: Local Observability Stack
description: Learn the duckdb-otlp to canardstack query flow by running it locally.
---

In this tutorial, you will run the local query-only flow with Docker:

```text
OpenTelemetry Demo -> duckdb-otlp OTLP writer -> Quack DuckLake catalog -> canardstack query APIs -> Grafana
```

You will send OpenTelemetry Demo telemetry to `duckdb-otlp`, query it through
canardstack, and open a provisioned Grafana dashboard.

![Grafana dashboard showing OTel demo data queried through canardstack](../../../assets/grafana-dash-demo.png)

## Prerequisites

- Docker Compose
- A local [OpenTelemetry Demo](https://opentelemetry.io/docs/demo/docker-deployment/) checkout

## Start the Stack

From the canardstack repository root:

```bash
docker compose -f compose.yaml -f compose.build.yaml up --build
```

This starts:

- DuckDB serving a DuckLake catalog over Quack
- DuckDB running `duckdb-otlp` OTLP/HTTP ingest on `localhost:4318`
- canardstack on `localhost:9090`
- Grafana on `localhost:3000`

Grafana and canardstack are ready when the containers finish starting.

## Connect the OpenTelemetry Demo

In your OpenTelemetry Demo checkout, add canardstack as an OTLP/HTTP backend in
`src/otel-collector/otelcol-config-extras.yml`:

```yaml
exporters:
  otlp_http/canardstack:
    endpoint: http://host.docker.internal:4318
    headers:
      Authorization: Bearer dev-otlp-token-123456

service:
  pipelines:
    traces:
      exporters: [debug, span_metrics, otlp_http/canardstack]
    metrics:
      exporters: [debug, otlp_http/canardstack]
    logs:
      exporters: [debug, otlp_http/canardstack]
```

Start the full demo with its extras layer:

```bash
docker compose \
  -f compose.yaml \
  -f compose.full.yaml \
  -f compose.extras.yaml \
  up --force-recreate --remove-orphans --detach
```

Open the demo store and let the load generator run:

```text
http://localhost:8080/
```

The demo collector exports OTLP/HTTP to `duckdb-otlp` on `localhost:4318`.

Flush buffered telemetry before opening Grafana:

```bash
docker compose exec ingest sh -c \
  "printf '%s\n' \"SELECT * FROM otlp_flush('otlp:0.0.0.0:4318');\" > /tmp/duckdb-otlp-ingest.sql"
```

## Open the Dashboard

Grafana is provisioned with Prometheus, Loki, and Tempo datasources pointing at
canardstack, plus a `Canardstack OTel Demo` dashboard:

```text
http://localhost:3000/d/canardstack-otel-demo/canardstack-otel-demo
```

The dashboard includes OTel demo activity metrics, service memory, recent logs,
and frontend trace search results.

## Query Directly

You can also call the Loki API directly:

```bash
curl -sS -G http://localhost:9090/loki/api/v1/query_range \
  -H 'Authorization: Bearer dev-canardstack-key' \
  --data-urlencode 'query={}' \
  --data-urlencode "start=$(python3 -c 'import time; print(int(time.time()) - 900)')" \
  --data-urlencode "end=$(python3 -c 'import time; print(int(time.time()))')" \
  --data-urlencode 'limit=10'
```

You should receive a Loki success response once demo logs have been flushed
into DuckLake.

## Optional Log Smoke

Post the sample logs from a sibling `../duckdb-otlp` checkout:

```bash
curl -sS -X POST http://localhost:4318/v1/logs \
  -H 'Authorization: Bearer dev-otlp-token-123456' \
  -H 'Content-Type: application/x-ndjson' \
  --data-binary @../duckdb-otlp/test/data/logs_simple.jsonl
```

For the small sample payload, force the buffered writer to commit before
querying:

```bash
docker compose exec ingest sh -c \
  "printf '%s\n' \"SELECT * FROM otlp_flush('otlp:0.0.0.0:4318');\" > /tmp/duckdb-otlp-ingest.sql"
```

The rows are now visible in the DuckLake catalog.

## Next

- [Serve an existing DuckLake catalog](/how-to/serve-ducklake/) shows the
  smallest server command.
- [Connect Grafana](/how-to/connect-grafana/) shows the datasource settings.
- [Storage schema reference](/reference/storage-schema/) lists the table
  contract.
