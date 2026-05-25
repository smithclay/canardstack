---
title: canardstack
description: OpenTelemetry metrics, logs, and traces stored in DuckLake.
hero:
  tagline: OpenTelemetry metrics, logs, and traces stored in DuckLake.
  image:
    file: ../../assets/canardstack.png
  actions:
    - text: Get Started
      link: '#start-locally'
      icon: right-arrow
    - text: View on GitHub
      link: https://github.com/smithclay/canardstack
      icon: external
      variant: secondary
---

canardstack is an experimental, single-binary observability backend for storing
OpenTelemetry logs, traces, gauge metrics, and sum metrics in DuckLake.

It accepts OTLP/HTTP, normalizes telemetry into Arrow batches, stores immutable
Parquet-backed DuckLake segments, and exposes bounded Prometheus, Loki, and
Tempo compatibility APIs for Grafana-style clients.

## What This Is

- A single Rust process with synchronous std-library HTTP serving.
- Local DuckLake storage by default, with remote DuckLake attach support.
- A focused compatibility surface for inspecting telemetry through existing
  Prometheus, Loki, Tempo, and DuckDB-compatible tools.

## What This Is Not

- A full observability suite.
- A multi-tenant production service.
- A full Prometheus, Loki, Tempo, PromQL, LogQL, or TraceQL implementation.

## Start Locally

Install and start canardstack. With no options, it uses local DuckLake storage
under `.canardstack` and listens on `127.0.0.1:4318`.

```bash
cargo install --locked canardstack
canardstack
```

In another terminal, send one OTLP/HTTP JSON log:

```bash
curl -sS -X POST http://127.0.0.1:4318/v1/logs \
  -H 'Authorization: Bearer dev-canardstack-key' \
  -H 'Content-Type: application/json' \
  --data '{"resourceLogs":[{"scopeLogs":[{"logRecords":[{"timeUnixNano":"1779667200000000000","body":{"stringValue":"hello world"}}]}]}]}'
```

Give the scheduler a moment to seal the row into DuckLake, then query it
directly from the DuckDB CLI:

```bash
sleep 2
duckdb
```

```sql
INSTALL ducklake;
LOAD ducklake;
ATTACH 'ducklake:.canardstack/canardstack.ducklake' AS canardlake
  (DATA_PATH '.canardstack/storage');
USE canardlake;

SELECT timestamp, body
FROM logs
WHERE body = 'hello world'
ORDER BY ingested_at DESC
LIMIT 1;
```

## Demo

### Start The Stack

Start canardstack and Grafana with local DuckLake storage:

```bash
docker compose up --build
```

The default local stack exposes:

- canardstack: `http://localhost:4318`
- Grafana: `http://localhost:3000`

### Send Sample Telemetry

In another terminal, run the smoke client:

```bash
docker compose run --rm smoke
```

The smoke workload sends logs, a multi-span trace, gauge samples, and
cumulative sum samples through OTLP/HTTP. It also checks storage health and the
Prometheus, Loki, and Tempo-compatible query paths.

### Open Grafana

Open the provisioned dashboard:

```text
http://localhost:3000/d/canardstack-overview/canardstack-overview
```

Use `admin/admin` if Grafana asks for credentials.

## Send Your Own OTLP/HTTP Data

Point an OpenTelemetry Collector `otlphttp` exporter at canardstack:

```yaml
exporters:
  otlphttp/canardstack:
    endpoint: http://localhost:4318
    headers:
      Authorization: Bearer dev-canardstack-key
```

canardstack accepts the standard OTLP/HTTP paths:

- `POST /v1/logs`
- `POST /v1/traces`
- `POST /v1/metrics`

## Deploy

Use the [deployment docs](/deployment/) for MotherDuck, GCP Cloud Run, and AWS
ECS/Fargate options.

## Query Data

Use the [Grafana datasource guide](/query-data/grafana/) to connect metrics,
logs, and traces. Use the [DuckDB SQL guide](/query-data/duckdb/) to query the
DuckLake tables directly.

## Build From Source

For host development:

```bash
cargo check
cargo test
cargo run
```

The host server defaults to `127.0.0.1:4318`.
