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

## Why canardstack?

canardstack is for exploring observability data as lakehouse data: OTLP in,
DuckLake tables out, with the same files queryable through Grafana-compatible
APIs and direct DuckDB SQL. The small single-binary shape keeps local
experiments understandable, while the storage format stays open enough to
inspect outside the service.

The project is intentionally narrow. It is useful when you want direct SQL
access to telemetry, cheap object-storage-backed retention, or a compact place
to test DuckDB and DuckLake observability ideas. It is not trying to replace a
production multi-tenant observability platform today.

## Comparison with other backends

tl;dr - canardstack aspires to be a good fit if you know you want to put ~terabyes of data in an data lake with low operational overhead and decent query performance. Architectually, queries will never be as fast as ClickStack or a large APM vendor.

| | canardstack | [OTel Parquet in S3](https://github.com/smithclay/otlp2parquet) | OTel protobuf in S3 | ClickStack | [Apache Iceberg](https://github.com/smithclay/otlp2pipeline) |
| --- | --- | --- | --- | --- | --- |
| Cost | low | low | low | medium-low | low-ish |
| Operational complexity | low | medium | medium | medium | medium |
| Storage efficiency | good | ok | not great | good | good |
| Query speed | ok | slow | very slow | fast | ok |


## Start Locally

Install and start canardstack with its local DuckLake catalog exposed over
Quack. The app listens on `127.0.0.1:4318`, and DuckDB clients can attach to
the live local catalog on `127.0.0.1:9494` without stopping canardstack.

```bash
cargo install --locked canardstack
CANARDSTACK_DUCKLAKE_QUACK_TOKEN=dev-quack-token canardstack serve --local-catalog
```

In another terminal, send one OTLP/HTTP JSON log:

```bash
OTLP_TIME_UNIX_NANO="$(date +%s)000000000"
curl -sS -X POST http://127.0.0.1:4318/v1/logs \
  -H 'Authorization: Bearer dev-canardstack-key' \
  -H 'Content-Type: application/json' \
  --data "{\"resourceLogs\":[{\"resource\":{\"attributes\":[{\"key\":\"service.name\",\"value\":{\"stringValue\":\"quickstart\"}}]},\"scopeLogs\":[{\"logRecords\":[{\"timeUnixNano\":\"${OTLP_TIME_UNIX_NANO}\",\"body\":{\"stringValue\":\"hello world\"}}]}]}]}"
```

canardstack acknowledges ingest after the raw request is fsynced locally. By
default, the scheduler seals buffered rows within about 10 seconds; wait a
moment, then attach to the live DuckLake catalog from DuckDB 1.5.3 or newer:

```bash
duckdb
```

```sql
INSTALL ducklake;
LOAD ducklake;
INSTALL quack;
LOAD quack;

CREATE OR REPLACE SECRET canardstack_ducklake_quack
  (TYPE quack, SCOPE 'quack:127.0.0.1:9494', TOKEN 'dev-quack-token');

ATTACH 'ducklake:quack:127.0.0.1:9494' AS canardlake;
USE canardlake;

SELECT timestamp, service_name, body
FROM logs
WHERE body = 'hello world'
ORDER BY ingested_at DESC
LIMIT 1;
```

The catalog endpoint is for local, single-process development. Cloud
deployments still recommend the separate `serve-catalog` role so the catalog
can be isolated and scaled independently.

## Schema

canardstack stores four DuckLake telemetry tables: `logs`, `spans`,
`metric_gauge`, and `metric_sum`. The physical columns come from
[`otlp2records`](https://crates.io/crates/otlp2records), with two local storage
columns added by canardstack: `ingested_at` and `source_format`.

Arbitrary OpenTelemetry resource, scope, log, span, and metric attributes are
stored as JSON strings in attribute columns. Grafana-facing labels such as
`deployment_environment`, `http_method`, and `http_route` are derived from those
canonical columns in bounded query and metadata paths instead of being stored as
separate raw-table columns.

:::caution[Schema stability]
The current DuckLake schema is experimental and will evolve with breaking
changes before canardstack is ready for stable production use. Medium-term
schema work is expected to align more closely with the OpenTelemetry Arrow
[`data_model.md`](https://github.com/open-telemetry/otel-arrow/blob/main/docs/data_model.md).
:::

## Deploy

Use the [send telemetry guide](/deployment/send-telemetry/) to configure
OTLP/HTTP producers. Use the [deployment docs](/deployment/) for MotherDuck, GCP
Cloud Run, and AWS ECS/Fargate options.

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
