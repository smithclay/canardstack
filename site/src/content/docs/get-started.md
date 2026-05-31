---
title: Get Started
description: Run the local duckdb-otlp to canardstack query flow.
---

This tutorial validates the query-only shape on your machine with Docker:

```text
duckdb-otlp OTLP writer -> Quack DuckLake catalog -> canardstack query APIs
```

## Prerequisites

- Docker Compose

## Start the Stack

From the canardstack repository root:

```bash
docker compose -f compose.yaml -f compose.build.yaml up --build
```

This starts:

- DuckDB serving a DuckLake catalog over Quack
- DuckDB running `duckdb-otlp` ingest on `localhost:4319`
- canardstack on `localhost:4318`
- Grafana on `localhost:3000`

## Send Logs

Use any OTLP/HTTP exporter, or post the sample logs from a sibling
`../duckdb-otlp` checkout:

```bash
curl -sS -X POST http://localhost:4319/v1/logs \
  -H 'Authorization: Bearer dev-otlp-token-123456' \
  -H 'Content-Type: application/x-ndjson' \
  --data-binary @../duckdb-otlp/test/data/logs_simple.jsonl
```

For the small sample payload, force the buffered writer to commit before
querying:

```bash
docker compose exec ingest sh -c \
  "printf '%s\n' \"SELECT * FROM otlp_flush('otlp:0.0.0.0:4319');\" > /tmp/duckdb-otlp-ingest.sql"
```

## Query Logs

Grafana is provisioned with Prometheus, Loki, and Tempo datasources pointing at
canardstack. You can also call the Loki API directly:

```bash
curl -sS -G http://localhost:4318/loki/api/v1/query_range \
  -H 'Authorization: Bearer dev-canardstack-key' \
  --data-urlencode 'query={service_name="test-service"}' \
  --data-urlencode 'start=1640000000000000000' \
  --data-urlencode 'end=1640000030000000000' \
  --data-urlencode 'limit=10'
```

For the local single-process smoke against a sibling `../duckdb-otlp` checkout,
run `scripts/e2e-duckdb-otlp-local.py`.

## Next

- [Serve DuckLake](/quickstart/serve/) shows the smallest server command.
- [Query with Grafana](/guides/query-with-grafana/) connects Grafana datasources.
- [Schema Reference](/reference/schema/) lists the table contract.
