---
title: Local Observability Stack
description: Learn the duckdb-otlp to canardstack query flow by running it locally.
---

In this tutorial, you will run the local query-only flow with Docker:

```text
duckdb-otlp OTLP writer -> Quack DuckLake catalog -> canardstack query APIs
```

You will send sample logs to `duckdb-otlp`, flush them to DuckLake, and query
them back through canardstack.

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

Grafana and canardstack are ready when the containers finish starting.

## Send Logs

Post the sample logs from a sibling `../duckdb-otlp` checkout:

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

The rows are now visible in the DuckLake catalog.

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

You should receive a Loki success response containing streams for
`test-service`.

## Next

- [Serve an existing DuckLake catalog](/how-to/serve-ducklake/) shows the
  smallest server command.
- [Connect Grafana](/how-to/connect-grafana/) shows the datasource settings.
- [Storage schema reference](/reference/storage-schema/) lists the table
  contract.
