---
title: Query with DuckDB SQL
description: Query the DuckLake telemetry tables directly with DuckDB.
---

canardstack does not expose SQL through its HTTP API. Use DuckDB, MotherDuck, or
another SQL client when you want direct access to the DuckLake tables.

## Attach DuckLake

```sql
INSTALL ducklake;
LOAD ducklake;

ATTACH 'ducklake:/path/to/catalog.ducklake' AS canardlake
  (DATA_PATH '/path/to/ducklake-data');
USE canardlake;
```

For MotherDuck-backed DuckLake:

```bash
export MOTHERDUCK_TOKEN='<your-motherduck-token>'
duckdb
```

```sql
ATTACH 'md:test-ducklake' AS canardlake;
USE canardlake;
```

## Query Logs

```sql
SELECT time_unix_nano, service_name, severity_text, body
FROM otlp_logs
WHERE time_unix_nano >= now() - INTERVAL 1 HOUR
ORDER BY time_unix_nano DESC
LIMIT 100;
```

## Query Traces

```sql
SELECT start_time_unix_nano, trace_id, span_id, service_name, name,
       duration_time_unix_nano
FROM otlp_traces
WHERE start_time_unix_nano >= now() - INTERVAL 1 HOUR
ORDER BY start_time_unix_nano DESC
LIMIT 100;
```

## Query Metrics

```sql
SELECT time_unix_nano, name, service_name,
       coalesce(double_value, int_value::DOUBLE) AS value
FROM otlp_metrics_gauge
WHERE time_unix_nano >= now() - INTERVAL 1 HOUR
ORDER BY time_unix_nano DESC
LIMIT 100;
```

Keep direct SQL bounded with event-time predicates, `LIMIT`, and selected
columns.
