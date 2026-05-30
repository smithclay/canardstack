---
title: DuckDB SQL
description: Query the DuckLake telemetry tables directly with DuckDB.
---

canardstack does not expose SQL through its HTTP API. Use DuckDB, MotherDuck, or
another SQL client when you want direct access to the DuckLake tables.

This path bypasses the compatibility API guards that Grafana uses, so put your
own bounds on time ranges and row counts.

## Attach DuckLake

Use the same catalog and data path that `duckdb-otlp` writes and canardstack
reads:

```bash
duckdb
```

```sql
INSTALL ducklake;
LOAD ducklake;

ATTACH 'ducklake:/path/to/catalog.ducklake' AS canardlake
  (DATA_PATH '/path/to/ducklake-data');
USE canardlake;
```

For MotherDuck-backed DuckLake, attach the `md:` URI directly:

```bash
export MOTHERDUCK_TOKEN='<your-motherduck-token>'
duckdb
```

```sql
ATTACH 'md:test-ducklake' AS canardlake;
USE canardlake;
```

## Query Telemetry

Logs:

```sql
SELECT time_unix_nano, service_name, severity_text, body
FROM otlp_logs
WHERE time_unix_nano >= now() - INTERVAL 1 HOUR
ORDER BY time_unix_nano DESC
LIMIT 100;
```

Traces:

```sql
SELECT start_time_unix_nano, trace_id, span_id, service_name, name,
       duration_time_unix_nano
FROM otlp_traces
WHERE start_time_unix_nano >= now() - INTERVAL 1 HOUR
ORDER BY start_time_unix_nano DESC
LIMIT 100;
```

Gauge metrics:

```sql
SELECT time_unix_nano, name, service_name,
       coalesce(double_value, int_value::DOUBLE) AS value
FROM otlp_metrics_gauge
WHERE time_unix_nano >= now() - INTERVAL 1 HOUR
ORDER BY time_unix_nano DESC
LIMIT 100;
```

Sum metrics:

```sql
SELECT time_unix_nano, name, service_name,
       coalesce(double_value, int_value::DOUBLE) AS value,
       aggregation_temporality, is_monotonic
FROM otlp_metrics_sum
WHERE time_unix_nano >= now() - INTERVAL 1 HOUR
ORDER BY time_unix_nano DESC
LIMIT 100;
```

## Tables

The query service expects these telemetry tables:

| Table | Signal |
| --- | --- |
| `otlp_logs` | OTLP log records |
| `otlp_traces` | OTLP spans |
| `otlp_metrics_gauge` | OTLP gauge datapoints |
| `otlp_metrics_sum` | OTLP sum datapoints |

Resource, scope, log, span, and metric attributes are stored as JSON strings in
their corresponding `*_attributes` columns. Compatibility labels such as
`deployment_environment`, `http_route`, and `http_method` are derived by
canardstack for Grafana. They are not extra physical columns in the raw
telemetry tables.

## Keep SQL bounded

DuckLake keeps the data open to normal DuckDB analysis. That also means a broad
scan can be broad. Prefer queries with:

- an event-time predicate
- a `LIMIT`
- selected columns instead of `SELECT *`
- explicit `service_name`, metric `name`, or `trace_id` filters when available
