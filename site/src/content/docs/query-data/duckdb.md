---
title: DuckDB SQL
description: Query canardstack DuckLake tables directly with DuckDB.
---

canardstack does not expose SQL through its HTTP API. Use DuckDB, MotherDuck, or
another SQL client when you want direct access to the stored DuckLake tables.

This path is for operators and analysis. It bypasses the compatibility API
guards that Grafana uses, so put your own bounds on time ranges and row counts.

## Local DuckLake

When canardstack runs on the host with default settings, it stores the local
DuckLake catalog and data files under `.canardstack`.

Open DuckDB from the repository root:

```bash
duckdb
```

Load DuckLake and attach the catalog:

```sql
INSTALL ducklake;
LOAD ducklake;

ATTACH 'ducklake:.canardstack/canardstack.ducklake' AS canardlake
  (DATA_PATH '.canardstack/storage');
USE canardlake;
```

Then query the physical telemetry tables:

```sql
SELECT timestamp, service_name, severity_text, body
FROM logs
WHERE timestamp >= TIMESTAMP '2026-05-25 00:00:00'
ORDER BY timestamp DESC
LIMIT 100;
```

```sql
SELECT timestamp, trace_id, span_id, service_name, span_name, duration
FROM spans
WHERE timestamp >= now() - INTERVAL 1 HOUR
ORDER BY timestamp DESC
LIMIT 100;
```

```sql
SELECT timestamp, metric_name, service_name, value
FROM metric_gauge
WHERE timestamp >= now() - INTERVAL 1 HOUR
ORDER BY timestamp DESC
LIMIT 100;
```

```sql
SELECT timestamp, metric_name, service_name, value, aggregation_temporality,
       is_monotonic
FROM metric_sum
WHERE timestamp >= now() - INTERVAL 1 HOUR
ORDER BY timestamp DESC
LIMIT 100;
```

## Tables

The v0 telemetry tables are:

| Table | Signal |
| --- | --- |
| `logs` | OTLP log records |
| `spans` | OTLP spans |
| `metric_gauge` | OTLP gauge datapoints |
| `metric_sum` | OTLP sum datapoints |

Each table has an event-time `timestamp`, storage-added `ingested_at`, and
`source_format`. Resource, scope, log, span, and metric attributes are stored as
JSON strings in their corresponding `*_attributes` columns.

Compatibility labels such as `deployment_environment`, `http_route`, and
`http_method` are derived by canardstack for Grafana. They are not physical
columns in the raw telemetry tables.

## Metadata summary

Grafana discovery endpoints use the `metadata_summary` table. It is useful when
you want to see what labels, tags, services, and metrics canardstack has
summarized:

```sql
SELECT signal, kind, name, value, row_count, first_seen, last_seen
FROM metadata_summary
ORDER BY last_seen DESC
LIMIT 100;
```

## MotherDuck

For a MotherDuck-backed DuckLake, attach the `md:` URI directly:

```bash
export MOTHERDUCK_TOKEN='<your-motherduck-token>'
duckdb
```

```sql
ATTACH 'md:test-ducklake' AS canardlake;
USE canardlake;

SELECT timestamp, body
FROM logs
ORDER BY timestamp DESC
LIMIT 20;
```

Use the same `md:` value that canardstack uses in
`CANARDSTACK_DUCKLAKE_ATTACH_URI`.

## Docker Compose data

The default Compose stack stores canardstack data in the named Docker volume
`canardstack-data`. For direct SQL, the simpler path is to run canardstack on
the host and query `.canardstack`, or use MotherDuck. If you need to inspect a
Compose volume, mount or copy it first, then attach the DuckLake catalog with the
matching `DATA_PATH`.

## Keep SQL bounded

DuckLake keeps the data open to normal DuckDB analysis. That also means a broad
scan can be broad. Prefer queries with:

- a `timestamp` predicate
- a `LIMIT`
- selected columns instead of `SELECT *`
- explicit `service_name`, `metric_name`, or `trace_id` filters when available
