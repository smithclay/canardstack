---
title: Write with duckdb-otlp
description: Populate DuckLake tables for canardstack to query.
---

Use `duckdb-otlp` as the OTLP writer. canardstack only reads the resulting
DuckLake catalog.

## Start a Local Writer

```bash
../duckdb-otlp/build/release/duckdb -unsigned /tmp/canardstack-e2e/writer.duckdb
```

```sql
LOAD '../duckdb-otlp/build/release/extension/otlp/otlp.duckdb_extension';
INSTALL ducklake;
LOAD ducklake;

ATTACH 'ducklake:/tmp/canardstack-e2e/metadata.ducklake' AS lake
  (DATA_PATH '/tmp/canardstack-e2e/data/');

SELECT *
FROM otlp_serve(
  'otlp:localhost:4318',
  catalog := 'lake',
  token := 'dev-token-123456'
);
```

Send OTLP/HTTP logs, traces, and metrics to the listener that `otlp_serve`
returns. For local smoke data:

```bash
curl -sS -X POST http://localhost:4318/v1/logs \
  -H 'Authorization: Bearer dev-token-123456' \
  -H 'Content-Type: application/x-ndjson' \
  --data-binary @../duckdb-otlp/test/data/logs_simple.jsonl
```

Flush before starting a second local DuckDB process against the same file-backed
catalog:

```sql
SELECT * FROM otlp_flush('otlp:localhost:4318');
SELECT * FROM otlp_stop('otlp:localhost:4318');
```

Local DuckDB metadata files are exclusively locked by the process that has them
open. Remote or service-backed catalogs can keep writer and query processes
separate.

## Validate Rows

```sql
SELECT count(*) FROM lake.main.otlp_logs;
```

Then start canardstack with the same catalog and data path.
