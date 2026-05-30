# Storage Schema

canardstack queries DuckLake tables with the OTAP-style columns used by
`duckdb-otlp` and the existing compatibility adapters:

- `otlp_logs`
- `otlp_traces`
- `otlp_metrics_gauge`
- `otlp_metrics_sum`
- `metadata_summary`

The query server creates the tables for local empty catalogs so development
startup is straightforward, and it stores a small `canardstack_meta` table with
schema compatibility metadata. Existing catalogs outside the supported schema
range fail closed at startup.

Telemetry writes are owned by the DuckDB writer process, not by canardstack.
When the writer changes table columns or partitioning, bump the schema version
and update the query adapters in the same change.
