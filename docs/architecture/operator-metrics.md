# Operator Metrics

## Metric Naming

The canardstack process exposes Prometheus-text metrics at `GET /metrics`. The names below are the implemented contract — only metrics that are actually emitted are listed here. Histograms are rendered as a counter pair (`*_count` / `*_sum`) rather than full HDR-style buckets.

Labels stay low-cardinality:

- `signal`: `logs`, `spans`, `metric_gauge`, `metric_sum`.
- `table`: `logs`, `spans`, `metric_gauge`, `metric_sum`, or `all`.
- `status`: HTTP status code or grouped class.
- `reason`: bounded rejection or failure reason.
- `job`: maintenance job name (`watchdog`, `flush`, `retention`).
- `query_class`: route path (e.g. `/api/v1/query_range`).
- `encoding`: `identity`, `gzip`.
- `triggered_by`: who initiated a partial-commit flush.

Do not label metrics by `service_name`, trace id, query text, API key, or arbitrary attributes.

## Ingest Metrics

| Metric | Type | Labels | Purpose |
| --- | --- | --- | --- |
| `canardstack_ingest_requests_total` | Counter | `signal`, `status`, `reason` | Request outcomes. |
| `canardstack_ingest_request_bytes_total` | Counter | `signal`, `encoding` | Compressed request bytes accepted. |
| `canardstack_ingest_records_total` | Counter | `signal` | Records accepted into the in-process queue. |
| `canardstack_ingest_queue_rows` | Gauge | `signal` | Current queued records. |
| `canardstack_ingest_queue_bytes` | Gauge | `signal` | Current queued bytes (approximate). |
| `canardstack_ingest_queue_oldest_age_seconds` | Gauge | `signal` | Oldest queued record age. |
| `canardstack_ingest_rejections_total` | Counter | `signal`, `status`, `reason` | Admission-control rejections (subset of `_ingest_requests_total`). |
| `canardstack_ingest_flush_attempted_bytes_total` | Counter | `signal` | Approximate queued Arrow bytes selected for flush attempts. |
| `canardstack_ingest_partial_commit_rows_total` | Counter | `signal`, `triggered_by` | Rows durably committed before a mid-batch flush failure; surfaces best-effort durability. |

## HTTP Metrics

| Metric | Type | Labels | Purpose |
| --- | --- | --- | --- |
| `canardstack_http_connection_errors_total` | Counter | `reason` | Per-connection failures: `max_connections_exceeded`, `socket_timeout`, `connection_reset`, `io_error`. |

## Storage Metrics

| Metric | Type | Labels | Purpose |
| --- | --- | --- | --- |
| `canardstack_storage_logical_rows` | Gauge | `table` | Row count per table from DuckDB. |
| `canardstack_storage_physical_bytes` | Gauge | `table=all` | Local storage directory size on disk. |
| `canardstack_ducklake_parquet_files` | Gauge | `table` | Active DuckLake Parquet data files per table. |
| `canardstack_ducklake_parquet_rows` | Gauge | `table` | Active rows stored in DuckLake Parquet data files per table. |
| `canardstack_ducklake_inlined_rows` | Gauge | `table` | Rows currently held in DuckLake inlined-data tables per table. |
| `canardstack_ducklake_flush_inlined_duration_seconds` | Histogram (`_count` / `_sum`) | `table` | Time spent in DuckLake inlined-data flush during maintenance. |
| `canardstack_ducklake_compaction_duration_seconds` | Histogram (`_count` / `_sum`) | `table` | Time spent in DuckLake adjacent-file compaction during maintenance. |

## Query Metrics

| Metric | Type | Labels | Purpose |
| --- | --- | --- | --- |
| `canardstack_query_requests_total` | Counter | `query_class`, `status`, `reason` | Query outcomes. |
| `canardstack_query_duration_seconds` | Histogram (`_count` / `_sum`) | `query_class` | User-visible latency. |
| `canardstack_query_rejections_total` | Counter | `query_class`, `reason` | Concurrency / shape rejections. |
| `canardstack_query_timeouts_total` | Counter | `query_class` | Timeout enforcement. |

## Maintenance Metrics

| Metric | Type | Labels | Purpose |
| --- | --- | --- | --- |
| `canardstack_maintenance_runs_total` | Counter | `job`, `status`, `reason` | Job outcomes (`status=ok` or `status=error`). |
| `canardstack_maintenance_duration_seconds` | Histogram (`_count` / `_sum`) | `job`, `table=all` | Job runtime. |
| `canardstack_maintenance_failures_total` | Counter | `job`, `reason` | Failures only, broken out by classified reason. Bounded reason set: `disk_full`, `flush_failed`, `retention_failed`, `scheduler_job_failed`. Reasons derive from the job name where possible (so dependency wording changes do not silently re-route alerts); only `disk_full` substring-matches OS / DuckDB errors. |
| `canardstack_maintenance_consecutive_failures` | Gauge | `job` | Consecutive failure count; resets to 0 on success. Drives exponential backoff. |
| `canardstack_maintenance_paused` | Gauge | none | `1` when paused. |

## Freshness Metrics

| Metric | Type | Labels | Purpose |
| --- | --- | --- | --- |
| `canardstack_freshness_watermark_timestamp` | Gauge | `table` | Newest query-visible event time (epoch seconds). |
| `canardstack_ingest_to_query_lag_seconds` | Gauge | `table` | Now minus visible watermark, clamped at 0. |

## Initial Alerts

| Alert | Condition | Severity |
| --- | --- | --- |
| Ingest rejecting sustained load | `rate(canardstack_ingest_requests_total{status=~"429\|503"}[5m]) / rate(canardstack_ingest_requests_total[5m]) > 0.05` for 10 minutes | Warning |
| Unsafe ingest rejection | Any `canardstack_ingest_requests_total{status="503"}` increase for 5 minutes | Critical |
| Maintenance failing repeatedly | `canardstack_maintenance_consecutive_failures > 3` for any job | Warning |
| Query timeouts spiking | `rate(canardstack_query_timeouts_total[5m]) > 0` | Warning |
| Connection cap saturated | `rate(canardstack_http_connection_errors_total{reason="max_connections_exceeded"}[5m]) > 0` | Warning |
| Partial-commit durability gap | `rate(canardstack_ingest_partial_commit_rows_total[5m]) > 0` | Warning |

## Not Currently Emitted

The following metrics from earlier design drafts are **not** emitted by the current implementation. They are listed here so dashboards and runbooks don't silently depend on them:

- `canardstack_ingest_decode_seconds`
- `canardstack_ducklake_insert_seconds`, `_commit_seconds`, `_inlined_bytes`, `_oldest_inlined_age_seconds`, `_snapshot_count`, `_flush_failures_total`
- `canardstack_storage_logical_bytes` (the implementation emits `_logical_rows` instead)
- `canardstack_object_store_errors_total`, `_request_seconds`
- `canardstack_query_active`, `_memory_high_water_bytes`, `_oom_total`
- `canardstack_maintenance_last_success_timestamp`, `_backlog_bytes`
- `canardstack_cleanup_deleted_files_total`, `_deleted_bytes_total`
- `canardstack_retention_oldest_retained_date`
- `canardstack_late_records_total`, `_rejected_skewed_records_total`
