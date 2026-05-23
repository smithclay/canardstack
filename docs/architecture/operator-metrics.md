# Operator Metrics

## Metric Naming

The canardstack process exposes Prometheus-text metrics at `GET /metrics`. The
scrape path records only cheap in-process gauges. Storage layout, row-count,
physical-byte, and freshness-watermark gauges are refreshed by the scheduler's
metrics-snapshot job, then rendered from the metric store. The scheduler also
writes a snapshot of the current samples every derived metrics-snapshot
maintenance cadence with `service_name="canardstack"`:
counters land in `metric_sum`, and gauges land in `metric_gauge`. Grafana can
query canardstack's own monitoring data through the normal Prometheus-compatible
datastore path. The names below are the implemented contract: only metrics that
are actually emitted are listed here. Histograms are rendered as a counter pair
(`*_count` / `*_sum`) rather than full HDR-style buckets.

Labels stay low-cardinality:

- `request_kind`: `logs`, `traces`, `metrics`, or `all`.
- `storage_signal`: `logs`, `spans`, `metric_gauge`, `metric_sum`.
- `spool_lane`: `logs`, `traces`, `metrics`, or `all`.
- `table`: `logs`, `spans`, `metric_gauge`, `metric_sum`, or `all`.
- `status`: HTTP status code or grouped class.
- `reason`: bounded rejection or failure reason.
- `job`: maintenance job name (`seal`, `metadata_refresh`, `metrics_snapshot`, `retention`).
- `route_template`: static query route template (e.g. `/api/v1/query_range` or `/api/v2/traces/:trace_id`).
- `encoding`: `identity`, `gzip`.
- `admission`: `seal`, `query_cheap`, `query_heavy`, `query`, or `freshness_budget`.

Do not label metrics by `service_name`, trace id, query text, API key, or arbitrary attributes.

## Ingest Metrics

| Metric | Type | Labels | Purpose |
| --- | --- | --- | --- |
| `canardstack_ingest_requests_total` | Counter | `request_kind`, `status`, `reason` | Request outcomes. |
| `canardstack_ingest_request_bytes_total` | Counter | `request_kind`, `encoding` | Compressed request bytes accepted. |
| `canardstack_raw_spool_records_total` | Counter | `spool_lane`, `status` | Raw request spool outcomes: `spooled`, `full`, `queue_full`, or `error`. `spooled` means written and fsynced to the local raw-spool file. |
| `canardstack_raw_spool_bytes_total` | Counter | `spool_lane` | Compressed raw request bytes written into the local spool. |
| `canardstack_raw_spool_append_batches_total` | Counter | `spool_lane` | Raw-spool append batches written by the writer. |
| `canardstack_raw_spool_append_batch_records_total` | Counter | `spool_lane` | Raw-spool records included in append batches. |
| `canardstack_raw_spool_append_batch_encoded_bytes_total` | Counter | `spool_lane` | Encoded raw-spool bytes included in append batches. |
| `canardstack_raw_spool_append_syncs_total` | Counter | `spool_lane` | Successful raw-spool append sync cycles. |
| `canardstack_raw_spool_append_file_fsyncs_total` | Counter | `spool_lane` | Segment file fsync calls performed by append sync cycles. |
| `canardstack_raw_spool_append_sync_failures_total` | Counter | `spool_lane` | Failed raw-spool append sync cycles. Any increase should make the raw spool unhealthy and subsequent ingest return `503`. |
| `canardstack_raw_spool_append_batch_records` | Gauge | `spool_lane`, `stat` | Last and max records per raw-spool append batch. |
| `canardstack_raw_spool_append_batch_encoded_bytes` | Gauge | `spool_lane`, `stat` | Last and max encoded bytes per raw-spool append batch. |
| `canardstack_raw_spool_replayed_records_total` | Counter | `request_kind`, `spool_lane`, `status` | Startup replay attempts and outcomes for uncheckpointed raw-spool records. |
| `canardstack_raw_spool_checkpointed_records_total` | Counter | `request_kind`, `reason` | Raw-spool records made reclaimable after terminal rejection or DuckLake storage commit. |
| `canardstack_raw_spool_pending_records` | Gauge | optional `spool_lane` | Uncheckpointed raw-spool records currently pending replay or storage commit. |
| `canardstack_raw_spool_pending_bytes` | Gauge | optional `spool_lane` | Compressed bytes for uncheckpointed raw-spool records. |
| `canardstack_raw_spool_unsynced_records` | Gauge | optional `spool_lane` | Written raw-spool records not yet covered by a successful append sync. |
| `canardstack_raw_spool_unsynced_bytes` | Gauge | optional `spool_lane` | Encoded raw-spool bytes not yet covered by a successful append sync. |
| `canardstack_raw_spool_unsynced_age_seconds` | Gauge | optional `spool_lane` | Age of the oldest unsynced append data, or `0` when fully synced. |
| `canardstack_raw_spool_healthy` | Gauge | optional `spool_lane` | `1` when the writer is accepting appends; `0` after a fatal append sync failure. |
| `canardstack_raw_spool_segment_bytes` | Gauge | optional `spool_lane` | Total raw-spool segment bytes on disk. |
| `canardstack_raw_spool_segments` | Gauge | optional `spool_lane` | Raw-spool segment file count. |
| `canardstack_ingest_records_total` | Counter | `request_kind` | Records accepted into the immutable buffer. |
| `canardstack_ingest_transformed_rows_total` | Counter | `storage_signal`, `request_kind` | Rows produced by worker-side `otlp2records` transform. |
| `canardstack_ingest_unsupported_histograms_total` | Counter | `request_kind` | Histogram datapoints observed and dropped by the v0 metrics transformer. Emitted only when nonzero. |
| `canardstack_ingest_buffered_rows_total` | Counter | `storage_signal` | Rows appended to the storage immutable buffer. |
| `canardstack_ingest_buffered_bytes_total` | Counter | `storage_signal` | Approximate Arrow bytes appended to the storage immutable buffer. |
| `canardstack_ingest_inflight_bytes` | Gauge | `storage_signal` | Bytes admitted (spooled, handed to a worker) but not yet appended to the immutable buffer. |
| `canardstack_ingest_inflight_capacity_bytes` | Gauge | `storage_signal` | Per-storage-signal in-flight ceiling. |
| `canardstack_ingest_inflight_pressure` | Gauge | `storage_signal` | In-flight bytes as a fraction of the per-storage-signal ceiling (`0..1`). |
| `canardstack_ingest_worker_queue_capacity` | Gauge | `state=capacity` | Configured bounded worker channel capacity. |
| `canardstack_ingest_storage_insert_total` | Counter | `request_kind`, `status` | Worker appends of Arrow batches into the immutable buffer. |
| `canardstack_ingest_worker_completed_total` | Counter | `request_kind`, `status` | Ingest worker tasks completed, by outcome. |
| `canardstack_ingest_rejections_total` | Counter | `request_kind`, `status`, `reason` | Admission-control rejections (subset of `_ingest_requests_total`). |
| `canardstack_ingest_freshness_budget_rejections_total` | Counter | none | Requests rejected before raw-spool append because projected visibility exceeded the freshness-budget SLA. |
| `canardstack_immutable_segments_sealed_rows_total` | Counter | `storage_signal` | Rows sealed into immutable Parquet segments. |
| `canardstack_immutable_segments_sealed_files_total` | Counter | `storage_signal` | Immutable Parquet files written and registered with DuckLake. |

## HTTP Metrics

| Metric | Type | Labels | Purpose |
| --- | --- | --- | --- |
| `canardstack_http_connection_errors_total` | Counter | `reason` | Per-connection failures: `max_connections_exceeded`, `socket_timeout`, `connection_reset`, `io_error`. |

## Storage Metrics

These gauges are scheduler-maintained snapshots, not fresh `/metrics` scrape
scans.

| Metric | Type | Labels | Purpose |
| --- | --- | --- | --- |
| `canardstack_storage_logical_rows` | Gauge | `table` | Row count per table from DuckDB. |
| `canardstack_storage_physical_bytes` | Gauge | `table=all` | Local storage directory size on disk. |
| `canardstack_ducklake_parquet_files` | Gauge | `table` | Active DuckLake Parquet data files per table. |
| `canardstack_ducklake_parquet_rows` | Gauge | `table` | Active rows stored in DuckLake Parquet data files per table. |

The shared phase metric `canardstack_phase_duration_seconds` also records
storage proof phases with `storage_signal` and `phase` labels:
`storage_prepare`, `storage_buffer`, `storage_partition_split`,
`storage_parquet_encode`, `storage_file_write`, `storage_file_fsync`,
`storage_file_rename`, `storage_ducklake_register`, and
`storage_ducklake_commit`.

`/api/admin/health/ingest` returns queue snapshots, raw-spool stats
(`segment_count`, `segment_bytes`, `pending_records`, `pending_bytes`,
`unsynced_records`, `unsynced_bytes`, `unsynced_age_seconds`, `healthy`, and
`error` when unhealthy), and the active raw-spool group-commit and append-sync
settings. It also returns the current admission snapshot. Operators can diagnose
replay backlog, unsynced append exposure, queue pressure, freshness-budget
projection, and admission pressure without arbitrary SQL.

The shared phase metric `canardstack_phase_duration_seconds` splits
request-visible `raw_spool_append` latency from raw-spool writer internals with
`spool_lane` and `phase` labels:
`raw_spool_append_batch_wait` is time spent collecting a group-commit batch,
`raw_spool_append_write` is file write time, and `raw_spool_append_fsync` is
append sync time. `raw_spool_append_fsync` is part of `202` latency because
accepted requests are fsynced before acknowledgement.

## Query Metrics

| Metric | Type | Labels | Purpose |
| --- | --- | --- | --- |
| `canardstack_query_requests_total` | Counter | `route_template`, `status`, `reason` | Query outcomes. |
| `canardstack_query_duration_seconds` | Histogram (`_count` / `_sum`) | `route_template` | User-visible latency. |
| `canardstack_query_rejections_total` | Counter | `route_template`, `reason` | Concurrency / shape rejections. |
| `canardstack_query_timeouts_total` | Counter | `route_template` | Timeout enforcement. |
| `canardstack_query_admission_reductions_total` | Counter | none | Heavy query admissions that ran at the degraded capacity because freshness debt was elevated. |
| `canardstack_query_admission_rejections_total` | Counter | none | Query admission rejections from cheap-query saturation, heavy-query saturation, or freshness debt. |

## Admission Metrics

| Metric | Type | Labels | Purpose |
| --- | --- | --- | --- |
| `canardstack_admission_capacity` | Gauge | `admission` | Current admission capacity. Heavy query capacity reports the effective capacity after freshness degradation. |
| `canardstack_admission_in_use` | Gauge | `admission` | Current admission occupancy. |
| `canardstack_admission_rejections_total` | Counter | `admission`, `reason` | Rejections at the admission controller. |
| `canardstack_seal_ewma_bytes_per_second` | Gauge | none | EWMA queue-byte seal throughput used for freshness-budget admission. |
| `canardstack_projected_seal_seconds` | Gauge | none | Queue byte debt divided by EWMA seal throughput. |
| `canardstack_projected_buffer_seconds` | Gauge | none | Immutable-buffer visibility debt past configured segment target or max age. |
| `canardstack_projected_visibility_seconds` | Gauge | none | Max of process-queue visibility debt and immutable-buffer visibility debt. |
| `canardstack_observed_freshness_lag_seconds` | Gauge | none | Max cached query-visible freshness lag from the last operator gauge refresh. |

## Maintenance Metrics

| Metric | Type | Labels | Purpose |
| --- | --- | --- | --- |
| `canardstack_maintenance_runs_total` | Counter | `job`, `status`, `reason` | Job outcomes (`status=ok` or `status=error`). |
| `canardstack_maintenance_duration_seconds` | Histogram (`_count` / `_sum`) | `job`, `table=all` | Job runtime. |
| `canardstack_maintenance_failures_total` | Counter | `job`, `reason` | Failures only, broken out by classified reason. Bounded reason set: `disk_full`, `seal_failed`, `metadata_refresh_failed`, `metrics_snapshot_failed`, `retention_failed`, `scheduler_job_failed`. Reasons derive from the job name where possible (so dependency wording changes do not silently re-route alerts); only `disk_full` substring-matches OS / DuckDB errors. |
| `canardstack_maintenance_consecutive_failures` | Gauge | `job` | Consecutive failure count; resets to 0 on success. Drives exponential backoff. |
| `canardstack_maintenance_paused` | Gauge | none | `1` when scheduled maintenance is paused. |

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
| Freshness admission active | `rate(canardstack_ingest_freshness_budget_rejections_total[5m]) > 0` | Warning |
| Connection cap saturated | `rate(canardstack_http_connection_errors_total{reason="max_connections_exceeded"}[5m]) > 0` | Warning |

## Not Currently Emitted

The following metrics from earlier design drafts are **not** emitted by the current implementation. They are listed here so dashboards and runbooks don't silently depend on them:

- `canardstack_ingest_decode_seconds`
- `canardstack_ingest_materialized_bytes_total`
- `canardstack_raw_spool_append_batch_deferred_commands_total`
- `canardstack_raw_spool_checkpoint_batch_deferred_commands_total`
- `canardstack_ingest_partial_commit_rows_total`
- `canardstack_ducklake_insert_seconds`, `_commit_seconds`, `_snapshot_count`
- `canardstack_storage_logical_bytes` (the implementation emits `_logical_rows` instead)
- `canardstack_object_store_errors_total`, `_request_seconds`
- `canardstack_query_active`, `_memory_high_water_bytes`, `_oom_total`
- `canardstack_query_route_active`
- `canardstack_maintenance_last_success_timestamp`, `_backlog_bytes`
- `canardstack_cleanup_deleted_files_total`, `_deleted_bytes_total`
- `canardstack_retention_oldest_retained_date`
- `canardstack_late_records_total`, `_rejected_skewed_records_total`
