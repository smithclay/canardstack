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

- `signal`: `logs`, `spans`, `metric_gauge`, `metric_sum`.
- `table`: `logs`, `spans`, `metric_gauge`, `metric_sum`, or `all`.
- `status`: HTTP status code or grouped class.
- `reason`: bounded rejection or failure reason.
- `job`: maintenance job name (`flush`, `metadata_refresh`, `metrics_snapshot`, `retention`).
- `query_class`: route path (e.g. `/api/v1/query_range`).
- `encoding`: `identity`, `gzip`.
- `triggered_by`: who initiated a partial-commit flush.
- `lane`: `flush`, `query_cheap`, `query_heavy`, `ingest`, or `query`.

Do not label metrics by `service_name`, trace id, query text, API key, or arbitrary attributes.

## Ingest Metrics

| Metric | Type | Labels | Purpose |
| --- | --- | --- | --- |
| `canardstack_ingest_requests_total` | Counter | `signal`, `status`, `reason` | Request outcomes. |
| `canardstack_ingest_request_bytes_total` | Counter | `signal`, `encoding` | Compressed request bytes accepted. |
| `canardstack_raw_spool_records_total` | Counter | `signal`, `status` | Raw request spool outcomes: `spooled`, `full`, `queue_full`, or `error`. `spooled` means written and fsynced to the local raw-spool file. |
| `canardstack_raw_spool_bytes_total` | Counter | `signal` | Compressed raw request bytes written into the local spool. |
| `canardstack_raw_spool_append_batches_total` | Counter | none | Raw-spool append batches written by the writer. |
| `canardstack_raw_spool_append_batch_records_total` | Counter | none | Raw-spool records included in append batches. |
| `canardstack_raw_spool_append_batch_encoded_bytes_total` | Counter | none | Encoded raw-spool bytes included in append batches. |
| `canardstack_raw_spool_append_syncs_total` | Counter | none | Successful raw-spool append sync cycles. |
| `canardstack_raw_spool_append_file_fsyncs_total` | Counter | none | Segment file fsync calls performed by append sync cycles. |
| `canardstack_raw_spool_append_sync_failures_total` | Counter | none | Failed raw-spool append sync cycles. Any increase should make the raw spool unhealthy and subsequent ingest return `503`. |
| `canardstack_raw_spool_append_batch_records` | Gauge | `stat` | Last and max records per raw-spool append batch. |
| `canardstack_raw_spool_append_batch_encoded_bytes` | Gauge | `stat` | Last and max encoded bytes per raw-spool append batch. |
| `canardstack_raw_spool_replayed_records_total` | Counter | `signal`, `status` | Startup replay attempts and outcomes for uncheckpointed raw-spool records. |
| `canardstack_raw_spool_checkpointed_records_total` | Counter | `signal`, `reason` | Raw-spool records made reclaimable after terminal rejection or DuckLake storage commit. |
| `canardstack_raw_spool_pending_records` | Gauge | none | Uncheckpointed raw-spool records currently pending replay or storage commit. |
| `canardstack_raw_spool_pending_bytes` | Gauge | none | Compressed bytes for uncheckpointed raw-spool records. |
| `canardstack_raw_spool_unsynced_records` | Gauge | none | Written raw-spool records not yet covered by a successful append sync. |
| `canardstack_raw_spool_unsynced_bytes` | Gauge | none | Encoded raw-spool bytes not yet covered by a successful append sync. |
| `canardstack_raw_spool_unsynced_age_seconds` | Gauge | none | Age of the oldest unsynced append data, or `0` when fully synced. |
| `canardstack_raw_spool_healthy` | Gauge | none | `1` when the writer is accepting appends; `0` after a fatal append sync failure. |
| `canardstack_raw_spool_segment_bytes` | Gauge | none | Total raw-spool segment bytes on disk. |
| `canardstack_raw_spool_segments` | Gauge | none | Raw-spool segment file count. |
| `canardstack_ingest_records_total` | Counter | `signal` | Records accepted into the immutable buffer. |
| `canardstack_ingest_transformed_rows_total` | Counter | `signal`, `request_signal` | Rows produced by worker-side `otlp2records` transform. |
| `canardstack_ingest_unsupported_histograms_total` | Counter | `signal` | Histogram datapoints observed and dropped by the v0 metrics transformer. Emitted only when nonzero. |
| `canardstack_ingest_buffered_rows_total` | Counter | `signal` | Rows appended to the storage immutable buffer. |
| `canardstack_ingest_buffered_bytes_total` | Counter | `signal` | Approximate Arrow bytes appended to the storage immutable buffer. |
| `canardstack_ingest_inflight_bytes` | Gauge | `signal` | Bytes admitted (spooled, handed to a worker) but not yet appended to the immutable buffer. |
| `canardstack_ingest_inflight_capacity_bytes` | Gauge | `signal` | Per-signal in-flight ceiling. |
| `canardstack_ingest_inflight_pressure` | Gauge | `signal` | In-flight bytes as a fraction of the per-signal ceiling (`0..1`). |
| `canardstack_ingest_worker_queue_capacity` | Gauge | `state=capacity` | Configured bounded worker channel capacity. |
| `canardstack_ingest_storage_insert_total` | Counter | `signal`, `status` | Worker appends of Arrow batches into the immutable buffer. |
| `canardstack_ingest_worker_completed_total` | Counter | `signal`, `status` | Ingest worker tasks completed, by outcome. |
| `canardstack_ingest_rejections_total` | Counter | `signal`, `status`, `reason` | Admission-control rejections (subset of `_ingest_requests_total`). |
| `canardstack_ingest_freshness_budget_rejections_total` | Counter | none | Requests rejected before raw-spool append because projected visibility exceeded the freshness SLA. |
| `canardstack_immutable_segments_sealed_rows_total` | Counter | `signal` | Rows sealed into immutable Parquet segments. |
| `canardstack_immutable_segments_sealed_files_total` | Counter | `signal` | Immutable Parquet files written and registered with DuckLake. |

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
storage proof phases with `signal` and `phase` labels:
`storage_prepare`, `storage_buffer`, `storage_partition_split`,
`storage_parquet_encode`, `storage_file_write`, `storage_file_fsync`,
`storage_file_rename`, `storage_ducklake_register`, and
`storage_ducklake_commit`.

`/api/admin/health/ingest` returns queue snapshots, raw-spool stats
(`segment_count`, `segment_bytes`, `pending_records`, `pending_bytes`,
`unsynced_records`, `unsynced_bytes`, `unsynced_age_seconds`, `healthy`, and
`error` when unhealthy), and the active raw-spool group-commit and append-sync
settings. It also returns the current lane snapshot. Operators can diagnose
replay backlog, unsynced append exposure, queue pressure, freshness-budget
projection, and lane pressure without arbitrary SQL.

The shared phase metric `canardstack_phase_duration_seconds` splits
request-visible `raw_spool_append` latency from raw-spool writer internals:
`raw_spool_append_batch_wait` is time spent collecting a group-commit batch,
`raw_spool_append_write` is file write time, and `raw_spool_append_fsync` is
append sync time. `raw_spool_append_fsync` is part of `202` latency because
accepted requests are fsynced before acknowledgement.

## Query Metrics

| Metric | Type | Labels | Purpose |
| --- | --- | --- | --- |
| `canardstack_query_requests_total` | Counter | `query_class`, `status`, `reason` | Query outcomes. |
| `canardstack_query_duration_seconds` | Histogram (`_count` / `_sum`) | `query_class` | User-visible latency. |
| `canardstack_query_rejections_total` | Counter | `query_class`, `reason` | Concurrency / shape rejections. |
| `canardstack_query_timeouts_total` | Counter | `query_class` | Timeout enforcement. |
| `canardstack_query_lane_reductions_total` | Counter | none | Heavy query admissions that ran at the degraded capacity because freshness debt was elevated. |
| `canardstack_query_lane_rejections_total` | Counter | none | Query lane rejections from cheap-lane saturation, heavy-lane saturation, or freshness debt. |

## Lane Metrics

| Metric | Type | Labels | Purpose |
| --- | --- | --- | --- |
| `canardstack_lane_capacity` | Gauge | `lane` | Current logical lane capacity. Heavy query capacity reports the effective capacity after freshness degradation. |
| `canardstack_lane_in_use` | Gauge | `lane` | Current logical lane occupancy. |
| `canardstack_lane_rejections_total` | Counter | `lane`, `reason` | Rejections at the lane controller. |
| `canardstack_flush_ewma_bytes_per_second` | Gauge | none | EWMA queue-byte flush throughput used for freshness-budget admission. |
| `canardstack_projected_flush_seconds` | Gauge | none | Queue byte debt divided by EWMA flush throughput. |
| `canardstack_projected_buffer_seconds` | Gauge | none | Immutable-buffer visibility debt past configured segment target or max age. |
| `canardstack_projected_visibility_seconds` | Gauge | none | Max of process-queue visibility debt and immutable-buffer visibility debt. |
| `canardstack_observed_freshness_lag_seconds` | Gauge | none | Max cached query-visible freshness lag from the last operator gauge refresh. |

## Maintenance Metrics

| Metric | Type | Labels | Purpose |
| --- | --- | --- | --- |
| `canardstack_maintenance_runs_total` | Counter | `job`, `status`, `reason` | Job outcomes (`status=ok` or `status=error`). |
| `canardstack_maintenance_duration_seconds` | Histogram (`_count` / `_sum`) | `job`, `table=all` | Job runtime. |
| `canardstack_maintenance_failures_total` | Counter | `job`, `reason` | Failures only, broken out by classified reason. Bounded reason set: `disk_full`, `flush_failed`, `metadata_refresh_failed`, `metrics_snapshot_failed`, `retention_failed`, `scheduler_job_failed`. Reasons derive from the job name where possible (so dependency wording changes do not silently re-route alerts); only `disk_full` substring-matches OS / DuckDB errors. |
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
- `canardstack_ducklake_insert_seconds`, `_commit_seconds`, `_snapshot_count`, `_flush_failures_total`
- `canardstack_storage_logical_bytes` (the implementation emits `_logical_rows` instead)
- `canardstack_object_store_errors_total`, `_request_seconds`
- `canardstack_query_active`, `_memory_high_water_bytes`, `_oom_total`
- `canardstack_query_class_active`
- `canardstack_maintenance_last_success_timestamp`, `_backlog_bytes`
- `canardstack_cleanup_deleted_files_total`, `_deleted_bytes_total`
- `canardstack_retention_oldest_retained_date`
- `canardstack_late_records_total`, `_rejected_skewed_records_total`
