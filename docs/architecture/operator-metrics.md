# Operator Metrics

## Metric Naming

The canardstack process exposes Prometheus-text metrics at `GET /metrics`. The
scrape path records only cheap in-process gauges. Storage layout, row-count,
physical-byte, and freshness-watermark gauges are refreshed by the scheduler's
metrics-snapshot job, then rendered from the metric store. Grafana can query
canardstack's own monitoring data through the normal Prometheus-compatible
datastore path. The names below are the implemented contract: only metrics that
are actually emitted are listed here. Histograms are rendered as a counter pair
(`*_count` / `*_sum`) rather than full HDR-style buckets.

The exact post-diet name set is pinned by the `render_prometheus`
snapshot tests in `src/metrics.rs` (`render_prometheus_matches_post_diet_surface`
and the feature-aware coarse/fine-phase tests). Update that test deliberately
when you add or drop a metric name.

### Persisting operator metrics to storage (opt-in)

By default the metrics-snapshot job refreshes the operator gauges but does NOT
persist a snapshot into the `metric_gauge` / `metric_sum` storage tables. Set
`CANARDSTACK_OPERATOR_METRICS_TO_STORAGE=true` (TOML
`[metrics] operator_metrics_to_storage = true`) to enable the write; the job then
writes the current samples with `service_name="canardstack"` (counters land in
`metric_sum`, gauges land in `metric_gauge`) so canardstack's own metrics are
queryable through the compat query path. With the flag off the job reports
`rows: 0` / `"operator_metrics_to_storage": false` and `/metrics` still serves
the live surface.

### Fine phase timings (`detailed-metrics` feature, opt-in)

`canardstack_phase_duration_seconds` always carries the coarse phases. The fine
spool micro-timings below are only emitted when the binary is built with
`--features detailed-metrics`, keeping the default `/metrics` surface lean:

- `raw_spool_append_queue_wait`, `raw_spool_append_batch_wait`,
  `raw_spool_append_encode`, `raw_spool_append_write`, `raw_spool_append_fsync`
- `raw_spool_checkpoint_queue_wait`, `raw_spool_checkpoint_batch_wait`
- the per-`request_kind` `raw_spool_append_fsync` observation

Labels stay low-cardinality:

- `request_kind`: `logs`, `traces`, or `metrics`. This is the single per-signal
  label name for the ingest/raw-spool surface (the former `spool_lane` label was
  retired in the metrics diet).
- `storage_signal`: `logs`, `spans`, `metric_gauge`, `metric_sum`.
- `table`: `logs`, `spans`, `metric_gauge`, `metric_sum`, or `all`.
- `status`: HTTP status code or grouped class.
- `outcome`: worker-channel handoff outcome (`queued`, `processed_inline`, `workers_unavailable`).
- `reason`: bounded rejection or failure reason.
- `job`: maintenance job name (`seal`, `metadata_refresh`, `metrics_snapshot`, `retention`).
- `route_template`: static query route template (e.g. `/api/v1/query_range` or `/api/v2/traces/:trace_id`).
- `encoding`: `identity`, `gzip`.
- `admission`: `seal`, `query_cheap`, `query_heavy`, `query`, or `freshness_budget`.

Do not label metrics by `service_name`, trace id, query text, API key, or arbitrary attributes.

## Ingest Metrics

| Metric | Type | Labels | Purpose |
| --- | --- | --- | --- |
| `canardstack_ingest_requests_total` | Counter | `request_kind`, `status`, `reason` | Request outcomes. Rejections are the `status=~"429\|503"` subset. |
| `canardstack_ingest_request_bytes_total` | Counter | `request_kind`, `encoding` | Compressed request bytes accepted. |
| `canardstack_raw_spool_records_total` | Counter | `request_kind`, `status` | Raw request spool outcomes: `spooled`, `full`, `queue_full`, or `error`. `spooled` means written and fsynced to the local raw-spool file. |
| `canardstack_raw_spool_bytes_total` | Counter | `request_kind` | Compressed raw request bytes written into the local spool. |
| `canardstack_raw_spool_append_batches_total` | Counter | `request_kind` | Raw-spool append batches written by the writer. |
| `canardstack_raw_spool_append_batch_records_total` | Counter | `request_kind` | Raw-spool records included in append batches. |
| `canardstack_raw_spool_append_batch_encoded_bytes_total` | Counter | `request_kind` | Encoded raw-spool bytes included in append batches. |
| `canardstack_raw_spool_append_syncs_total` | Counter | `request_kind` | Successful raw-spool append sync cycles. |
| `canardstack_raw_spool_append_file_fsyncs_total` | Counter | `request_kind` | Segment file fsync calls performed by append sync cycles. |
| `canardstack_raw_spool_append_sync_failures_total` | Counter | `request_kind` | Failed raw-spool append sync cycles. Any increase should make the raw spool unhealthy and subsequent ingest return `503`. |
| `canardstack_raw_spool_replayed_records_total` | Counter | `request_kind`, `status` | Startup replay attempts and outcomes for uncheckpointed raw-spool records. |
| `canardstack_raw_spool_checkpointed_records_total` | Counter | `request_kind`, `reason` | Raw-spool records made reclaimable after terminal rejection or DuckLake storage commit. |
| `canardstack_raw_spool_pending_records` | Gauge | `request_kind` | Uncheckpointed raw-spool records currently pending replay or storage commit. |
| `canardstack_raw_spool_pending_bytes` | Gauge | `request_kind` | Compressed bytes for uncheckpointed raw-spool records. |
| `canardstack_raw_spool_unsynced_records` | Gauge | `request_kind` | Written raw-spool records not yet covered by a successful append sync. |
| `canardstack_raw_spool_unsynced_bytes` | Gauge | `request_kind` | Encoded raw-spool bytes not yet covered by a successful append sync. |
| `canardstack_raw_spool_unsynced_age_seconds` | Gauge | `request_kind` | Age of the oldest unsynced append data, or `0` when fully synced. |
| `canardstack_raw_spool_healthy` | Gauge | `request_kind` | `1` when the writer is accepting appends; `0` after a fatal append sync failure. |
| `canardstack_raw_spool_segment_bytes` | Gauge | `request_kind` | Total raw-spool segment bytes on disk. |
| `canardstack_raw_spool_segments` | Gauge | `request_kind` | Raw-spool segment file count. |
| `canardstack_ingest_records_total` | Counter | `request_kind` | Records accepted into the Arrow write buffer. |
| `canardstack_ingest_transformed_rows_total` | Counter | `storage_signal`, `request_kind` | Rows produced by worker-side `otlp2records` transform. |
| `canardstack_ingest_unsupported_histograms_total` | Counter | `request_kind` | Histogram datapoints observed and dropped by the v0 metrics transformer. Emitted only when nonzero. |
| `canardstack_ingest_buffered_rows_total` | Counter | `storage_signal` | Rows appended to the Arrow write buffer. |
| `canardstack_ingest_buffered_bytes_total` | Counter | `storage_signal` | Approximate Arrow bytes appended to the Arrow write buffer. |
| `canardstack_ingest_inflight_bytes` | Gauge | `storage_signal` | Bytes admitted (spooled, handed to a worker) but not yet appended to the Arrow write buffer. Feeds the freshness in-flight total. Peaks are derivable with `max_over_time()`. |
| `canardstack_ingest_worker_queue_capacity` | Gauge | `state=capacity` | Configured bounded worker channel capacity. |
| `canardstack_ingest_worker_dispatch_total` | Counter | `request_kind`, `outcome` | Worker-channel handoff outcomes: `queued`, `processed_inline`, or `workers_unavailable`. A rising `outcome="processed_inline"` rate signals worker-pool saturation: every worker channel was full, so the request was processed inline on the connection thread (back-pressure via latency). The first transition into each saturation episode also emits the `ingest_worker_pool_saturated` log event (logged once per episode, cleared on the next successful queued dispatch). |
| `canardstack_ingest_storage_insert_total` | Counter | `request_kind`, `status` | Worker appends of Arrow batches into the Arrow write buffer. |
| `canardstack_ingest_worker_completed_total` | Counter | `request_kind`, `status` | Ingest worker tasks completed, by outcome. |
| `canardstack_duckdb_arrow_appends_total` | Counter | `storage_signal` | DuckDB Arrow appender calls per flushed storage signal. |
| `canardstack_duckdb_arrow_appended_rows_total` | Counter | `storage_signal` | Rows handed to DuckDB through the Arrow appender. |
| `canardstack_arrow_flushes_total` | Counter | `storage_signal` | Arrow write-buffer flushes that reached DuckLake commit. |
| `canardstack_arrow_flush_rows_total` | Counter | `storage_signal` | Rows made durable by DuckLake commit. |

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
| `canardstack_ducklake_active_data_files` | Gauge | `table` | Active DuckLake data files per table. |
| `canardstack_ducklake_active_data_file_rows` | Gauge | `table` | Active rows stored in DuckLake data files per table. |

The shared phase metric `canardstack_phase_duration_seconds` also records
storage proof phases with `storage_signal` and `phase` labels:
`storage_prepare`, `storage_arrow_write_buffer`,
`storage_arrow_write_coalesce`, `storage_duckdb_arrow_append`, and
`storage_ducklake_commit`. It also records `writer_lock_wait`, the time spent
waiting to acquire the single write connection lock, on the flush path
(`request_kind`, `phase=writer_lock_wait`) and the metadata-refresh path
(`phase=writer_lock_wait`, `path=metadata_refresh`).

`/api/admin/health/ingest` returns queue snapshots, raw-spool stats
(`segment_count`, `segment_bytes`, `pending_records`, `pending_bytes`,
`unsynced_records`, `unsynced_bytes`, `unsynced_age_seconds`, `healthy`, and
`error` when unhealthy), and the active raw-spool group-commit and append-sync
settings. It also returns the current admission snapshot. Operators can diagnose
replay backlog, unsynced append exposure, queue pressure, freshness-budget
projection, and admission pressure without arbitrary SQL.

The shared phase metric `canardstack_phase_duration_seconds` records the coarse
request-visible `raw_spool_append` and `raw_spool_checkpoint` phases with
`request_kind` and `phase` labels (the coarse batch-checkpoint phase is emitted
once, label-free). The raw-spool writer internals
(`raw_spool_append_batch_wait` collecting a group-commit batch,
`raw_spool_append_write` file write time, `raw_spool_append_fsync` append sync
time, and the checkpoint micro-timings) are only emitted when built with
`--features detailed-metrics`. `raw_spool_append_fsync` is part of `202` latency
because accepted requests are fsynced before acknowledgement.

## Query Metrics

| Metric | Type | Labels | Purpose |
| --- | --- | --- | --- |
| `canardstack_query_requests_total` | Counter | `route_template`, `status`, `reason` | Query outcomes. Rejections are the `status="429"` subset. |
| `canardstack_query_duration_seconds` | Histogram (`_count` / `_sum`) | `route_template` | User-visible latency. |
| `canardstack_query_timeouts_total` | Counter | `route_template` | Timeout enforcement. |

## Admission Metrics

| Metric | Type | Labels | Purpose |
| --- | --- | --- | --- |
| `canardstack_admission_capacity` | Gauge | `admission` | Current admission capacity. Heavy query capacity reports the effective capacity after freshness degradation. |
| `canardstack_admission_in_use` | Gauge | `admission` | Current admission occupancy. |
| `canardstack_admission_rejections_total` | Counter | `admission`, `reason` | Rejections at the admission controller, including seal saturation, cheap/heavy query saturation, freshness debt, and `admission="freshness_budget"` ingest rejections. |
| `canardstack_admission_reductions_total` | Counter | none | Heavy query admissions that ran at the degraded capacity because freshness debt was elevated. |
| `canardstack_seal_ewma_bytes_per_second` | Gauge | none | EWMA seal throughput used for freshness-budget admission. |
| `canardstack_projected_seal_seconds` | Gauge | none | Queue byte debt divided by EWMA seal throughput. |
| `canardstack_projected_buffer_seconds` | Gauge | none | Arrow write-buffer visibility debt past configured buffer target or max age. |
| `canardstack_projected_visibility_seconds` | Gauge | none | Max of process-queue visibility debt and Arrow write-buffer visibility debt. |
| `canardstack_observed_freshness_lag_seconds` | Gauge | none | Max cached query-visible freshness lag from the last operator gauge refresh. |
| `canardstack_ingest_inflight_memory_bound_bytes` | Gauge | none | Approximate in-flight byte bound implied by freshness-first admission (`0.95 x freshness_budget_sla_seconds x ewma_seal_bytes_per_second`). The 0.95 is the headline `INGEST_FRESHNESS_BUDGET_FRACTION`; the with-heavy-query path tightens to 0.90. During EWMA warm-up it rides on the configured seal-rate seed. This is the implicit memory backstop now that the per-signal in-flight ceiling is gone and the RSS hard cap is opt-in. |

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
| Freshness admission active | `rate(canardstack_admission_rejections_total{admission="freshness_budget"}[5m]) > 0` | Warning |
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

The metrics diet dropped the following derivable / superseded series. Use the
listed replacement instead:

- `canardstack_ingest_inflight_bytes_max`, `canardstack_ingest_inflight_pressure_max` (use `max_over_time()` of the live gauges).
- `canardstack_ingest_inflight_capacity_bytes`, `canardstack_ingest_inflight_pressure` (vestigial of the dropped per-signal in-flight ceiling; freshness-first admission is the sole soft shed, so there is no per-signal capacity denominator to express a pressure fraction against).
- `canardstack_raw_spool_append_batch_records`, `canardstack_raw_spool_append_batch_encoded_bytes` gauges (the `*_total` counters remain; per-batch averages are derivable).
- The aggregate (no-`request_kind`) copies of the raw-spool gauges/counters (use `sum without(request_kind)`).
- `canardstack_ingest_rejections_total`, `canardstack_query_rejections_total` (use the `status=~"429\|503"` / `status="429"` subset of `*_requests_total`).
- `canardstack_query_admission_rejections_total`, `canardstack_ingest_freshness_budget_rejections_total` (use `canardstack_admission_rejections_total{admission,reason}`).
- `canardstack_query_admission_reductions_total` (renamed to `canardstack_admission_reductions_total`).
- `canardstack_ingest_requests_queued_total` (renamed to `canardstack_ingest_worker_dispatch_total`, label `status` -> `outcome`).
- The `spool_lane` label key (renamed to `request_kind`).
