# Failure Runbooks

These runbooks describe the implemented v0 operator surface. Metrics listed in
`operator-metrics.md` as "not currently emitted" are intentionally not used
here.

## Health Endpoint Contract

The admin health endpoints return **503** when the underlying subsystem is in a
page-worthy state — not just 200 with counters the operator has to interpret.
The contract is:

- `/healthz` → 503 when storage is unhealthy or DuckLake required-but-unavailable.
- `/api/admin/health/storage` → mirrors /healthz (same `is_ready` predicate).
- `/api/admin/health/maintenance` → 503 when any job has consecutive failures
  ≥ 3 (matches `canardstack_maintenance_consecutive_failures{job} >= 3`).
- `/api/admin/health/queries` → 503 when storage is not ready (queries depend
  on DuckDB).
- `/api/admin/health/ingest` → always 200; check queue gauges for backpressure.

Where a step below says "check /api/admin/health/...", the expected paging
status is the HTTP code, not just the body.

## 1. DuckLake Flushes Are Stuck

### Symptoms

- `canardstack_maintenance_consecutive_failures{job="flush"}` rises.
- `canardstack_ingest_queue_oldest_age_seconds` rises.
- Query freshness gets worse even though ingest is accepting data.
- Maintenance logs show flush/checkpoint failures.

### Metrics

- `canardstack_maintenance_runs_total{job="flush",status="error"}`.
- `canardstack_maintenance_failures_total{job="flush"}`.
- `canardstack_maintenance_consecutive_failures{job="flush"}`.
- `canardstack_ingest_queue_bytes{signal}`.
- `canardstack_ingest_queue_oldest_age_seconds{signal}`.
- `canardstack_ingest_to_query_lag_seconds{table}`.
- `canardstack_storage_physical_bytes{table="all"}`.

### Immediate Mitigation

1. Check `/api/admin/health/storage` and `/api/admin/health/maintenance`.
2. Trigger `POST /api/admin/maintenance/flush`.
3. If flush still fails, reduce upstream exporter concurrency or batch volume.
4. Watch `canardstack_ingest_requests_total{status=~"429|503"}` and queue gauges.
5. If storage health is unsafe, keep returning `503` for ingest until the dependency recovers.

### Safe Degradation

- Keep existing queries available if they do not increase storage pressure.
- Prefer `429` over accepting more data into full queues.
- Temporarily lower configured query concurrency and restart if query load contends with flush.

### Escalation

- Inspect DuckLake flush errors for object storage auth, network, or catalog lock contention.
- Add storage capacity if disk is the immediate risk.
- If flush cannot be recovered, snapshot diagnostics and prepare for restore from last known good catalog backup.

## 2. DuckDB Query OOM Takes Down The Query Role

### Symptoms

- Query role restarts.
- In-flight Grafana or compatibility queries fail with `503`.
- System logs show DuckDB allocation failure or process OOM kill.
- Ingest may remain healthy if process roles are isolated; otherwise all service endpoints restart.

### Metrics

- `canardstack_query_requests_total{status="503"}`.
- `canardstack_query_timeouts_total`.
- `canardstack_query_rejections_total`.
- `/api/admin/health/queries` for active and limit counts.

### Immediate Mitigation

All query lane knobs are env vars applied at boot — there is no hot-reload
endpoint. Steps 2 and 3 require an operator-driven restart.

1. Restart query role if supervisor has not already done so.
2. Lower query memory by 50%: set `CANARDSTACK_QUERY_INTERACTIVE_MEMORY_LIMIT`
   (e.g. `256MiB`) and restart.
3. Lower global query concurrency: set
   `CANARDSTACK_QUERY_INTERACTIVE_CONCURRENCY=1` and restart.
4. Reduce compatibility query traffic from Grafana or other clients.

### Safe Degradation

- Keep ingest running.
- Return `503` for query APIs while the query role restarts.
- Keep admin health endpoints available if they do not need DuckDB.

### Escalation

- Move query execution to a separate process role if not already isolated.
- Add stricter time-range caps for the failing query class. (Per-query-shape
  rejection rules are not implemented in v0; tightening must happen at the
  reverse proxy / Grafana datasource until a server-side allow-list lands.)
- Add preflight estimates or query templates that avoid full scans.

## 3. Object Storage 5xx Storm Causes Flush Backlog

### Symptoms

- DuckLake inserts or flushes fail with object storage 5xx.
- Freshness lag grows.
- Maintenance retries increase.
- `503` may begin for ingest if storage health is unsafe.

### Metrics

- `canardstack_maintenance_failures_total`.
- `canardstack_maintenance_consecutive_failures`.
- `canardstack_ingest_requests_total{status="503"}`.
- `canardstack_ingest_queue_bytes{signal}`.
- `canardstack_ingest_to_query_lag_seconds{table}`.

### Immediate Mitigation

1. Confirm whether the object storage incident is regional or credential-related.
2. Reduce upstream exporter concurrency.
3. Trigger `POST /api/admin/maintenance/flush` after the storage incident clears.
4. Let admission control return `429` while queue memory is full but storage is healthy.
5. Return `503` if accepting data would endanger memory, catalog, or disk.

### Safe Degradation

- Serve queries from already committed data.
- Surface stale freshness watermarks in Grafana.
- Prioritize flush recovery; compaction and orphan cleanup are not implemented v0 controls.

### Escalation

- Fail over to a configured alternate bucket only if that path was tested.
- Increase catalog capacity only as a temporary measure.
- Preserve logs and metrics for DuckLake/storage vendor support.

## 4. Ingest Overload Triggers Sustained `429` Or `503`

### Symptoms

- Exporters report retryable failures.
- `429` or `503` rate remains high for more than 10 minutes.
- Ingest queues stay above 85%.
- CPU, memory, catalog, or storage is saturated.

### Metrics

- `canardstack_ingest_requests_total{status}`.
- `canardstack_ingest_queue_bytes{signal}`.
- `canardstack_ingest_queue_oldest_age_seconds{signal}`.
- `canardstack_ingest_records_total{signal}`.
- `canardstack_http_connection_errors_total`.

### Immediate Mitigation

1. Identify the limiting resource: CPU, memory, catalog, object storage, or maintenance backlog.
2. Increase exporter batch interval or reduce exporter concurrency if controlled by the operator.
3. Temporarily drop lower-priority signals upstream if configured in the exporter or Collector.
4. Increase process memory or add a larger instance if CPU/memory bound.
5. Keep returning retryable failures until queues return below 70%.

### Safe Degradation

- Prefer `429` when the system is healthy but full.
- Use `503` when dependencies are unhealthy.
- Do not accept data that would exceed memory bounds.

### Escalation

- Split ingest/query/maintenance roles.
- Increase batch sizes if inserts are too small.
- Lower retention temporarily only if storage pressure is part of the overload.

## 5. Retention Cleanup Fails And Storage Usage Keeps Growing

### Symptoms

- Physical storage bytes keep increasing after retention horizon.
- Retention job fails or times out.
- Old files remain after expected cleanup.

### Metrics

- `canardstack_storage_physical_bytes{table="all"}`.
- `canardstack_storage_logical_rows{table}`.
- `canardstack_maintenance_failures_total{job="retention"}`.
- `canardstack_maintenance_consecutive_failures{job="retention"}`.

### Immediate Mitigation

1. Run `POST /api/admin/maintenance/retention/dry-run`.
2. Check the returned table counts and `/api/admin/health/storage`.
3. Run `POST /api/admin/maintenance/retention/run`.
4. If storage is near full, reduce upstream ingest or add storage before accepting more data.
5. Shorten retention only after confirming the run succeeds and physical bytes fall.

### Safe Degradation

- Keep ingest running if storage headroom is sufficient.
- If bucket or disk quota is near exhaustion, return `503` for ingest before writes fail unpredictably.
- Keep queries bounded to retained dates.

### Escalation

- Add storage capacity.
- Investigate whether partition deletes are producing delete files instead of reclaiming physical files.
- If needed, migrate to physical day tables behind stable views.
