# Compatibility Query API

Canardstack v0 exposes bounded compatibility adapters for existing
observability clients. It does not define a Canardstack-specific product query
protocol and does not expose arbitrary SQL through the normal HTTP API.

All telemetry query paths use the internal query engine protections:

- Required or server-bounded time ranges.
- Result limits.
- Server-owned timeouts.
- DuckDB memory limits.
- Query concurrency guards.

## Discovery Metadata Spine

Prometheus label values, Prometheus series, Prometheus metric metadata, Loki
label values, Loki series, and Tempo tag values read from the shared
`metadata_summary` table. The `metadata_refresh` scheduler job re-aggregates the
daily `(signal, event_date)` summary buckets derived from committed telemetry
timestamps, keeping the timestamp-day scan off the ingest commit path. It uses
canonical `otlp2records` columns directly where available and derives
compatibility labels such as `deployment_environment`, `http_route`, and
`http_method` from canonical JSON attribute columns.

These reads still run through the bounded `QueryEngine` path with normal
concurrency, timeout, memory, range, and result-limit controls. An in-process
cache stores only bounded discovery responses, keyed by API family, metadata
kind, label/tag name, and exact normalized time range. Successful metadata refreshes
advance a generation counter, invalidating stale cache entries.

## Prometheus Metrics Subset

Endpoints:

- `GET/POST /api/v1/query`
- `GET/POST /api/v1/query_range`
- `GET /api/v1/labels`
- `GET /api/v1/label/{name}/values`
- `GET /api/v1/series`
- `GET /api/v1/metadata`

Responses use Prometheus-style envelopes:

```json
{"status":"success","data":{"resultType":"matrix","result":[]}}
```

Errors use:

```json
{"status":"error","errorType":"unsupported_promql","error":"..."}
```

Supported PromQL subset:

- A bare metric name such as `smoke.gauge`.
- A metric selector such as `smoke.gauge{service_name="checkout"}`.
- Equality filters over supported labels such as `service_name` and
  `deployment_environment`.
- `avg`, `min`, `max`, `sum`, `count`, and `rate` around a single selector.
- `avg`, `min`, `max`, `sum`, and `count` with explicit `by(...)` or
  `without(...)` grouping over supported labels such as `service_name` and
  `deployment_environment`.

Not implemented: full PromQL expression evaluation, joins, binary operators,
subqueries, histograms, exemplars, rule evaluation, staleness semantics, and
remote read/write.

## Loki Logs Subset

Endpoints:

- `GET /loki/api/v1/query_range`
- `GET /loki/api/v1/query`
- `GET /loki/api/v1/labels`
- `GET /loki/api/v1/label/{name}/values`
- `GET /loki/api/v1/series`

Responses use Loki-style stream results:

```json
{
  "status": "success",
  "data": {
    "resultType": "streams",
    "result": [{"stream": {"service_name": "checkout"}, "values": []}]
  }
}
```

Supported LogQL subset:

- Stream selectors such as `{service_name="checkout"}`.
- Equality label filters over supported labels.
- `start`, `end`, `limit`, and `direction`.
- Simple text contains filters with `|= "text"`.

Not implemented: regex/negative matchers, parser stages, unwrap, line format,
label format, metric queries, aggregations, recording rules, and full LogQL.

## Tempo Traces Subset

Endpoints:

- `GET /api/v2/traces/{traceID}`
- `GET /api/traces/{traceID}`
- `GET /api/search`
- `GET /api/search/tags`
- `GET /api/search/tag/{tag}/values`
- `GET /api/v2/search/tags`
- `GET /api/v2/search/tag/{tag}/values`

Grafana probe shims:

- `GET /api/status/buildinfo`

Supported search filters map to canonical span/resource columns or derived
attribute labels:

- `service.name` or `service_name`.
- Span `name` or `span_name`.
- `http.route`.
- `status.code` or `status_code`.
- `traceID` or `trace_id`.

Trace lookup returns a Tempo-compatible JSON shape containing batches and spans.
Search returns trace summaries. TraceQL is not implemented.

## Non-API Surfaces

The remaining HTTP endpoints are operational or ingest endpoints:

- `POST /v1/logs`
- `POST /v1/traces`
- `POST /v1/metrics`
- `GET /`
- `GET /metrics`
- `GET /api/admin/health/storage`
- `GET /api/admin/health/ingest`
- `GET /api/admin/health/maintenance`
- `GET /api/admin/health/queries`
- `POST /api/admin/maintenance/pause`
- `POST /api/admin/maintenance/resume`
- `POST /api/admin/maintenance/seal`
- `POST /api/admin/maintenance/retention/dry-run`
- `POST /api/admin/maintenance/retention/run`

Direct DuckDB/DuckLake SQL access is available outside Canardstack through
DuckDB CLI, MotherDuck, or SQL clients. That path is intentionally separate from
the normal HTTP product surface.
