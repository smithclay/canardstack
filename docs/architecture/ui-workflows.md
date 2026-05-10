# Local Investigation UI

The human UI is a thin local browser surface over the compatibility query APIs.
It does not own a separate query language, dashboard state, alert setup, saved
searches, hidden pagination, or trace-specific product model.

## UI Role

The UI calls:

- `GET /api/v1/query_range` for metrics.
- `GET /loki/api/v1/query_range` for logs.
- `GET /api/search` for trace search.
- `GET /api/traces/{traceID}` or `GET /api/v2/traces/{traceID}` for trace
  lookup.
- Label and series endpoints for quick discovery.

The UI renders returned JSON directly as simple tables or raw structured output.
It is intended for bounded local investigation and smoke verification, not as a
Grafana replacement.

## Supported Workflows

### Metric Range Inspection

The operator chooses the Prometheus-compatible surface, enters a supported
metric selector or simple aggregate, and runs a bounded range query.

### Log Stream Inspection

The operator chooses the Loki-compatible surface, enters a stream selector and
optional `|= "text"` filter, and runs a bounded range query.

### Trace Search And Lookup

The operator chooses Tempo search with promoted filters such as `service.name`,
or uses Tempo trace lookup with a known trace id.

### Discovery

The UI can call label and series endpoints for the selected metrics or logs
surface. Returned values are bounded and should be treated as discovery hints.

## Explicit Non-Goals

- Dashboard CRUD.
- Alert CRUD.
- Saved searches.
- Live tail.
- Custom trace waterfall.
- Viewer-originated hidden pagination.
- Arbitrary SQL.
- Full PromQL, LogQL, or TraceQL editing support.

TODO: A richer table renderer such as Perspective may be useful later, but the
v0 UI should remain a thin local investigation layer over the compatibility
endpoints.
