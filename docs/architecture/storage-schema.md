# V0 Storage Schema

## Schema Contract

Canardstack v0 treats `otlp2records` as the canonical telemetry schema. The application must create DuckLake tables compatible with the Arrow schemas returned by:

- `logs_schema()`
- `traces_schema()`
- `gauge_schema()`
- `sum_schema()`

The table names are:

- `logs`
- `spans`
- `metric_gauge`
- `metric_sum`

All arbitrary attributes are JSON strings in v0. Do not use DuckDB `VARIANT` while Postgres is the DuckLake catalog.

## Common Physical Columns

Each table should add a small number of storage-management columns around the canonical payload:

| Column | Type | Purpose |
| --- | --- | --- |
| `event_date` | `DATE` | Day partition/retention key derived from `timestamp`. |
| `ingested_at` | `TIMESTAMP` | Server receive/commit time for freshness diagnostics. |
| `source_format` | `VARCHAR` | `otlp_proto` or `otlp_json`. |

`event_date` must be derived from event time, not ingest time.

## Logs

Canonical fields from `otlp2records`:

| Column | Type | Notes |
| --- | --- | --- |
| `timestamp` | `TIMESTAMP` | Log record timestamp. |
| `trace_id` | `VARCHAR` | Hex trace id. |
| `span_id` | `VARCHAR` | Hex span id. |
| `service_name` | `VARCHAR` | Resource `service.name`. |
| `service_namespace` | `VARCHAR` | Resource `service.namespace`. |
| `service_instance_id` | `VARCHAR` | Resource `service.instance.id`. |
| `severity_number` | `INTEGER` | OTel severity number. |
| `severity_text` | `VARCHAR` | OTel severity text. |
| `body` | `VARCHAR` | Log body rendered as string. |
| `resource_attributes` | `VARCHAR` | JSON string. |
| `scope_name` | `VARCHAR` | Instrumentation scope name. |
| `scope_version` | `VARCHAR` | Instrumentation scope version. |
| `scope_attributes` | `VARCHAR` | JSON string. |
| `log_attributes` | `VARCHAR` | JSON string. |

Derived promoted fields for product filters:

| Column | Type | Source |
| --- | --- | --- |
| `deployment_environment` | `VARCHAR` | `resource_attributes["deployment.environment"]`. |
| `http_method` | `VARCHAR` | `log_attributes["http.request.method"]` or legacy equivalent. |
| `http_status_code` | `INTEGER` | `log_attributes["http.response.status_code"]` or legacy equivalent. |
| `http_route` | `VARCHAR` | `log_attributes["http.route"]`. |
| `exception_type` | `VARCHAR` | `log_attributes["exception.type"]`. |

## Spans

Canonical fields from `otlp2records`:

| Column | Type | Notes |
| --- | --- | --- |
| `timestamp` | `TIMESTAMP` | Span start time. |
| `end_timestamp` | `BIGINT` | Span end time in milliseconds. |
| `duration` | `BIGINT` | Duration in milliseconds. |
| `trace_id` | `VARCHAR` | Hex trace id. |
| `span_id` | `VARCHAR` | Hex span id. |
| `parent_span_id` | `VARCHAR` | Hex parent span id. |
| `trace_state` | `VARCHAR` | W3C trace state. |
| `span_name` | `VARCHAR` | Operation name. |
| `span_kind` | `INTEGER` | OTel span kind enum. |
| `status_code` | `INTEGER` | OTel status code. |
| `status_message` | `VARCHAR` | OTel status message. |
| `service_name` | `VARCHAR` | Resource `service.name`. |
| `service_namespace` | `VARCHAR` | Resource `service.namespace`. |
| `service_instance_id` | `VARCHAR` | Resource `service.instance.id`. |
| `scope_name` | `VARCHAR` | Instrumentation scope name. |
| `scope_version` | `VARCHAR` | Instrumentation scope version. |
| `scope_attributes` | `VARCHAR` | JSON string. |
| `span_attributes` | `VARCHAR` | JSON string. |
| `resource_attributes` | `VARCHAR` | JSON string. |
| `events_json` | `VARCHAR` | JSON string. |
| `links_json` | `VARCHAR` | JSON string. |
| `dropped_attributes_count` | `INTEGER` | OTel dropped count. |
| `dropped_events_count` | `INTEGER` | OTel dropped count. |
| `dropped_links_count` | `INTEGER` | OTel dropped count. |
| `flags` | `INTEGER` | Span flags. |

Derived promoted fields:

| Column | Type | Source |
| --- | --- | --- |
| `deployment_environment` | `VARCHAR` | `resource_attributes["deployment.environment"]`. |
| `http_method` | `VARCHAR` | `span_attributes["http.request.method"]` or legacy equivalent. |
| `http_status_code` | `INTEGER` | `span_attributes["http.response.status_code"]` or legacy equivalent. |
| `http_route` | `VARCHAR` | `span_attributes["http.route"]`. |
| `exception_type` | `VARCHAR` | Span event exception or `span_attributes["exception.type"]`. |

## Gauge Metrics

Canonical fields from `otlp2records`:

| Column | Type | Notes |
| --- | --- | --- |
| `timestamp` | `TIMESTAMP` | Data point timestamp. |
| `start_timestamp` | `BIGINT` | Start of measurement window in milliseconds. |
| `metric_name` | `VARCHAR` | Metric name. |
| `metric_description` | `VARCHAR` | Metric description. |
| `metric_unit` | `VARCHAR` | Unit. |
| `value` | `DOUBLE` | Metric value. |
| `service_name` | `VARCHAR` | Resource `service.name`. |
| `service_namespace` | `VARCHAR` | Resource `service.namespace`. |
| `service_instance_id` | `VARCHAR` | Resource `service.instance.id`. |
| `resource_attributes` | `VARCHAR` | JSON string. |
| `scope_name` | `VARCHAR` | Instrumentation scope name. |
| `scope_version` | `VARCHAR` | Instrumentation scope version. |
| `scope_attributes` | `VARCHAR` | JSON string. |
| `metric_attributes` | `VARCHAR` | JSON string. |
| `flags` | `INTEGER` | Data point flags. |
| `exemplars_json` | `VARCHAR` | JSON string. |

Derived promoted fields:

| Column | Type | Source |
| --- | --- | --- |
| `deployment_environment` | `VARCHAR` | `resource_attributes["deployment.environment"]`. |

## Sum Metrics

`metric_sum` includes every `metric_gauge` canonical and derived field, plus:

| Column | Type | Notes |
| --- | --- | --- |
| `aggregation_temporality` | `INTEGER` | `1 = delta`, `2 = cumulative`. |
| `is_monotonic` | `BOOLEAN` | OTel monotonic flag. |

## Partitioning

Default:

- Partition by `event_date`.
- Do not partition by `service_name` in v0.
- Consider hourly physical layout only after benchmark evidence shows day files are too broad.

If DuckLake partition-drop behavior is not cheap enough, switch to physical day tables behind stable views.

## Schema Evolution Rules

- Adding nullable derived promoted columns is allowed.
- Renaming or removing canonical `otlp2records` columns is not allowed in v0.
- Attribute JSON fields remain strings; extraction happens at insert time for the small promoted set.
- Store unknown fields only inside the existing JSON attribute columns.

