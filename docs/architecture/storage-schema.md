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
| `ingested_at` | `TIMESTAMP` | Server receive/commit time for freshness diagnostics. |
| `source_format` | `VARCHAR` | `otlp_proto` or `otlp_json`. |

Raw telemetry tables are partitioned from event time using the canonical
`timestamp` column. Do not add a separate day column to the raw tables.

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

Compatibility labels such as `deployment_environment`, `http_method`, and
`http_route` are derived from `resource_attributes` and `log_attributes` in
bounded query or metadata-refresh paths. They are not physical telemetry-table
columns.

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

Compatibility labels such as `deployment_environment`, `http_method`,
`http_status_code`, `http_route`, and `exception_type` are derived from
`resource_attributes` and `span_attributes` in bounded query or
metadata-refresh paths. They are not physical telemetry-table columns.

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

Compatibility labels such as `deployment_environment` are derived from
`resource_attributes` in bounded query or metadata-refresh paths. They are not
physical telemetry-table columns.

## Sum Metrics

`metric_sum` includes every `metric_gauge` canonical field, plus:

| Column | Type | Notes |
| --- | --- | --- |
| `aggregation_temporality` | `INTEGER` | `1 = delta`, `2 = cumulative`. |
| `is_monotonic` | `BOOLEAN` | OTel monotonic flag. |

## Metadata Summary

Discovery endpoints use one shared `metadata_summary` table instead of scanning
raw telemetry for every Grafana label, series, metric metadata, and Tempo tag
lookup. The table is durable in both local DuckDB and DuckLake modes.

| Column | Type | Purpose |
| --- | --- | --- |
| `signal` | `VARCHAR` | `logs`, `spans`, `metric_gauge`, or `metric_sum`. |
| `event_date` | `DATE` | Daily summary bucket derived from telemetry event time. |
| `kind` | `VARCHAR` | `label_value`, `series`, `metric_metadata`, or `tag_value`. |
| `name` | `VARCHAR` | Label, tag, metric, or series name. |
| `value` | `VARCHAR` | Label/tag value when applicable. |
| `metric_type` | `VARCHAR` | Prometheus metadata type (`gauge` or `counter`). |
| `metric_unit` | `VARCHAR` | Representative metric unit. |
| `metric_description` | `VARCHAR` | Representative metric help text. |
| `service_name` | `VARCHAR` | Series dimension derived during metadata refresh. |
| `deployment_environment` | `VARCHAR` | Series dimension derived during metadata refresh. |
| `severity_text` | `VARCHAR` | Loki stream dimension derived during metadata refresh. |
| `row_count` | `BIGINT` | Rows represented by this summary entry. |
| `first_seen` | `TIMESTAMP` | Earliest event timestamp in the summary entry. |
| `last_seen` | `TIMESTAMP` | Latest event timestamp in the summary entry. |

Committed inserts record their affected `(signal, event_date)` buckets as dirty.
The `metadata_refresh` scheduler job drains that set, rebuilding each bucket's
summary rows from canonical columns and JSON attribute extraction; a failed
refresh re-queues the buckets for the next tick. Keeping the day-partition scan
off the commit path stops it from blocking the writer on every seal. An
in-process generation counter, bumped after each committed refresh, lets bounded
discovery caches invalidate.

## Partitioning

Default:

- Partition telemetry tables in DuckLake by
  `year(timestamp), month(timestamp), day(timestamp)`.
- Do not partition by `service_name` in v0.
- Immutable segment files may be pre-split by timestamp day/hour before
  registration, but DuckLake partition pruning should use the configured
  timestamp transforms, not a duplicated raw-table date column.

If DuckLake partition-drop behavior is not cheap enough, switch to physical day tables behind stable views.

## Schema Evolution Rules

- Adding physical promoted telemetry columns is not allowed in v0; derive those
  labels from canonical `otlp2records` columns instead.
- Renaming or removing canonical `otlp2records` columns is not allowed in v0.
- Attribute JSON fields remain strings; extraction happens in bounded query or
  metadata-refresh paths for the small compatibility label set.
- Store unknown fields only inside the existing JSON attribute columns.
- The current schema contract is proven for fresh DuckLake catalogs. In-place
  migration of catalogs that already contain older promoted telemetry columns is
  not yet a proven path and should be gated separately before reuse.
