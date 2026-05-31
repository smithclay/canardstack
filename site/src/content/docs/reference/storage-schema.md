---
title: Storage Schema Reference
description: DuckLake table contract expected by canardstack.
---

canardstack reads the tables written by `duckdb-otlp`.

| Table | Event time column | Signal |
| --- | --- | --- |
| `otlp_logs` | `time_unix_nano` | OTLP log records |
| `otlp_traces` | `start_time_unix_nano` | OTLP spans |
| `otlp_metrics_gauge` | `time_unix_nano` | OTLP gauge datapoints |
| `otlp_metrics_sum` | `time_unix_nano` | OTLP sum datapoints |

## Logs

Required columns include:

```text
time_unix_nano, observed_time_unix_nano, trace_id, span_id, service_name,
service_namespace, service_instance_id, severity_number, severity_text,
event_name, body, resource_attributes, scope_name, scope_version,
scope_attributes, log_attributes, dropped_attributes_count, flags
```

## Traces

Required columns include:

```text
start_time_unix_nano, duration_time_unix_nano, trace_id, span_id,
parent_span_id, trace_state, service_name, service_namespace,
service_instance_id, name, kind, status_code, status_status_message,
resource_attributes, scope_name, scope_version, scope_attributes,
span_attributes, events_json, links_json, dropped_attributes_count,
dropped_events_count, dropped_links_count, flags
```

## Gauge Metrics

Required columns include:

```text
time_unix_nano, start_time_unix_nano, name, description, unit, int_value,
double_value, service_name, service_namespace, service_instance_id,
resource_attributes, scope_name, scope_version, scope_attributes,
metric_attributes, flags, exemplars_json
```

## Sum Metrics

Required columns include all gauge metric columns plus:

```text
aggregation_temporality, is_monotonic
```

Trace and span IDs are lowercase hex strings. Attribute columns are JSON strings.
There are no canardstack-owned `ingested_at` or `source_format` columns.
