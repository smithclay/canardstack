---
title: Send Telemetry
description: Configure OTLP/HTTP producers and OpenTelemetry Collectors to send data to canardstack.
---

canardstack accepts logs, traces, gauge metrics, and sum metrics over the
standard OTLP/HTTP paths. OTLP/HTTP protobuf is preferred over JSON for
performance.

- `POST /v1/logs`
- `POST /v1/traces`
- `POST /v1/metrics`

## OpenTelemetry Collector

Point an OpenTelemetry Collector `otlphttp` exporter at canardstack:

```yaml
exporters:
  otlphttp/canardstack:
    endpoint: http://localhost:4318
    headers:
      Authorization: Bearer dev-canardstack-key
```

Route traces, logs, and metrics through that exporter:

```yaml
service:
  pipelines:
    traces:
      receivers: [otlp]
      exporters: [otlphttp/canardstack]
    logs:
      receivers: [otlp]
      exporters: [otlphttp/canardstack]
    metrics:
      receivers: [otlp]
      exporters: [otlphttp/canardstack]
```

## Local Smoke Workload

For a local proof without changing an app, use the bundled smoke workload:

```bash
docker compose run --rm smoke
```

The smoke workload sends logs, a multi-span trace, gauge samples, and cumulative
sum samples through OTLP/HTTP, then checks the storage health and compatibility
query paths.
