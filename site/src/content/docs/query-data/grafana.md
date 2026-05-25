---
title: Grafana Datasources
description: Connect Grafana to canardstack metrics, logs, and traces.
---

canardstack speaks enough Prometheus, Loki, and Tempo for Grafana to inspect
stored telemetry. It is one endpoint with three faces:

- metrics use the Prometheus datasource
- logs use the Loki datasource
- traces use the Tempo datasource

The Docker Compose stack already provisions all three. Start it with:

```bash
docker compose up
```

Grafana listens on `http://localhost:3000`. Use `admin/admin` if Grafana asks
for credentials.

## Provisioned datasources

The bundled datasource file is
`config/grafana/provisioning/datasources/canardstack.yaml`.

It creates:

| Grafana name | Type | URL from inside Compose | UID |
| --- | --- | --- | --- |
| `Canardstack Prometheus` | Prometheus | `http://canardstack:4318` | `canardstack-prometheus` |
| `Canardstack Loki` | Loki | `http://canardstack:4318` | `canardstack-loki` |
| `Canardstack Tempo` | Tempo | `http://canardstack:4318` | `canardstack-tempo` |

Each datasource sends:

```text
Authorization: Bearer ${CANARDSTACK_API_KEY}
```

Compose sets `CANARDSTACK_API_KEY` to `dev-canardstack-key` unless you override
it.

## Add datasources manually

Use these settings when Grafana runs outside the bundled Compose stack.

For metrics:

- Type: `Prometheus`
- URL: the canardstack HTTP endpoint, for example `http://localhost:4318`
- Custom HTTP header: `Authorization`
- Header value: `Bearer dev-canardstack-key`
- HTTP method: `GET`

For logs:

- Type: `Loki`
- URL: the same canardstack HTTP endpoint
- Custom HTTP header: `Authorization`
- Header value: `Bearer dev-canardstack-key`
- Max lines: keep this bounded; the bundled datasource uses `1000`

For traces:

- Type: `Tempo`
- URL: the same canardstack HTTP endpoint
- Custom HTTP header: `Authorization`
- Header value: `Bearer dev-canardstack-key`
- Search: enabled
- Streaming search and metrics: disabled

For trace-to-logs, point Tempo at the canardstack Loki datasource and filter by
trace ID. The bundled datasource uses a five-minute window on each side of the
span.

## What works

The query APIs are compatibility subsets.

Metrics support bare metric names, simple selectors, equality label filters, and
basic aggregations such as `avg`, `min`, `max`, `sum`, `count`, and `rate` over
one selector.

Logs support stream selectors such as `{service_name="checkout"}`, equality
label filters, time bounds, limits, direction, and simple text contains filters
with `|= "text"`.

Traces support trace lookup by ID and search over fields such as `service.name`,
`name`, `http.route`, `status.code`, and `traceID`.

canardstack is not a full PromQL, LogQL, TraceQL, Prometheus, Loki, or Tempo
implementation. Keep dashboards simple, bounded, and explicit.

## Smoke checks

After sending telemetry, these checks should return successful JSON responses:

```bash
curl -sS -H 'Authorization: Bearer dev-canardstack-key' \
  http://127.0.0.1:4318/api/v1/labels

curl -sS -H 'Authorization: Bearer dev-canardstack-key' \
  http://127.0.0.1:4318/loki/api/v1/label/service_name/values

curl -sS -H 'Authorization: Bearer dev-canardstack-key' \
  http://127.0.0.1:4318/api/search/tags
```

The bundled dashboard is:

```text
http://localhost:3000/d/canardstack-overview/canardstack-overview
```
