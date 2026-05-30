---
title: canardstack
description: Query OpenTelemetry-shaped logs, traces, and metrics stored in DuckLake.
hero:
  tagline: Query OpenTelemetry-shaped logs, traces, and metrics stored in DuckLake.
  image:
    file: ../../assets/canardstack.png
  actions:
    - text: Get Started
      link: /get-started/
      icon: right-arrow
    - text: View on GitHub
      link: https://github.com/smithclay/canardstack
      icon: external
      variant: secondary
---

canardstack is an experimental query server for observability data stored in
[DuckLake](https://ducklake.select/). It exposes bounded Prometheus, Loki, and
Tempo-compatible HTTP APIs for Grafana-style clients.

Telemetry writes are handled outside canardstack. Use
[`duckdb-otlp`](https://github.com/smithclay/duckdb-otlp) in a DuckDB process to
write OpenTelemetry data into DuckLake tables, then point canardstack at that
catalog for query serving.

## Docs Map

| Need | Start here |
| --- | --- |
| Learn the local flow | [Get Started](/get-started/) |
| Serve an existing DuckLake catalog | [Serve DuckLake](/quickstart/serve/) |
| Do a specific task | [How-to guides](/guides/lakehouse-ingest/) |
| Look up exact contracts | [Reference](/reference/api/) |
| Understand the design | [Architecture](/architecture/) |
