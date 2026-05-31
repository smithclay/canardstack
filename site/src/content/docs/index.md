---
title: canardstack
description: Query OpenTelemetry-shaped logs, traces, and metrics stored in DuckLake.
hero:
  tagline: Query OpenTelemetry-shaped logs, traces, and metrics stored in DuckLake.
  image:
    file: ../../assets/canardstack.png
  actions:
    - text: Get Started
      link: /tutorials/local-observability-stack/
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
[`duckdb-otlp`](https://smithclay.github.io/duckdb-otlp/) to write
OpenTelemetry data into DuckLake tables, then point canardstack at that catalog
for query serving.

## Choose a path

| Need | Start here |
| --- | --- |
| Learn the local flow by running it | [Local observability stack tutorial](/tutorials/local-observability-stack/) |
| Run canardstack against existing data | [Serve an existing DuckLake catalog](/how-to/serve-ducklake/) |
| Populate DuckLake for canardstack | [duckdb-otlp documentation](https://smithclay.github.io/duckdb-otlp/) |
| Connect Grafana or query directly | [How-to guides](/how-to/connect-grafana/) |
| Look up exact contracts | [HTTP API reference](/reference/http-api/) |
| Understand the query-only design | [Query-only architecture](/explanation/query-only-architecture/) |
