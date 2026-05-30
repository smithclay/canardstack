---
title: Operational Limits
description: Query limits and unsupported behavior.
---

canardstack is experimental and intentionally bounded.

## Query Bounds

Query paths are constrained by:

- time range
- row limit
- request timeout
- DuckDB memory limit
- query concurrency
- admission class

Broad scans can still be expensive. Keep dashboard panels explicit and bounded.

## Compatibility Scope

canardstack implements subsets of Prometheus, Loki, and Tempo APIs. It is not a
full PromQL, LogQL, TraceQL, Prometheus, Loki, or Tempo implementation.

## Unsupported

- OTLP ingest
- OTLP/gRPC
- Kafka
- arbitrary SQL over HTTP
- canardstack-owned raw spool durability
- canardstack-owned background sealing
- bundled `serve-catalog`
- histograms and exponential histograms through the compatibility APIs

Use direct DuckDB SQL for analysis outside the compatibility surface.
