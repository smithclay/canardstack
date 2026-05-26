---
title: Architecture
description: High-level canardstack data flow.
---

canardstack is one synchronous Rust process backed by one DuckDB process. It
accepts OTLP over HTTP, normalizes telemetry into Arrow record batches, commits
immutable Parquet segments through DuckLake, and serves bounded compatibility
query APIs over the same tables.

```mermaid
flowchart LR
    Apps["Apps / collectors"]
    Clients["Grafana / SQL clients"]

    subgraph Canardstack["canardstack (one process)"]
        direction TB
        Ingest["OTLP/HTTP ingest + validation"]
        Admission["freshness-first admission"]
        Spool["fsynced raw spool"]
        Workers["worker pool: OTLP to Arrow"]
        Buffer["Arrow write buffer"]
        Seal["scheduler seal"]
        Adapters["Prometheus / Loki / Tempo adapters"]

        Ingest --> Admission
        Admission --> Spool
        Spool --> Workers
        Workers --> Buffer
        Buffer --> Seal
    end

    Apps -->|logs / traces / metrics| Ingest
    Admission -.->|429 under pressure| Apps
    Seal -->|commit Parquet| Lake[("DuckLake catalog + Parquet files")]
    Adapters --> Lake
    Clients -->|queries| Adapters
```

For a deeper implementation map, use the
[repository architecture docs](https://github.com/smithclay/canardstack/tree/main/docs/architecture).
