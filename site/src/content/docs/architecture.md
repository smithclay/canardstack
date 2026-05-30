---
title: Architecture
description: High-level canardstack query-only data flow.
---

canardstack is one synchronous Rust process backed by DuckDB. It attaches to a
DuckLake catalog that another DuckDB process writes through `duckdb-otlp`, then
serves bounded compatibility query APIs over the visible tables.

```mermaid
flowchart LR
    Producers["Apps / OpenTelemetry Collectors"]
    Writer["DuckDB + duckdb-otlp"]
    Lake[("DuckLake tables")]
    Clients["Grafana / API clients"]

    subgraph Canardstack["canardstack query server"]
        direction TB
        Http["std-library HTTP server"]
        Auth["auth + validation"]
        Admission["query admission"]
        Adapters["Prometheus / Loki / Tempo adapters"]
        DuckDB["DuckDB query connections"]

        Http --> Auth
        Auth --> Admission
        Admission --> Adapters
        Adapters --> DuckDB
    end

    Producers -->|"OTLP/HTTP"| Writer
    Writer -->|"append / flush"| Lake
    Clients -->|"bounded query APIs"| Http
    DuckDB -->|"SQL over attached catalog"| Lake
```

The binary does not include OTLP ingest, gRPC, Kafka, ingest durability, table
maintenance, or a bundled DuckLake catalog service. Those concerns belong to
the DuckDB writer process and the catalog deployment chosen around DuckLake.

For a deeper implementation map, use the
[repository architecture docs](https://github.com/smithclay/canardstack/tree/main/docs/architecture).
