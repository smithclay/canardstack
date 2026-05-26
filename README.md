<h1>
  <img src="canardstack.png" alt="canardstack logo" height="48" align="left" />
  canardstack
</h1>

[![CI](https://github.com/smithclay/canardstack/actions/workflows/ci.yml/badge.svg)](https://github.com/smithclay/canardstack/actions/workflows/ci.yml)
[![License: Apache 2.0](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![DuckLake](https://img.shields.io/badge/storage-DuckLake-fff000.svg?logo=duckdb&logoColor=black)](https://ducklake.select/)
[![OpenTelemetry](https://img.shields.io/badge/OTLP-HTTP-425cc7.svg?logo=opentelemetry&logoColor=white)](https://opentelemetry.io/)
[![Deploy to AWS](https://img.shields.io/badge/deploy-AWS%20CloudFormation-ff9900.svg?logo=amazonwebservices&logoColor=white)](https://console.aws.amazon.com/cloudformation/home#/stacks/create/review?stackName=canardstack&templateURL=https%3A%2F%2Fraw.githubusercontent.com%2Fsmithclay%2Fcanardstack%2Fmain%2Fdeploy%2Faws%2Fecs-express%2Ftemplate.yaml)

> OpenTelemetry logs, traces, and metrics stored in DuckLake, visualized in Grafana.

canardstack is an experimental observability backend that stores data in [DuckLake](https://ducklake.select/), an open-standard lakehouse format from the creators of duckdb. Inspired by [ClickStack](https://clickhouse.com/docs/use-cases/observability/clickstack), the project goal is to explore cheap and simple ways to store and query terabytes of observability data *from a single node*.

It accepts OpenTelemetry logs, traces, gauge metrics, and sum metrics over OTLP/HTTP, stores normalized tables in [DuckLake](https://ducklake.select/), and exposes query APIs for Grafana to visualize the data.

Builds on prior work from [otlp2parquet](https://github.com/smithclay/otlp2parquet), [otlp2pipeline](https://github.com/smithclay/otlp2pipeline), and [duckdb-otlp](https://github.com/smithclay/duckdb-otlp).

## Contents

- [Quickstart](#quickstart)
- [Demo](#demo)
- [What You Can Do](#what-you-can-do)
- [Send Telemetry](#send-telemetry)
- [Query Data](#query-data)
- [Deployment](#deployment)
- [Architecture](#architecture)
- [Operations](#operations)
- [Limits](#limits)
- [For Developers](#for-developers)
- [Documentation](#documentation)

## Quick Start

Install and start canardstack. With no options, it uses local DuckLake storage
under `.canardstack` and listens for OTLP data on `127.0.0.1:4318`.

```bash
# requires rust toolchain: `curl https://sh.rustup.rs -sSf | sh`
# also assumes you have duckdb installed (`brew install duckdb`)
cargo install --locked canardstack
canardstack
```

In another terminal, send one OTLP/HTTP JSON log:

```bash
curl -sS -X POST http://127.0.0.1:4318/v1/logs \
  -H 'Authorization: Bearer dev-canardstack-key' \
  -H 'Content-Type: application/json' \
  --data '{"resourceLogs":[{"scopeLogs":[{"logRecords":[{"timeUnixNano":"1779667200000000000","body":{"stringValue":"hello world"}}]}]}]}'
```

canardstack acknowledges ingest after the raw request is fsynced locally. Give
the scheduler a few seconds to sync the log into DuckLake, then query it directly:

```bash
duckdb
```

```sql
INSTALL ducklake;
LOAD ducklake;
ATTACH 'ducklake:.canardstack/canardstack.ducklake' AS canardlake
  (DATA_PATH '.canardstack/storage');
USE canardlake;

SELECT * FROM logs;
```

## Demo

Run canardstack with the full
[OpenTelemetry demo](https://github.com/open-telemetry/opentelemetry-demo) using
the [demo guide](https://smithclay.github.io/canardstack/demo/).

## What You Can Do

Use canardstack to:

- Receive OTLP/HTTP logs, traces, gauge metrics, and sum metrics.
- Store normalized telemetry in DuckLake-backed DuckDB tables.
- Inspect data in Grafana through Prometheus, Loki, and Tempo-compatible APIs.
- Query the same DuckLake data directly from DuckDB, MotherDuck, or another SQL
  client.
- Run local experiments with a single Rust binary and one DuckDB process.

canardstack is best suited for local, single-tenant, or experimental deployments
where the operator wants direct access to lakehouse telemetry data and can
accept the current v0 durability and compatibility limits.

## Send Telemetry

Configure OTLP/HTTP producers and OpenTelemetry Collectors with the
[send telemetry guide](https://smithclay.github.io/canardstack/deployment/send-telemetry/).

## Query Data

- [Query with DuckDB](https://smithclay.github.io/canardstack/query-data/duckdb/)
- [Query with Grafana](https://smithclay.github.io/canardstack/query-data/grafana/)

## Deployment

- [Deployment overview](https://smithclay.github.io/canardstack/deployment/)
- [Send telemetry](https://smithclay.github.io/canardstack/deployment/send-telemetry/)
- [MotherDuck](https://smithclay.github.io/canardstack/deployment/motherduck/)
- [GCP Cloud Run](https://smithclay.github.io/canardstack/deployment/gcp-cloud-run/)
- [AWS ECS/Fargate](https://smithclay.github.io/canardstack/deployment/aws-ecs-fargate/)

## Architecture

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

## Operations

Operator notes, configuration guidance, diagnostics, and failure response
runbooks live in the [operations docs](https://smithclay.github.io/canardstack/operations/).

## Limits

canardstack is experimental and not production-ready.

Known v0 limits:

- Current single-node throughput is bounded by raw-spool append and backlog
  behavior. On May 20, 2026, the highest clean 10-minute mixed-signal run was
  `2000 GB/day` with `--ingest-concurrency 64` (`23.1 MB/s` accepted decoded
  throughput, no `429`/`503` or query failures). A `2500 GB/day` mixed run
  reached Vector-like log event rates briefly, but failed the 10-minute
  guardrail with `429` queue-pressure responses after roughly eight minutes.
- No exactly-once ingest acknowledgement. A crash after `2xx` should replay a
  fsynced raw-spool record if it was not checkpointed, but duplicate replay can
  occur when storage commit succeeds before raw-spool checkpoint.
- No OTLP/gRPC endpoint. Use an OpenTelemetry Collector if your clients need
  gRPC.
- No histograms or exponential histograms.
- No multi-tenancy.
- No full PromQL, LogQL, TraceQL, Prometheus, Loki, or Tempo implementation.
- No arbitrary SQL through compatibility APIs.
- No sub-second freshness target.

## For Developers

Contributor setup and implementation details live in
[docs/developer.md](docs/developer.md). Start there when changing canardstack
itself.

## Documentation

- [Developer guide](docs/developer.md)
- [Deployment overview](https://smithclay.github.io/canardstack/deployment/)
- [V0 architecture](docs/architecture/v0-architecture.md)
- [Storage schema](docs/architecture/storage-schema.md)
- [Query API](docs/architecture/query-api.md)
- [Operator metrics](docs/architecture/operator-metrics.md)
- [Operations](https://smithclay.github.io/canardstack/operations/)
- [Benchmarking](docs/BENCHMARKING.md)
- [Benchmark gates](docs/planning/benchmark.md)
- [Failure runbooks](https://smithclay.github.io/canardstack/operations/failure-runbooks/)

## Acknowledgements

Thanks to @hanorigins, Tyler Hillery, and @decalek from the DuckDB Discord for
starting a discussion that led to this proof of concept.
