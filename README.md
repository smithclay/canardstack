<h1>
  <img src="canardstack.png" alt="canardstack logo" height="48" align="left" />
  canardstack
</h1>

[![Crates.io](https://img.shields.io/crates/v/canardstack)](https://crates.io/crates/canardstack)
[![CI](https://github.com/smithclay/canardstack/actions/workflows/ci.yml/badge.svg)](https://github.com/smithclay/canardstack/actions/workflows/ci.yml)
[![License: Apache 2.0](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![DuckLake](https://img.shields.io/badge/storage-DuckLake-fff000.svg?logo=duckdb&logoColor=black)](https://ducklake.select/)
[![OpenTelemetry](https://img.shields.io/badge/OTLP-HTTP-425cc7.svg?logo=opentelemetry&logoColor=white)](https://opentelemetry.io/)

> OpenTelemetry logs, traces, and metrics stored in DuckLake, visualized in Grafana.

canardstack is an experimental project that makes it possible to stream OpenTelemetry data to [DuckLake](https://ducklake.select/), a lakehouse standard from the creators of duckdb. The project goal is to explore cheap and simple ways to query terabytes of observability data *from a single node* stored in open formats on object storage.

Builds on prior work from [otlp2parquet](https://github.com/smithclay/otlp2parquet), [otlp2pipeline](https://github.com/smithclay/otlp2pipeline), and [duckdb-otlp](https://github.com/smithclay/duckdb-otlp).

## Contents

- [Quickstart](#quickstart)
- [Demo](#demo)
- [What You Can Do](#what-you-can-do)
- [Differences From ClickStack](#differences-from-clickstack)
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
cargo install --locked canardstack

# starts server on :4318
canardstack
```

In another terminal, send one OTLP/HTTP JSON log:

```bash
OTLP_TIME_UNIX_NANO="$(date +%s)000000000"
curl -sS -X POST http://127.0.0.1:4318/v1/logs \
  -H 'Authorization: Bearer dev-canardstack-key' \
  -H 'Content-Type: application/json' \
  --data "{\"resourceLogs\":[{\"resource\":{\"attributes\":[{\"key\":\"service.name\",\"value\":{\"stringValue\":\"quickstart\"}}]},\"scopeLogs\":[{\"logRecords\":[{\"timeUnixNano\":\"${OTLP_TIME_UNIX_NANO}\",\"body\":{\"stringValue\":\"hello world\"}}]}]}]}"
```

canardstack acknowledges ingest after the raw request is fsynced locally. Give
the scheduler a few seconds to register the log with the DuckLake catalog, then
query it through the Loki-compatible API:

```bash
curl -sS -H 'Authorization: Bearer dev-canardstack-key' \
  'http://127.0.0.1:4318/loki/api/v1/query?query=%7Bservice_name%3D%22quickstart%22%7D'
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

## Differences from ClickStack

ClickStack is the production-grade observability stack built around
ClickHouse, HyperDX, and an OpenTelemetry Collector.

canardstack is a narrower experiment with different tradeoffs:

- Storage is DuckLake over DuckDB, not ClickHouse. Telemetry lands in open
  DuckLake tables backed by Parquet data files, so DuckDB-native clients can
  inspect the same data directly.
- The Grafana-facing APIs are compatibility adapters, not the primary query
  path. canardstack implements bounded Prometheus, Loki, and Tempo subsets;
  it does not try to match ClickStack's HyperDX UI or query experience.
- Deployment is intentionally small: one Rust binary, one synchronous HTTP
  server, one DuckDB process per role, and no async runtime, Kafka, or separate
  hot store.
- Ingest durability is local-spool-first and at-least-once. A `2xx` means the
  raw request was fsynced and accepted for bounded processing, not that rows are
  already query-visible.
- Direct SQL access. Local clients can attach the
  DuckLake catalog directly, and cloud deployments can expose the catalog over
  Quack for DuckDB-native clients when the operator chooses to manage that
  access boundary.
- The scope is intentionally single-tenant and experimental.

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

The current high-level data-flow diagram lives in the
[site architecture guide](https://smithclay.github.io/canardstack/architecture/).

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
