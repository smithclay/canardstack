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

- [Quickstart](#quick-start)
- [Demo](#demo)
- [Why canardstack?](#why-canardstack)
- [Send Telemetry](#send-telemetry)
- [Query Data](#query-data)
- [Deployment](#deployment)
- [Architecture](#architecture)
- [Operations](#operations)
- [Limits](#limits)
- [For Developers](#for-developers)
- [Documentation](#documentation)

## Quick Start

You can get started by installing canardstack with `cargo` and connecting to a local Ducklake catalog using the [new Quack protocol](https://duckdb.org/quack/).


```bash
# requires rust toolchain: `curl https://sh.rustup.rs -sSf | sh`
cargo install --locked canardstack

# starts server on 127.0.0.1:4318 with a Ducklake catalog accessible over Quack
CANARDSTACK_DUCKLAKE_QUACK_TOKEN=dev-quack-token canardstack serve --local-catalog
```

In another terminal, send one OTLP/HTTP JSON log:

```bash
OTLP_TIME_UNIX_NANO="$(date +%s)000000000"
curl -sS -X POST http://127.0.0.1:4318/v1/logs \
  -H 'Authorization: Bearer dev-canardstack-key' \
  -H 'Content-Type: application/json' \
  --data "{\"resourceLogs\":[{\"resource\":{\"attributes\":[{\"key\":\"service.name\",\"value\":{\"stringValue\":\"quickstart\"}}]},\"scopeLogs\":[{\"logRecords\":[{\"timeUnixNano\":\"${OTLP_TIME_UNIX_NANO}\",\"body\":{\"stringValue\":\"hello world\"}}]}]}]}"
```

canardstack acknowledges ingest after the raw request is fsynced locally. By default, the scheduler seals buffered rows within about 10 seconds; wait a moment, then attach to the Ducklake in duckdb 1.5.3+ or higher:

```bash
duckdb --version
duckdb
```

```sql
INSTALL ducklake;
LOAD ducklake;
INSTALL quack;
LOAD quack;

CREATE OR REPLACE SECRET canardstack_ducklake_quack
  (TYPE quack, SCOPE 'quack:127.0.0.1:9494', TOKEN 'dev-quack-token');

ATTACH 'ducklake:quack:127.0.0.1:9494' AS canardlake;
USE canardlake;

SELECT * FROM logs;
┌─────────────────────┬────────────────────────────┬───────────────┬───┬──────────────────┬────────────────┐
│      timestamp      │        ingested_at         │ source_format │ … │ scope_attributes │ log_attributes │
│      timestamp      │         timestamp          │    varchar    │ … │     varchar      │    varchar     │
├─────────────────────┼────────────────────────────┼───────────────┼───┼──────────────────┼────────────────┤
│ 2026-05-27 00:14:12 │ 2026-05-27 00:14:12.086916 │ otlp_json     │ … │ NULL             │ NULL           │
└─────────────────────┴────────────────────────────┴───────────────┴───┴──────────────────┴────────────────┘
```

You just streamed an OTLP log to Ducklake! For a more comprehensive example, see the demo below or review cloud [deployment examples](https://smithclay.github.io/canardstack/deployment/).

## Demo

Run canardstack with the full [OpenTelemetry demo](https://github.com/open-telemetry/opentelemetry-demo) using
the [demo guide](https://smithclay.github.io/canardstack/demo/).

![Grafana dashboard showing canardstack OpenTelemetry demo data](site/src/assets/grafana-dash-demo.png)

## Why canardstack?

canardstack's goal is to make it easy and cheap for anyone to store and query terabytes of observability data on cheap hardware using vendor-neutral stanards with few moving pieces. There are many high-quality open-source observability solutions like [ClickStack](https://clickhouse.com/docs/use-cases/observability/clickstack) and [SigNoz](https://github.com/SigNoz/signoz), canardstack's primary difference is the deep integration with duckdb/DuckLake and (intentionally) simple single-node architecture.

See the [overview docs](https://smithclay.github.io/canardstack/) for more information.

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
[architecture guide in docs](https://smithclay.github.io/canardstack/architecture/).

## Operations

Operator notes, configuration guidance, diagnostics, and failure response
runbooks live in the [operations docs](https://smithclay.github.io/canardstack/operations/).

## Limits

canardstack is experimental and not production-ready. The schema may drift and query performance is uncertain.

Other limits:

- Current single-node throughput is in the ballpark of `23.1 MB/s` accepted decoded
  throughput without `429`/`503` or query failures.
- Data weirdness possible: A crash after a `2xx` should replay a
  fsynced raw-spool record if it was not checkpointed, but duplicate replay can
  occur when storage commit succeeds before raw-spool checkpoint.
- No histograms or exponential histograms metric support yet.
- No full PromQL, LogQL, TraceQL, Prometheus, Loki, or Tempo implementation.
- It takes time between data being accepted for it to appear in the DuckLake, this is configurable but at least several seconds.

## For Developers

Contributor setup and implementation details live in
[docs/developer.md](docs/developer.md). Start there when changing canardstack
itself.

## Documentation

Docs are published at [https://smithclay.github.io/canardstack/](https://smithclay.github.io/canardstack/).

## Acknowledgements

Thanks to @hanorigins, Tyler Hillery, and @decalek from the DuckDB Discord for
starting a discussion that led to this proof of concept.
