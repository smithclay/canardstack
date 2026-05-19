# Canardstack V0 Architecture

Canardstack is a single-binary OTLP/HTTP observability backend. It accepts
OpenTelemetry logs, traces, gauge metrics, and sum metrics; normalizes them with
`otlp2records`; stores immutable Parquet segments registered in DuckLake; and
serves bounded Prometheus, Loki, and Tempo compatibility APIs.

## Boundaries

Canardstack keeps these constraints:

- One Rust binary named `canardstack`.
- One synchronous standard-library HTTP server.
- One DuckDB process with DuckLake attached.
- OTLP/HTTP JSON and protobuf ingest only.
- No async runtime, OTLP/gRPC, Kafka, separate hot store, bundled Collector,
  DataFusion, Vortex, arbitrary SQL HTTP API, or second long-running service.

DuckLake/DuckDB is the source of truth for registered telemetry files and query
execution. Compatibility APIs must not plan or read raw Parquet files directly,
and canardstack must not maintain a custom manifest duplicating DuckLake file
membership.

Metrics are supported for ingest and bounded compatibility behavior, but the
current sustained MVP performance envelope is only claimed for logs and traces.

## Data Flow

```text
OTLP/HTTP (JSON or protobuf, optional gzip)
  -> request validation
  -> local fsync raw spool
  -> inline decode and otlp2records transform
  -> bounded in-process queues
  -> immutable Parquet segment files
  -> ducklake_add_data_files registration
  -> logical DuckLake SQL compatibility queries
```

```mermaid
flowchart LR
  A["OTLP/HTTP exporter"] --> B["HTTP parser and auth"]
  B --> C["Size, content-type, compression, timestamp checks"]
  C --> D["Fsync raw request to local spool"]
  D --> E["Decode OTLP and transform with otlp2records"]
  E --> F["Bounded per-signal queues"]
  F --> G["Flush worker"]
  G --> H["Immutable Parquet segment seal"]
  H --> I["DuckLake registration"]
  I --> J["Raw spool checkpoint"]
  I --> K["Logical DuckLake SQL"]
  K --> L["Prometheus / Loki / Tempo adapters"]
```

## Ingest Semantics

A successful ingest response is `202`.

`202` means:

- The API key, content type, body size, compression, and timestamp skew passed.
- The compressed raw request was fsynced into the local raw spool.
- The request was accepted for at-least-once processing.

`202` does not mean:

- The rows are DuckLake-committed.
- The rows are query-visible.
- Exactly-once delivery is guaranteed.

At-least-once duplicate window:

- If the process crashes after DuckLake storage commit but before raw-spool
  checkpoint, restart replay can duplicate that raw request.
- Exactly-once or request-id dedupe is post-MVP.

Retryable failure behavior:

- `429 raw_spool_full` when the durable raw-spool byte budget is exhausted.
- `429` for queue, process-memory, or runtime-memory pressure.
- `503 raw_spool_unavailable` when the local spool cannot be opened, written,
  or fsynced.
- `503 dependency_unhealthy` when storage is unavailable.

## Raw Spool

The raw spool sits after cheap request validation and before decompression or
transform. It stores exactly the accepted request unit needed for replay:

- signal route
- content type
- optional content encoding
- accepted timestamp
- compressed body bytes
- sequence id and checksum

Recovery sequence:

```text
open segment -> append record on raw-spool writer
  -> fsync when group-commit count or delay is reached -> return 202
  -> transform/enqueue -> DuckLake storage commit
  -> checkpoint raw-spool record -> segment reclaimable
```

Startup replays uncheckpointed fsynced records before scheduler work starts.
Replay enters the same decode, transform, queue, flush, and DuckLake commit path
as normal ingest.

The raw-spool writer is on the ingest acknowledgement path. It batches appends
and checkpoints up to `CANARDSTACK_RAW_SPOOL_GROUP_COMMIT_RECORDS` records, or
until `CANARDSTACK_RAW_SPOOL_GROUP_COMMIT_MS` elapses from the first record in
the group. These are capacity knobs, not cosmetic settings: too small and
storage spends the ingest budget on fsyncs; too large and `202` acknowledgement
latency rises even when downstream queues are healthy. `0ms` is rejected at
startup so operators do not accidentally disable batching and return to
per-request fsync behavior.

Main knobs:

- `CANARDSTACK_RAW_SPOOL_DIR`
- `CANARDSTACK_RAW_SPOOL_MAX_SEGMENT_BYTES`
- `CANARDSTACK_RAW_SPOOL_MAX_RECORD_BYTES`
- `CANARDSTACK_RAW_SPOOL_MAX_TOTAL_BYTES`
- `CANARDSTACK_RAW_SPOOL_WRITER_QUEUE_CAPACITY`
- `CANARDSTACK_RAW_SPOOL_GROUP_COMMIT_RECORDS`
- `CANARDSTACK_RAW_SPOOL_GROUP_COMMIT_MS`

## Queues And Flush

Ingest transforms inline on the HTTP request thread, then admits Arrow
`RecordBatch`es into bounded in-process queues. Queue ownership is intentionally
low-cardinality: signal plus source encoding.

Memory and queue guardrails:

- `CANARDSTACK_MAX_BODY_BYTES`, default 8 MiB.
- `CANARDSTACK_PER_SIGNAL_QUEUE_BYTES`, default 512 MiB per signal.
- `CANARDSTACK_PROCESS_INGEST_BYTES`, default 2 GiB.
- Optional `CANARDSTACK_RUNTIME_MEMORY_LIMIT_BYTES`.

Flush triggers:

- `CANARDSTACK_MAX_ROWS_PER_FLUSH`, default 5,000 rows.
- `CANARDSTACK_MAX_BYTES_PER_FLUSH`, default 4 MiB.
- `CANARDSTACK_MAX_FLUSH_AGE_SECS` or `_MS`, default 10 seconds.
- `CANARDSTACK_HIGH_PRESSURE_FLUSH_AGE_SECS` or `_MS`, default 2 seconds.

Flush drains queues, coalesces batches, appends them to immutable segment
buffers, seals due Parquet files, registers those files in DuckLake, and then
checkpoints the corresponding raw-spool records.

## Storage

DuckLake is the storage coordinator. It owns snapshots, registered data-file
membership, partition metadata, and table visibility. Canardstack writes
immutable Parquet segment files and registers them through
`ducklake_add_data_files`.

Tables:

- `logs`
- `spans`
- `metric_gauge`
- `metric_sum`
- `metadata_summary`

DuckLake inlined telemetry rows should normally remain zero. Segment sizing is
controlled by:

- `CANARDSTACK_IMMUTABLE_SEGMENT_TARGET_BYTES`
- `CANARDSTACK_IMMUTABLE_SEGMENT_MAX_AGE_SECS` or `_MS`

Local DuckLake is the default. Remote DuckLake attach URIs are supported, but
the core architecture remains the same.

## Query Path

All HTTP compatibility queries go through `QueryEngine`, which applies:

- time-range bounds
- row/result limits
- server-owned timeout
- DuckDB memory limit
- query concurrency limits

Prometheus, Loki, and Tempo adapters execute bounded logical SQL against
DuckLake tables. DuckDB/DuckLake owns physical file planning and reads.

Direct SQL is intentionally outside the normal HTTP API. Operators or users who
need SQL should use DuckDB CLI, MotherDuck, or another SQL client against the
same DuckLake catalog.

## Operator Surface

Public health:

- `GET /healthz`

Admin health:

- `GET /api/admin/health/storage`
- `GET /api/admin/health/ingest`
- `GET /api/admin/health/maintenance`
- `GET /api/admin/health/queries`

Metrics:

- `GET /metrics`

The operator surface distinguishes:

- accepted requests
- raw-spooled records and bytes
- pending replay records and bytes
- queued rows and bytes
- flushed and sealed rows
- DuckLake-visible rows and files
- checkpointed raw-spool records
- raw-spool full
- raw-spool unavailable
- storage unavailable

See [operator metrics](operator-metrics.md) and
[failure runbooks](../runbooks/failure-runbooks.md) for the concrete metric and
endpoint names.

## Maintenance

The API binary starts one in-process scheduler unless
`CANARDSTACK_SCHEDULER_ENABLED=false`.

Scheduler jobs:

- queue watchdog / due flush
- metadata refresh
- operator metric snapshot
- DuckLake inlined-data flush
- retention
- snapshot expiration and cleanup when supported by DuckLake

Maintenance can be paused and resumed through admin endpoints. There is no
Postgres-backed maintenance lease yet, so assume one in-process scheduler and
one writer.

## Retention

Retention is whole-day oriented and bounded by configured horizons:

- logs: `CANARDSTACK_LOGS_RETENTION_DAYS`, default 14
- spans: `CANARDSTACK_SPANS_RETENTION_DAYS`, default 14
- metrics: `CANARDSTACK_METRICS_RETENTION_DAYS`, default 30

The current implementation uses bounded table deletes plus DuckLake snapshot
expiration/cleanup hooks where available. Physical day-table layouts are not
part of the current MVP.

## Compatibility Surface

Prometheus:

- `GET/POST /api/v1/query`
- `GET/POST /api/v1/query_range`
- `GET /api/v1/labels`
- `GET /api/v1/label/{name}/values`
- `GET /api/v1/series`
- `GET /api/v1/metadata`

Loki:

- `GET /loki/api/v1/query_range`
- `GET /loki/api/v1/query`
- `GET /loki/api/v1/labels`
- `GET /loki/api/v1/label/{name}/values`
- `GET /loki/api/v1/series`

Tempo:

- `GET /api/v2/traces/{traceID}`
- `GET /api/traces/{traceID}`
- `GET /api/search`
- `GET /api/search/tags`
- `GET /api/search/tag/{tag}/values`
- `GET /api/v2/search/tags`
- `GET /api/v2/search/tag/{tag}/values`

Grafana probe shim:

- `GET /api/status/buildinfo`

These are compatibility subsets, not complete protocol implementations.
Unsupported query forms return protocol-shaped error envelopes.
