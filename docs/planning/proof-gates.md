# Proof Gates Before V1 Claims

## Required Before Claiming 25-100 GB/day

- Docker/local quickstart starts in local DuckLake mode with one documented command.
- Docker/local smoke ingests representative OTLP fixtures through port `4318` and verifies Prometheus/Loki/Tempo compatibility results.
- Capped Docker Desktop sentinel benchmark records the configured CPU/memory envelope and passes at 25 GB/day ingest-only and mixed-query profiles.
- A real Linux VM 2-hour benchmark records instance/disk/object-store details and passes at the claimed GB/day volume, both ingest-only and with query interference.
- Local Docker volume persistence survives container restart and reset behavior is documented.
- OTLP/HTTP protobuf and JSON decode pass compatibility fixtures.
- `otlp2records` schemas are mapped without lossy column renames.
- Process memory admission control reliably returns `429` before OOM.
- DuckLake insert path survives process restart with expected best-effort loss window.
- Basic compatibility telemetry endpoints enforce time range, limit, timeout, memory, and concurrency.
- Day retention reclaims physical storage in local filesystem mode.
- Existing OpenTelemetry Collector can export to all three v0 ingest endpoints.

## Required Before Claiming 100-500 GB/day

- 2-hour and overnight mixed workload benchmarks at each claimed ladder step: 100 GB/day, then 250 GB/day, then 500 GB/day equivalent.
- Each claimed ladder step has both clean ingest-only evidence and ingest-plus-query-interference evidence under a documented CPU/memory envelope.
- Object storage mode benchmark, not local-only.
- DuckLake inlined data is bounded under sustained load.
- Inlined data reliably becomes Parquet through maintenance.
- Postgres catalog growth is measured and bounded.
- Maintenance catches up after a controlled pause.
- Retention physically reclaims storage after snapshot expiration and cleanup.
- Query OOM cannot take down ingest, or query role isolation is implemented.
- Broad query attempts are rejected or terminated within configured limits.
- Compatibility query load cannot starve ingest.

## Required Before Claiming 500 GB-1 TB/day Stretch

- Separate ingest, query, and maintenance process roles.
- Tuned batch sizes and storage-specific write concurrency.
- Proven recovery after object storage 5xx storm.
- Proven recovery after maintenance backlog.
- Proven query isolation under compatibility query fanout.
- Documented hardware and storage requirements.

## Explicit Non-Claims For V0

- Durable acknowledgement after `2xx`.
- Multi-tenant isolation.
- Arbitrary SQL safety for users.
- Unlimited cardinality.
- Unlimited retention.
- Sub-second freshness.
- ClickHouse-class ingest ceilings.

## Architecture Validation Checklist

- Process crash after `2xx`: data accepted into memory but not committed may be lost; this is documented and surfaced in ingest semantics.
- Overload: admission control returns `429` or `503` before unsafe memory, catalog, or storage pressure.
- Inlined data bounded: maintenance flush lag and inlined byte limits drive backpressure.
- Data becomes Parquet: DuckLake flush/checkpoint maintenance is measured and operator-visible.
- Retention reclaims storage: whole-day retention is followed by snapshot expiration and cleanup.
- Maintenance avoids starvation: singleton maintenance has low concurrency, max runtime, priorities, and pause controls.
- Broad queries avoid OOM: all compatibility telemetry endpoints enforce time, limit, timeout, memory, and concurrency.
- Collector integration: users configure upstream OTLP/HTTP exporter; gRPC users translate through Collector.
- Simplicity: v0 excludes Kafka, ClickHouse, custom hot store, bundled Collector, WAL, multi-tenancy, and arbitrary SQL.
- DuckLake risk: inlining, retention, object storage, and catalog growth are treated as proof gates.
