# Transform worker pool experiment

Date: 2026-05-18

## Proposal

Prototype an opt-in bounded synchronous transform worker pool for OTLP/HTTP ingest:

- HTTP request threads keep cheap validation, dependency health checks, and bounded admission.
- A bounded queue holds accepted compressed request bodies by request count and compressed bytes.
- Worker threads perform decompression, `otlp2records` transform, timestamp-skew validation, and enqueue into the existing ingest queues.
- `2xx` with the experiment enabled would mean accepted into bounded process memory, not durably committed and not necessarily transformed yet.

This keeps the single-binary synchronous architecture and is reversible because the default path remains inline.

## Fast-Fail Evidence

The prototype was benchmarked in OrbStack Linux VMs with release `--features otlp2records-observer`, fresh data dirs before each run, and identical 3m measured / 15s warmup ingest-only shapes.

| Signal | Target | Disabled actual | Enabled actual | 429s | Result |
| --- | ---: | ---: | ---: | ---: | --- |
| logs | 2000 GB/day | 23,143,397 B/s | 23,137,954 B/s | none | flat/slightly lower |
| spans | 1500 GB/day | 17,356,014 B/s | 17,358,526 B/s | none | flat |

Report paths:

- `target/canardstack-bench/transform-pool-fastfail/disabled-logs-ingestonly/20260518T025900Z/report.json`
- `target/canardstack-bench/transform-pool-fastfail/enabled-logs-ingestonly-w2/20260518T030307Z/report.json`
- `target/canardstack-bench/transform-pool-fastfail/disabled-spans-ingestonly/20260518T030704Z/report.json`
- `target/canardstack-bench/transform-pool-fastfail/enabled-spans-ingestonly-w2/20260518T031047Z/report.json`

The enabled runs used `CANARDSTACK_TRANSFORM_WORKERS=2`, `CANARDSTACK_TRANSFORM_QUEUE_BYTES=67108864`, and `CANARDSTACK_TRANSFORM_QUEUE_REQUESTS=1024`.

## Decision

Do not keep the transform worker pool experiment from this pass. The fast-fail ingest-only gate did not show a real directional signal, so running low-query validation would not be justified. Keeping the code would add a second bounded memory queue, post-accept failure accounting, and worker shutdown behavior without proof that it improves the mixed-query failure mode.

Rollback action: remove the prototype code and retain this note as evidence. Revisit only if a narrower experiment first shows material ingest-only headroom or a benchmark with server CPU sampling demonstrates that inline transform work is the limiting interference source.
