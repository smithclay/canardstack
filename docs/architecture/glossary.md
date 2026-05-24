# Canardstack Core Vocabulary

One written definition per core term, so names map 1:1 to concepts. The "Unit"
column says whether the term is an INGRESS unit (what arrives over OTLP/HTTP), a
STORAGE unit (what canardstack persists), or a QUERY concept (what the
compatibility query surface uses), where that distinction applies.

## Signal vocabulary

| Term (type) | Unit | Definition |
| --- | --- | --- |
| request kind (`OtlpRequestKind`, `src/ingest/mod.rs`) | INGRESS | The OTLP request, one of `logs` / `traces` / `metrics`. This is the per-signal label `request_kind` on the ingest/raw-spool metric surface. It is also the dimension the raw spool is **partitioned** by: one durable writer per request kind, with per-kind capacity. A request is spooled and checkpointed under one request-kind identity even when it fans out to several storage signals, so a new storage signal reuses its request kind's existing writer; only a new request kind adds one. |
| storage signal (`StorageSignal`, `src/signal.rs`) | STORAGE | One physical signal per DuckLake table: `logs` / `spans` / `metric_gauge` / `metric_sum`. A single `metrics` request kind fans out into both `metric_gauge` and `metric_sum` storage signals; histograms are rejected in v0. |
| metric subtype (`MetricSignal`, `src/query/plan.rs`) | QUERY | The query-side metric distinction `gauge` / `sum`. It maps onto a `StorageSignal` via `MetricSignal::storage_signal()` (`Gauge -> MetricGauge`, `Sum -> MetricSum`). |

## Data units along the pipeline

| Term (type) | Definition |
| --- | --- |
| raw record (`spool::Record`, `src/ingest/spool/mod.rs`) | The durably-spooled raw OTLP request: exactly what the client sent, fsynced to the local raw spool before any transform. At-least-once delivery is anchored on this record. |
| pending raw record (`PendingRawRecord`, `src/ingest/raw_spool.rs`) | A raw record recovered from the local raw spool because it has not been checkpointed. Startup replay hands these records back to the ingest pipeline without appending a second raw-spool copy. |
| replay-backed record ref (`ReplayBackedRecordRef`, `src/ingest/raw_spool.rs`) | The checkpoint identity carried by normal ingest rows after they enter the Arrow write buffer: the request kind it was spooled under (which is also its raw-spool writer) and the raw record id. Seal checkpoints these refs only after DuckLake commit. |
| Arrow row batch (`RecordBatch`, a "batch") | The transformed columnar unit produced by `otlp2records`, grouped by storage signal. The columnar form of a chunk of rows; do not use this sense of "batch" for a raw-spool record or worker handoff. |
| replay-backed Arrow batch (`ReplayBackedArrowBatch`, `src/storage/mod.rs`) | A storage-buffer input produced from a durably-spooled OTLP request. It must carry a replay-backed record ref. |
| raw-spool group-commit batch (`AppendBatch`, `AppendBatchStats`, `CheckpointBatchStats`, `src/ingest/spool/mod.rs`) | A writer-thread durability unit that groups append or checkpoint commands for one fsync policy decision. It is internal to the raw spool and is unrelated to an Arrow row batch. |
| internal telemetry commit (`Storage::commit_operator_metrics_snapshot`, `src/storage/arrow_write_buffer.rs`) | A separate storage path for sanctioned operator self-telemetry. It commits directly to DuckLake and does not enter the replay-backed ingest write buffer. |
| write buffer (`ArrowWriteBuffer`, `src/storage/arrow_write_buffer.rs`) | The in-memory, per-storage-signal accumulator that coalesces replay-backed Arrow row batches before a seal flushes them. Every buffered unit came from a durably-spooled external request and carries a replay-backed record ref. |
| row | A single logical record inside a batch or DuckLake table (one log line, one span, one metric data point). |

## Operations

| Term | Definition |
| --- | --- |
| flush | Moving the write buffer's accumulated batches into DuckDB via the Arrow appender. |
| seal | The scheduler operation owned by `SealDriver` / `seal::run` (`src/seal.rs`): decide when a flush is due, reserve seal admission, snapshot typed buffered rows, flush and commit them to DuckLake, then checkpoint exactly the replay-backed raw-spool refs from that committed snapshot. Snapshot-before-flush is load-bearing for at-least-once. |
| checkpoint | Disposing of a raw-spool record — after a successful DuckLake commit, or after a terminal rejection — so it will not replay on restart. |

## Reserved term

| Term | Definition |
| --- | --- |
| queue | RESERVED: a bounded `mpsc` channel ONLY. In v0 that is exactly the raw-spool writer command channel (`RAW_SPOOL_WRITER_QUEUE_CAPACITY`) and the ingest worker handoff channel (`canardstack_ingest_worker_queue_capacity`), plus their channel timing (`queued_at`, `queue_seconds`) and the `outcome="queued"` handoff outcome. Never use "queue" for in-flight bytes, the write buffer, freshness debt, or a generic work list. |

## Related freshness terms

These are not units but appear alongside the vocabulary above; the canonical
freshness projection formula and its `FreshnessBudgetInputs` fields live in
`src/admission_control.rs` and are mirrored in
[v0-architecture.md](v0-architecture.md).

- in-flight bytes: accepted-but-not-yet-buffered request bytes (`inflight_bytes`,
  plus `incoming_bytes` for the current request), drained by the seal pipeline.
- buffer debt: write-buffer bytes/age beyond the configured buffer target and
  max age (`buffered_bytes`, `buffered_active_count`, `oldest_buffer_age_seconds`).
- visibility debt: the max of projected seal seconds and projected buffer
  seconds; ingest rejects when this exceeds the freshness budget band.
- freshness lag: observed query visibility delay from committed storage
  watermarks, distinct from event-time age.
- seal-rate EWMA (`ewma_seal_bytes_per_second`): the single observed seal
  throughput estimate that drains both the in-flight seal debt and the
  buffer-size debt.
