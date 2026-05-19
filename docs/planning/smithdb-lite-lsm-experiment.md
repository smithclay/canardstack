# SmithDB-Lite LSM Experiment Plan

This is a proof-gated plan for a single-binary Canardstack LSM-shaped ingest and
query path:

```text
HTTP -> durable raw spool -> transform lanes -> immutable Parquet segments
  -> DuckLake-backed segment planning -> progressive queries
```

This is not a distributed SmithDB port. Keep the current constraints: one Rust
binary, synchronous std-library HTTP, DuckDB/DuckLake storage, bounded
compatibility APIs, no async runtime, no DataFusion, no Vortex, no Kafka, no
gRPC, no Postgres requirement, and no second long-running service.

## Current End-to-End Direction

The clean architecture is narrower than the early scouts:

```text
HTTP -> optional durable raw spool -> transform/admission -> immutable Parquet
  -> DuckLake registration -> logical DuckLake SQL -> compatibility response
```

DuckLake metadata is allowed to shape bounded logical predicates, such as a
newest-candidate timestamp lower bound for Loki `query_range`. Canardstack
should not serve compatibility queries by opening raw Parquet file paths.
DuckDB/DuckLake should plan and execute reads from registered logical tables.

The committed Loki `query_range` path gets newest DuckLake candidate metadata,
derives a candidate window, queries the logical `logs` table, and returns the
normal Loki response shape. Forward Loki ranges and instant log queries still
use the full logical table query shape. Direct raw-Parquet shadow execution was
useful as a scout, but it is now retired from active code paths.

## Current Ingest Lifecycle

1. The std-library HTTP server accepts a connection, parses one request, checks
   bounded request/header/body limits, and routes OTLP paths.
2. Ingest validates API key, content type, compressed size, storage readiness,
   dependency health, and runtime memory admission.
3. The request thread decompresses the body, decodes OTLP, transforms through
   `otlp2records`, and validates timestamp skew.
4. The request thread converts transformed Arrow batches into pending queue
   batches, reserves peak runtime memory, and enqueues into the bounded
   in-process queue map.
5. A `202` currently means accepted into process memory, not durably committed.
6. Scheduler or admin flush takes the single flush lock, drains per-signal
   batches, coalesces them, and appends to per-signal immutable buffers.
7. Immutable buffer sealing splits by DuckLake timestamp partition, writes and
   fsyncs Parquet segment files, then registers them through
   `ducklake_add_data_files` in one writer transaction.
8. Query-visible freshness advances only after registered data files are visible
   to DuckLake-backed compatibility queries; metadata discovery refresh is a
   later scheduler job.

## Durable Raw Spool Fit

A raw spool would sit after cheap HTTP validation and before decompression or
OTLP transform. It would store the exact accepted request unit needed for
replay:

- signal route
- content type and content encoding
- compressed body bytes
- accepted-at time
- request byte length
- optional checksum and sequence id

The spool should not store transformed Arrow or DuckLake file membership. Its
job is crash recovery before transform/enqueue, not query planning.

Recovery state machine:

```text
open segment -> append record -> fsync record/segment -> ack 202
  -> transform started -> transform committed to memory queue
  -> segment checkpointed -> segment reclaimable
```

Crash recovery replays fsynced records whose checkpoint is not complete. The
first implementation should prove bounded replay, duplicate policy, disk-full
backpressure, and shutdown drain behavior before changing the public `202`
contract.

If `202` changes to mean durably spooled, ingest must change in these places:

- runtime memory admission must no longer be the only accepted-work admission;
  disk bytes and spool segment count need separate limits and `429` behavior
- storage dependency health must include spool directory writability/fsync
- response body acknowledgement must stop saying process-memory-only
- recovery must run before scheduler flush work can assume all accepted data is
  represented in memory queues
- metrics must distinguish spooled, replayed, transformed, enqueued, sealed,
  registered, and query-visible rows/bytes

## DuckLake Metadata Proof Gate

The first implementation is an admin-only DuckLake file metadata probe:

```text
GET /api/admin/storage/ducklake-files?table=logs&limit=100
```

It reads DuckLake metadata tables for current registered data files and exposes
planner-relevant facts:

- file path: `ducklake_data_file.path`
- table/signal: `ducklake_table.table_name`
- snapshot visibility: current files have `ducklake_data_file.end_snapshot IS
  NULL`; `begin_snapshot` and snapshot time are exposed
- partition values: `ducklake_file_partition_value`; table-level partition
  transforms are available separately through `ducklake_partition_column`
- row count: `ducklake_data_file.record_count`
- file size: `ducklake_data_file.file_size_bytes`
- timestamp min/max: `ducklake_file_column_stats` for the `timestamp` column
- delete-file awareness: active `ducklake_delete_file` counts and delete rows
- newest-first ordering: `timestamp_max DESC`, then snapshot and file id

Passing this gate means DuckLake can replace a custom manifest for registered
segment/file membership. A custom manifest remains disallowed unless a later
benchmark proves this metadata path is inaccessible, too slow, or unstable.

## Auxiliary Planner Index Scope

Auxiliary indexes are justified only for facts DuckLake metadata does not
provide:

- service-name sets or sketches per file
- trace-id bloom/sketch per file
- text-token sketches for log body filters
- large-field sidecar pointers
- freshness and backlog state spanning spool, memory queue, and DuckLake
- benchmark/debug timing breadcrumbs

These indexes must not duplicate file membership, snapshot visibility, row
count, file size, partition values, or timestamp min/max while DuckLake metadata
continues to provide them.

## Committed Loki Query Path

Backward Loki `query_range` is no longer an experimental sidecar. It uses
DuckLake metadata to choose newest candidate files for a bounded time range and
limit, converts that candidate set into a timestamp lower bound, then executes
one logical DuckLake query against `logs`. DuckDB/DuckLake owns physical file
planning and reads.

The serving path asks DuckLake for the full bounded candidate-file set, not
only `limit` files, because Loki label/text filters can make the newest files
nonmatching. It expands the candidate window until the requested row limit is
satisfied or the candidate set is exhausted. If timestamp min/max metadata is
missing for a candidate window, it keeps correctness by executing the same
logical query without the extra lower-bound predicate.

Retained diagnostics:

- `GET /api/admin/query/loki-candidates` returns the DuckLake candidate file
  list for Loki query-range params.
- `GET /api/admin/query/loki-progressive-explain` compares the full logical
  query shape with the candidate-window logical query shape through DuckDB
  `EXPLAIN` / `EXPLAIN ANALYZE`.

Proof gates passed:

1. Metadata probe returns planner facts for local DuckLake after flush.
2. Metadata probe latency stays small relative to the interactive query budget
   on benchmark data.
3. A benchmark-only candidate planner can list newest log files intersecting a
   time range without changing query results. Implemented locally; benchmark
   pressure validation showed low sidecar overhead, but default one-hour Loki
   ranges covered nearly all files in a 10-minute advancing-timestamp run.
4. Shadow progressive executor rows matched normal Loki rows under benchmark
   pressure, so the next proof could remove the double-query tax.
5. Authoritative progressive execution preserved Loki compatibility behavior
   and removed shadow fallback for measured requests.
6. The timer-fix and idle-reconnect proof passed under mixed ingest plus query
   pressure with no transport errors and low candidate execution timing.

Remaining proof gates:

- durable raw spool semantics for `202`
- explicit query-visible freshness controls under backlog
- auxiliary planner indexes only for facts DuckLake cannot provide

First shadow pressure result: a 120s logs-only mixed run matched normal Loki
rows for all 16 measured shadow executions and needed only 1 of 12 candidate
files for the final 100-row query. That strengthens the newest-first planning
thesis. It did not yet prove an execution win: the direct `read_parquet` shadow
path recorded roughly 895ms average sidecar execution and raised query tail
latency directionally versus the same-shape baseline. This was historical scout
evidence only; it did not become the serving design.

The next historical scout tested batched raw-file windows. The proof gate
stayed unchanged: normal Loki remained authoritative, shadow rows had to
match, and the benchmark had to show lower shadow execution timing or lower
total query pressure before progressive execution could serve compatibility
responses.

Batch-4 result: the batched shadow executor still matched normal Loki rows, but
average shadow execution stayed roughly flat versus one-file shadow while the
final query scanned 4 files instead of 1. This did not justify wiring raw file
execution as the response source. The next useful proof was to split shadow
timing into metadata planning versus execution and test a candidate-limited
logical DuckLake query shape before adding any auxiliary planner index.

Historical timing/logical scout: the shadow executor recorded separate
candidate-planning and candidate-execution phase timings. It uses DuckLake
metadata only to derive a newest-candidate timestamp lower bound, then queries
the normal DuckLake `logs` table with the narrowed time predicate. This keeps
execution in the logical DuckLake path while preserving exact Loki response
compatibility.

Logical-window result: all measured shadow executions matched normal Loki rows,
and candidate planning averaged only ~3.8ms. Candidate execution still averaged
~892ms, roughly the same as direct Parquet shadow. That points away from
DuckLake metadata as the bottleneck and toward the cost of running an extra
DuckDB query under mixed pressure. The next architecture proof should either
run candidate-limited execution as the only response source behind a stricter
experimental gate, or inspect DuckDB plans/profiling for why a 100-row newest
query over one candidate window still costs close to the full shadow query.

Historical authoritative scout: Loki `query_range` served from the DuckLake
candidate-window path instead of running the full query first. This was the
first proof that removed the shadow double-query tax while preserving the
public Loki response shape.

Authoritative result: the experimental path served all measured Loki requests
without fallback and improved observed query p50/p95 versus logical-window
shadow, but candidate execution still averaged about 900ms. This says the
architecture direction is coherent, while the next performance proof must
attack DuckDB query shape/plan cost rather than DuckLake metadata or double
execution alone.

DuckDB/DuckLake plan proof: the admin explain probe confirms the correct
boundary. Canardstack generated logical SQL against `logs`; DuckDB/DuckLake
planned physical reads. On benchmark data, the full logical query read 13 files
and 6256 rows, while the progressive logical-window query read 1 file and 311
rows. This is the desired architecture: use DuckLake metadata to derive bounded
logical predicates, then let DuckDB/DuckLake assemble the result. Raw Parquet
file execution should remain a retired scout, not a serving design.

Timer-fix result: rerunning the authoritative progressive query benchmark
after fixing query-timeout timer wakeup behavior dropped average candidate
execution timing from ~900ms to ~15ms while still scanning 1 of 12 candidate
files for the final Loki sample. The run still had ingest socket resets, so it
is not a clean pass, but it strongly explains the old mixed-pressure timing as
a timer/reporting artifact rather than DuckDB failing to prune the logical
query. The architecture proof should now move to reliability/freshness gates:
transport reset diagnosis, durable raw spool semantics, and query-visible
freshness under backlog.

Idle-reconnect result: after retiring stale persistent benchmark sockets before
the server read timeout, the same mixed-pressure proof passed with zero
transport errors. Candidate execution stayed in the low tens of milliseconds,
the candidate window scanned 1 of 12 files, and queue age stayed flat. This is
the commitment gate for making backward Loki `query_range` progressive by
default.

Committed default-path result: after removing the experimental Loki flag and
retired shadow code, the same mixed-pressure regression passed with backward
Loki `query_range` progressive by default. It served 16 measured progressive
queries, scanned 1 of 12 candidate files for the final sample, averaged ~14.5ms
candidate execution, had zero transport errors, and kept queue age flat. This
confirms the Brooksian end-to-end direction for Loki logs: metadata bounds a
logical query, and DuckDB/DuckLake executes the physical plan.
