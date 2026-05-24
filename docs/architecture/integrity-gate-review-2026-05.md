# Architectural integrity gate review — 2026-05

**Scope:** OTLP → DuckLake streaming ingest path (receipt → durable raw spool →
transform → Arrow write buffer → seal → DuckLake commit).
**Reviewers:** two independent passes, synthesized.
**Method:** inspection-only. No code was changed and no tests were run. All line
citations were verified against the working tree at review time; line numbers may
drift as the code evolves — treat the named concept/function as the anchor.

## Verdict: PASS WITH WATCHLIST

Feature work may proceed; no architectural decision must be revisited before the
next feature cycle. The most likely next feature (histograms) exercises exactly
the expansion path the `OtlpRequestKind` × `StorageSignal` vocabulary was built
for: add a `StorageSignal` variant + a column set + an `otlp2records` mapping, and
it reuses the existing `metrics` raw-spool writer (adding a `StorageSignal` does
not add a writer).

Two of the four watch items below were raised independently by both reviewers
(concordant) — the strongest signal that they are real and not a single lens's
artifact.

## Core design choices to preserve

- The two-axis vocabulary `OtlpRequestKind` (3 ingress kinds) × `StorageSignal`
  (4 physical tables), joined by `OtlpRequestKind::storage_signals()`. Never
  conflate the two; keep `request_kind` and `storage_signal` distinct as metric
  labels and partitioning dimensions.
- `VisibilityDebt` as the single freshness-first admission primitive: every
  admission decision (ingest, cheap/heavy query, seal) is a pure policy over one
  debt-in-seconds vs the configured SLA. Keep `InflightBytes` as *accounting that
  feeds the debt*, not a second ceiling.
- Commit-then-checkpoint ordering in the seal: checkpoint the raw spool only
  after the DuckLake `COMMIT`. This is load-bearing for at-least-once; the
  duplicate-on-replay hazard is named (`SealStage::DuplicateRisk`), not hidden.

## Watchlist

### 1. Static schema / version fence under typed-field pressure — confidence: HIGH (both reviewers)

- **Concept:** schema-evolution boundary.
- **Evidence:** static columns, no online migration, JSON for everything unmapped
  (`src/storage/schema.rs`, module doc). Fail-closed boot fence
  (`enforce_schema_version_on`): boot aborts below `MIN_COMPATIBLE_SCHEMA_VERSION`
  or above `SCHEMA_VERSION`. Today `MIN_COMPATIBLE == SCHEMA_VERSION == 1`, so the
  fence is at its strictest and the expand/contract (schema-on-read) path is
  designed but not yet exercised by any real change.
- **3-year cost:** the first feature needing a promoted typed column (histograms,
  schema-rich query features) forces an up-front decision with no tooling —
  additive (keep `MIN_COMPATIBLE` low, but there is no schema-on-read code yet) or
  breaking (bump both, stop-the-world catalog migration under continuous ingest).
- **What to watch:** whether feature requests keep landing safely in JSON
  attributes, or start demanding typed columns. The latter is the trigger to build
  the migration path. Do not pre-build it.

### 2. Best-effort buffer ingress if it spreads — confidence: MEDIUM (both reviewers)

- **Concept:** durability class at the storage-buffer ingress.
- **Evidence:** `BestEffortArrowBatch` carries no replay ref and can never be
  checkpointed/replayed (`src/storage/mod.rs`); it shares the same buffer
  machinery as replay-backed ingest via `BufferDurability`
  (`src/storage/arrow_write.rs`); the sole sanctioned production caller is operator
  self-telemetry (`Metrics::write_snapshot_to_storage`, `src/metrics.rs`). The
  invariant is enforced by convention + `#[doc(hidden)]`, not by the type system.
- **3-year cost:** any externally meaningful data later routed through this path
  (recording rules, rollups, downsampling, backfill) silently opts out of the
  at-least-once model.
- **What to watch:** the first new caller of the best-effort path. That is the
  moment to make the durability class a type-level distinction, not a doc comment.

### 3. Flush/commit vocabulary around the checkpoint boundary — confidence: LOW today → MEDIUM on first new write path

- **Concept:** seal durability-boundary legibility.
- **Evidence:** three distinct "flush" meanings on one path —
  `seal::run` flush = "append + commit" (`src/seal.rs`);
  `flush_arrow_write_buffer` = the durable DuckLake `COMMIT`
  (`src/storage/arrow_write_buffer.rs`);
  `appender.flush()` = an in-memory appender drain that runs *inside the
  transaction, before* `COMMIT` (`src/storage/arrow_write_buffer.rs`).
  Raw-spool checkpointing becomes legal only after the third event commits. Today
  the ordering is correctly centralized in one function
  (`commit_buffered_rows_with`) and loudly documented, so current risk is low.
- **3-year cost:** future compaction or an alternate write path could obscure
  which "flush" makes checkpointing legal, risking a checkpoint-before-commit
  regression.
- **What to watch:** any change adding a committing write path — pair it with
  renaming the three flushes to distinct verbs (seal / commit / appender-drain).

### 4. Small files / no compaction under continuous arrival — confidence: MEDIUM (single-sourced)

- **Concept:** write amplification / storage growth.
- **Evidence:** `ducklake_merge_adjacent_files` deliberately disabled; only the
  Arrow-write-buffer size/age coalescing bounds file count
  (`src/maintenance.rs`, retention block).
- **3-year cost:** the one risk that materializes with zero code change — pure
  time. Unbounded Parquet file count and catalog metadata growth degrade query
  planning over years.
- **What to watch:** segment-count and query-planning-latency trends; the roadmap
  entry in `v0-architecture.md` already names the revisit condition.

> **Items 3 and 4 share a trigger.** Introducing compaction is both the most
> likely cause of the checkpoint-legibility risk (3) and the resolution for the
> storage-growth risk (4). When compaction is scheduled, treat them as one
> coordinated change.

## Below the watchlist bar (non-blocking)

- `scheduler_jobs` health JSON omits `seal`, the most important scheduler duty
  (`src/maintenance.rs`, `Maintenance::health`) — cheap one-line observability fix.
- `signal_index()` + `Transformed`'s four named fields are hand-rolled parallels
  to `StorageSignal::ALL`; bounded and pinned by the
  `stored_columns_align_with_otlp2records_output` test.
- Caller-runs inline fallback couples HTTP connection threads to ingest throughput
  under worker saturation (`src/ingest/mod.rs`, `dispatch_ingest_work`) —
  intentional, documented, bounded (no writer lock on that path).
