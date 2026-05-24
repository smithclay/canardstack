# DuckLake write path experiment

Status: concluded. DuckDB Arrow append is now the single supported ingest write
path. The legacy Parquet registration path and temporary experiment flags were
removed after the Arrow append path passed correctness, smoke, and the
BENCHMARKING.md-shaped 10 minute mixed-query run.

## Hypothesis

DuckDB/DuckLake Arrow appends may reduce commit overhead and low-volume file
growth without weakening the raw-spool checkpoint contract. The experiment
confirmed that raw-spool durability remains separate from DuckLake commit
visibility: `202` still means the request was locally fsynced and accepted, and
raw-spool checkpointing still happens only after durable DuckLake commit.

## Historical gates

Scenarios compared:

1. Legacy Parquet registration, no physical file compaction.
2. Legacy Parquet registration plus DuckLake merge-adjacent.
3. DuckDB Arrow append, no physical file compaction.
4. DuckDB Arrow append plus DuckLake merge-adjacent.
5. Remote DuckLake smoke only when `CANARDSTACK_DUCKLAKE_ATTACH_URI` or the documented credentials are available.

Correctness gates:

- `cargo check` passes.
- Targeted config/write-routing tests pass.
- Smoke or integration path confirms ingest, seal/flush, query visibility, and row counts.
- Existing compatibility tests or smoke queries cover Prometheus, Loki, and Tempo payloads.
- Raw spool checkpointing remains after durable DuckLake commit.
- The temporary flags defaulted to then-current behavior while the experiment
  was active; they have since been removed.

Performance gates:

- Same input size and reset local data directory for every measured run.
- Capture rows ingested, wall time, rows/sec, phase timings, DuckLake file count, physical bytes, catalog size when readily measurable, and simple post-ingest query latency.
- Stop an appender scenario on any correctness failure.
- `duckdb_append` must reach at least 70% of baseline rows/sec or show a strong compensating win, such as greater than 80% fewer files without query regression.
- Query p95 should not be more than 25% worse than baseline after maintenance.
- File count must improve materially for low-volume/frequent-ingest workloads to justify further work.

## Commands run

- `cargo check`
- `cargo check --benches`
- `cargo test arrow_write_buffer_defaults_to_current_flush_policy`
- `cargo test config_file_values_load_before_env_overrides`
- `cargo test arrow_append_write_path_flushes_visible_rows -- --nocapture`
- `cargo test raw_spool_checkpoints_after_seal_commits_multirow_record`
- `cargo fmt --all -- --check`
- `cargo clippy --all-targets --all-features --locked -- -D warnings`
- `cargo bench --bench storage_pipeline -- --rows 5000 --iterations 6 --signal all --data-dir /private/tmp/canardstack-writepath-baseline`
- legacy Parquet registration plus merge-adjacent storage-pipeline run
- DuckDB Arrow append storage-pipeline run
- `CANARDSTACK_DATA_DIR=/private/tmp/canardstack-smoke-add cargo run -- smoke`
- `CANARDSTACK_DATA_DIR=/private/tmp/canardstack-smoke-append cargo run -- smoke`
- `CANARDSTACK_DATA_DIR=/private/tmp/canardstack-http-add CANARDSTACK_BIND=127.0.0.1:4331 cargo run --release -- serve`
- `cargo bench --bench throughput_iteration -- --base-url http://127.0.0.1:4331 --warmup 0s --duration 5s --profile ingest-only --signals all --ingest-concurrency 1 --connection-mode persistent --items-per-batch 128 --log-records 16 --log-body-bytes 512 --trace-spans 16 --trace-attribute-bytes 512 --metric-series 64 --metric-description-bytes 256 --progress-interval 5s --max-runtime 10s --no-queries --report-dir /private/tmp/canardstack-http-add-report`
- `CANARDSTACK_DATA_DIR=/private/tmp/canardstack-http-append CANARDSTACK_BIND=127.0.0.1:4332 cargo run --release -- serve`
- `cargo bench --bench throughput_iteration -- --base-url http://127.0.0.1:4332 --warmup 0s --duration 5s --profile ingest-only --signals all --ingest-concurrency 1 --connection-mode persistent --items-per-batch 128 --log-records 16 --log-body-bytes 512 --trace-spans 16 --trace-attribute-bytes 512 --metric-series 64 --metric-description-bytes 256 --progress-interval 5s --max-runtime 10s --no-queries --report-dir /private/tmp/canardstack-http-append-report`

## Results

Local storage workload: all four telemetry signals, 5,000 rows per signal per iteration, six iterations, fresh local data directory per scenario.

| Scenario | Status | Rows | Rows/sec | Wall seconds | Active data files | Physical bytes | Log query ms | Metric query ms | Notes |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| Legacy Parquet registration, no merge | passed | 120,000 | 619,062 | 0.193842 | 24 | 7,368,390 | 2.988 | 2.194 | Baseline. |
| Legacy Parquet registration, merge-adjacent | failed | n/a | n/a | n/a | n/a | n/a | n/a | n/a | First merge call returned zero processed files, then a later merge call failed with DuckDB internal error: `Attempted to dereference shared_ptr that is NULL!`. Stopped merge scenarios. |
| `duckdb_append`, no merge | passed | 120,000 | 834,247 | 0.143842 | 24 | 2,048,774 | 2.983 | 1.879 | Direct Arrow appender succeeded; fallback INSERT was not used. |
| `duckdb_append`, merge-adjacent | skipped | n/a | n/a | n/a | n/a | n/a | n/a | n/a | Skipped because merge-adjacent already failed a correctness gate. |
| Remote DuckLake smoke | skipped | n/a | n/a | n/a | n/a | n/a | n/a | n/a | No remote credentials were present. Needed env: `CANARDSTACK_DUCKLAKE_ATTACH_URI=md:<database>` with a valid MotherDuck token in the DuckDB/MotherDuck environment, or `CANARDSTACK_DUCKLAKE_ATTACH_URI=ducklake:<remote-uri>`. Set `CANARDSTACK_DUCKDB_EXTENSION_DIR` too if the extensions are not in DuckDB's default extension path. |

Smoke compatibility:

- Legacy Parquet registration smoke returned `202` for logs/traces/metrics and successful Prometheus, Loki, and Tempo payloads. Storage layout after smoke: 8 active data files, 68,166 physical bytes.
- `duckdb_append` smoke returned the same successful compatibility payloads. Storage layout after smoke: 2 active data files, 8,169 physical bytes. Logs and spans had zero active data files in DuckLake metadata, which indicates DuckLake inlining kicked in for this low-volume case.

Short HTTP ingest-only latency run:

| Scenario | Accepted requests | Accepted decoded B/s | Ingest p50 ms | Ingest p95 ms | Ingest p99 ms |
| --- | ---: | ---: | ---: | ---: | ---: |
| Legacy Parquet registration, no merge | 528 | 1,155,698 | 6.0 | 7.6 | 10.6 |
| `duckdb_append`, no merge | 529 | 1,157,702 | 6.0 | 7.1 | 9.1 |

The HTTP runs used `--no-queries`, so they measure request latency only. They were paced ingest-only runs and did not wait for all rows to become storage-visible inside the measured window; smoke covered visibility and compatibility.

10 minute mixed-query baseline after adopting Arrow append:

- Report: [report.json](/private/tmp/canardstack-arrow-cutover-400-10m-20260523/20260523T171602Z/report.json)
- Target: `400 GB/day` logs mixed-query, `4,629,630 B/s`.
- Actual: `4,631,120 B/s`.
- Statuses: `200:240`, `202:2890`, no `429`.
- Queries: `240/240`, transport errors `0`.
- Ingest p50/p95/p99: `10.6 / 14.4 / 17.2 ms`.
- Query p50/p95/p99: `82.1 / 97.8 / 102.0 ms`.

Correctness notes:

- The raw-spool checkpoint order remains unchanged: `seal::commit_buffered_rows`
  snapshots typed buffered rows, calls `storage.flush_arrow_write_buffer(true)`,
  observes the committed outcome, then checkpoints the replay-backed refs from
  that committed snapshot. The checkpoint regression test still passes.
- The temporary write-path and merge-adjacent flags have been removed.
- The direct appender path writes the existing prepared Arrow `RecordBatch` values into `canardlake.main.<table>` inside an explicit DuckDB transaction and records append timing separately from DuckLake commit timing.

## Decision

Arrow append became the architecture because it passed the local correctness
gates, smoke coverage for Prometheus/Loki/Tempo, the raw-spool checkpoint
regression, and the 10 minute mixed-query benchmark at the 400 GB/day logs
target with no 429s or transport errors. It also reduced physical bytes in the
storage workload and avoided the explicit file-registration machinery in
canardstack.

DuckLake merge-adjacent physical file compaction remains disabled. It failed a
correctness gate with a DuckDB internal error and should not become default
until `ducklake_merge_adjacent_files` is proven stable for the Arrow append
write pattern.
