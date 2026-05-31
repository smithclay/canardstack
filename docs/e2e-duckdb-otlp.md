# End-to-End Local DuckDB OTLP Smoke

This smoke validates the query-only shape:

```text
duckdb-otlp OTLP/HTTP writer -> DuckLake tables -> canardstack Loki query
```

It expects a sibling `duckdb-otlp` checkout with release artifacts already built:

```text
../duckdb-otlp/build/release/duckdb
../duckdb-otlp/build/release/extension/otlp/otlp.duckdb_extension
```

Run it from the canardstack repository root:

```bash
scripts/e2e-duckdb-otlp-local.py
```

The harness:

1. Starts the local DuckDB CLI with the unsigned `otlp.duckdb_extension`.
2. Attaches a temporary local DuckLake catalog.
3. Starts `otlp_serve(...)` on a random localhost port.
4. Posts `../duckdb-otlp/test/data/logs_simple.jsonl` to `/v1/logs`.
5. Flushes and stops the OTLP writer.
6. Starts `cargo run -- serve` against the same DuckLake catalog.
7. Calls Loki `query_range` and asserts the three sample log bodies are returned.

Use `--keep-temp` to keep the temporary DuckLake directory and process logs:

```bash
scripts/e2e-duckdb-otlp-local.py --keep-temp
```

The local file-backed catalog cannot be attached by both DuckDB processes at
once because DuckDB holds an exclusive lock on the metadata file. The smoke
therefore flushes and stops the local writer before starting canardstack. Remote
or service-backed catalog deployments can keep the writer and query server as
separate long-running processes.

The equivalent manual writer setup is:

```bash
../duckdb-otlp/build/release/duckdb -unsigned /tmp/canardstack-e2e/writer.duckdb
```

```sql
LOAD '../duckdb-otlp/build/release/extension/otlp/otlp.duckdb_extension';
INSTALL ducklake;
LOAD ducklake;

ATTACH 'ducklake:/tmp/canardstack-e2e/metadata.ducklake' AS lake
  (DATA_PATH '/tmp/canardstack-e2e/data/');

SELECT *
FROM otlp_serve(
  'otlp:localhost:4318',
  catalog := 'lake',
  token := 'dev-token-123456'
);
```

Then start canardstack against the same catalog:

```bash
CANARDSTACK_DUCKLAKE_ATTACH_URI=ducklake:/tmp/canardstack-e2e/metadata.ducklake \
CANARDSTACK_DUCKLAKE_DATA_PATH=/tmp/canardstack-e2e/data/ \
CANARDSTACK_API_KEY=dev-canardstack-key \
cargo run -- serve --listen localhost:4320
```
