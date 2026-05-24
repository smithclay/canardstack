# Benchmarking

This document contains reusable local benchmark recipes for canardstack. Keep
entries concrete: include the command, full report path, target, result, and the
pass/fail criteria that future agents should compare against.

## 10 Minute Mixed-Query Smoke

Use this as the reusable performance smoke for ingest-topology or scheduler
changes when the machine can afford a 10 minute run.

## Mac Studio Logs Ratchet, 2026-05-24

Local ratchet on `Darwin cludio 25.5.0 arm64`, git `517048d`, with a dirty
worktree from a pre-existing untracked `.github/workflows/pages.yml`.

Shape: `logs`, `mixed-query`, persistent connections, `ingest-concurrency=16`,
`query-concurrency=2`, `query-pressure=medium`, `query-interval=5s`,
`warmup=5s`.

Confirmed 10 minute clean runs:

- `10000 GB/day`: pass, actual `115,740,893 B/s`, statuses `200:240`,
  `202:72227`, query `240/240`, ingest p50/p95/p99
  `8.5 / 11.0 / 15.0 ms`, query p50/p95/p99 `89.1 / 125.4 / 134.0 ms`,
  final freshness lag `0.5s`.
  [report.json](/private/tmp/canardstack-mac-ratchet-20260524T035608Z/confirm-10000-10m/20260524T043145Z/report.json)
- `20000 GB/day`: pass, actual `231,478,930 B/s`, statuses `200:240`,
  `202:144453`, query `240/240`, ingest p50/p95/p99
  `11.5 / 16.0 / 21.0 ms`, query p50/p95/p99 `80.0 / 117.0 / 128.3 ms`,
  final freshness lag `0.2s`.
  [report.json](/private/tmp/canardstack-mac-ratchet-20260524T035608Z/confirm-20000-10m/20260524T045421Z/report.json)
- `22500 GB/day`: pass, actual `260,411,925 B/s`, statuses `200:240`,
  `202:162509`, query `240/240`, ingest p50/p95/p99
  `11.6 / 16.1 / 24.3 ms`, query p50/p95/p99 `78.2 / 112.4 / 120.8 ms`,
  final freshness lag `0.2s`.
  [report.json](/private/tmp/canardstack-mac-ratchet-20260524T035608Z/confirm-22500-10m/20260524T050519Z/report.json)

Short probes after the sustained runs:

- Fresh `25000 GB/day`, `120s`: pass, actual `289,326,101 B/s`, statuses
  `200:48`, `202:36112`, query p95 `66.5 ms`, freshness lag `0.1s`.
  [report.json](/private/tmp/canardstack-mac-ratchet-20260524T035608Z/probe-25000-fresh-120s/20260524T050808Z/report.json)
- Cumulative `37500 GB/day`, `120s`: harness pass, actual `433,993,813 B/s`,
  statuses `200:48`, `202:54167`, no `429`, query p95 `169.7 ms`, which is
  above the `150 ms` investigation threshold.
  [report.json](/private/tmp/canardstack-mac-ratchet-20260524T035608Z/probe-37500-120s/20260524T051354Z/report.json)
- Cumulative `42500 GB/day`, `120s`: fail, actual `477,138,053 B/s` against
  target `491,898,148 B/s`, statuses `200:48`, `202:59552`, `429:1839`,
  query p95 `169.5 ms`.
  [report.json](/private/tmp/canardstack-mac-ratchet-20260524T035608Z/probe-42500-120s/20260524T051618Z/report.json)
- Cumulative `50000 GB/day`, `120s`: fail, actual `577,984,421 B/s` against
  target `578,703,704 B/s`, statuses `200:48`, `202:72144`, `429:81`.
  [report.json](/private/tmp/canardstack-mac-ratchet-20260524T035608Z/probe-50000-120s/20260524T051134Z/report.json)

Conclusion: the highest confirmed clean 10 minute logs mixed-query target is
`22500 GB/day`. A fresh `25000 GB/day` two minute probe passed, but no 10 minute
confirmation was run. The short-run admission edge under accumulated data is
between `37500 GB/day` and `42500 GB/day`; `37500 GB/day` already exceeds the
query p95 investigation threshold. Across the high probes, `raw_spool_append`
was the dominant measured phase.

## OrbStack Logs VM Ratchet, 2026-05-24

Run through `scripts/orbstack-bench-vms.sh` on profile `logs`, Ubuntu Questing
arm64 in OrbStack, kernel `7.0.5-orbstack-00330-ge3df4e19b0a0-dirty`, `14`
visible CPUs, `15Gi` RAM. The host could not reach `.orb.local` or the VM IPv4
from the sandbox, so the benchmark driver was run inside the VM against
`http://127.0.0.1:4318`.

Shape: `logs`, `mixed-query`, persistent connections, `ingest-concurrency=16`,
`query-concurrency=2`, `query-pressure=medium`, `query-interval=5s`,
`warmup=5s`.

Sustained 10 minute runs:

- `20000 GB/day`: pass, actual `231,479,609 B/s`, statuses `200:240`,
  `202:144452`, query `240/240`, ingest p50/p95/p99
  `6.3 / 19.0 / 190.9 ms`, query p50/p95/p99 `65.6 / 129.6 / 259.6 ms`,
  final freshness lag `0.2s`.
  [report.json](/Users/clay/workspace/canardstack/target/canardstack-bench/orbstack-logs-20000-10m/20260524T150618Z/report.json)
- `30000 GB/day`: fail, actual `344,895,998 B/s` against target
  `347,222,222 B/s`, statuses `200:240`, `202:215230`, `429:1058`, query
  `240/240`, ingest p50/p95/p99 `6.3 / 38.4 / 159.7 ms`, query p50/p95/p99
  `63.1 / 266.7 / 999.8 ms`, final freshness lag `0.2s`.
  [report.json](/Users/clay/workspace/canardstack/target/canardstack-bench/orbstack-logs-30000-10m/20260524T145522Z/report.json)

Short probes before the sustained runs:

- `1000`, `5000`, `10000`, `20000`, and `30000 GB/day` each passed `120s`
  harness probes without measured `429`.
- `40000 GB/day` was interrupted after measured `429` appeared by the one
  minute mark (`429:174`); no final report was written.

Conclusion: the highest confirmed clean 10 minute logs mixed-query target in the
OrbStack logs VM is `20000 GB/day`. `30000 GB/day` passes as a `120s` probe but
fails sustained 10 minute validation with measured admission rejections and
query p95 above the investigation threshold.

Current baseline, latest clean run (branch `arch-v3`, after the full architecture
action plan through the ingest-lifecycle collapse to coarse boundary counters):
[report.json](/private/tmp/cs-lifecycle-report/20260523T232746Z/report.json)

- Target: `400 GB/day`, `logs`, `mixed-query`, persistent connections.
- Write path: Arrow write buffer -> DuckDB Arrow append -> DuckLake commit.
- Actual: `4,631,113 B/s` against target `4,629,630 B/s`.
- Statuses: `200:240`, `202:2890`, no `429`.
- Queries: `240/240`, transport errors `0`.
- Ingest p50/p95/p99: `11.7 / 13.6 / 16.6 ms`.
- Query p50/p95/p99: `84.1 / 99.9 / 104.2 ms`.
- Freshness lag final: `0.8s`; progress logs oscillated between `0.4s` and `1.0s`
  without sustained growth.

Run the server in one shell with a fresh data directory and a free local port:

```bash
env CANARDSTACK_DATA_DIR=/private/tmp/canardstack-arch-cutover-data-400-rerunN \
  CANARDSTACK_BIND=127.0.0.1:4330 \
  cargo run --release -- serve
```

Run the benchmark in another shell:

```bash
cargo bench --bench throughput_iteration -- \
  --base-url http://127.0.0.1:4330 \
  --warmup 5s \
  --duration 600s \
  --target-gb-day 400 \
  --profile mixed-query \
  --signals logs \
  --ingest-concurrency 16 \
  --connection-mode persistent \
  --query-pressure medium \
  --query-concurrency 2 \
  --query-interval 5s \
  --progress-interval 30s \
  --max-runtime 660s \
  --report-dir /private/tmp/canardstack-arch-cutover-400-10m-rerunN
```

Pass criteria:

- `pass=true`.
- No `429` unless the change intentionally tightens freshness/admission limits.
- Query success count equals attempted query count.
- No transport errors.
- Freshness lag must oscillate or recover; sustained monotonic growth is a
  regression.
- Treat ingest p95 above `50 ms` or query p95 above `150 ms` as requiring
  investigation.
- Stop the release server after the run and report the full `report.json` path.
