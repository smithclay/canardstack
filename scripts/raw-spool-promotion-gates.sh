#!/usr/bin/env bash
set -euo pipefail

ROOT="${CANARDSTACK_RAW_SPOOL_GATE_ROOT:-/private/tmp/canardstack-raw-spool-gates-$(date -u +%Y%m%dT%H%M%SZ)}"
PORT="${CANARDSTACK_RAW_SPOOL_GATE_PORT:-$((4300 + RANDOM % 500))}"
BASE_URL="http://127.0.0.1:${PORT}"
API_KEY="${CANARDSTACK_API_KEY:-dev-canardstack-key}"
ADMIN_KEY="${CANARDSTACK_ADMIN_API_KEY:-dev-canardstack-admin-key}"
SERVER_BIN="${CANARDSTACK_RAW_SPOOL_GATE_SERVER_BIN:-target/release/canardstack}"
BENCH_WARMUP="${CANARDSTACK_RAW_SPOOL_GATE_WARMUP:-10s}"
BENCH_DURATION="${CANARDSTACK_RAW_SPOOL_GATE_DURATION:-60s}"
BENCH_TARGET_GB_DAY="${CANARDSTACK_RAW_SPOOL_GATE_TARGET_GB_DAY:-500}"
TRACE_BENCH_TARGET_GB_DAY="${CANARDSTACK_RAW_SPOOL_GATE_TRACE_TARGET_GB_DAY:-${BENCH_TARGET_GB_DAY}}"
BENCH_FRESHNESS_SLA="${CANARDSTACK_RAW_SPOOL_GATE_FRESHNESS_SLA:-15s}"
BENCH_MAX_RUNTIME="${CANARDSTACK_RAW_SPOOL_GATE_MAX_RUNTIME:-3m}"
BACKLOG_RECORDS_TARGET="${CANARDSTACK_RAW_SPOOL_GATE_BACKLOG_RECORDS:-10000}"
BACKLOG_BYTES_TARGET="${CANARDSTACK_RAW_SPOOL_GATE_BACKLOG_BYTES:-134217728}"
BACKLOG_SAMPLE_INTERVAL="${CANARDSTACK_RAW_SPOOL_GATE_BACKLOG_SAMPLE_INTERVAL:-100}"
SERVER_PID=""
SERVER_LOG=""

mkdir -p "${ROOT}"

log() {
  printf '[raw-spool-gates] %s\n' "$*"
}

stop_server() {
  if [[ -n "${SERVER_PID}" ]] && kill -0 "${SERVER_PID}" 2>/dev/null; then
    kill "${SERVER_PID}" 2>/dev/null || true
    for _ in $(seq 1 100); do
      if ! kill -0 "${SERVER_PID}" 2>/dev/null; then
        break
      fi
      sleep 0.1
    done
    if kill -0 "${SERVER_PID}" 2>/dev/null; then
      kill -9 "${SERVER_PID}" 2>/dev/null || true
    fi
  fi
  SERVER_PID=""
}

trap stop_server EXIT

wait_health() {
  for _ in $(seq 1 300); do
    if curl -fsS "${BASE_URL}/healthz" >/dev/null 2>&1; then
      return 0
    fi
    sleep 0.1
  done
  log "server log follows:"
  cat "${SERVER_LOG}" || true
  return 1
}

start_server() {
  local data_dir="$1"
  local log_path="$2"
  local scheduler_enabled="${3:-true}"
  SERVER_LOG="${log_path}"
  stop_server
  CANARDSTACK_BIND="127.0.0.1:${PORT}" \
    CANARDSTACK_API_KEY="${API_KEY}" \
    CANARDSTACK_ADMIN_API_KEY="${ADMIN_KEY}" \
    CANARDSTACK_DATA_DIR="${data_dir}" \
    CANARDSTACK_ARROW_WRITE_BUFFER_MAX_AGE_MS=500 \
    CANARDSTACK_MAINTENANCE_INTERVAL_MS=100 \
    CANARDSTACK_SCHEDULER_ENABLED="${scheduler_enabled}" \
    "${SERVER_BIN}" serve >"${log_path}" 2>&1 &
  SERVER_PID="$!"
  wait_health
}

write_log_fixture() {
  local path="$1"
  local now_nanos
  now_nanos="$(($(date +%s) * 1000000000))"
  cat >"${path}" <<JSON
{"resourceLogs":[{"resource":{"attributes":[{"key":"service.name","value":{"stringValue":"checkout"}},{"key":"deployment.environment","value":{"stringValue":"dev"}}]},"scopeLogs":[{"scope":{"name":"raw-spool-gate","version":"1"},"logRecords":[{"timeUnixNano":"${now_nanos}","observedTimeUnixNano":"${now_nanos}","severityNumber":17,"severityText":"ERROR","traceId":"11111111111111111111111111111111","spanId":"2222222222222222","body":{"stringValue":"raw spool promotion gate"},"attributes":[{"key":"http.route","value":{"stringValue":"/raw-spool-gate"}},{"key":"gate","value":{"stringValue":"restart-replay"}}]}]}]}]}
JSON
}

metric_scalar() {
  local metrics="$1"
  local name="$2"
  awk -v name="${name}" '$1 == name { print $2; found=1 } END { if (!found) print "" }' "${metrics}"
}

assert_metric() {
  local metrics="$1"
  local needle="$2"
  if ! grep -Fq "${needle}" "${metrics}"; then
    log "missing metric: ${needle}"
    cat "${metrics}"
    return 1
  fi
}

logical_rows() {
  local storage_json="$1"
  local table="$2"
  sed -n "s/.*\"${table}\":\([0-9][0-9]*\).*/\1/p" "${storage_json}" | head -n 1
}

flush_until_rows() {
  local dir="$1"
  local table="$2"
  local expected="$3"
  local attempt rows
  for attempt in $(seq 1 40); do
    curl -fsS -H "Authorization: Bearer ${ADMIN_KEY}" -H "Content-Type: application/json" \
      --data-binary '{}' "${BASE_URL}/api/admin/maintenance/flush" >"${dir}/flush-${attempt}.json"
    curl -fsS "${BASE_URL}/metrics" >"${dir}/metrics.prom"
    curl -fsS -H "Authorization: Bearer ${ADMIN_KEY}" \
      "${BASE_URL}/api/admin/health/storage" >"${dir}/storage.json"
    rows="$(logical_rows "${dir}/storage.json" "${table}")"
    rows="${rows:-0}"
    if [[ "${rows}" -ge "${expected}" ]] && grep -Fq 'canardstack_raw_spool_pending_records 0.000000' "${dir}/metrics.prom"; then
      return 0
    fi
    sleep 0.2
  done
  log "expected ${table} logical rows >= ${expected} and pending raw spool records = 0"
  cat "${dir}/storage.json" || true
  cat "${dir}/metrics.prom" || true
  return 1
}

post_log_payload() {
  local payload="$1"
  curl -sS -o /dev/null -w '%{http_code}' --max-time 10 \
    -H "Authorization: Bearer ${API_KEY}" \
    -H "Content-Type: application/json" \
    --data-binary @"${payload}" \
    "${BASE_URL}/v1/logs" || true
}

log "building release server"
cargo build --release

TERM_DIR="${ROOT}/sigterm-sustained-ingest"
mkdir -p "${TERM_DIR}"
TERM_PAYLOAD="${TERM_DIR}/log.json"
write_log_fixture "${TERM_PAYLOAD}"

log "gate 1: SIGTERM during sustained ingest, then restart/replay/flush"
start_server "${TERM_DIR}/data" "${TERM_DIR}/server-before-term.log" false
ACCEPTED_FILE="${TERM_DIR}/accepted.txt"
ERROR_FILE="${TERM_DIR}/errors.txt"
: >"${ACCEPTED_FILE}"
: >"${ERROR_FILE}"
(
  while kill -0 "${SERVER_PID}" 2>/dev/null; do
    status="$(post_log_payload "${TERM_PAYLOAD}")"
    if [[ "${status}" == "202" ]]; then
      printf '1\n' >>"${ACCEPTED_FILE}"
    elif [[ -n "${status}" ]]; then
      printf '%s\n' "${status}" >>"${ERROR_FILE}"
    fi
  done
) &
INGEST_LOOP_PID="$!"
for _ in $(seq 1 100); do
  if [[ "$(wc -l <"${ACCEPTED_FILE}" | tr -d ' ')" -ge 100 ]]; then
    break
  fi
  sleep 0.1
done
kill -TERM "${SERVER_PID}"
wait "${SERVER_PID}" || true
SERVER_PID=""
wait "${INGEST_LOOP_PID}" || true
accepted="$(wc -l <"${ACCEPTED_FILE}" | tr -d ' ')"
if [[ "${accepted}" -le 0 ]]; then
  log "SIGTERM gate accepted no requests"
  cat "${ERROR_FILE}" || true
  exit 1
fi
restart_start="$(date +%s)"
start_server "${TERM_DIR}/data" "${TERM_DIR}/server-after-term.log" true
replay_seconds="$(( $(date +%s) - restart_start ))"
flush_until_rows "${TERM_DIR}" "logs" "${accepted}"
assert_metric "${TERM_DIR}/metrics.prom" "canardstack_raw_spool_replayed_records_total{signal=\"logs\",status=\"ok\"} ${accepted}"
{
  printf 'accepted=%s\n' "${accepted}"
  printf 'restart_replay_seconds=%s\n' "${replay_seconds}"
  printf 'logical_rows.logs=%s\n' "$(logical_rows "${TERM_DIR}/storage.json" "logs")"
} >"${TERM_DIR}/summary.txt"
stop_server

log "gate 2: pending replay backlog drains on restart"
BACKLOG_DIR="${ROOT}/replay-backlog"
mkdir -p "${BACKLOG_DIR}"
BACKLOG_PAYLOAD="${BACKLOG_DIR}/log.json"
write_log_fixture "${BACKLOG_PAYLOAD}"
start_server "${BACKLOG_DIR}/data" "${BACKLOG_DIR}/server-seed.log" false
seed_start="$(date +%s)"
backlog_count=0
pending_bytes=0
while [[ "${backlog_count}" -lt "${BACKLOG_RECORDS_TARGET}" && "${pending_bytes}" -lt "${BACKLOG_BYTES_TARGET}" ]]; do
  status="$(post_log_payload "${BACKLOG_PAYLOAD}")"
  if [[ "${status}" != "202" ]]; then
    log "backlog seed request failed with HTTP ${status}"
    exit 1
  fi
  backlog_count="$((backlog_count + 1))"
  if [[ "$((backlog_count % BACKLOG_SAMPLE_INTERVAL))" -eq 0 || "${backlog_count}" -eq "${BACKLOG_RECORDS_TARGET}" ]]; then
    curl -fsS "${BASE_URL}/metrics" >"${BACKLOG_DIR}/seed-metrics.prom"
    pending_bytes="$(metric_scalar "${BACKLOG_DIR}/seed-metrics.prom" "canardstack_raw_spool_pending_bytes")"
    pending_bytes="${pending_bytes%.*}"
    pending_bytes="${pending_bytes:-0}"
  fi
done
seed_seconds="$(( $(date +%s) - seed_start ))"
curl -fsS "${BASE_URL}/metrics" >"${BACKLOG_DIR}/seed-metrics.prom"
pending_bytes="$(metric_scalar "${BACKLOG_DIR}/seed-metrics.prom" "canardstack_raw_spool_pending_bytes")"
assert_metric "${BACKLOG_DIR}/seed-metrics.prom" "canardstack_raw_spool_pending_records ${backlog_count}.000000"
stop_server
restart_start="$(date +%s)"
start_server "${BACKLOG_DIR}/data" "${BACKLOG_DIR}/server-replay.log" true
replay_seconds="$(( $(date +%s) - restart_start ))"
flush_until_rows "${BACKLOG_DIR}" "logs" "${backlog_count}"
assert_metric "${BACKLOG_DIR}/metrics.prom" "canardstack_raw_spool_replayed_records_total{signal=\"logs\",status=\"ok\"} ${backlog_count}"
{
  printf 'accepted=%s\n' "${backlog_count}"
  printf 'seed_seconds=%s\n' "${seed_seconds}"
  printf 'restart_replay_seconds=%s\n' "${replay_seconds}"
  printf 'pending_bytes_before_replay=%s\n' "${pending_bytes}"
  printf 'logical_rows.logs=%s\n' "$(logical_rows "${BACKLOG_DIR}/storage.json" "logs")"
  printf 'pending_records_after_flush=0\n'
} >"${BACKLOG_DIR}/summary.txt"
stop_server

log "gate 3: mixed logs ingest/query pressure with raw spool enabled"
LOGS_DIR="${ROOT}/mixed-pressure-logs"
mkdir -p "${LOGS_DIR}"
start_server "${LOGS_DIR}/data" "${LOGS_DIR}/server.log" true
set +e
CANARDSTACK_API_KEY="${API_KEY}" \
  CANARDSTACK_ADMIN_API_KEY="${ADMIN_KEY}" \
  cargo bench --bench throughput_iteration -- \
    --base-url "${BASE_URL}" \
    --warmup "${BENCH_WARMUP}" \
    --duration "${BENCH_DURATION}" \
    --target-gb-day "${BENCH_TARGET_GB_DAY}" \
    --profile mixed-query \
    --query-pressure medium \
    --ingest-concurrency 16 \
    --connection-mode persistent \
    --signals logs \
    --items-per-batch 256 \
    --log-body-bytes 512 \
    --trace-attribute-bytes 256 \
    --metric-description-bytes 64 \
    --timestamp-mode advancing \
    --freshness-sla "${BENCH_FRESHNESS_SLA}" \
    --progress-interval 15s \
    --max-runtime "${BENCH_MAX_RUNTIME}" \
    --server-pid "${SERVER_PID}" \
    --resource-sample-interval 5s \
    --report-dir "${LOGS_DIR}/report" >"${LOGS_DIR}/bench.out" 2>"${LOGS_DIR}/bench.err"
LOGS_BENCH_EXIT="$?"
set -e
curl -fsS "${BASE_URL}/metrics" >"${LOGS_DIR}/metrics.prom" || true
stop_server
if [[ "${LOGS_BENCH_EXIT}" -ne 0 ]]; then
  log "logs mixed-pressure benchmark failed; stdout/stderr are in ${LOGS_DIR}"
  exit "${LOGS_BENCH_EXIT}"
fi

log "gate 4: mixed trace ingest/Tempo-search pressure with raw spool enabled"
TRACE_DIR="${ROOT}/mixed-pressure-traces"
mkdir -p "${TRACE_DIR}"
start_server "${TRACE_DIR}/data" "${TRACE_DIR}/server.log" true
set +e
CANARDSTACK_API_KEY="${API_KEY}" \
  CANARDSTACK_ADMIN_API_KEY="${ADMIN_KEY}" \
  cargo bench --bench throughput_iteration -- \
    --base-url "${BASE_URL}" \
    --warmup "${BENCH_WARMUP}" \
    --duration "${BENCH_DURATION}" \
    --target-gb-day "${TRACE_BENCH_TARGET_GB_DAY}" \
    --profile mixed-query \
    --query-pressure medium \
    --ingest-concurrency 16 \
    --connection-mode persistent \
    --signals spans \
    --items-per-batch 256 \
    --trace-attribute-bytes 256 \
    --timestamp-mode advancing \
    --freshness-sla "${BENCH_FRESHNESS_SLA}" \
    --progress-interval 15s \
    --max-runtime "${BENCH_MAX_RUNTIME}" \
    --server-pid "${SERVER_PID}" \
    --resource-sample-interval 5s \
    --report-dir "${TRACE_DIR}/report" >"${TRACE_DIR}/bench.out" 2>"${TRACE_DIR}/bench.err"
TRACE_BENCH_EXIT="$?"
set -e
curl -fsS "${BASE_URL}/metrics" >"${TRACE_DIR}/metrics.prom" || true
stop_server
if [[ "${TRACE_BENCH_EXIT}" -ne 0 ]]; then
  log "trace mixed-pressure benchmark failed; stdout/stderr are in ${TRACE_DIR}"
  exit "${TRACE_BENCH_EXIT}"
fi

log "raw spool promotion gates complete"
log "artifacts: ${ROOT}"
