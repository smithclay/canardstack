#!/bin/sh
set -eu

ingest_db="${DUCKDB_INGEST_DB:-/var/lib/duckdb/ingest.duckdb}"
attach_uri="${DUCKLAKE_ATTACH_URI:-ducklake:quack:ducklake-catalog:9494}"
data_path="${DUCKLAKE_DATA_PATH:-/var/lib/ducklake/data/}"
otlp_bind="${OTLP_BIND:-otlp:0.0.0.0:4318}"
otlp_token="${OTLP_TOKEN:-dev-otlp-token-123456}"
quack_token="${QUACK_TOKEN:-dev-quack-token}"
quack_scope="${attach_uri#ducklake:}"
quack_host="${quack_scope#quack:}"
fifo="/tmp/duckdb-otlp-ingest.sql"

case "${attach_uri}${data_path}${otlp_bind}${otlp_token}${quack_token}" in
  *"'"*)
    echo "DUCKLAKE, OTLP, and QUACK settings must not contain single quotes" >&2
    exit 1
    ;;
esac

mkdir -p "$(dirname "$ingest_db")" "$data_path/main"
chmod -R 0777 "$data_path"
rm -f "$fifo"
mkfifo "$fifo"

duckdb -unsigned "$ingest_db" < "$fifo" &
duckdb_pid="$!"

cleanup() {
  set +e
  if kill -0 "$duckdb_pid" 2>/dev/null; then
    printf "SELECT * FROM otlp_stop('%s');\n.quit\n" "$otlp_bind" >&3
    wait "$duckdb_pid"
  fi
}

trap cleanup INT TERM

exec 3>"$fifo"

cat >&3 <<SQL
INSTALL ducklake;
LOAD ducklake;
INSTALL quack;
LOAD quack;
INSTALL otlp FROM 'https://smithclay.github.io/duckdb-otlp';
LOAD otlp;
CREATE OR REPLACE SECRET canardstack_quack_tls (
  TYPE HTTP,
  SCOPE 'https://$quack_host',
  VERIFY_SSL 0
);
CREATE OR REPLACE SECRET canardstack_ducklake_quack (
  TYPE quack,
  SCOPE '$quack_scope',
  TOKEN '$quack_token'
);
ATTACH '$attach_uri' AS canardlake (
  DATA_PATH '$data_path',
  AUTOMATIC_MIGRATION true
);
USE canardlake;
SELECT *
FROM otlp_serve(
  '$otlp_bind',
  catalog := 'canardlake',
  token := '$otlp_token',
  allow_other_hostname := true
);
SELECT * FROM otlp_server_list();
SQL

wait "$duckdb_pid"
