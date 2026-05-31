#!/bin/sh
set -eu

catalog_db="${DUCKLAKE_CATALOG_DB:-/var/lib/ducklake/catalog/catalog.duckdb}"
quack_bind="${QUACK_BIND:-quack:0.0.0.0:9494}"
quack_tls_port="${QUACK_TLS_PORT:-9494}"
quack_token="${QUACK_TOKEN:-dev-quack-token}"
quack_host_port="${quack_bind#quack:}"
quack_inner_port="${quack_host_port##*:}"
cert="/tmp/quack.crt"
key="/tmp/quack.key"
fifo="/tmp/duckdb-quack-catalog.sql"

case "${quack_bind}${quack_token}" in
  *"'"*)
    echo "QUACK_BIND and QUACK_TOKEN must not contain single quotes" >&2
    exit 1
    ;;
esac

mkdir -p "$(dirname "$catalog_db")"
rm -f "$fifo"
mkfifo "$fifo"
openssl req \
  -x509 \
  -newkey rsa:2048 \
  -nodes \
  -subj "/CN=ducklake-catalog" \
  -addext "subjectAltName=DNS:ducklake-catalog,DNS:localhost,IP:127.0.0.1" \
  -keyout "$key" \
  -out "$cert" \
  -days 1 >/dev/null 2>&1

duckdb -unsigned "$catalog_db" < "$fifo" &
duckdb_pid="$!"
socat \
  "OPENSSL-LISTEN:${quack_tls_port},bind=0.0.0.0,reuseaddr,fork,cert=${cert},key=${key},verify=0" \
  "TCP:127.0.0.1:${quack_inner_port}" &
proxy_pid="$!"

cleanup() {
  set +e
  if kill -0 "$proxy_pid" 2>/dev/null; then
    kill "$proxy_pid"
  fi
  if kill -0 "$duckdb_pid" 2>/dev/null; then
    printf "SELECT * FROM quack_stop('%s');\n.quit\n" "$quack_bind" >&3
    wait "$duckdb_pid"
  fi
}

trap cleanup INT TERM

exec 3>"$fifo"

cat >&3 <<SQL
INSTALL quack;
LOAD quack;
CALL quack_serve(
  '$quack_bind',
  token => '$quack_token',
  allow_other_hostname => true
);
SELECT * FROM quack_server_list();
SQL

while kill -0 "$duckdb_pid" 2>/dev/null && kill -0 "$proxy_pid" 2>/dev/null; do
  sleep 1
done
cleanup
