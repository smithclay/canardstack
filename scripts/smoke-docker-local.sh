#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

compose() {
  docker compose -f compose.yaml -f compose.build.yaml "$@"
}

echo "==> This smoke resets Docker Compose volumes: canardstack_canardstack-data, canardstack_grafana-data"

echo "==> Building Canardstack image"
compose build

echo "==> Starting Canardstack in local DuckLake mode"
compose up -d canardstack

echo "==> Running fixture ingest and compatibility API smoke"
compose run --rm smoke

echo "==> Restarting service and verifying named-volume persistence"
compose restart canardstack
compose run --rm smoke canardstack smoke-http --endpoint http://canardstack:4318 --verify-only

echo "==> Removing named volume and verifying fixture data disappears"
compose down -v
compose up -d canardstack
compose run --rm smoke canardstack smoke-http --endpoint http://canardstack:4318 --expect-empty

echo "==> Stopping clean empty stack"
compose down -v

echo "Docker local smoke passed"
