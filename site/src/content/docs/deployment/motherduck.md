---
title: MotherDuck
description: Use MotherDuck as the hosted DuckLake catalog for canardstack.
---

MotherDuck is the shortest path to a remote DuckLake experiment. The
canardstack process and Grafana still run where you start them. The DuckLake
catalog lives in MotherDuck.

Use this when you want remote storage behavior without standing up a catalog
service, object bucket, VPC, or load balancer.

## Create the DuckLake

1. Log in to [MotherDuck](https://app.motherduck.com/).
2. Create a database and choose `DuckLake` under `Advanced`.
3. Copy the connection string, usually `md:your-database-name`.
4. Create a Read/Write token under Account Settings > Access Tokens.

## Run canardstack

Set the MotherDuck token and DuckLake attach URI:

```bash
export MOTHERDUCK_TOKEN='<your-motherduck-token>'
export CANARDSTACK_DUCKLAKE_ATTACH_URI='md:test-ducklake'
```

Then start the local stack:

```bash
docker compose up
```

The canardstack container uses `CANARDSTACK_DUCKLAKE_ATTACH_URI` and
`MOTHERDUCK_TOKEN` for storage. Grafana stays local and queries canardstack
through the provisioned Prometheus, Loki, and Tempo-compatible datasources.

## Notes

This path does not run `canardstack serve-catalog`. MotherDuck provides the
remote DuckLake catalog.

The local canardstack process still owns the OTLP ingest endpoint, raw spool,
scheduler, and compatibility APIs. A `2xx` ingest response still means the raw
request was accepted after local spool durability, not that rows are already
query-visible in DuckLake.

For a full cloud deployment with a managed app endpoint, use
[GCP Cloud Run](/deployment/gcp-cloud-run/) or
[AWS ECS/Fargate](/deployment/aws-ecs-fargate/).
