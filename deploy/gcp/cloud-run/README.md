# Cloud Run

This Terraform module creates two Cloud Run services:

- `canardstack`: the public or IAM-gated app container. It runs `canardstack
  serve`, so ingest and query compatibility APIs are available from one
  container.
- `canardstack-catalog`: an internal DuckDB/Quack catalog container. It owns the
  DuckDB-backed DuckLake metadata file and is fixed at one instance.

DuckLake data files live in a GCS prefix. The canardstack service mounts a
dedicated GCS prefix at `/var/lib/canardstack` for `CANARDSTACK_DATA_DIR`,
including the durable raw spool. The catalog service mounts a separate GCS
prefix at `/catalog` for `canardstack.ducklake`, matching the simplified
AzQuack-style layout. Cloud Run also supports NFS volumes; for strict POSIX
filesystem and fsync behavior, prefer Filestore/NFS over Cloud Storage FUSE.

The catalog service defaults to internal ingress. `catalog_invoker_members`
defaults to `["allUsers"]` because Quack clients usually authenticate with a
Quack token rather than a Google identity token; the service is still restricted
to internal ingress and the catalog image should enforce `QUACK_TOKEN`.

Local Terraform example:

```bash
cd deploy/gcp/cloud-run
cp terraform.tfvars.example terraform.tfvars
$EDITOR terraform.tfvars
terraform init
terraform apply
```

Infrastructure Manager example:

```bash
gcloud infra-manager deployments apply projects/PROJECT_ID/locations/us-central1/deployments/canardstack \
  --service-account=projects/PROJECT_ID/serviceAccounts/INFRA_MANAGER_SA@PROJECT_ID.iam.gserviceaccount.com \
  --git-source-repo=https://github.com/<owner>/<repo>.git \
  --git-source-directory=deploy/gcp/cloud-run \
  --git-source-ref=main \
  --input-values=project_id=PROJECT_ID,region=us-central1,image=ghcr.io/<owner>/canardstack:<version>,catalog_image=ghcr.io/<owner>/duckdb-quack:<version>,api_key=REPLACE_ME,admin_api_key=REPLACE_ME_TOO,quack_token=REPLACE_ME_THREE
```

By default, the canardstack service has no public invoker binding. Set
`invoker_members = ["allUsers"]` only when you want a public endpoint protected
by `CANARDSTACK_API_KEY` and `CANARDSTACK_ADMIN_API_KEY`.
