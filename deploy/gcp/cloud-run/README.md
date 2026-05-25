# Cloud Run

This Terraform module creates two Cloud Run services:

- `canardstack`: the public or IAM-gated app container. It runs `canardstack
  serve`, so ingest and query compatibility APIs are available from one
  container.
- `canardstack-catalog`: an internal catalog container running the **same
  canardstack image** as `canardstack serve-catalog`. It serves the DuckDB-backed
  DuckLake metadata file over Quack and is fixed at one instance.

DuckLake data files live in a GCS prefix. The canardstack service mounts a
dedicated GCS prefix at `/var/lib/canardstack` for `CANARDSTACK_DATA_DIR`,
including the durable raw spool. The catalog service mounts a separate GCS
prefix at `/catalog` for `canardstack.ducklake`, matching the simplified
AzQuack-style layout.

## DuckLake data store credentials (GCS HMAC)

DuckDB reaches `gcs://` through the S3-compatible interop API, which
authenticates with HMAC keys — a bare GCP service-account identity does not work
with core DuckDB. The module mints a `google_storage_hmac_key` for the app
service account, stores the secret half in Secret Manager, and injects the pair
into the app as `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` so DuckDB's
`credential_chain` resolves it for the `gcs://` `DATA_PATH`. The key acts as the
app service account, which already holds `roles/storage.objectUser` on the
bucket, so no extra IAM is needed. This is automatic; there is no manual step.
The principal running Terraform (including the Infrastructure Manager service
account) needs permission to mint HMAC keys, e.g. `roles/storage.admin` or
`roles/storage.hmacKeyAdmin`.

## Storage durability caveat (demo-grade default)

> [!WARNING]
> Both Cloud Run services keep their state on **Cloud Storage FUSE** mounts:
> the catalog's `canardstack.ducklake` DuckDB file, plus the app's working
> DuckDB file and durable raw spool under `CANARDSTACK_DATA_DIR`. Cloud Storage
> FUSE is **not** a POSIX filesystem — it lacks byte-range locking and reliable
> `fsync`, and it does not support the in-place random writes a live DuckDB
> database file performs. This can corrupt the catalog/working DuckDB files and
> breaks the raw spool's at-least-once durability guarantee (a crash can lose
> requests that already returned `202`). Treat this layout as **demo-grade**.
> For anything you care about, mount the catalog and `CANARDSTACK_DATA_DIR` on
> Filestore/NFS (Cloud Run Gen2 supports NFS volumes) for real POSIX semantics,
> which also requires giving the services VPC access.

The catalog service defaults to `INGRESS_TRAFFIC_ALL` so the app can reach it
over Cloud Run-managed TLS. Access is gated by the shared Quack token
(`CANARDSTACK_DUCKLAKE_QUACK_TOKEN`) that `serve-catalog` enforces, and
`catalog_invoker_members` defaults to `["allUsers"]` because Quack authenticates
with that token rather than a Google identity token. Because Cloud Run terminates
TLS, the app connects over HTTPS and needs no `DISABLE_SSL`.

> [!IMPORTANT]
> With these defaults the catalog endpoint — a DuckDB database served over Quack
> — is reachable from the public internet, and the Quack token is the **only**
> authentication boundary. Use a long, random `quack_token`, and for anything
> beyond a demo prefer a private catalog: set
> `catalog_ingress = "INGRESS_TRAFFIC_INTERNAL_ONLY"` and give the app VPC egress
> (Direct VPC egress or a Serverless VPC Access connector) so it routes to the
> catalog internally instead of over the public URL.

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
  --input-values=project_id=PROJECT_ID,region=us-central1,api_key=REPLACE_ME,admin_api_key=REPLACE_ME_TOO,quack_token=REPLACE_ME_THREE
```

By default, the canardstack service has no public invoker binding. Set
`invoker_members = ["allUsers"]` only when you want a public endpoint protected
by `CANARDSTACK_API_KEY` and `CANARDSTACK_ADMIN_API_KEY`.
