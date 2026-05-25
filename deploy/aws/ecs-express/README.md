# ECS Express Mode

`template.yaml` creates a push-button ECS/Fargate deployment from
CloudFormation:

- A public-facing canardstack ECS service behind an Application Load Balancer.
  The container runs `canardstack serve`, so ingest and query APIs are available
  from one service.
- A private catalog ECS service running the **same canardstack image** as
  `canardstack serve-catalog`, registered in Cloud Map. It serves the
  DuckDB-backed DuckLake metadata catalog file over the Quack protocol.
- A VPC with two public subnets, internet routing, security groups, and an S3
  bucket/prefix for DuckLake data files.
- A service-managed EBS volume mounted at `/var/lib/canardstack` on each service:
  the app's durable raw spool, and the catalog's DuckLake metadata DuckDB file.
- Secrets Manager secrets for the canardstack API keys and the Quack token.

S3 Files is intentionally not used for the DuckDB catalog file. The catalog
service uses a separate service-managed EBS volume; canardstack reaches the
catalog over Quack and writes DuckLake data files to S3.

One caveat: ECS service-managed EBS volumes attached to service tasks are simple
and push-button, but validate their lifecycle behavior for your durability
requirements before treating this as production. The template keeps both ECS
services at `DesiredCount: 1`. The app and catalog containers default to running
as root in ECS so they can write the root-owned EBS mounts; override
`AppContainerUser` or `CatalogContainerUser` if your image prepares writable
mount ownership another way.

CloudFormation does not currently expose the ECS `resourceManagementType` service
property in the `AWS::ECS::Service` resource schema. The template sticks to the
underlying resources that ECS Express Mode creates and manages: ECS services,
Fargate task definitions, network security groups, service-managed EBS volume,
Cloud Map, CloudWatch logs, Secrets Manager secrets, S3, and IAM roles.
CloudFormation now also exposes `AWS::ECS::ExpressGatewayService`, but that
resource only models the primary web container surface and does not cover the
service-managed EBS mounts this deployment needs for the canardstack raw spool
and the DuckDB/Quack catalog metadata file.

Example deployment. `CatalogImage` defaults to the same canardstack image as the
app, and `ApiKey`/`AdminApiKey`/`QuackToken` auto-generate when omitted, so the
minimal deploy is:

```bash
aws cloudformation deploy \
  --stack-name canardstack \
  --template-file deploy/aws/ecs-express/template.yaml \
  --capabilities CAPABILITY_IAM
```

Retrieve the (possibly auto-generated) keys from the stack outputs after deploy:

```bash
aws cloudformation describe-stacks --stack-name canardstack \
  --query "Stacks[0].Outputs[?ends_with(OutputKey, 'RetrieveCommand')].OutputValue" \
  --output text
# run each printed `aws secretsmanager get-secret-value ...` to read a key
```

Pin explicit values by passing them as `--parameter-overrides` (`ApiKey=...`,
`AdminApiKey=...`, `QuackToken=...`, or a different `CatalogImage=...`).

Both tasks run on `CpuArchitecture: ARM64` (Graviton) by default, which is ~20%
cheaper than X86_64 Fargate; the canardstack image is multi-arch so it runs on
either. Pass `CpuArchitecture=X86_64` to switch.

The catalog container runs `canardstack serve-catalog`: it opens the DuckLake
catalog DuckDB file on the EBS mount (`CANARDSTACK_DUCKLAKE_CATALOG_PATH`) and
serves it over Quack on `CatalogPort` (default 9494), exposing `/healthz` on
container port 8080 for the ECS health check. Because the Quack server is
plaintext inside the VPC (no TLS proxy), the app sets
`CANARDSTACK_DUCKLAKE_QUACK_DISABLE_SSL=true` to reach it. Override `CatalogImage`
only if you are not using the canardstack image.
