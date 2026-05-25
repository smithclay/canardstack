# ECS Express Mode

`template.yaml` creates a push-button ECS/Fargate deployment from
CloudFormation:

- A public-facing canardstack ECS service behind an Application Load Balancer.
  The container runs `canardstack serve`, so ingest and query APIs are available
  from one service.
- A private DuckDB/Quack ECS service registered in Cloud Map. This service owns
  the DuckDB-backed DuckLake metadata catalog file.
- A VPC with two public subnets, internet routing, security groups, and an S3
  bucket/prefix for DuckLake data files.
- A service-managed EBS volume mounted at `/var/lib/canardstack` for
  `CANARDSTACK_DATA_DIR`, including the durable raw spool.
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

Example deployment:

```bash
aws cloudformation deploy \
  --stack-name canardstack \
  --template-file deploy/aws/ecs-express/template.yaml \
  --capabilities CAPABILITY_IAM \
  --parameter-overrides \
    CatalogImage=ghcr.io/<owner>/duckdb-quack:<version> \
    ApiKey=replace-with-a-long-random-value \
    AdminApiKey=replace-with-a-different-long-random-value \
    QuackToken=replace-with-a-third-long-random-value
```

The catalog image is expected to listen on `QUACK_HOST`/`QUACK_PORT`, persist its
DuckDB database at `DUCKDB_DATABASE`, and enforce `QUACK_TOKEN`. If the selected
image uses different names, adjust the catalog container environment in
`template.yaml`.
