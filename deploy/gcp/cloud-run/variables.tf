variable "project_id" {
  description = "Google Cloud project id."
  type        = string
}

variable "region" {
  description = "Cloud Run region."
  type        = string
  default     = "us-central1"
}

variable "service_name" {
  description = "Public canardstack Cloud Run service name."
  type        = string
  default     = "canardstack"
}

variable "catalog_service_name" {
  description = "Internal DuckDB/Quack Cloud Run service name."
  type        = string
  default     = "canardstack-catalog"
}

variable "image" {
  description = "Public GHCR canardstack image."
  type        = string
  default     = "ghcr.io/smithclay/canardstack:latest"
}

variable "catalog_image" {
  description = "Catalog image. Defaults to the same canardstack image as the app; the catalog runs `canardstack serve-catalog` to serve the DuckLake catalog DuckDB file over Quack."
  type        = string
  default     = "ghcr.io/smithclay/canardstack:latest"
}

variable "catalog_command" {
  description = "Entrypoint override for the catalog container (defaults to [\"canardstack\"])."
  type        = list(string)
  default     = []
}

variable "catalog_args" {
  description = "Args override for the catalog container (defaults to [\"serve-catalog\"])."
  type        = list(string)
  default     = []
}

variable "bucket_name" {
  description = "Optional GCS bucket name for the Quack catalog metadata file and DuckLake data files. Leave empty to generate one."
  type        = string
  default     = ""
}

variable "bucket_name_prefix" {
  description = "Prefix used when generating the GCS bucket name."
  type        = string
  default     = "canardstack-ducklake"
}

variable "bucket_location" {
  description = "GCS bucket location."
  type        = string
  default     = "US"
}

variable "catalog_prefix" {
  description = "Prefix mounted into the catalog Cloud Run service for the DuckDB-backed DuckLake metadata file."
  type        = string
  default     = "ducklake-catalog"
}

variable "app_data_prefix" {
  description = "Prefix mounted into the canardstack Cloud Run service for CANARDSTACK_DATA_DIR, including the durable raw spool."
  type        = string
  default     = "canardstack-data-dir"
}

variable "data_prefix" {
  description = "GCS prefix used as DuckLake DATA_PATH."
  type        = string
  default     = "ducklake-data"
}

variable "api_key" {
  description = "Bearer token for OTLP ingest requests."
  type        = string
  sensitive   = true
}

variable "admin_api_key" {
  description = "Bearer token for admin/operator endpoints."
  type        = string
  sensitive   = true
}

variable "quack_token" {
  description = "Shared token for canardstack to authenticate to the DuckDB/Quack catalog service."
  type        = string
  sensitive   = true
}

variable "invoker_members" {
  description = "IAM members allowed to invoke the canardstack service. Use [\"allUsers\"] only for a public endpoint protected by canardstack API keys."
  type        = list(string)
  default     = []
}

variable "catalog_invoker_members" {
  description = "IAM members allowed to invoke the internal catalog service. Quack clients usually do not send Google identity tokens, so this defaults to allUsers and relies on internal ingress plus QUACK_TOKEN."
  type        = list(string)
  default     = ["allUsers"]
}

variable "ingress" {
  description = "Cloud Run ingress setting for canardstack."
  type        = string
  default     = "INGRESS_TRAFFIC_ALL"
}

variable "catalog_ingress" {
  description = "Cloud Run ingress for the Quack catalog. Defaults to INGRESS_TRAFFIC_ALL so the app reaches it over Cloud Run-managed TLS, gated by the Quack token. Set INGRESS_TRAFFIC_INTERNAL_ONLY only if the app has VPC egress to the catalog (Direct VPC egress or a Serverless VPC Access connector)."
  type        = string
  default     = "INGRESS_TRAFFIC_ALL"
}

variable "cpu" {
  description = "canardstack Cloud Run CPU limit."
  type        = string
  default     = "2"
}

variable "memory" {
  description = "canardstack Cloud Run memory limit."
  type        = string
  default     = "4Gi"
}

variable "catalog_cpu" {
  description = "DuckDB/Quack Cloud Run CPU limit."
  type        = string
  default     = "1"
}

variable "catalog_memory" {
  description = "DuckDB/Quack Cloud Run memory limit."
  type        = string
  default     = "2Gi"
}

variable "catalog_port" {
  description = "Container port the catalog serves Quack on (Cloud Run terminates TLS in front of it)."
  type        = number
  default     = 9494
}

variable "process_memory_limit_bytes" {
  description = "canardstack runtime RSS admission limit."
  type        = number
  default     = 3221225472
}

variable "container_concurrency" {
  description = "Cloud Run request concurrency per canardstack instance."
  type        = number
  default     = 80
}

variable "catalog_container_concurrency" {
  description = "Cloud Run request concurrency per DuckDB/Quack catalog instance."
  type        = number
  default     = 80
}

variable "request_timeout_seconds" {
  description = "Cloud Run request timeout in seconds."
  type        = number
  default     = 300
}

variable "min_instances" {
  description = "Minimum canardstack Cloud Run instances."
  type        = number
  default     = 1
}

variable "catalog_uid" {
  description = "UID used by the DuckDB/Quack image for the mounted catalog prefix."
  type        = number
  default     = 10001
}

variable "catalog_gid" {
  description = "GID used by the DuckDB/Quack image for the mounted catalog prefix."
  type        = number
  default     = 10001
}

variable "app_uid" {
  description = "UID used by the canardstack image for the mounted app data prefix."
  type        = number
  default     = 10001
}

variable "app_gid" {
  description = "GID used by the canardstack image for the mounted app data prefix."
  type        = number
  default     = 10001
}

variable "enable_bucket_versioning" {
  description = "Enable bucket object versioning."
  type        = bool
  default     = true
}

variable "deletion_protection" {
  description = "Enable Cloud Run deletion protection."
  type        = bool
  default     = false
}
