output "service_url" {
  description = "canardstack Cloud Run service URL."
  value       = google_cloud_run_v2_service.app.uri
}

output "catalog_service_url" {
  description = "Internal DuckDB/Quack Cloud Run service URL."
  value       = google_cloud_run_v2_service.catalog.uri
}

output "ducklake_attach_uri" {
  description = "DuckLake Quack attach URI configured for canardstack."
  value       = "ducklake:quack:${local.catalog_host}:443"
}

output "app_service_account_email" {
  description = "Service account used by canardstack."
  value       = google_service_account.app.email
}

output "catalog_service_account_email" {
  description = "Service account used by the DuckDB/Quack catalog service."
  value       = google_service_account.catalog.email
}

output "bucket_name" {
  description = "GCS bucket created for canardstack deployment state and DuckLake data files."
  value       = google_storage_bucket.ducklake.name
}

output "ducklake_data_path" {
  description = "DuckLake DATA_PATH configured for canardstack."
  value       = "gcs://${google_storage_bucket.ducklake.name}/${trimsuffix(var.data_prefix, "/")}/"
}
