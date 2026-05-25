terraform {
  required_version = ">= 1.5.0"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = ">= 5.17.0"
    }
    google-beta = {
      source  = "hashicorp/google-beta"
      version = ">= 5.17.0"
    }
    random = {
      source  = "hashicorp/random"
      version = ">= 3.6.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

provider "google-beta" {
  project = var.project_id
  region  = var.region
}

locals {
  app_service_account_id     = substr(var.service_name, 0, 30)
  catalog_service_account_id = substr(var.catalog_service_name, 0, 30)
  bucket_name                = var.bucket_name != "" ? var.bucket_name : "${var.bucket_name_prefix}-${random_id.bucket.hex}"
  catalog_host               = replace(google_cloud_run_v2_service.catalog.uri, "https://", "")
  catalog_mount_options = [
    "only-dir=${var.catalog_prefix}",
    "implicit-dirs",
    "uid=${var.catalog_uid}",
    "gid=${var.catalog_gid}",
  ]
  app_data_mount_options = [
    "only-dir=${var.app_data_prefix}",
    "implicit-dirs",
    "uid=${var.app_uid}",
    "gid=${var.app_gid}",
  ]
  app_invoker_members     = toset(var.invoker_members)
  catalog_invoker_members = toset(var.catalog_invoker_members)
}

resource "random_id" "bucket" {
  byte_length = 4
}

resource "google_project_service" "required" {
  for_each = toset([
    "iam.googleapis.com",
    "run.googleapis.com",
    "secretmanager.googleapis.com",
    "serviceusage.googleapis.com",
    "storage.googleapis.com",
  ])

  project            = var.project_id
  service            = each.key
  disable_on_destroy = false
}

resource "google_service_account" "app" {
  account_id   = local.app_service_account_id
  display_name = "canardstack app"

  depends_on = [google_project_service.required]
}

resource "google_service_account" "catalog" {
  account_id   = local.catalog_service_account_id
  display_name = "canardstack DuckDB Quack catalog"

  depends_on = [google_project_service.required]
}

resource "google_storage_bucket" "ducklake" {
  name                        = local.bucket_name
  location                    = var.bucket_location
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"

  versioning {
    enabled = var.enable_bucket_versioning
  }

  depends_on = [google_project_service.required]
}

resource "google_storage_bucket_object" "catalog_prefix" {
  name    = "${trimsuffix(var.catalog_prefix, "/")}/"
  bucket  = google_storage_bucket.ducklake.name
  content = ""
}

resource "google_storage_bucket_object" "app_data_prefix" {
  name    = "${trimsuffix(var.app_data_prefix, "/")}/"
  bucket  = google_storage_bucket.ducklake.name
  content = ""
}

resource "google_storage_bucket_object" "data_prefix" {
  name    = "${trimsuffix(var.data_prefix, "/")}/"
  bucket  = google_storage_bucket.ducklake.name
  content = ""
}

resource "google_storage_bucket_iam_member" "app_object_user" {
  bucket = google_storage_bucket.ducklake.name
  role   = "roles/storage.objectUser"
  member = "serviceAccount:${google_service_account.app.email}"
}

resource "google_storage_bucket_iam_member" "catalog_object_user" {
  bucket = google_storage_bucket.ducklake.name
  role   = "roles/storage.objectUser"
  member = "serviceAccount:${google_service_account.catalog.email}"
}

resource "google_secret_manager_secret" "api_key" {
  secret_id = "${var.service_name}-api-key"

  replication {
    auto {}
  }

  depends_on = [google_project_service.required]
}

resource "google_secret_manager_secret_version" "api_key" {
  secret      = google_secret_manager_secret.api_key.id
  secret_data = var.api_key
}

resource "google_secret_manager_secret" "admin_api_key" {
  secret_id = "${var.service_name}-admin-api-key"

  replication {
    auto {}
  }

  depends_on = [google_project_service.required]
}

resource "google_secret_manager_secret_version" "admin_api_key" {
  secret      = google_secret_manager_secret.admin_api_key.id
  secret_data = var.admin_api_key
}

resource "google_secret_manager_secret" "quack_token" {
  secret_id = "${var.catalog_service_name}-quack-token"

  replication {
    auto {}
  }

  depends_on = [google_project_service.required]
}

resource "google_secret_manager_secret_version" "quack_token" {
  secret      = google_secret_manager_secret.quack_token.id
  secret_data = var.quack_token
}

resource "google_secret_manager_secret_iam_member" "api_key_accessor" {
  secret_id = google_secret_manager_secret.api_key.id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.app.email}"
}

resource "google_secret_manager_secret_iam_member" "admin_api_key_accessor" {
  secret_id = google_secret_manager_secret.admin_api_key.id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.app.email}"
}

resource "google_secret_manager_secret_iam_member" "app_quack_token_accessor" {
  secret_id = google_secret_manager_secret.quack_token.id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.app.email}"
}

resource "google_secret_manager_secret_iam_member" "catalog_quack_token_accessor" {
  secret_id = google_secret_manager_secret.quack_token.id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.catalog.email}"
}

resource "google_cloud_run_v2_service" "catalog" {
  provider            = google-beta
  name                = var.catalog_service_name
  location            = var.region
  deletion_protection = var.deletion_protection
  ingress             = var.catalog_ingress

  template {
    service_account                  = google_service_account.catalog.email
    execution_environment            = "EXECUTION_ENVIRONMENT_GEN2"
    max_instance_request_concurrency = var.catalog_container_concurrency
    timeout                          = "${var.request_timeout_seconds}s"

    scaling {
      min_instance_count = 1
      max_instance_count = 1
    }

    containers {
      name    = "canardstack-catalog"
      image   = var.catalog_image
      command = length(var.catalog_command) == 0 ? ["canardstack"] : var.catalog_command
      args    = length(var.catalog_args) == 0 ? ["serve-catalog"] : var.catalog_args

      ports {
        container_port = var.catalog_port
      }

      resources {
        limits = {
          cpu    = var.catalog_cpu
          memory = var.catalog_memory
        }
      }

      env {
        name  = "CANARDSTACK_DUCKLAKE_CATALOG_PATH"
        value = "/catalog/canardstack.ducklake"
      }

      env {
        name  = "CANARDSTACK_DUCKDB_EXTENSION_DIR"
        value = "/usr/local/lib/duckdb/extensions"
      }

      # Quack listens on the Cloud Run container port; Cloud Run terminates TLS
      # in front of it, so the app connects over HTTPS (:443) with no DISABLE_SSL.
      env {
        name  = "CANARDSTACK_CATALOG_LISTEN"
        value = "0.0.0.0:${var.catalog_port}"
      }

      # Liveness endpoint is loopback-only; Cloud Run probes the container port.
      env {
        name  = "CANARDSTACK_CATALOG_HEALTH_BIND"
        value = "127.0.0.1:8080"
      }

      env {
        name = "CANARDSTACK_DUCKLAKE_QUACK_TOKEN"
        value_source {
          secret_key_ref {
            secret  = google_secret_manager_secret.quack_token.secret_id
            version = "latest"
          }
        }
      }

      volume_mounts {
        name       = "ducklake-catalog"
        mount_path = "/catalog"
      }
    }

    volumes {
      name = "ducklake-catalog"
      gcs {
        bucket        = google_storage_bucket.ducklake.name
        read_only     = false
        mount_options = local.catalog_mount_options
      }
    }
  }

  depends_on = [
    google_secret_manager_secret_iam_member.catalog_quack_token_accessor,
    google_storage_bucket_iam_member.catalog_object_user,
    google_storage_bucket_object.catalog_prefix,
  ]
}

resource "google_cloud_run_v2_service_iam_member" "catalog_invoker" {
  for_each = local.catalog_invoker_members

  project  = var.project_id
  location = google_cloud_run_v2_service.catalog.location
  name     = google_cloud_run_v2_service.catalog.name
  role     = "roles/run.invoker"
  member   = each.value
}

resource "google_cloud_run_v2_service" "app" {
  provider            = google-beta
  name                = var.service_name
  location            = var.region
  deletion_protection = var.deletion_protection
  ingress             = var.ingress

  template {
    service_account                  = google_service_account.app.email
    execution_environment            = "EXECUTION_ENVIRONMENT_GEN2"
    max_instance_request_concurrency = var.container_concurrency
    timeout                          = "${var.request_timeout_seconds}s"

    scaling {
      min_instance_count = var.min_instances
      max_instance_count = 1
    }

    containers {
      name    = "canardstack"
      image   = var.image
      command = ["canardstack"]
      args    = ["serve"]

      ports {
        container_port = 4318
      }

      resources {
        limits = {
          cpu    = var.cpu
          memory = var.memory
        }
      }

      env {
        name  = "CANARDSTACK_BIND"
        value = "0.0.0.0:4318"
      }

      env {
        name  = "CANARDSTACK_DATA_DIR"
        value = "/var/lib/canardstack"
      }

      env {
        name  = "CANARDSTACK_DUCKDB_EXTENSION_DIR"
        value = "/usr/local/lib/duckdb/extensions"
      }

      env {
        name  = "CANARDSTACK_DUCKLAKE_ATTACH_URI"
        value = "ducklake:quack:${local.catalog_host}:443"
      }

      env {
        name  = "CANARDSTACK_DUCKLAKE_DATA_PATH"
        value = "gcs://${google_storage_bucket.ducklake.name}/${trimsuffix(var.data_prefix, "/")}/"
      }

      env {
        name  = "CANARDSTACK_DUCKLAKE_MAINTENANCE_ENABLED"
        value = "true"
      }

      env {
        name  = "CANARDSTACK_PROCESS_MEMORY_LIMIT_BYTES"
        value = tostring(var.process_memory_limit_bytes)
      }

      env {
        name = "CANARDSTACK_DUCKLAKE_QUACK_TOKEN"
        value_source {
          secret_key_ref {
            secret  = google_secret_manager_secret.quack_token.secret_id
            version = "latest"
          }
        }
      }

      env {
        name = "CANARDSTACK_API_KEY"
        value_source {
          secret_key_ref {
            secret  = google_secret_manager_secret.api_key.secret_id
            version = "latest"
          }
        }
      }

      env {
        name = "CANARDSTACK_ADMIN_API_KEY"
        value_source {
          secret_key_ref {
            secret  = google_secret_manager_secret.admin_api_key.secret_id
            version = "latest"
          }
        }
      }

      volume_mounts {
        name       = "canardstack-data"
        mount_path = "/var/lib/canardstack"
      }
    }

    volumes {
      name = "canardstack-data"
      gcs {
        bucket        = google_storage_bucket.ducklake.name
        read_only     = false
        mount_options = local.app_data_mount_options
      }
    }
  }

  depends_on = [
    google_cloud_run_v2_service.catalog,
    google_cloud_run_v2_service_iam_member.catalog_invoker,
    google_secret_manager_secret_iam_member.api_key_accessor,
    google_secret_manager_secret_iam_member.admin_api_key_accessor,
    google_secret_manager_secret_iam_member.app_quack_token_accessor,
    google_storage_bucket_iam_member.app_object_user,
    google_storage_bucket_object.app_data_prefix,
    google_storage_bucket_object.data_prefix,
  ]
}

resource "google_cloud_run_v2_service_iam_member" "app_invoker" {
  for_each = local.app_invoker_members

  project  = var.project_id
  location = google_cloud_run_v2_service.app.location
  name     = google_cloud_run_v2_service.app.name
  role     = "roles/run.invoker"
  member   = each.value
}
