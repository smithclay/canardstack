use crate::config::Config;
use crate::db::sql::escape_value;
use anyhow::Result;
use duckdb::Connection;
use std::fs;
use std::path::Path;

const DUCKDB_THREADS: usize = 1;
pub(super) const DUCKLAKE_CATALOG_NAME: &str = "canardlake";
pub(super) const DUCKLAKE_TARGET_PREFIX: &str = "canardlake.";

pub(super) fn sql_path(path: &Path) -> String {
    path.to_string_lossy().replace('\'', "''")
}

pub(super) fn attach_ducklake_connection(conn: &Connection, config: &Config) -> Result<()> {
    configure_extension_directory(conn, config.operator.duckdb_extension_dir.as_deref())?;
    let plan = ducklake_attach_plan(config)?;

    if plan.needs_motherduck && conn.execute_batch("LOAD md;").is_err() {
        conn.execute_batch("INSTALL md; LOAD md;")?;
    }
    if plan.needs_ducklake && conn.execute_batch("LOAD ducklake;").is_err() {
        conn.execute_batch("INSTALL ducklake; LOAD ducklake;")?;
    }
    if plan.needs_quack && conn.execute_batch("LOAD quack;").is_err() {
        conn.execute_batch("INSTALL quack; LOAD quack;")?;
    }
    if plan.needs_postgres {
        conn.execute_batch("INSTALL postgres; LOAD postgres;")?;
    }
    if let Some(kind) = plan.object_store {
        configure_object_store_credentials(conn, kind)?;
    }
    conn.execute_batch(&plan.sql)?;
    Ok(())
}

/// Object store backing the DuckLake `DATA_PATH`. A cloud path needs `httpfs`
/// plus a credential secret before any read/write; a local path needs neither.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ObjectStore {
    S3,
    Gcs,
}

pub(super) fn object_store_kind(data_path: &str) -> Option<ObjectStore> {
    let lower = data_path.trim_start().to_ascii_lowercase();
    if lower.starts_with("s3://") || lower.starts_with("s3a://") {
        Some(ObjectStore::S3)
    } else if lower.starts_with("gcs://") || lower.starts_with("gs://") {
        Some(ObjectStore::Gcs)
    } else {
        None
    }
}

/// DuckDB secret that lets the writer/reader authenticate to the DuckLake data
/// store. `credential_chain` resolves ambient credentials (ECS task role, GCE/
/// Cloud Run service account, env, or shared config), so no static keys are
/// baked into the deployment.
pub(super) fn object_store_secret_sql(kind: ObjectStore, region: Option<&str>) -> String {
    match kind {
        ObjectStore::S3 => {
            let region_clause = region
                .map(str::trim)
                .filter(|region| !region.is_empty())
                .map(|region| format!(", REGION '{}'", sql_string(region)))
                .unwrap_or_default();
            format!(
                "CREATE OR REPLACE SECRET canardstack_object_store (TYPE s3, PROVIDER credential_chain{region_clause});"
            )
        }
        ObjectStore::Gcs => "CREATE OR REPLACE SECRET canardstack_object_store (TYPE gcs, PROVIDER credential_chain);".to_string(),
    }
}

fn configure_object_store_credentials(conn: &Connection, kind: ObjectStore) -> Result<()> {
    // httpfs provides the s3:// / gcs:// filesystems; the aws extension provides
    // the S3 credential_chain provider. Both are baked into the runtime image by
    // install_ducklake_extension, so LOAD succeeds offline; INSTALL is a fallback.
    if conn.execute_batch("LOAD httpfs;").is_err() {
        conn.execute_batch("INSTALL httpfs; LOAD httpfs;")?;
    }
    if matches!(kind, ObjectStore::S3) && conn.execute_batch("LOAD aws;").is_err() {
        conn.execute_batch("INSTALL aws; LOAD aws;")?;
    }
    let region = object_store_region();
    conn.execute_batch(&object_store_secret_sql(kind, region.as_deref()))?;
    Ok(())
}

/// Region for the S3 credential secret, resolved from the canardstack override or
/// the standard AWS environment the task/instance already carries.
fn object_store_region() -> Option<String> {
    [
        "CANARDSTACK_OBJECT_STORE_REGION",
        "AWS_REGION",
        "AWS_DEFAULT_REGION",
    ]
    .into_iter()
    .find_map(|name| std::env::var(name).ok())
    .map(|value| value.trim().to_string())
    .filter(|value| !value.is_empty())
}

/// Configure object-store credentials on a standalone DuckDB connection from a
/// DuckLake `DATA_PATH`. Returns the configured store scheme, or `None` for a
/// local data path.
#[cfg(test)]
pub fn configure_object_store_for_data_path(
    conn: &Connection,
    data_path: &str,
) -> Result<Option<&'static str>> {
    match object_store_kind(data_path) {
        Some(kind) => {
            configure_object_store_credentials(conn, kind)?;
            Ok(Some(match kind {
                ObjectStore::S3 => "s3",
                ObjectStore::Gcs => "gcs",
            }))
        }
        None => Ok(None),
    }
}

#[derive(Clone, Debug)]
pub(super) struct DuckLakeAttachPlan {
    pub(super) sql: String,
    pub(super) mode: &'static str,
    pub(super) needs_ducklake: bool,
    pub(super) needs_motherduck: bool,
    pub(super) needs_quack: bool,
    pub(super) needs_postgres: bool,
    pub(super) object_store: Option<ObjectStore>,
}

pub(super) fn ducklake_attach_plan(config: &Config) -> Result<DuckLakeAttachPlan> {
    build_ducklake_attach_plan(
        config.operator.postgres_dsn.as_deref(),
        config.operator.ducklake_attach_uri.as_deref(),
        config.operator.ducklake_catalog_path.as_deref(),
        config.operator.ducklake_data_path.as_deref(),
        config.operator.ducklake_quack_token.as_deref(),
        config.operator.ducklake_quack_insecure_tls,
        &config.operator.duckdb_path,
        &config.operator.local_storage_dir,
    )
}

#[allow(clippy::too_many_arguments)]
pub(super) fn build_ducklake_attach_plan(
    postgres_dsn: Option<&str>,
    attach_uri: Option<&str>,
    catalog_path: Option<&Path>,
    data_path: Option<&str>,
    quack_token: Option<&str>,
    quack_insecure_tls: bool,
    duckdb_path: &Path,
    local_storage_dir: &Path,
) -> Result<DuckLakeAttachPlan> {
    if postgres_dsn.is_some() && attach_uri.is_some() {
        anyhow::bail!(
            "set only one of CANARDSTACK_POSTGRES_DSN or CANARDSTACK_DUCKLAKE_ATTACH_URI"
        );
    }
    if catalog_path.is_some() && (postgres_dsn.is_some() || attach_uri.is_some()) {
        anyhow::bail!(
            "CANARDSTACK_DUCKLAKE_CATALOG_PATH can only be set with the local DuckDB-backed DuckLake catalog"
        );
    }

    let data_path = data_path
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string);

    if let Some(uri) = attach_uri {
        let uri = uri.trim();
        if uri.is_empty() {
            anyhow::bail!("CANARDSTACK_DUCKLAKE_ATTACH_URI must not be empty");
        }
        if uri.to_ascii_uppercase().starts_with("ATTACH ") {
            anyhow::bail!(
                "CANARDSTACK_DUCKLAKE_ATTACH_URI must be the URI only, not an ATTACH statement"
            );
        }
        let is_motherduck = uri.starts_with("md:");
        let is_ducklake = uri.starts_with("ducklake:");
        let is_quack = uri.starts_with("ducklake:quack:");
        if !is_motherduck && !is_ducklake {
            anyhow::bail!(
                "CANARDSTACK_DUCKLAKE_ATTACH_URI must be an md: or ducklake: URI because Arrow appends write through DuckLake"
            );
        }
        let quack_secret_sql = if is_quack {
            let token = quack_token
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "CANARDSTACK_DUCKLAKE_QUACK_TOKEN must be set when CANARDSTACK_DUCKLAKE_ATTACH_URI uses ducklake:quack:"
                    )
                })?;
            let scope = uri.strip_prefix("ducklake:").unwrap_or(uri);
            // When a Quack catalog is fronted by TLS with a self-signed cert,
            // the Quack client assumes HTTPS for the non-local host and would
            // reject the cert. A scoped HTTP secret with VERIFY_SSL 0 skips
            // verification for the catalog URL only (S3/other HTTPS still
            // verifies); the Quack token still authenticates.
            // `DISABLE_SSL` is intentionally not used — DuckLake rejects it, and the
            // quack secret has no SSL parameter.
            let insecure_tls_sql = if quack_insecure_tls {
                let host = scope.strip_prefix("quack:").unwrap_or(scope);
                format!(
                    "CREATE OR REPLACE SECRET canardstack_quack_tls (TYPE HTTP, SCOPE 'https://{}', VERIFY_SSL 0); ",
                    sql_string(host),
                )
            } else {
                String::new()
            };
            format!(
                "{insecure_tls_sql}CREATE OR REPLACE SECRET canardstack_ducklake_quack (TYPE quack, SCOPE '{}', TOKEN '{}'); ",
                sql_string(scope),
                sql_string(token),
            )
        } else {
            String::new()
        };
        if is_motherduck && data_path.is_some() {
            anyhow::bail!(
                "CANARDSTACK_DUCKLAKE_DATA_PATH cannot be set with a MotherDuck md: attach URI"
            );
        }
        let mut attach_options = Vec::new();
        if let Some(path) = data_path.as_ref() {
            attach_options.push(format!("DATA_PATH '{}'", sql_string(path)));
        }
        let attach_options = if attach_options.is_empty() {
            String::new()
        } else {
            format!(" ({})", attach_options.join(", "))
        };
        return Ok(DuckLakeAttachPlan {
            sql: format!(
                "{quack_secret_sql}ATTACH '{}' AS {DUCKLAKE_CATALOG_NAME}{attach_options}; USE {DUCKLAKE_CATALOG_NAME};",
                uri.replace('\'', "''"),
            ),
            mode: if is_motherduck {
                "ducklake_motherduck_remote"
            } else {
                "ducklake_custom_uri"
            },
            needs_ducklake: is_ducklake,
            needs_motherduck: is_motherduck,
            needs_quack: is_quack,
            needs_postgres: false,
            object_store: data_path.as_deref().and_then(object_store_kind),
        });
    }

    let resolved_data_path =
        data_path.unwrap_or_else(|| local_storage_dir.to_string_lossy().to_string());
    let object_store = object_store_kind(&resolved_data_path);
    let data_path = sql_string(&resolved_data_path);
    if let Some(dsn) = postgres_dsn {
        return Ok(DuckLakeAttachPlan {
            sql: format!(
                "ATTACH 'ducklake:postgres:{}' AS {DUCKLAKE_CATALOG_NAME} (DATA_PATH '{}'); USE {DUCKLAKE_CATALOG_NAME};",
                dsn.replace('\'', "''"),
                data_path,
            ),
            mode: "ducklake_postgres_catalog",
            needs_ducklake: true,
            needs_motherduck: false,
            needs_quack: false,
            needs_postgres: true,
            object_store,
        });
    }

    let metadata = catalog_path.map(Path::to_path_buf).unwrap_or_else(|| {
        duckdb_path
            .parent()
            .unwrap_or_else(|| std::path::Path::new("."))
            .join("canardstack.ducklake")
    });
    Ok(DuckLakeAttachPlan {
        sql: format!(
            "ATTACH 'ducklake:{}' AS {DUCKLAKE_CATALOG_NAME} (DATA_PATH '{}'); USE {DUCKLAKE_CATALOG_NAME};",
            sql_path(&metadata),
            data_path,
        ),
        mode: "ducklake_duckdb_catalog",
        needs_ducklake: true,
        needs_motherduck: false,
        needs_quack: false,
        needs_postgres: false,
        object_store,
    })
}

fn sql_string(value: &str) -> String {
    value.replace('\'', "''")
}

pub(super) fn configure_extension_directory(
    conn: &Connection,
    extension_dir: Option<&Path>,
) -> Result<()> {
    if let Some(path) = extension_dir {
        fs::create_dir_all(path)?;
        conn.execute_batch(&format!("SET extension_directory = '{}';", sql_path(path)))?;
    }
    Ok(())
}

pub(super) fn configure_base_connection(conn: &Connection) -> Result<()> {
    conn.execute_batch(&format!(
        "SET preserve_insertion_order=false;\nPRAGMA threads={DUCKDB_THREADS};"
    ))?;
    Ok(())
}

pub(super) fn configure_write_connection(conn: &Connection, memory_limit: &str) -> Result<()> {
    conn.execute_batch(&format!(
        "SET memory_limit = '{}';",
        escape_value(memory_limit)
    ))?;
    Ok(())
}
pub(super) fn quote_ident(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}

pub(super) fn ducklake_metadata_prefix(catalog_name: &str) -> String {
    format!(
        "{}.",
        quote_ident(&format!("__ducklake_metadata_{catalog_name}"))
    )
}
