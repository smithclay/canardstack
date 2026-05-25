use crate::config::{Config, DuckLakeMaintenanceConfig};
use crate::db::sql::escape_value;
use anyhow::Result;
use duckdb::Connection;
use std::fs;
use std::path::Path;

const DUCKDB_THREADS: usize = 1;
pub(super) const DUCKLAKE_CATALOG_NAME: &str = "canardlake";
pub(super) const DUCKLAKE_TARGET_PREFIX: &str = "canardlake.";

pub fn install_ducklake_extension(extension_dir: Option<&Path>) -> Result<()> {
    let conn = Connection::open_in_memory()?;
    configure_extension_directory(&conn, extension_dir)?;
    conn.execute_batch("INSTALL ducklake; LOAD ducklake; INSTALL json; LOAD json;")?;
    Ok(())
}
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
    conn.execute_batch(&plan.sql)?;
    Ok(())
}

#[derive(Clone, Debug)]
pub(super) struct DuckLakeAttachPlan {
    pub(super) sql: String,
    pub(super) mode: &'static str,
    pub(super) needs_ducklake: bool,
    pub(super) needs_motherduck: bool,
    pub(super) needs_quack: bool,
    pub(super) needs_postgres: bool,
}

#[derive(Clone, Debug)]
pub(super) struct DuckLakeMaintenanceCapability {
    pub(super) options_supported: bool,
    pub(super) checkpoint_supported: bool,
    pub(super) reason: Option<String>,
}

pub(super) fn ducklake_attach_plan(config: &Config) -> Result<DuckLakeAttachPlan> {
    build_ducklake_attach_plan(
        config.operator.postgres_dsn.as_deref(),
        config.operator.ducklake_attach_uri.as_deref(),
        config.operator.ducklake_catalog_path.as_deref(),
        config.operator.ducklake_data_path.as_deref(),
        config.operator.ducklake_quack_token.as_deref(),
        &config.operator.duckdb_path,
        &config.operator.local_storage_dir,
    )
}

pub(super) fn build_ducklake_attach_plan(
    postgres_dsn: Option<&str>,
    attach_uri: Option<&str>,
    catalog_path: Option<&Path>,
    data_path: Option<&str>,
    quack_token: Option<&str>,
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
            format!(
                "CREATE OR REPLACE SECRET canardstack_ducklake_quack (TYPE quack, SCOPE '{}', TOKEN '{}'); ",
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
        let attach_options = data_path
            .as_ref()
            .map(|path| format!(" (DATA_PATH '{}')", sql_string(path)))
            .unwrap_or_default();
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
        });
    }

    let data_path = data_path.unwrap_or_else(|| local_storage_dir.to_string_lossy().to_string());
    let data_path = sql_string(&data_path);
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
    })
}

fn sql_string(value: &str) -> String {
    value.replace('\'', "''")
}

pub(super) fn configure_ducklake_maintenance_options(
    conn: &Connection,
    config: &DuckLakeMaintenanceConfig,
) -> Result<DuckLakeMaintenanceCapability> {
    let sql = ducklake_maintenance_options_sql(config);
    match conn.execute_batch(&sql) {
        Ok(()) => Ok(DuckLakeMaintenanceCapability {
            options_supported: true,
            checkpoint_supported: true,
            reason: None,
        }),
        Err(err) if is_unsupported_ducklake_maintenance_error(&err.to_string()) => {
            Ok(DuckLakeMaintenanceCapability {
                options_supported: false,
                checkpoint_supported: false,
                reason: Some(err.to_string()),
            })
        }
        Err(err) => Err(err.into()),
    }
}

pub(super) fn ducklake_maintenance_options_sql(config: &DuckLakeMaintenanceConfig) -> String {
    let auto_compact = if config.enabled { "true" } else { "false" };
    format!(
        "\
        CALL {DUCKLAKE_CATALOG_NAME}.set_option('data_inlining_row_limit', {});\n\
        CALL {DUCKLAKE_CATALOG_NAME}.set_option('auto_compact', {auto_compact});\n\
        CALL {DUCKLAKE_CATALOG_NAME}.set_option('expire_older_than', '{} days');\n\
        CALL {DUCKLAKE_CATALOG_NAME}.set_option('delete_older_than', '{} seconds');",
        config.data_inlining_row_limit,
        config.expire_older_than_days,
        config.delete_older_than_secs,
    )
}

pub(super) fn is_unsupported_ducklake_maintenance_error(error: &str) -> bool {
    let lower = error.to_ascii_lowercase();
    (lower.contains("checkpoint")
        || lower.contains("set_option")
        || lower.contains("ducklake")
        || lower.contains("auto_compact")
        || lower.contains("data_inlining_row_limit"))
        && (lower.contains("not implemented")
            || lower.contains("not supported")
            || lower.contains("unsupported")
            || lower.contains("does not support")
            || lower.contains("does not exist")
            || lower.contains("catalog error")
            || lower.contains("unknown function"))
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
