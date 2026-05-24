use crate::config::Config;
use crate::db::sql::escape_value;
use anyhow::Result;
use duckdb::Connection;
use std::fs;
use std::path::Path;

const DUCKDB_THREADS: usize = 1;

pub fn install_ducklake_extension(extension_dir: Option<&Path>) -> Result<()> {
    let conn = Connection::open_in_memory()?;
    configure_extension_directory(&conn, extension_dir)?;
    conn.execute_batch("INSTALL ducklake; LOAD ducklake;")?;
    Ok(())
}
pub(super) fn sql_path(path: &Path) -> String {
    path.to_string_lossy().replace('\'', "''")
}

pub(super) fn attach_ducklake_connection(
    conn: &Connection,
    postgres_dsn: Option<&str>,
    attach_uri: Option<&str>,
    duckdb_path: &Path,
    local_storage_dir: &Path,
    extension_dir: Option<&Path>,
) -> Result<()> {
    configure_extension_directory(conn, extension_dir)?;
    let plan =
        build_ducklake_attach_plan(postgres_dsn, attach_uri, duckdb_path, local_storage_dir)?;

    if plan.needs_motherduck && conn.execute_batch("LOAD md;").is_err() {
        conn.execute_batch("INSTALL md; LOAD md;")?;
    }
    if plan.needs_ducklake && conn.execute_batch("LOAD ducklake;").is_err() {
        conn.execute_batch("INSTALL ducklake; LOAD ducklake;")?;
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
    pub(super) needs_postgres: bool,
    pub(super) managed_maintenance: bool,
}

pub(super) fn ducklake_attach_plan(config: &Config) -> Result<DuckLakeAttachPlan> {
    build_ducklake_attach_plan(
        config.operator.postgres_dsn.as_deref(),
        config.operator.ducklake_attach_uri.as_deref(),
        &config.operator.duckdb_path,
        &config.operator.local_storage_dir,
    )
}

pub(super) fn build_ducklake_attach_plan(
    postgres_dsn: Option<&str>,
    attach_uri: Option<&str>,
    duckdb_path: &Path,
    local_storage_dir: &Path,
) -> Result<DuckLakeAttachPlan> {
    if postgres_dsn.is_some() && attach_uri.is_some() {
        anyhow::bail!(
            "set only one of CANARDSTACK_POSTGRES_DSN or CANARDSTACK_DUCKLAKE_ATTACH_URI"
        );
    }

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
        if !is_motherduck && !is_ducklake {
            anyhow::bail!(
                "CANARDSTACK_DUCKLAKE_ATTACH_URI must be an md: or ducklake: URI because Arrow appends write through DuckLake"
            );
        }
        return Ok(DuckLakeAttachPlan {
            sql: format!(
                "ATTACH '{}' AS canardlake; USE canardlake;",
                uri.replace('\'', "''"),
            ),
            mode: if is_motherduck {
                "ducklake_motherduck_remote"
            } else {
                "ducklake_custom_uri"
            },
            needs_ducklake: is_ducklake,
            needs_motherduck: is_motherduck,
            needs_postgres: false,
            managed_maintenance: is_ducklake,
        });
    }

    let data_path = sql_path(local_storage_dir);
    if let Some(dsn) = postgres_dsn {
        return Ok(DuckLakeAttachPlan {
            sql: format!(
                "ATTACH 'ducklake:postgres:{}' AS canardlake (DATA_PATH '{}'); USE canardlake;",
                dsn.replace('\'', "''"),
                data_path,
            ),
            mode: "ducklake_postgres_catalog",
            needs_ducklake: true,
            needs_motherduck: false,
            needs_postgres: true,
            managed_maintenance: true,
        });
    }

    let metadata = duckdb_path
        .parent()
        .unwrap_or_else(|| std::path::Path::new("."))
        .join("canardstack.ducklake");
    Ok(DuckLakeAttachPlan {
        sql: format!(
            "ATTACH 'ducklake:{}' AS canardlake (DATA_PATH '{}'); USE canardlake;",
            sql_path(&metadata),
            data_path,
        ),
        mode: "ducklake_duckdb_catalog",
        needs_ducklake: true,
        needs_motherduck: false,
        needs_postgres: false,
        managed_maintenance: true,
    })
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
