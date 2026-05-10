use crate::config::Config;
use crate::ingest::Signal;
use crate::sql::{escape_value, quote as sql_quote};
use crate::validation::record_timestamp_ms;
use crate::LockExt;
use anyhow::{Context, Result};
use chrono::{TimeZone, Utc};
use duckdb::types::Value as DuckValue;
use duckdb::{params_from_iter, Connection};
use serde::Serialize;
use serde_json::{json, Value};
use std::fs;
use std::path::Path;
use std::path::PathBuf;
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::Duration;

const DUCKDB_THREADS: usize = 1;
const INSERT_TRANSACTION_MAX_ROWS: usize = 500;
const INSERT_TRANSACTION_MAX_BYTES: usize = 1024 * 1024;

#[derive(Clone, Debug, Serialize)]
pub struct StorageHealth {
    pub healthy: bool,
    pub mode: String,
    pub ducklake_catalog: String,
    pub ducklake_available: bool,
    pub ducklake_required: bool,
    pub postgres_catalog_configured: bool,
    pub last_error: Option<String>,
    pub capabilities: StorageCapabilities,
    pub freshness_watermarks: Value,
    pub logical_rows: Value,
    pub physical_bytes: u64,
}

#[derive(Clone, Debug, Serialize)]
pub struct StorageProbe {
    pub healthy: bool,
    pub mode: String,
    pub ducklake_available: bool,
    pub ducklake_required: bool,
    pub last_error: Option<String>,
}

impl StorageProbe {
    pub fn is_ready(&self) -> bool {
        self.healthy && (!self.ducklake_required || self.ducklake_available)
    }
}

impl StorageHealth {
    pub fn is_ready(&self) -> bool {
        self.healthy && (!self.ducklake_required || self.ducklake_available)
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct StorageCapabilities {
    pub insert: bool,
    pub query: bool,
    pub inlined_flush: bool,
    pub snapshot_expiration: bool,
    pub cleanup_old_files: bool,
    pub merge_adjacent_files: bool,
    pub whole_day_retention: bool,
}

pub struct Storage {
    /// Write-side connection. Held for inserts, DDL, and DuckLake maintenance.
    /// Reader path never touches this mutex — that decoupling keeps /healthz,
    /// /metrics, and queries responsive while a flush is in flight.
    writer: Mutex<Connection>,
    /// Cloned from `writer` at startup after ATTACH + DDL; shares the
    /// underlying Database (attached schemas survive `try_clone`). Source of
    /// per-query clones in `with_query_conn`.
    reader: Mutex<Connection>,
    target_prefix: String,
    mode: String,
    catalog_name: String,
    ducklake_available: bool,
    postgres_catalog_configured: bool,
    local_storage_dir: PathBuf,
    ducklake_required: bool,
    ducklake_managed_maintenance: bool,
    write_memory_limit: String,
    last_error: Mutex<Option<String>>,
}

pub struct RetentionPolicy {
    pub logs_days: i64,
    pub spans_days: i64,
    pub metrics_days: i64,
}

/// Typed timeout from `with_query_conn`. Downcastable so callers can classify
/// the 503 as `query_timeout` without substring-matching the error message.
#[derive(Debug)]
pub struct QueryTimeoutError {
    pub timeout: Duration,
}

impl std::fmt::Display for QueryTimeoutError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "query timeout after {}ms", self.timeout.as_millis())
    }
}

impl std::error::Error for QueryTimeoutError {}

/// Storage insert failed after one or more smaller transactions may already
/// have committed. Callers use `committed_rows` to avoid retrying rows that
/// DuckDB has already accepted.
#[derive(Debug)]
pub struct InsertRecordsError {
    pub table: Signal,
    pub committed_rows: usize,
    pub attempted_rows: usize,
    pub source: anyhow::Error,
}

impl std::fmt::Display for InsertRecordsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "insert failed for {} after committing {}/{} row(s): {}",
            self.table, self.committed_rows, self.attempted_rows, self.source
        )
    }
}

impl std::error::Error for InsertRecordsError {}

impl Storage {
    pub fn open(config: &Config) -> Result<Self> {
        if let Some(parent) = config.duckdb_path.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::create_dir_all(&config.local_storage_dir)?;

        let writer = Connection::open(&config.duckdb_path)
            .with_context(|| format!("open DuckDB file {}", config.duckdb_path.display()))?;
        configure_base_connection(&writer)?;
        configure_write_connection(&writer, &config.duckdb_write_memory_limit)?;

        let mut target_prefix = String::new();
        let mut mode = "duckdb_local".to_string();
        let mut ducklake_available = false;
        let mut ducklake_managed_maintenance = false;

        if config.use_ducklake {
            attach_ducklake_connection(
                &writer,
                config.postgres_dsn.as_deref(),
                config.ducklake_attach_uri.as_deref(),
                &config.duckdb_path,
                &config.local_storage_dir,
                config.duckdb_extension_dir.as_deref(),
            )
            .context(
                "DuckLake attach failed. Fix the catalog config (URI, token, network) and \
                 restart, or set CANARDSTACK_USE_DUCKLAKE=false to run in local-only mode \
                 (development/testing — telemetry stays in a local DuckDB file).",
            )?;
            target_prefix = "canardlake.".to_string();
            let plan = ducklake_attach_plan(config)?;
            mode = plan.mode.to_string();
            ducklake_managed_maintenance = plan.managed_maintenance;
            ducklake_available = true;
        }

        create_tables_on(&writer, &target_prefix)?;

        // Reader must be cloned AFTER attach + create_tables so the new
        // session inherits the catalog.
        let reader = writer
            .try_clone()
            .context("clone writer connection for reader pool")?;
        configure_base_connection(&reader)?;

        Ok(Self {
            writer: Mutex::new(writer),
            reader: Mutex::new(reader),
            target_prefix,
            mode,
            catalog_name: "canardlake".to_string(),
            ducklake_available,
            postgres_catalog_configured: config.postgres_dsn.is_some(),
            local_storage_dir: config.local_storage_dir.clone(),
            ducklake_required: config.use_ducklake,
            ducklake_managed_maintenance,
            write_memory_limit: config.duckdb_write_memory_limit.clone(),
            last_error: Mutex::new(None),
        })
    }

    pub fn healthy(&self) -> bool {
        match self.check_health_target() {
            Ok(()) => {
                *self.last_error.lock_or_poisoned() = None;
                true
            }
            Err(err) => {
                *self.last_error.lock_or_poisoned() = Some(err.to_string());
                false
            }
        }
    }

    pub fn accepts_memory_ingest(&self) -> bool {
        !self.ducklake_required || self.ducklake_available
    }

    pub fn health(&self) -> StorageHealth {
        StorageHealth {
            healthy: self.healthy(),
            mode: self.mode.clone(),
            ducklake_catalog: self.catalog_name.clone(),
            ducklake_available: self.ducklake_available,
            ducklake_required: self.ducklake_required,
            postgres_catalog_configured: self.postgres_catalog_configured,
            last_error: self.last_error.lock_or_poisoned().clone(),
            capabilities: StorageCapabilities {
                insert: true,
                query: true,
                inlined_flush: self.ducklake_managed_maintenance,
                snapshot_expiration: self.ducklake_managed_maintenance,
                cleanup_old_files: self.ducklake_managed_maintenance,
                merge_adjacent_files: self.ducklake_managed_maintenance,
                whole_day_retention: true,
            },
            freshness_watermarks: self
                .freshness_watermarks()
                .unwrap_or_else(|err| json!({"error": err.to_string()})),
            logical_rows: self
                .logical_rows()
                .unwrap_or_else(|err| json!({"error": err.to_string()})),
            physical_bytes: dir_size(&self.local_storage_dir).unwrap_or(0),
        }
    }

    pub fn probe(&self) -> StorageProbe {
        StorageProbe {
            healthy: self.healthy(),
            mode: self.mode.clone(),
            ducklake_available: self.ducklake_available,
            ducklake_required: self.ducklake_required,
            last_error: self.last_error.lock_or_poisoned().clone(),
        }
    }

    pub fn insert_records(
        &self,
        table: Signal,
        records: &[Value],
        source_format: &str,
    ) -> Result<usize> {
        if records.is_empty() {
            return Ok(0);
        }
        let cols = table_columns(table);
        let sql = insert_sql(&self.target_prefix, table.as_str(), cols);
        let mut conn = self.writer.lock_or_poisoned();
        configure_write_connection(&conn, &self.write_memory_limit)?;
        let mut committed_rows = 0;
        for chunk in insert_chunks(records) {
            if let Err(err) = insert_record_chunk(&mut conn, table, chunk, source_format, &sql) {
                *self.last_error.lock_or_poisoned() = Some(err.to_string());
                return Err(anyhow::Error::new(InsertRecordsError {
                    table,
                    committed_rows,
                    attempted_rows: records.len(),
                    source: err,
                }));
            }
            committed_rows += chunk.len();
        }
        *self.last_error.lock_or_poisoned() = None;
        Ok(committed_rows)
    }

    /// Read-side connection access for SELECT-only paths. Do not call into
    /// write paths from inside the closure.
    pub fn with_conn<T>(&self, f: impl FnOnce(&Connection, &str) -> Result<T>) -> Result<T> {
        let conn = self.reader.lock_or_poisoned();
        f(&conn, &self.target_prefix)
    }

    pub fn with_query_conn<T>(
        &self,
        memory_limit: &str,
        timeout: Duration,
        f: impl FnOnce(&Connection, &str) -> Result<T>,
    ) -> Result<T> {
        let conn = self.open_scoped_query_connection()?;
        conn.execute_batch(&format!(
            "SET memory_limit = '{}';",
            escape_value(memory_limit)
        ))?;

        let interrupt = conn.interrupt_handle();
        let state = Arc::new((Mutex::new(false), Condvar::new()));
        let timer_state = state.clone();
        let timer = thread::spawn(move || {
            let (done, cvar) = &*timer_state;
            let done = done.lock_or_poisoned();
            let (done, wait) = cvar.wait_timeout(done, timeout).unwrap();
            if !*done && wait.timed_out() {
                interrupt.interrupt();
                return true;
            }
            false
        });

        let result = f(&conn, &self.target_prefix);
        let (done, cvar) = &*state;
        *done.lock_or_poisoned() = true;
        cvar.notify_one();
        // Timer panic ⇒ "not timed out": trust the query result over a panicked timer.
        let timed_out = match timer.join() {
            Ok(fired) => fired,
            Err(_) => {
                crate::log_event("warn", "query_timer_panicked", &[]);
                false
            }
        };
        // Timer fired ⇒ surface a typed timeout even if the query raced to Ok.
        if timed_out {
            return Err(anyhow::Error::new(QueryTimeoutError { timeout }));
        }
        result
    }

    fn open_scoped_query_connection(&self) -> Result<Connection> {
        // Clone from `reader`, not `writer`: queries don't block on flushes.
        // Attached schemas + extensions are inherited; only per-conn PRAGMAs
        // are reapplied here.
        let cloned = {
            let parent = self.reader.lock_or_poisoned();
            parent.try_clone().context("clone DuckDB connection")?
        };
        configure_base_connection(&cloned)?;
        Ok(cloned)
    }

    pub fn flush_inlined_data(&self, table: Option<&str>) -> Result<Value> {
        if !self.ducklake_managed_maintenance {
            return Ok(
                json!({"supported": false, "reason": "ducklake maintenance is not managed by this process"}),
            );
        }
        let conn = self.writer.lock_or_poisoned();
        let sql = match table {
            Some(t) => format!(
                "SELECT * FROM ducklake_flush_inlined_data('{}', table_name => '{}')",
                self.catalog_name,
                escape_value(t)
            ),
            None => format!(
                "SELECT * FROM ducklake_flush_inlined_data('{}')",
                self.catalog_name
            ),
        };
        conn.execute_batch(&sql)?;
        Ok(json!({"supported": true, "status": "ok"}))
    }

    pub fn cleanup_old_files(&self, dry_run: bool) -> Result<Value> {
        if !self.ducklake_managed_maintenance {
            return Ok(
                json!({"supported": false, "reason": "ducklake maintenance is not managed by this process"}),
            );
        }
        self.writer.lock_or_poisoned().execute_batch(&format!(
            "SELECT * FROM ducklake_cleanup_old_files('{}', dry_run => {})",
            self.catalog_name, dry_run
        ))?;
        Ok(json!({"supported": true, "status": "ok", "dry_run": dry_run}))
    }

    pub fn expire_snapshots(&self, older_than_days: i64) -> Result<Value> {
        if !self.ducklake_managed_maintenance {
            return Ok(
                json!({"supported": false, "reason": "ducklake maintenance is not managed by this process"}),
            );
        }
        let older_than = (Utc::now() - chrono::Duration::days(older_than_days)).to_rfc3339();
        self.writer.lock_or_poisoned().execute_batch(&format!(
            "SELECT * FROM ducklake_expire_snapshots('{}', older_than => TIMESTAMPTZ '{}')",
            self.catalog_name,
            older_than.replace('\'', "''")
        ))?;
        Ok(json!({"supported": true, "status": "ok", "older_than": older_than}))
    }

    pub fn enforce_retention(&self, policy: &RetentionPolicy, dry_run: bool) -> Result<Value> {
        let conn = self.writer.lock_or_poisoned();
        let mut results = Vec::new();
        for target in [
            ("logs", policy.logs_days, "event_date"),
            ("spans", policy.spans_days, "event_date"),
            ("metric_gauge", policy.metrics_days, "event_date"),
            ("metric_sum", policy.metrics_days, "event_date"),
        ] {
            let (table, retention_days, date_column) = target;
            let cutoff = (Utc::now() - chrono::Duration::days(retention_days))
                .format("%Y-%m-%d")
                .to_string();
            let full_table = format!("{}{}", self.target_prefix, table);
            let predicate = if date_column == "event_date" {
                format!("{date_column} < DATE {}", sql_quote(&cutoff))
            } else {
                format!(
                    "{date_column} < TIMESTAMP {}",
                    sql_quote(&format!("{cutoff} 00:00:00"))
                )
            };
            let count_sql = format!("SELECT count(*) FROM {full_table} WHERE {predicate}");
            let matching_rows: i64 = conn.query_row(&count_sql, [], |row| row.get(0))?;
            let deleted_rows = if dry_run || matching_rows == 0 {
                0
            } else {
                let delete_sql = format!("DELETE FROM {full_table} WHERE {predicate}");
                conn.execute(&delete_sql, [])? as i64
            };
            results.push(json!({
                "table": table,
                "retention_days": retention_days,
                "cutoff_date": cutoff,
                "matching_rows": matching_rows,
                "deleted_rows": deleted_rows
            }));
        }
        Ok(json!({"dry_run": dry_run, "tables": results}))
    }

    pub fn freshness_watermarks(&self) -> Result<Value> {
        self.with_conn(|conn, prefix| {
            let mut map = serde_json::Map::new();
            for table in ["logs", "spans", "metric_gauge", "metric_sum"] {
                let sql = format!(
                    "SELECT max(timestamp)::VARCHAR, epoch(max(timestamp)), max(ingested_at)::VARCHAR, epoch(max(ingested_at)) FROM {prefix}{table}"
                );
                let (
                    event_watermark,
                    event_watermark_epoch,
                    ingest_watermark,
                    ingest_watermark_epoch,
                ): (Option<String>, Option<f64>, Option<String>, Option<f64>) =
                    conn.query_row(&sql, [], |row| {
                        Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
                    })?;
                let ingest_lag_seconds = ingest_watermark_epoch
                    .map(|epoch| Utc::now().timestamp_millis() as f64 / 1000.0 - epoch);
                let event_lag_seconds = event_watermark_epoch
                    .map(|epoch| Utc::now().timestamp_millis() as f64 / 1000.0 - epoch);
                map.insert(
                    table.to_string(),
                    json!({
                        "timestamp": event_watermark,
                        "epoch_seconds": event_watermark_epoch,
                        "event_lag_seconds": event_lag_seconds,
                        "ingested_at": ingest_watermark,
                        "ingested_at_epoch_seconds": ingest_watermark_epoch,
                        "lag_seconds": ingest_lag_seconds
                    }),
                );
            }
            Ok(Value::Object(map))
        })
    }

    pub fn logical_rows(&self) -> Result<Value> {
        self.with_conn(|conn, prefix| {
            let mut map = serde_json::Map::new();
            for table in ["logs", "spans", "metric_gauge", "metric_sum"] {
                let sql = format!("SELECT count(*) FROM {prefix}{table}");
                let rows: i64 = conn.query_row(&sql, [], |row| row.get(0))?;
                map.insert(table.to_string(), json!(rows));
            }
            Ok(Value::Object(map))
        })
    }

    fn check_health_target(&self) -> Result<()> {
        // Reader, not writer — a stuck flush must not hang /healthz.
        let conn = self.reader.lock_or_poisoned();
        conn.query_row("SELECT 1", [], |_| Ok(()))?;
        let sql = format!("SELECT * FROM {}logs LIMIT 0", self.target_prefix);
        let _stmt = conn.prepare(&sql)?;
        Ok(())
    }
}

fn create_tables_on(conn: &Connection, prefix: &str) -> Result<()> {
    let mut ddl = String::new();
    for table in [
        Signal::Logs,
        Signal::Spans,
        Signal::MetricGauge,
        Signal::MetricSum,
    ] {
        ddl.push_str(&create_table_sql(
            prefix,
            table.as_str(),
            table_columns(table),
        ));
        ddl.push('\n');
    }
    conn.execute_batch(&ddl)?;
    Ok(())
}

pub fn install_ducklake_extension(extension_dir: Option<&Path>) -> Result<()> {
    let conn = Connection::open_in_memory()?;
    configure_extension_directory(&conn, extension_dir)?;
    conn.execute_batch("INSTALL ducklake; LOAD ducklake;")?;
    Ok(())
}

fn timestamp_string(record: &Value) -> String {
    let ms = record_timestamp_ms(record).unwrap_or_else(|| Utc::now().timestamp_millis());
    timestamp_ms_string(ms)
}

fn now_timestamp_string() -> String {
    timestamp_ms_string(Utc::now().timestamp_millis())
}

fn timestamp_ms_string(ms: i64) -> String {
    Utc.timestamp_millis_opt(ms)
        .single()
        .unwrap_or_else(Utc::now)
        .format("%Y-%m-%d %H:%M:%S%.3f")
        .to_string()
}

fn event_date(record: &Value) -> String {
    let ms = record_timestamp_ms(record).unwrap_or_else(|| Utc::now().timestamp_millis());
    Utc.timestamp_millis_opt(ms)
        .single()
        .unwrap_or_else(Utc::now)
        .format("%Y-%m-%d")
        .to_string()
}

fn opt_s(record: &Value, key: &str) -> Option<String> {
    record.get(key).and_then(|v| {
        if v.is_null() {
            None
        } else if let Some(s) = v.as_str() {
            Some(s.to_string())
        } else {
            Some(v.to_string())
        }
    })
}

fn opt_i(record: &Value, key: &str) -> Option<i64> {
    record
        .get(key)
        .and_then(|v| v.as_i64().or_else(|| v.as_str()?.parse().ok()))
}

fn opt_f(record: &Value, key: &str) -> Option<f64> {
    record
        .get(key)
        .and_then(|v| v.as_f64().or_else(|| v.as_str()?.parse().ok()))
}

fn opt_b(record: &Value, key: &str) -> Option<bool> {
    record
        .get(key)
        .and_then(|v| v.as_bool().or_else(|| v.as_str()?.parse().ok()))
}

fn promoted(record: &Value, attr_field: &str, attr_key: &str) -> Option<String> {
    let raw = opt_s(record, attr_field)?;
    let parsed: Value = serde_json::from_str(&raw).ok()?;
    parsed.get(attr_key).map(|v| {
        if let Some(s) = v.as_str() {
            s.to_string()
        } else {
            v.to_string()
        }
    })
}

fn promoted_i(record: &Value, attr_field: &str, attr_key: &str) -> Option<i64> {
    let raw = opt_s(record, attr_field)?;
    let parsed: Value = serde_json::from_str(&raw).ok()?;
    parsed
        .get(attr_key)
        .and_then(|v| v.as_i64().or_else(|| v.as_str()?.parse().ok()))
}

fn sql_path(path: &Path) -> String {
    path.to_string_lossy().replace('\'', "''")
}

fn attach_ducklake_connection(
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
struct DuckLakeAttachPlan {
    sql: String,
    mode: &'static str,
    needs_ducklake: bool,
    needs_motherduck: bool,
    needs_postgres: bool,
    managed_maintenance: bool,
}

fn ducklake_attach_plan(config: &Config) -> Result<DuckLakeAttachPlan> {
    build_ducklake_attach_plan(
        config.postgres_dsn.as_deref(),
        config.ducklake_attach_uri.as_deref(),
        &config.duckdb_path,
        &config.local_storage_dir,
    )
}

fn build_ducklake_attach_plan(
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
        return Ok(DuckLakeAttachPlan {
            sql: format!(
                "ATTACH '{}' AS canardlake; USE canardlake;",
                uri.replace('\'', "''")
            ),
            mode: if is_motherduck {
                "ducklake_motherduck_remote"
            } else if is_ducklake {
                "ducklake_custom_uri"
            } else {
                "duckdb_custom_attach_uri"
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
                "ATTACH 'ducklake:postgres:{}' AS canardlake (DATA_PATH '{}', DATA_INLINING_ROW_LIMIT 1000); USE canardlake;",
                dsn.replace('\'', "''"),
                data_path
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
            "ATTACH 'ducklake:{}' AS canardlake (DATA_PATH '{}', DATA_INLINING_ROW_LIMIT 1000); USE canardlake;",
            sql_path(&metadata),
            data_path
        ),
        mode: "ducklake_duckdb_catalog",
        needs_ducklake: true,
        needs_motherduck: false,
        needs_postgres: false,
        managed_maintenance: true,
    })
}

fn configure_extension_directory(conn: &Connection, extension_dir: Option<&Path>) -> Result<()> {
    if let Some(path) = extension_dir {
        fs::create_dir_all(path)?;
        conn.execute_batch(&format!("SET extension_directory = '{}';", sql_path(path)))?;
    }
    Ok(())
}

fn configure_base_connection(conn: &Connection) -> Result<()> {
    conn.execute_batch(&format!(
        "SET preserve_insertion_order=false;\nPRAGMA threads={DUCKDB_THREADS};"
    ))?;
    Ok(())
}

fn configure_write_connection(conn: &Connection, memory_limit: &str) -> Result<()> {
    conn.execute_batch(&format!(
        "SET memory_limit = '{}';",
        escape_value(memory_limit)
    ))?;
    Ok(())
}

fn insert_record_chunk(
    conn: &mut Connection,
    table: Signal,
    records: &[Value],
    source_format: &str,
    sql: &str,
) -> Result<()> {
    let tx = conn.transaction()?;
    {
        let mut stmt = tx.prepare(sql)?;
        for record in records {
            let bound = bind_record(table, record, source_format);
            stmt.execute(params_from_iter(bound.iter()))?;
        }
    }
    tx.commit()?;
    Ok(())
}

fn insert_chunks(records: &[Value]) -> Vec<&[Value]> {
    let mut chunks = Vec::new();
    let mut start = 0;
    let mut rows = 0;
    let mut bytes = 0;
    for (idx, record) in records.iter().enumerate() {
        let row_bytes = record.to_string().len().max(1);
        let would_exceed = rows > 0
            && (rows >= INSERT_TRANSACTION_MAX_ROWS
                || bytes + row_bytes > INSERT_TRANSACTION_MAX_BYTES);
        if would_exceed {
            chunks.push(&records[start..idx]);
            start = idx;
            rows = 0;
            bytes = 0;
        }
        rows += 1;
        bytes += row_bytes;
    }
    if start < records.len() {
        chunks.push(&records[start..]);
    }
    chunks
}

fn dir_size(path: &Path) -> Result<u64> {
    if !path.exists() {
        return Ok(0);
    }
    let mut total = 0;
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        let metadata = entry.metadata()?;
        if metadata.is_dir() {
            total += dir_size(&entry.path())?;
        } else {
            total += metadata.len();
        }
    }
    Ok(total)
}

const LOGS_COLUMNS: &[(&str, &str)] = &[
    ("timestamp", "TIMESTAMP"),
    ("ingested_at", "TIMESTAMP"),
    ("event_date", "DATE"),
    ("source_format", "VARCHAR"),
    ("trace_id", "VARCHAR"),
    ("span_id", "VARCHAR"),
    ("service_name", "VARCHAR"),
    ("service_namespace", "VARCHAR"),
    ("service_instance_id", "VARCHAR"),
    ("severity_number", "INTEGER"),
    ("severity_text", "VARCHAR"),
    ("body", "VARCHAR"),
    ("resource_attributes", "VARCHAR"),
    ("scope_name", "VARCHAR"),
    ("scope_version", "VARCHAR"),
    ("scope_attributes", "VARCHAR"),
    ("log_attributes", "VARCHAR"),
    ("deployment_environment", "VARCHAR"),
    ("http_method", "VARCHAR"),
    ("http_status_code", "INTEGER"),
    ("http_route", "VARCHAR"),
    ("exception_type", "VARCHAR"),
];

const SPANS_COLUMNS: &[(&str, &str)] = &[
    ("timestamp", "TIMESTAMP"),
    ("ingested_at", "TIMESTAMP"),
    ("event_date", "DATE"),
    ("source_format", "VARCHAR"),
    ("end_timestamp", "BIGINT"),
    ("duration", "BIGINT"),
    ("trace_id", "VARCHAR"),
    ("span_id", "VARCHAR"),
    ("parent_span_id", "VARCHAR"),
    ("trace_state", "VARCHAR"),
    ("span_name", "VARCHAR"),
    ("span_kind", "INTEGER"),
    ("status_code", "INTEGER"),
    ("status_message", "VARCHAR"),
    ("service_name", "VARCHAR"),
    ("service_namespace", "VARCHAR"),
    ("service_instance_id", "VARCHAR"),
    ("scope_name", "VARCHAR"),
    ("scope_version", "VARCHAR"),
    ("scope_attributes", "VARCHAR"),
    ("span_attributes", "VARCHAR"),
    ("resource_attributes", "VARCHAR"),
    ("events_json", "VARCHAR"),
    ("links_json", "VARCHAR"),
    ("dropped_attributes_count", "INTEGER"),
    ("dropped_events_count", "INTEGER"),
    ("dropped_links_count", "INTEGER"),
    ("flags", "INTEGER"),
    ("deployment_environment", "VARCHAR"),
    ("http_method", "VARCHAR"),
    ("http_status_code", "INTEGER"),
    ("http_route", "VARCHAR"),
    ("exception_type", "VARCHAR"),
];

const METRIC_GAUGE_COLUMNS: &[(&str, &str)] = &[
    ("timestamp", "TIMESTAMP"),
    ("ingested_at", "TIMESTAMP"),
    ("event_date", "DATE"),
    ("source_format", "VARCHAR"),
    ("start_timestamp", "BIGINT"),
    ("metric_name", "VARCHAR"),
    ("metric_description", "VARCHAR"),
    ("metric_unit", "VARCHAR"),
    ("value", "DOUBLE"),
    ("service_name", "VARCHAR"),
    ("service_namespace", "VARCHAR"),
    ("service_instance_id", "VARCHAR"),
    ("resource_attributes", "VARCHAR"),
    ("scope_name", "VARCHAR"),
    ("scope_version", "VARCHAR"),
    ("scope_attributes", "VARCHAR"),
    ("metric_attributes", "VARCHAR"),
    ("flags", "INTEGER"),
    ("exemplars_json", "VARCHAR"),
    ("deployment_environment", "VARCHAR"),
];

const METRIC_SUM_COLUMNS: &[(&str, &str)] = &[
    ("timestamp", "TIMESTAMP"),
    ("ingested_at", "TIMESTAMP"),
    ("event_date", "DATE"),
    ("source_format", "VARCHAR"),
    ("start_timestamp", "BIGINT"),
    ("metric_name", "VARCHAR"),
    ("metric_description", "VARCHAR"),
    ("metric_unit", "VARCHAR"),
    ("value", "DOUBLE"),
    ("service_name", "VARCHAR"),
    ("service_namespace", "VARCHAR"),
    ("service_instance_id", "VARCHAR"),
    ("resource_attributes", "VARCHAR"),
    ("scope_name", "VARCHAR"),
    ("scope_version", "VARCHAR"),
    ("scope_attributes", "VARCHAR"),
    ("metric_attributes", "VARCHAR"),
    ("flags", "INTEGER"),
    ("exemplars_json", "VARCHAR"),
    ("deployment_environment", "VARCHAR"),
    ("aggregation_temporality", "INTEGER"),
    ("is_monotonic", "BOOLEAN"),
];

fn table_columns(table: Signal) -> &'static [(&'static str, &'static str)] {
    match table {
        Signal::Logs => LOGS_COLUMNS,
        Signal::Spans => SPANS_COLUMNS,
        Signal::MetricGauge => METRIC_GAUGE_COLUMNS,
        Signal::MetricSum => METRIC_SUM_COLUMNS,
    }
}

fn create_table_sql(prefix: &str, name: &str, cols: &[(&str, &str)]) -> String {
    let body = cols
        .iter()
        .map(|(col, ty)| format!("  {col} {ty}"))
        .collect::<Vec<_>>()
        .join(",\n");
    format!("CREATE TABLE IF NOT EXISTS {prefix}{name} (\n{body}\n);")
}

fn insert_sql(prefix: &str, name: &str, cols: &[(&str, &str)]) -> String {
    let names = cols
        .iter()
        .map(|(col, _)| *col)
        .collect::<Vec<_>>()
        .join(", ");
    let placeholders = cols
        .iter()
        .map(|(_, ty)| placeholder_for(ty))
        .collect::<Vec<_>>()
        .join(", ");
    format!("INSERT INTO {prefix}{name} ({names}) VALUES ({placeholders})")
}

fn placeholder_for(sql_type: &str) -> &'static str {
    match sql_type {
        "TIMESTAMP" => "CAST(? AS TIMESTAMP)",
        "DATE" => "CAST(? AS DATE)",
        _ => "?",
    }
}

fn bind_record(table: Signal, record: &Value, source_format: &str) -> Vec<DuckValue> {
    match table {
        Signal::Logs => bind_logs(record, source_format),
        Signal::Spans => bind_spans(record, source_format),
        Signal::MetricGauge => bind_metric_gauge(record, source_format),
        Signal::MetricSum => bind_metric_sum(record, source_format),
    }
}

fn bind_logs(record: &Value, source_format: &str) -> Vec<DuckValue> {
    vec![
        DuckValue::Text(timestamp_string(record)),
        DuckValue::Text(now_timestamp_string()),
        DuckValue::Text(event_date(record)),
        DuckValue::Text(source_format.to_string()),
        opt_str(record, "trace_id"),
        opt_str(record, "span_id"),
        opt_str(record, "service_name"),
        opt_str(record, "service_namespace"),
        opt_str(record, "service_instance_id"),
        opt_int(record, "severity_number"),
        opt_str(record, "severity_text"),
        opt_str(record, "body"),
        opt_str(record, "resource_attributes"),
        opt_str(record, "scope_name"),
        opt_str(record, "scope_version"),
        opt_str(record, "scope_attributes"),
        opt_str(record, "log_attributes"),
        promoted_str(record, "resource_attributes", "deployment.environment"),
        promoted_str_alts(
            record,
            "log_attributes",
            &["http.request.method", "http.method"],
        ),
        promoted_int_alts(
            record,
            "log_attributes",
            &["http.response.status_code", "http.status_code"],
        ),
        promoted_str(record, "log_attributes", "http.route"),
        promoted_str(record, "log_attributes", "exception.type"),
    ]
}

fn bind_spans(record: &Value, source_format: &str) -> Vec<DuckValue> {
    vec![
        DuckValue::Text(timestamp_string(record)),
        DuckValue::Text(now_timestamp_string()),
        DuckValue::Text(event_date(record)),
        DuckValue::Text(source_format.to_string()),
        opt_int(record, "end_timestamp"),
        opt_int(record, "duration"),
        opt_str(record, "trace_id"),
        opt_str(record, "span_id"),
        opt_str(record, "parent_span_id"),
        opt_str(record, "trace_state"),
        opt_str(record, "span_name"),
        opt_int(record, "span_kind"),
        opt_int(record, "status_code"),
        opt_str(record, "status_message"),
        opt_str(record, "service_name"),
        opt_str(record, "service_namespace"),
        opt_str(record, "service_instance_id"),
        opt_str(record, "scope_name"),
        opt_str(record, "scope_version"),
        opt_str(record, "scope_attributes"),
        opt_str(record, "span_attributes"),
        opt_str(record, "resource_attributes"),
        opt_str(record, "events_json"),
        opt_str(record, "links_json"),
        opt_int(record, "dropped_attributes_count"),
        opt_int(record, "dropped_events_count"),
        opt_int(record, "dropped_links_count"),
        opt_int(record, "flags"),
        promoted_str(record, "resource_attributes", "deployment.environment"),
        promoted_str_alts(
            record,
            "span_attributes",
            &["http.request.method", "http.method"],
        ),
        promoted_int_alts(
            record,
            "span_attributes",
            &["http.response.status_code", "http.status_code"],
        ),
        promoted_str(record, "span_attributes", "http.route"),
        promoted_str(record, "span_attributes", "exception.type"),
    ]
}

fn bind_metric_gauge(record: &Value, source_format: &str) -> Vec<DuckValue> {
    metric_common_bind(record, source_format)
}

fn bind_metric_sum(record: &Value, source_format: &str) -> Vec<DuckValue> {
    let mut values = metric_common_bind(record, source_format);
    values.push(opt_int(record, "aggregation_temporality"));
    values.push(opt_bool(record, "is_monotonic"));
    values
}

fn metric_common_bind(record: &Value, source_format: &str) -> Vec<DuckValue> {
    vec![
        DuckValue::Text(timestamp_string(record)),
        DuckValue::Text(now_timestamp_string()),
        DuckValue::Text(event_date(record)),
        DuckValue::Text(source_format.to_string()),
        opt_int(record, "start_timestamp"),
        opt_str(record, "metric_name"),
        opt_str(record, "metric_description"),
        opt_str(record, "metric_unit"),
        opt_double(record, "value"),
        opt_str(record, "service_name"),
        opt_str(record, "service_namespace"),
        opt_str(record, "service_instance_id"),
        opt_str(record, "resource_attributes"),
        opt_str(record, "scope_name"),
        opt_str(record, "scope_version"),
        opt_str(record, "scope_attributes"),
        opt_str(record, "metric_attributes"),
        opt_int(record, "flags"),
        opt_str(record, "exemplars_json"),
        promoted_str(record, "resource_attributes", "deployment.environment"),
    ]
}

fn opt_str(record: &Value, key: &str) -> DuckValue {
    opt_s(record, key)
        .map(DuckValue::Text)
        .unwrap_or(DuckValue::Null)
}

fn opt_int(record: &Value, key: &str) -> DuckValue {
    opt_i(record, key)
        .map(DuckValue::BigInt)
        .unwrap_or(DuckValue::Null)
}

fn opt_double(record: &Value, key: &str) -> DuckValue {
    opt_f(record, key)
        .map(DuckValue::Double)
        .unwrap_or(DuckValue::Null)
}

fn opt_bool(record: &Value, key: &str) -> DuckValue {
    opt_b(record, key)
        .map(DuckValue::Boolean)
        .unwrap_or(DuckValue::Null)
}

fn promoted_str(record: &Value, attr_field: &str, attr_key: &str) -> DuckValue {
    promoted(record, attr_field, attr_key)
        .map(DuckValue::Text)
        .unwrap_or(DuckValue::Null)
}

fn promoted_str_alts(record: &Value, attr_field: &str, keys: &[&str]) -> DuckValue {
    for key in keys {
        if let Some(value) = promoted(record, attr_field, key) {
            return DuckValue::Text(value);
        }
    }
    DuckValue::Null
}

fn promoted_int_alts(record: &Value, attr_field: &str, keys: &[&str]) -> DuckValue {
    for key in keys {
        if let Some(value) = promoted_i(record, attr_field, key) {
            return DuckValue::BigInt(value);
        }
    }
    DuckValue::Null
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn motherduck_attach_uri_uses_md_extension_and_canardlake_alias() {
        let dir = tempdir().unwrap();
        let plan = build_ducklake_attach_plan(
            None,
            Some("md:test-ducklake"),
            &dir.path().join("canardstack.duckdb"),
            &dir.path().join("storage"),
        )
        .unwrap();

        assert_eq!(
            plan.sql,
            "ATTACH 'md:test-ducklake' AS canardlake; USE canardlake;"
        );
        assert_eq!(plan.mode, "ducklake_motherduck_remote");
        assert!(!plan.needs_ducklake);
        assert!(plan.needs_motherduck);
        assert!(!plan.needs_postgres);
        assert!(!plan.managed_maintenance);
    }

    #[test]
    fn custom_attach_uri_and_postgres_catalog_are_mutually_exclusive() {
        let dir = tempdir().unwrap();
        let err = build_ducklake_attach_plan(
            Some("dbname=ducklake_catalog host=localhost"),
            Some("md:test-ducklake"),
            &dir.path().join("canardstack.duckdb"),
            &dir.path().join("storage"),
        )
        .unwrap_err();

        assert!(err.to_string().contains(
            "set only one of CANARDSTACK_POSTGRES_DSN or CANARDSTACK_DUCKLAKE_ATTACH_URI"
        ));
    }

    #[test]
    fn custom_attach_uri_must_be_uri_not_attach_statement() {
        let dir = tempdir().unwrap();
        let err = build_ducklake_attach_plan(
            None,
            Some("ATTACH 'md:test-ducklake';"),
            &dir.path().join("canardstack.duckdb"),
            &dir.path().join("storage"),
        )
        .unwrap_err();

        assert!(err
            .to_string()
            .contains("must be the URI only, not an ATTACH statement"));
    }
}
