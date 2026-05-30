use super::ducklake::ducklake_metadata_prefix;
use super::{Storage, StorageCapabilities, StorageHealth, StorageProbe};
use crate::signal::StorageSignal;
use crate::LockExt;
use anyhow::Result;
use chrono::Utc;
use duckdb::Connection;
use serde_json::{json, Value};
use std::fs;

impl Storage {
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

    pub fn health(&self) -> StorageHealth {
        StorageHealth {
            healthy: self.healthy(),
            mode: self.mode.clone(),
            ducklake_catalog: self.catalog_name.clone(),
            postgres_catalog_configured: self.postgres_catalog_configured,
            last_error: self.last_error.lock_or_poisoned().clone(),
            capabilities: StorageCapabilities {
                insert: false,
                query: true,
            },
            freshness_watermarks: self
                .freshness_watermarks()
                .unwrap_or_else(|err| json!({"error": err.to_string()})),
            logical_rows: self
                .logical_rows()
                .unwrap_or_else(|err| json!({"error": err.to_string()})),
            ducklake_storage_layout: self
                .ducklake_storage_layout()
                .unwrap_or_else(|err| json!({"error": err.to_string()})),
            physical_bytes: dir_size(&self.local_storage_dir).unwrap_or(0),
        }
    }

    pub fn probe(&self) -> StorageProbe {
        StorageProbe {
            healthy: self.healthy(),
            mode: self.mode.clone(),
            last_error: self.last_error.lock_or_poisoned().clone(),
        }
    }
    pub fn ducklake_storage_layout(&self) -> Result<Value> {
        self.with_conn(|conn, _| {
            let tables = self.ducklake_storage_layout_on(conn)?;
            Ok(json!({"supported": true, "tables": tables}))
        })
    }

    fn ducklake_storage_layout_on(&self, conn: &Connection) -> Result<Value> {
        let metadata_prefix = ducklake_metadata_prefix(&self.catalog_name);
        let mut tables = serde_json::Map::new();
        let sql = format!(
            "\
            SELECT t.table_id, t.table_name, count(f.data_file_id), coalesce(sum(f.record_count), 0) \
            FROM {metadata_prefix}ducklake_table t \
            LEFT JOIN {metadata_prefix}ducklake_data_file f \
              ON f.table_id = t.table_id AND f.end_snapshot IS NULL \
            WHERE t.end_snapshot IS NULL \
            GROUP BY t.table_id, t.table_name"
        );
        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map([], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, i64>(2)?,
                row.get::<_, i64>(3)?,
            ))
        })?;
        for row in rows {
            let (table_id, table_name, active_data_files, active_data_file_rows) = row?;
            tables.insert(
                table_name,
                json!({
                    "table_id": table_id,
                    "active_data_files": active_data_files,
                    "active_data_file_rows": active_data_file_rows
                }),
            );
        }
        Ok(Value::Object(tables))
    }
    pub fn freshness_watermarks(&self) -> Result<Value> {
        self.with_conn(|conn, prefix| {
            let mut map = serde_json::Map::new();
            for signal in StorageSignal::ALL {
                let table = signal.as_str();
                let ts = crate::storage::schema::table_timestamp_column(signal);
                let sql =
                    format!("SELECT max({ts})::VARCHAR, epoch(max({ts})) FROM {prefix}{table}");
                let (event_watermark, event_watermark_epoch): (Option<String>, Option<f64>) =
                    conn.query_row(&sql, [], |row| Ok((row.get(0)?, row.get(1)?)))?;
                let event_lag_seconds = event_watermark_epoch
                    .map(|epoch| Utc::now().timestamp_millis() as f64 / 1000.0 - epoch);
                map.insert(
                    table.to_string(),
                    json!({
                        "timestamp": event_watermark,
                        "epoch_seconds": event_watermark_epoch,
                        "event_lag_seconds": event_lag_seconds,
                        "lag_seconds": event_lag_seconds
                    }),
                );
            }
            Ok(Value::Object(map))
        })
    }

    pub fn logical_rows(&self) -> Result<Value> {
        self.with_conn(|conn, prefix| {
            let mut map = serde_json::Map::new();
            for signal in StorageSignal::ALL {
                let table = signal.as_str();
                let sql = format!("SELECT count(*) FROM {prefix}{table}");
                let rows: i64 = conn.query_row(&sql, [], |row| row.get(0))?;
                map.insert(table.to_string(), json!(rows));
            }
            Ok(Value::Object(map))
        })
    }

    fn check_health_target(&self) -> Result<()> {
        // Reader, not writer — a stuck seal must not hang /healthz.
        let conn = self.reader.lock_or_poisoned();
        conn.query_row("SELECT 1", [], |_| Ok(()))?;
        let sql = format!(
            "SELECT * FROM {}{} LIMIT 0",
            self.target_prefix,
            StorageSignal::Logs.as_str()
        );
        let _stmt = conn.prepare(&sql)?;
        Ok(())
    }
}

fn dir_size(path: &std::path::Path) -> Result<u64> {
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
