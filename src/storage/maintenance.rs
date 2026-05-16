use super::{RetentionPolicy, Storage};
use crate::db::sql::{escape_value, quote as sql_quote};
use crate::LockExt;
use anyhow::Result;
use chrono::Utc;
use serde_json::{json, Value};
use std::sync::atomic::Ordering;

impl Storage {
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

    pub fn compaction_decision(&self, table: Option<&str>, _min_files: usize) -> Result<Value> {
        if !self.ducklake_managed_maintenance {
            return Ok(
                json!({"supported": false, "reason": "ducklake maintenance is not managed by this process"}),
            );
        }
        Ok(json!({
            "supported": true,
            "status": "disabled",
            "should_compact": false,
            "table": table,
            "reason": "immutable_segments"
        }))
    }

    pub fn merge_adjacent_files(&self, table: Option<&str>) -> Result<Value> {
        if !self.ducklake_managed_maintenance {
            return Ok(
                json!({"supported": false, "reason": "ducklake maintenance is not managed by this process"}),
            );
        }
        Ok(json!({
            "supported": true,
            "status": "disabled",
            "table": table,
            "reason": "immutable_segments"
        }))
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
        let mut metadata_deleted_total = 0_i64;
        for target in [
            ("logs", policy.logs_days),
            ("spans", policy.spans_days),
            ("metric_gauge", policy.metrics_days),
            ("metric_sum", policy.metrics_days),
        ] {
            let (table, retention_days) = target;
            let cutoff = (Utc::now() - chrono::Duration::days(retention_days))
                .format("%Y-%m-%d")
                .to_string();
            let full_table = format!("{}{}", self.target_prefix, table);
            let predicate = format!(
                "timestamp < TIMESTAMP {}",
                sql_quote(&format!("{cutoff} 00:00:00"))
            );
            let count_sql = format!("SELECT count(*) FROM {full_table} WHERE {predicate}");
            let matching_rows: i64 = conn.query_row(&count_sql, [], |row| row.get(0))?;
            let deleted_rows = if dry_run || matching_rows == 0 {
                0
            } else {
                let delete_sql = format!("DELETE FROM {full_table} WHERE {predicate}");
                conn.execute(&delete_sql, [])? as i64
            };
            let metadata_predicate = format!(
                "signal = {} AND event_date < DATE {}",
                sql_quote(table),
                sql_quote(&cutoff)
            );
            let metadata_count_sql = format!(
                "SELECT count(*) FROM {prefix}metadata_summary WHERE {metadata_predicate}",
                prefix = self.target_prefix
            );
            let matching_metadata_rows: i64 =
                conn.query_row(&metadata_count_sql, [], |row| row.get(0))?;
            let deleted_metadata_rows = if dry_run || matching_metadata_rows == 0 {
                0
            } else {
                let delete_sql = format!(
                    "DELETE FROM {prefix}metadata_summary WHERE {metadata_predicate}",
                    prefix = self.target_prefix
                );
                conn.execute(&delete_sql, [])? as i64
            };
            metadata_deleted_total += deleted_metadata_rows;
            results.push(json!({
                "table": table,
                "retention_days": retention_days,
                "cutoff_date": cutoff,
                "matching_rows": matching_rows,
                "deleted_rows": deleted_rows,
                "matching_metadata_rows": matching_metadata_rows,
                "deleted_metadata_rows": deleted_metadata_rows
            }));
        }
        if metadata_deleted_total > 0 {
            self.metadata_generation.fetch_add(1, Ordering::SeqCst);
        }
        Ok(json!({"dry_run": dry_run, "tables": results}))
    }
}
