use super::{RetentionPolicy, Storage};
use crate::db::sql::quote as sql_quote;
use crate::signal::StorageSignal;
use crate::LockExt;
use anyhow::Result;
use chrono::Utc;
use serde_json::{json, Value};
use std::sync::atomic::Ordering;

impl Storage {
    pub fn checkpoint_maintenance(&self, dry_run: bool) -> Result<Value> {
        let checkpoint_supported = self.ducklake_checkpoint_supported.load(Ordering::SeqCst);
        if dry_run {
            return Ok(json!({
                "supported": checkpoint_supported,
                "enabled": self.ducklake_maintenance_enabled,
                "ran": false,
                "status": "skipped",
                "reason": "dry_run"
            }));
        }
        if !self.ducklake_maintenance_enabled {
            return Ok(json!({
                "supported": checkpoint_supported,
                "enabled": false,
                "ran": false,
                "status": "skipped",
                "reason": "ducklake_maintenance_disabled"
            }));
        }
        if !checkpoint_supported {
            return Ok(json!({
                "supported": false,
                "enabled": true,
                "ran": false,
                "status": "skipped",
                "reason": "unsupported",
                "details": self.ducklake_maintenance_capability_reason.lock_or_poisoned().clone()
            }));
        }

        let result = self.writer.lock_or_poisoned().execute_batch("CHECKPOINT;");
        match result {
            Ok(()) => Ok(json!({
                "supported": true,
                "enabled": true,
                "ran": true,
                "status": "ok"
            })),
            Err(err)
                if super::ducklake::is_unsupported_ducklake_maintenance_error(&err.to_string()) =>
            {
                let reason = err.to_string();
                self.ducklake_checkpoint_supported
                    .store(false, Ordering::SeqCst);
                *self
                    .ducklake_maintenance_capability_reason
                    .lock_or_poisoned() = Some(reason.clone());
                Ok(json!({
                    "supported": false,
                    "enabled": true,
                    "ran": false,
                    "status": "skipped",
                    "reason": "unsupported",
                    "details": reason
                }))
            }
            Err(err) => {
                let err = anyhow::Error::from(err).context("run DuckLake CHECKPOINT maintenance");
                // Log the full cause chain: the HTTP error envelope and the
                // scheduler job log both render only the top context, so without
                // this the underlying DuckDB/object-store error (e.g. an S3 403 on
                // the catalog-side deletion scan) is invisible to operators.
                tracing::error!(event = "ducklake_checkpoint_failed", error = ?err);
                Err(err)
            }
        }
    }

    pub fn enforce_retention(&self, policy: &RetentionPolicy, dry_run: bool) -> Result<Value> {
        let conn = self.writer.lock_or_poisoned();
        let mut results = Vec::new();
        let mut metadata_deleted_total = 0_i64;
        for target in [
            (StorageSignal::Logs, policy.logs_days),
            (StorageSignal::Spans, policy.spans_days),
            (StorageSignal::MetricGauge, policy.metrics_days),
            (StorageSignal::MetricSum, policy.metrics_days),
        ] {
            let (signal, retention_days) = target;
            let table = signal.as_str();
            let ts_col = super::schema::table_timestamp_column(signal);
            let cutoff = (Utc::now() - chrono::Duration::days(retention_days))
                .format("%Y-%m-%d")
                .to_string();
            let full_table = format!("{}{}", self.target_prefix, table);
            // v2 uses the per-signal record-time column for retention cutoff,
            // not the v1 `timestamp` column that no longer exists.
            let predicate = format!(
                "{ts_col} < TIMESTAMP {}",
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
