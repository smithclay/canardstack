use super::ducklake::ducklake_metadata_prefix;
use super::Storage;
use crate::db::sql::quote as sql_quote;
use crate::ingest::Signal;
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use duckdb::Connection;
use serde::Serialize;

#[derive(Clone, Debug, Serialize)]
pub struct DuckLakePlannerPartition {
    pub key_index: i64,
    pub transform: Option<String>,
    pub value: String,
}

#[derive(Clone, Debug, Serialize)]
pub struct DuckLakePlannerFile {
    pub table: String,
    pub data_file_id: i64,
    pub path: String,
    pub path_is_relative: bool,
    pub begin_snapshot: i64,
    pub begin_snapshot_time: Option<String>,
    pub row_count: i64,
    pub file_size_bytes: i64,
    pub partition_values: Vec<DuckLakePlannerPartition>,
    pub timestamp_min: Option<String>,
    pub timestamp_max: Option<String>,
    pub active_delete_files: i64,
    pub active_delete_rows: i64,
}

#[derive(Clone, Debug)]
pub struct DuckLakeLogCandidatePlan {
    pub candidate_files: usize,
    pub candidate_rows: i64,
    pub candidate_bytes: i64,
    pub windows: Vec<DuckLakeLogCandidateWindow>,
}

#[derive(Clone, Debug)]
pub struct DuckLakeLogCandidateWindow {
    pub timestamp_lower_bound: Option<String>,
    pub files_scanned: usize,
    pub rows_scanned: i64,
    pub bytes_scanned: i64,
}

impl Storage {
    pub fn ducklake_planner_files(
        &self,
        table: Option<Signal>,
        limit: usize,
    ) -> Result<Vec<DuckLakePlannerFile>> {
        if !self.ducklake_available {
            anyhow::bail!("DuckLake metadata is unavailable");
        }
        let limit = limit.clamp(1, 10_000);
        self.with_conn(|conn, _| planner_files_on(conn, &self.catalog_name, table, None, limit))
    }

    pub fn ducklake_log_candidate_files(
        &self,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
        limit: usize,
    ) -> Result<Vec<DuckLakePlannerFile>> {
        if !self.ducklake_available {
            anyhow::bail!("DuckLake metadata is unavailable");
        }
        let limit = limit.clamp(1, 10_000);
        self.with_conn(|conn, _| {
            planner_files_on(
                conn,
                &self.catalog_name,
                Some(Signal::Logs),
                Some((from, to)),
                limit,
            )
        })
    }

    pub fn ducklake_log_candidate_plan(
        &self,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
        max_files: usize,
        batch_size: usize,
    ) -> Result<DuckLakeLogCandidatePlan> {
        let files = self.ducklake_log_candidate_files(from, to, max_files)?;
        let batch_size = batch_size.max(1);
        let candidate_files = files.len();
        let candidate_rows = candidate_row_count(&files);
        let candidate_bytes = candidate_byte_count(&files);
        let mut windows = Vec::new();
        let mut selected_len = 0usize;
        for batch in files.chunks(batch_size) {
            selected_len += batch.len();
            let selected = &files[..selected_len];
            windows.push(DuckLakeLogCandidateWindow {
                timestamp_lower_bound: candidate_lower_bound(selected),
                files_scanned: selected_len,
                rows_scanned: candidate_row_count(selected),
                bytes_scanned: candidate_byte_count(selected),
            });
        }
        Ok(DuckLakeLogCandidatePlan {
            candidate_files,
            candidate_rows,
            candidate_bytes,
            windows,
        })
    }
}

fn planner_files_on(
    conn: &Connection,
    catalog_name: &str,
    table: Option<Signal>,
    time_range: Option<(DateTime<Utc>, DateTime<Utc>)>,
    limit: usize,
) -> Result<Vec<DuckLakePlannerFile>> {
    let metadata = ducklake_metadata_prefix(catalog_name);
    let table_filter = table
        .map(|signal| format!(" AND t.table_name = {}", sql_quote(signal.as_str())))
        .unwrap_or_default();
    let time_filter = time_range
        .map(|(from, to)| {
            format!(
                " AND (ts.min_value IS NULL OR ts.max_value IS NULL OR (ts.max_value >= {} AND ts.min_value < {}))",
                sql_quote(&planner_time(from)),
                sql_quote(&planner_time(to))
            )
        })
        .unwrap_or_default();
    let sql = format!(
        "\
        SELECT \
            t.table_name, \
            f.table_id, \
            f.data_file_id, \
            f.path, \
            f.path_is_relative, \
            f.begin_snapshot, \
            s.snapshot_time::VARCHAR, \
            f.record_count, \
            f.file_size_bytes, \
            ts.min_value, \
            ts.max_value \
        FROM {metadata}ducklake_data_file f \
        JOIN {metadata}ducklake_table t \
          ON t.table_id = f.table_id AND t.end_snapshot IS NULL \
        LEFT JOIN {metadata}ducklake_snapshot s \
          ON s.snapshot_id = f.begin_snapshot \
        LEFT JOIN {metadata}ducklake_column c \
          ON c.table_id = f.table_id AND c.column_name = 'timestamp' AND c.end_snapshot IS NULL \
        LEFT JOIN {metadata}ducklake_file_column_stats ts \
          ON ts.table_id = f.table_id AND ts.data_file_id = f.data_file_id AND ts.column_id = c.column_id \
        WHERE f.end_snapshot IS NULL{table_filter}{time_filter} \
        ORDER BY ts.max_value DESC NULLS LAST, f.begin_snapshot DESC, f.data_file_id DESC \
        LIMIT {limit}"
    );

    let mut stmt = conn
        .prepare(&sql)
        .context("prepare DuckLake planner metadata probe")?;
    let rows = stmt
        .query_map([], |row| {
            Ok(RawPlannerFile {
                table: row.get(0)?,
                table_id: row.get(1)?,
                data_file_id: row.get(2)?,
                path: row.get(3)?,
                path_is_relative: row.get(4)?,
                begin_snapshot: row.get(5)?,
                begin_snapshot_time: row.get(6)?,
                row_count: row.get(7)?,
                file_size_bytes: row.get(8)?,
                timestamp_min: row.get(9)?,
                timestamp_max: row.get(10)?,
            })
        })
        .context("query DuckLake planner metadata probe")?;
    let raw_files = rows
        .collect::<Result<Vec<_>, _>>()
        .context("read DuckLake planner metadata rows")?;
    drop(stmt);

    raw_files
        .into_iter()
        .map(|raw| {
            let partition_values =
                partition_values_on(conn, &metadata, raw.table_id, raw.data_file_id)?;
            let (active_delete_files, active_delete_rows) =
                active_delete_stats_on(conn, &metadata, raw.table_id, raw.data_file_id)?;
            Ok(DuckLakePlannerFile {
                table: raw.table,
                data_file_id: raw.data_file_id,
                path: raw.path,
                path_is_relative: raw.path_is_relative,
                begin_snapshot: raw.begin_snapshot,
                begin_snapshot_time: raw.begin_snapshot_time,
                row_count: raw.row_count,
                file_size_bytes: raw.file_size_bytes,
                partition_values,
                timestamp_min: raw.timestamp_min,
                timestamp_max: raw.timestamp_max,
                active_delete_files,
                active_delete_rows,
            })
        })
        .collect()
}

struct RawPlannerFile {
    table: String,
    table_id: i64,
    data_file_id: i64,
    path: String,
    path_is_relative: bool,
    begin_snapshot: i64,
    begin_snapshot_time: Option<String>,
    row_count: i64,
    file_size_bytes: i64,
    timestamp_min: Option<String>,
    timestamp_max: Option<String>,
}

fn partition_values_on(
    conn: &Connection,
    metadata: &str,
    table_id: i64,
    data_file_id: i64,
) -> Result<Vec<DuckLakePlannerPartition>> {
    let sql = format!(
        "\
        SELECT partition_key_index, NULL::VARCHAR, partition_value \
        FROM {metadata}ducklake_file_partition_value pv \
        WHERE pv.table_id = {table_id} AND pv.data_file_id = {data_file_id} \
        ORDER BY partition_value"
    );
    let mut stmt = conn
        .prepare(&sql)
        .context("prepare DuckLake partition metadata probe")?;
    let rows = stmt
        .query_map([], |row| {
            Ok(DuckLakePlannerPartition {
                key_index: row.get(0)?,
                transform: row.get(1)?,
                value: row.get(2)?,
            })
        })
        .context("query DuckLake partition metadata probe")?;
    let mut partitions = rows
        .collect::<Result<Vec<_>, _>>()
        .context("read DuckLake partition metadata rows")?;
    partitions.sort_by_key(partition_sort_key);
    Ok(partitions)
}

fn active_delete_stats_on(
    conn: &Connection,
    metadata: &str,
    table_id: i64,
    data_file_id: i64,
) -> Result<(i64, i64)> {
    let sql = format!(
        "\
        SELECT count(*), coalesce(cast(sum(delete_count) AS BIGINT), 0) \
        FROM {metadata}ducklake_delete_file \
        WHERE end_snapshot IS NULL AND table_id = {table_id} AND data_file_id = {data_file_id}"
    );
    conn.query_row(&sql, [], |row| Ok((row.get(0)?, row.get(1)?)))
        .context("query DuckLake delete-file metadata probe")
}

fn partition_sort_key(partition: &DuckLakePlannerPartition) -> (i32, i64) {
    let rank = match partition.transform.as_deref() {
        Some("year") => 0,
        Some("month") => 1,
        Some("day") => 2,
        Some("hour") => 3,
        _ => 100,
    };
    (rank, partition.key_index)
}

fn planner_time(value: DateTime<Utc>) -> String {
    value.format("%Y-%m-%d %H:%M:%S%.6f").to_string()
}

fn candidate_row_count(files: &[DuckLakePlannerFile]) -> i64 {
    files.iter().map(|file| file.row_count.max(0)).sum()
}

fn candidate_byte_count(files: &[DuckLakePlannerFile]) -> i64 {
    files.iter().map(|file| file.file_size_bytes.max(0)).sum()
}

fn candidate_lower_bound(files: &[DuckLakePlannerFile]) -> Option<String> {
    files
        .iter()
        .filter_map(|file| file.timestamp_min.as_deref())
        .min()
        .map(str::to_string)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn planner_partition_values_are_structured() {
        let partition = DuckLakePlannerPartition {
            key_index: 2,
            transform: None,
            value: "2026".to_string(),
        };

        assert_eq!(partition.key_index, 2);
        assert_eq!(partition.transform, None);
        assert_eq!(partition.value, "2026");
    }
}
