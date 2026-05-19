use super::ducklake::ducklake_metadata_prefix;
use super::Storage;
use crate::db::sql::quote as sql_quote;
use crate::ingest::Signal;
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use duckdb::Connection;

#[derive(Clone, Debug)]
struct DuckLakePlannerFile {
    row_count: i64,
    file_size_bytes: i64,
    timestamp_min: Option<String>,
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
    fn ducklake_log_candidate_files(
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
            f.record_count, \
            f.file_size_bytes, \
            ts.min_value \
        FROM {metadata}ducklake_data_file f \
        JOIN {metadata}ducklake_table t \
          ON t.table_id = f.table_id AND t.end_snapshot IS NULL \
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
        .context("prepare DuckLake candidate metadata query")?;
    let rows = stmt
        .query_map([], |row| {
            Ok(DuckLakePlannerFile {
                row_count: row.get(0)?,
                file_size_bytes: row.get(1)?,
                timestamp_min: row.get(2)?,
            })
        })
        .context("query DuckLake candidate metadata")?;
    rows.collect::<Result<Vec<_>, _>>()
        .context("read DuckLake candidate metadata rows")
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
