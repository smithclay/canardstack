use chrono::{DateTime, Utc};
use duckdb::Row;
use serde_json::{json, Value};
use std::collections::BTreeSet;

pub fn escape_value(value: &str) -> String {
    value.replace('\'', "''")
}

pub fn quote(value: &str) -> String {
    format!("'{}'", escape_value(value))
}

pub fn time_predicate(from: DateTime<Utc>, to: DateTime<Utc>) -> String {
    format!(
        "timestamp >= TIMESTAMP {} AND timestamp < TIMESTAMP {}",
        quote(&from.format("%Y-%m-%d %H:%M:%S%.3f").to_string()),
        quote(&to.format("%Y-%m-%d %H:%M:%S%.3f").to_string())
    )
}

pub fn push_eq(where_sql: &mut Vec<String>, column: &str, value: Option<&Value>) {
    if let Some(v) = value.and_then(Value::as_str).filter(|s| !s.is_empty()) {
        where_sql.push(format!("{column} = {}", quote(v)));
    }
}

pub fn span_row(row: &Row<'_>) -> duckdb::Result<Value> {
    Ok(json!({
        "timestamp": row.get::<_, String>(0)?,
        "trace_id": row.get::<_, Option<String>>(1)?,
        "span_id": row.get::<_, Option<String>>(2)?,
        "parent_span_id": row.get::<_, Option<String>>(3)?,
        "service_name": row.get::<_, Option<String>>(4)?,
        "span_name": row.get::<_, Option<String>>(5)?,
        "duration": row.get::<_, Option<i64>>(6)?,
        "status_code": row.get::<_, Option<i32>>(7)?,
        "http_method": row.get::<_, Option<String>>(8)?,
        "http_status_code": row.get::<_, Option<i32>>(9)?,
    }))
}

pub fn missing_parent_count(rows: &[Value]) -> usize {
    let spans = rows
        .iter()
        .filter_map(|r| r.get("span_id").and_then(Value::as_str))
        .collect::<BTreeSet<_>>();
    rows.iter()
        .filter_map(|r| r.get("parent_span_id").and_then(Value::as_str))
        .filter(|parent| !parent.is_empty() && !spans.contains(parent))
        .count()
}
