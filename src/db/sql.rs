use chrono::{DateTime, Utc};
use duckdb::Row;
use serde_json::{json, Value};

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

pub fn json_attr(column: &str, key: &str) -> String {
    format!("json_extract_string({column}, '$.\"{key}\"')")
}

pub fn json_attr_i32(column: &str, key: &str) -> String {
    format!("try_cast({} AS INTEGER)", json_attr(column, key))
}

pub fn coalesce_expr(exprs: &[String]) -> String {
    format!("coalesce({})", exprs.join(", "))
}

pub fn logs_deployment_environment_expr() -> String {
    json_attr("resource_attributes", "deployment.environment")
}

pub fn logs_http_method_expr() -> String {
    coalesce_expr(&[
        json_attr("log_attributes", "http.request.method"),
        json_attr("log_attributes", "http.method"),
    ])
}

pub fn logs_http_status_code_expr() -> String {
    coalesce_expr(&[
        json_attr_i32("log_attributes", "http.response.status_code"),
        json_attr_i32("log_attributes", "http.status_code"),
    ])
}

pub fn logs_http_route_expr() -> String {
    json_attr("log_attributes", "http.route")
}

pub fn spans_deployment_environment_expr() -> String {
    json_attr("resource_attributes", "deployment.environment")
}

pub fn spans_http_method_expr() -> String {
    coalesce_expr(&[
        json_attr("span_attributes", "http.request.method"),
        json_attr("span_attributes", "http.method"),
    ])
}

pub fn spans_http_status_code_expr() -> String {
    coalesce_expr(&[
        json_attr_i32("span_attributes", "http.response.status_code"),
        json_attr_i32("span_attributes", "http.status_code"),
    ])
}

pub fn spans_http_route_expr() -> String {
    json_attr("span_attributes", "http.route")
}

pub fn metrics_deployment_environment_expr() -> String {
    json_attr("resource_attributes", "deployment.environment")
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
