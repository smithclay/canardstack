use crate::ingest::Signal;
use anyhow::{Context, Result};
use duckdb::Connection;

pub(super) fn create_tables_on(conn: &Connection, prefix: &str) -> Result<()> {
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
    ddl.push_str(&create_table_sql(
        prefix,
        "metadata_summary",
        METADATA_SUMMARY_COLUMNS,
    ));
    ddl.push('\n');
    conn.execute_batch(&ddl)?;
    configure_telemetry_partitioning_on(conn, prefix)?;
    Ok(())
}

pub(super) fn configure_telemetry_partitioning_on(conn: &Connection, prefix: &str) -> Result<()> {
    for table in [
        Signal::Logs,
        Signal::Spans,
        Signal::MetricGauge,
        Signal::MetricSum,
    ] {
        conn.execute_batch(&format!(
            "ALTER TABLE {prefix}{} SET PARTITIONED BY (year(timestamp), month(timestamp), day(timestamp));",
            table.as_str()
        ))
        .with_context(|| format!("configure DuckLake timestamp partitioning for {table}"))?;
    }
    Ok(())
}
pub(super) const LOGS_COLUMNS: &[(&str, &str)] = &[
    ("timestamp", "TIMESTAMP"),
    ("ingested_at", "TIMESTAMP"),
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
];

pub(super) const SPANS_COLUMNS: &[(&str, &str)] = &[
    ("timestamp", "TIMESTAMP"),
    ("ingested_at", "TIMESTAMP"),
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
];

pub(super) const METRIC_GAUGE_COLUMNS: &[(&str, &str)] = &[
    ("timestamp", "TIMESTAMP"),
    ("ingested_at", "TIMESTAMP"),
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
];

pub(super) const METRIC_SUM_COLUMNS: &[(&str, &str)] = &[
    ("timestamp", "TIMESTAMP"),
    ("ingested_at", "TIMESTAMP"),
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
    ("aggregation_temporality", "INTEGER"),
    ("is_monotonic", "BOOLEAN"),
];

pub(super) const METADATA_SUMMARY_COLUMNS: &[(&str, &str)] = &[
    ("signal", "VARCHAR"),
    ("event_date", "DATE"),
    ("kind", "VARCHAR"),
    ("name", "VARCHAR"),
    ("value", "VARCHAR"),
    ("metric_type", "VARCHAR"),
    ("metric_unit", "VARCHAR"),
    ("metric_description", "VARCHAR"),
    ("service_name", "VARCHAR"),
    ("deployment_environment", "VARCHAR"),
    ("severity_text", "VARCHAR"),
    ("row_count", "BIGINT"),
    ("first_seen", "TIMESTAMP"),
    ("last_seen", "TIMESTAMP"),
];

pub(super) fn table_columns(table: Signal) -> &'static [(&'static str, &'static str)] {
    match table {
        Signal::Logs => LOGS_COLUMNS,
        Signal::Spans => SPANS_COLUMNS,
        Signal::MetricGauge => METRIC_GAUGE_COLUMNS,
        Signal::MetricSum => METRIC_SUM_COLUMNS,
    }
}

pub(super) fn create_table_sql(prefix: &str, name: &str, cols: &[(&str, &str)]) -> String {
    let body = cols
        .iter()
        .map(|(col, ty)| format!("  {col} {ty}"))
        .collect::<Vec<_>>()
        .join(",\n");
    format!("CREATE TABLE IF NOT EXISTS {prefix}{name} (\n{body}\n);")
}
