use crate::db::sql::quote as sql_quote;
use crate::ingest::Signal;
use anyhow::{Context, Result};
use chrono::NaiveDate;
use duckdb::Connection;
use std::collections::{BTreeMap, BTreeSet};

pub(super) fn merge_dirty_metadata(
    dst: &mut BTreeMap<Signal, BTreeSet<String>>,
    src: BTreeMap<Signal, BTreeSet<String>>,
) {
    for (signal, dates) in src {
        dst.entry(signal).or_default().extend(dates);
    }
}

pub(super) fn refresh_metadata_summaries_on(
    conn: &Connection,
    prefix: &str,
    affected: &BTreeMap<Signal, BTreeSet<String>>,
) -> Result<usize> {
    if affected.is_empty() {
        return Ok(0);
    }

    let result = (|| -> Result<usize> {
        conn.execute_batch("BEGIN TRANSACTION;")?;
        let mut buckets = 0;
        for (signal, dates) in affected {
            for date in dates {
                buckets += 1;
                conn.execute_batch(&format!(
                    "DELETE FROM {prefix}metadata_summary \
                     WHERE signal = {} AND event_date = DATE {}",
                    sql_quote(signal.as_str()),
                    sql_quote(date)
                ))?;
                conn.execute_batch(&metadata_refresh_sql(prefix, *signal, date)?)?;
            }
        }
        conn.execute_batch("COMMIT;")?;
        Ok(buckets)
    })();

    if result.is_err() {
        if let Err(rollback_err) = conn.execute_batch("ROLLBACK;") {
            // A failed ROLLBACK can leave the shared writer connection with a
            // dangling transaction; surface it so a wedged refresh path is
            // observable instead of silently cascading into later appends.
            crate::log_event(
                "error",
                "metadata_refresh_rollback_failed",
                &[("error", &rollback_err.to_string())],
            );
        }
    }
    result
}

pub(super) fn metadata_refresh_sql(prefix: &str, signal: Signal, date: &str) -> Result<String> {
    let day = MetadataRefreshDay::new(date)?;
    let selects = match signal {
        Signal::Logs => logs_metadata_sql(prefix, &day),
        Signal::Spans => spans_metadata_sql(prefix, &day),
        Signal::MetricGauge => metric_metadata_sql(prefix, Signal::MetricGauge, &day, "gauge"),
        Signal::MetricSum => metric_metadata_sql(prefix, Signal::MetricSum, &day, "counter"),
    };
    if selects.is_empty() {
        anyhow::bail!("metadata refresh for {signal} produced no SELECT statements");
    }
    Ok(format!(
        "INSERT INTO {prefix}metadata_summary ({}) {}",
        metadata_summary_columns(),
        selects.join("\nUNION ALL\n")
    ))
}

pub(super) struct MetadataRefreshDay<'a> {
    date: &'a str,
    start: String,
    end: String,
}

impl<'a> MetadataRefreshDay<'a> {
    fn new(date: &'a str) -> Result<Self> {
        let start = NaiveDate::parse_from_str(date, "%Y-%m-%d")
            .with_context(|| format!("parse metadata refresh date {date}"))?;
        let end = start
            .succ_opt()
            .with_context(|| format!("metadata refresh date {date} has no successor"))?;
        Ok(Self {
            date,
            start: format!("{start} 00:00:00"),
            end: format!("{end} 00:00:00"),
        })
    }

    fn predicate(&self) -> String {
        format!(
            "timestamp >= TIMESTAMP {} AND timestamp < TIMESTAMP {}",
            sql_quote(&self.start),
            sql_quote(&self.end)
        )
    }
}

pub(super) fn logs_metadata_sql(prefix: &str, day: &MetadataRefreshDay<'_>) -> Vec<String> {
    let mut sql = Vec::new();
    for (name, column) in [
        ("service_name", "service_name"),
        ("deployment_environment", "deployment_environment"),
        ("severity_text", "severity_text"),
        ("trace_id", "trace_id"),
        ("span_id", "span_id"),
        ("http_route", "http_route"),
        ("http_method", "http_method"),
    ] {
        sql.push(label_value_insert_sql(
            prefix,
            "logs",
            "logs",
            day,
            "label_value",
            name,
            column,
        ));
    }
    sql.push(format!(
        "\
        SELECT 'logs', DATE {}, 'series', 'stream', NULL, NULL, NULL, NULL, \
               service_name, deployment_environment, severity_text, \
               count(*), min(timestamp), max(timestamp) \
        FROM {prefix}logs \
        WHERE {} \
        GROUP BY service_name, deployment_environment, severity_text",
        sql_quote(day.date),
        day.predicate()
    ));
    sql
}

pub(super) fn spans_metadata_sql(prefix: &str, day: &MetadataRefreshDay<'_>) -> Vec<String> {
    let mut sql = Vec::new();
    for (name, column) in [
        ("service.name", "service_name"),
        ("span.name", "span_name"),
        ("http.route", "http_route"),
        ("status", "status_code"),
        ("status.code", "status_code"),
        ("traceID", "trace_id"),
    ] {
        sql.push(label_value_insert_sql(
            prefix,
            "spans",
            "spans",
            day,
            "tag_value",
            name,
            column,
        ));
    }
    sql
}

pub(super) fn metric_metadata_sql(
    prefix: &str,
    signal: Signal,
    day: &MetadataRefreshDay<'_>,
    metric_type: &str,
) -> Vec<String> {
    let table = signal.as_str();
    let signal = signal.as_str();
    let mut sql = Vec::new();
    for (name, column) in [
        ("__name__", "metric_name"),
        ("service_name", "service_name"),
        ("deployment_environment", "deployment_environment"),
    ] {
        sql.push(label_value_insert_sql(
            prefix,
            signal,
            table,
            day,
            "label_value",
            name,
            column,
        ));
    }
    sql.push(format!(
        "\
        SELECT {}, DATE {}, 'series', metric_name, NULL, {}, NULL, NULL, \
               service_name, deployment_environment, NULL, \
               count(*), min(timestamp), max(timestamp) \
        FROM {prefix}{table} \
        WHERE {} AND metric_name IS NOT NULL AND metric_name <> '' \
        GROUP BY metric_name, service_name, deployment_environment",
        sql_quote(signal),
        sql_quote(day.date),
        sql_quote(metric_type),
        day.predicate()
    ));
    sql.push(format!(
        "\
        SELECT {}, DATE {}, 'metric_metadata', metric_name, NULL, {}, \
               max(coalesce(metric_unit, '')), max(coalesce(metric_description, '')), \
               NULL, NULL, NULL, count(*), min(timestamp), max(timestamp) \
        FROM {prefix}{table} \
        WHERE {} AND metric_name IS NOT NULL AND metric_name <> '' \
        GROUP BY metric_name",
        sql_quote(signal),
        sql_quote(day.date),
        sql_quote(metric_type),
        day.predicate()
    ));
    sql
}

pub(super) fn label_value_insert_sql(
    prefix: &str,
    signal: &str,
    table: &str,
    day: &MetadataRefreshDay<'_>,
    kind: &str,
    name: &str,
    column: &str,
) -> String {
    format!(
        "\
        SELECT {}, DATE {}, {}, {}, {column}::VARCHAR, NULL, NULL, NULL, \
               NULL, NULL, NULL, count(*), min(timestamp), max(timestamp) \
        FROM {prefix}{table} \
        WHERE {} AND {column} IS NOT NULL AND {column}::VARCHAR <> '' \
        GROUP BY {column}",
        sql_quote(signal),
        sql_quote(day.date),
        sql_quote(kind),
        sql_quote(name),
        day.predicate()
    )
}

pub(super) fn metadata_summary_columns() -> &'static str {
    "signal, event_date, kind, name, value, metric_type, metric_unit, \
     metric_description, service_name, deployment_environment, severity_text, \
     row_count, first_seen, last_seen"
}
