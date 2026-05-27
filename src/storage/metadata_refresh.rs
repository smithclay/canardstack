//! Derived metadata refresh stage (the write side).
//!
//! This is a FIRST-CLASS derived-metadata pipeline stage, not part of the
//! ingest commit path. Ingest never re-aggregates discovery metadata inline:
//! when a seal flush commits rows, the storage layer only records which
//! signal/date buckets changed (`Storage::mark_metadata_dirty`). This stage
//! later re-derives the bounded `metadata_summary` rows for those buckets, off
//! the commit path:
//!
//! ```text
//! seal commit -> mark_metadata_dirty (dirty signal/date buckets)
//!   -> scheduler `metadata_refresh` job (bounded per tick, yields under
//!      seal/freshness pressure, re-marks drained buckets dirty on failure)
//!   -> refresh_metadata_limited -> refresh_metadata_summaries_on (this module)
//!      re-aggregates `metadata_summary` for the affected buckets
//!   -> bumps the storage `metadata_generation`
//!   -> generation-keyed discovery cache in `crate::metadata` is invalidated
//! ```
//!
//! Running off the commit path keeps ingest cheap and bounded: discovery
//! re-aggregation is scheduled, capped per tick, and yields to the writer when a
//! seal is approaching its freshness budget, so it can never delay a due seal.
//! Eventual visibility is preserved because the scheduler job re-marks dirty any
//! buckets it failed to refresh. `crate::metadata` is the read side of this same
//! stage.

use super::schema::table_timestamp_column;
use crate::db::sql::{metrics_deployment_environment_expr, quote as sql_quote};
use crate::semantic_labels::{self, LabelScope};
use crate::signal::StorageSignal;
use anyhow::{Context, Result};
use chrono::NaiveDate;
use duckdb::Connection;
use std::collections::{BTreeMap, BTreeSet};

pub(super) fn merge_dirty_metadata(
    dst: &mut BTreeMap<StorageSignal, BTreeSet<String>>,
    src: BTreeMap<StorageSignal, BTreeSet<String>>,
) {
    for (signal, dates) in src {
        dst.entry(signal).or_default().extend(dates);
    }
}

pub(super) fn refresh_metadata_summaries_on(
    conn: &Connection,
    prefix: &str,
    affected: &BTreeMap<StorageSignal, BTreeSet<String>>,
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
            tracing::error!(
                event = "metadata_refresh_rollback_failed",
                error = %rollback_err
            );
        }
    }
    result
}

pub(super) fn metadata_refresh_sql(
    prefix: &str,
    signal: StorageSignal,
    date: &str,
) -> Result<String> {
    let day = MetadataRefreshDay::new(date, signal)?;
    let selects = match signal {
        StorageSignal::Logs => logs_metadata_sql(prefix, &day),
        StorageSignal::Spans => spans_metadata_sql(prefix, &day),
        StorageSignal::MetricGauge => {
            metric_metadata_sql(prefix, StorageSignal::MetricGauge, &day, "gauge")
        }
        StorageSignal::MetricSum => {
            metric_metadata_sql(prefix, StorageSignal::MetricSum, &day, "counter")
        }
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
    /// The per-signal record-time column (`time_unix_nano` for logs/metrics,
    /// `start_time_unix_nano` for spans). Threaded so the day predicate and the
    /// `min`/`max` aggregations don't have to be per-call-site lookups.
    time_col: &'static str,
}

impl<'a> MetadataRefreshDay<'a> {
    fn new(date: &'a str, signal: StorageSignal) -> Result<Self> {
        let start = NaiveDate::parse_from_str(date, "%Y-%m-%d")
            .with_context(|| format!("parse metadata refresh date {date}"))?;
        let end = start
            .succ_opt()
            .with_context(|| format!("metadata refresh date {date} has no successor"))?;
        Ok(Self {
            date,
            start: format!("{start} 00:00:00"),
            end: format!("{end} 00:00:00"),
            time_col: table_timestamp_column(signal),
        })
    }

    fn predicate(&self) -> String {
        format!(
            "{} >= TIMESTAMP {} AND {} < TIMESTAMP {}",
            self.time_col,
            sql_quote(&self.start),
            self.time_col,
            sql_quote(&self.end)
        )
    }

    fn time_col(&self) -> &'static str {
        self.time_col
    }
}

pub(super) fn logs_metadata_sql(prefix: &str, day: &MetadataRefreshDay<'_>) -> Vec<String> {
    let mut sql = Vec::new();
    for (name, value_expr) in semantic_labels::metadata_labels(LabelScope::Logs) {
        sql.push(label_value_insert_sql(
            prefix,
            "logs",
            "logs",
            day,
            "label_value",
            name,
            &value_expr,
        ));
    }
    let deployment_environment =
        semantic_labels::label_expr(LabelScope::Logs, "deployment_environment")
            .expect("deployment_environment is registered for logs");
    let ts = day.time_col();
    sql.push(format!(
        "\
        SELECT 'logs', DATE {}, 'series', 'stream', NULL, NULL, NULL, NULL, \
               service_name, {deployment_environment}, severity_text, \
               count(*), min({ts}), max({ts}) \
        FROM {prefix}logs \
        WHERE {} \
        GROUP BY service_name, {deployment_environment}, severity_text",
        sql_quote(day.date),
        day.predicate()
    ));
    sql
}

pub(super) fn spans_metadata_sql(prefix: &str, day: &MetadataRefreshDay<'_>) -> Vec<String> {
    let mut sql = Vec::new();
    for (name, value_expr) in semantic_labels::metadata_labels(LabelScope::Spans) {
        sql.push(label_value_insert_sql(
            prefix,
            "spans",
            "spans",
            day,
            "tag_value",
            name,
            &value_expr,
        ));
    }
    sql
}

pub(super) fn metric_metadata_sql(
    prefix: &str,
    signal: StorageSignal,
    day: &MetadataRefreshDay<'_>,
    metric_type: &str,
) -> Vec<String> {
    let table = signal.as_str();
    let signal = signal.as_str();
    let ts = day.time_col();
    let mut sql = Vec::new();
    // The metric-name column on the v2 OTAP schema is just `name`; the
    // discovery vocabulary still calls it `__name__` (Prometheus convention).
    let mut label_values = vec![("__name__", "name".to_string())];
    label_values.extend(semantic_labels::metadata_labels(LabelScope::Metrics));
    for (name, value_expr) in label_values {
        sql.push(label_value_insert_sql(
            prefix,
            signal,
            table,
            day,
            "label_value",
            name,
            &value_expr,
        ));
    }
    let deployment_environment = metrics_deployment_environment_expr();
    sql.push(format!(
        "\
        SELECT {}, DATE {}, 'series', name, NULL, {}, NULL, NULL, \
               service_name, {deployment_environment}, NULL, \
               count(*), min({ts}), max({ts}) \
        FROM {prefix}{table} \
        WHERE {} AND name IS NOT NULL AND name <> '' \
        GROUP BY name, service_name, {deployment_environment}",
        sql_quote(signal),
        sql_quote(day.date),
        sql_quote(metric_type),
        day.predicate()
    ));
    sql.push(format!(
        "\
        SELECT {}, DATE {}, 'metric_metadata', name, NULL, {}, \
               max(coalesce(unit, '')), max(coalesce(description, '')), \
               NULL, NULL, NULL, count(*), min({ts}), max({ts}) \
        FROM {prefix}{table} \
        WHERE {} AND name IS NOT NULL AND name <> '' \
        GROUP BY name",
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
    value_expr: &str,
) -> String {
    let ts = day.time_col();
    format!(
        "\
        SELECT {}, DATE {}, {}, {}, {value_expr}::VARCHAR, NULL, NULL, NULL, \
               NULL, NULL, NULL, count(*), min({ts}), max({ts}) \
        FROM {prefix}{table} \
        WHERE {} AND {value_expr} IS NOT NULL AND {value_expr}::VARCHAR <> '' \
        GROUP BY {value_expr}",
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
