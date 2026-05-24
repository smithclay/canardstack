//! Static v0 storage schema.
//!
//! The table schema is STATIC in v0: every column list is a fixed `const`
//! (`LOGS_COLUMNS`, `SPANS_COLUMNS`, `METRIC_GAUGE_COLUMNS`,
//! `METRIC_SUM_COLUMNS`, `METADATA_SUMMARY_COLUMNS`) and tables are created with
//! `CREATE TABLE IF NOT EXISTS`. There is no online schema evolution, no
//! migration tool, and no `ALTER TABLE ... ADD COLUMN` path. Changing a column
//! set is a deliberate v0 non-goal: it requires a coordinated manual migration
//! against the DuckLake catalog (or starting from a fresh catalog).
//!
//! Incoming OTLP fields that have no dedicated typed column are NOT promoted to
//! new columns. Resource/scope/record attributes are carried as JSON in the
//! `*_attributes` columns (`resource_attributes`, `scope_attributes`,
//! `log_attributes`, `span_attributes`, `metric_attributes`); the compatibility
//! layer extracts the few well-known keys it needs from that JSON at query time
//! (see `crate::db::sql`) instead of widening the schema.

use crate::signal::StorageSignal;
use anyhow::{Context, Result};
use duckdb::Connection;

pub(super) fn create_tables_on(conn: &Connection, prefix: &str) -> Result<()> {
    let mut ddl = String::new();
    for table in [
        StorageSignal::Logs,
        StorageSignal::Spans,
        StorageSignal::MetricGauge,
        StorageSignal::MetricSum,
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
        StorageSignal::Logs,
        StorageSignal::Spans,
        StorageSignal::MetricGauge,
        StorageSignal::MetricSum,
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

pub(super) fn table_columns(
    storage_signal: StorageSignal,
) -> &'static [(&'static str, &'static str)] {
    match storage_signal {
        StorageSignal::Logs => LOGS_COLUMNS,
        StorageSignal::Spans => SPANS_COLUMNS,
        StorageSignal::MetricGauge => METRIC_GAUGE_COLUMNS,
        StorageSignal::MetricSum => METRIC_SUM_COLUMNS,
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

/// The storage schema generation this binary WRITES. Bump when a `*_COLUMNS`
/// set or the partitioning changes (see the module doc on schema evolution).
pub(super) const SCHEMA_VERSION: u32 = 1;
/// The oldest catalog schema generation this binary can safely operate on. Keep
/// it equal to [`SCHEMA_VERSION`] for a breaking change; an additive,
/// schema-on-read-tolerant (expand/contract) change can leave this low so a
/// newer binary still opens an older catalog. This is the min-reader/min-writer
/// window the lakehouse formats (Delta/Iceberg) use, scaled to v0.
pub(super) const MIN_COMPATIBLE_SCHEMA_VERSION: u32 = 1;

const META_TABLE: &str = "canardstack_meta";
const META_COLUMNS: &[(&str, &str)] = &[("key", "VARCHAR"), ("value", "VARCHAR")];

/// Enforce binary/catalog schema compatibility, fail-closed on the
/// [`MIN_COMPATIBLE_SCHEMA_VERSION`]..=[`SCHEMA_VERSION`] window. On a fresh or
/// pre-versioning catalog the current version is stamped; a catalog older than
/// the minimum, or newer than this binary writes, aborts boot loudly (matching
/// the "startup fails loudly" stance). Provenance rows (`canardstack_version`,
/// `otlp2records_schema_fingerprint`) are recorded for forensics and written
/// only when changed, so an unchanged restart adds no catalog writes. Runs on
/// the writer connection in [`super::Storage::open`], after `create_tables_on`.
pub(super) fn enforce_schema_version_on(conn: &Connection, prefix: &str) -> Result<()> {
    conn.execute_batch(&create_table_sql(prefix, META_TABLE, META_COLUMNS))
        .context("create canardstack_meta table")?;

    match read_meta_value(conn, prefix, "schema_version")? {
        None => {
            // Fresh or pre-versioning catalog: adopt the current schema. Today's
            // schema IS version 1, so adopting a legacy catalog is correct.
            set_meta_if_changed(conn, prefix, "schema_version", &SCHEMA_VERSION.to_string())?;
        }
        Some(raw) => {
            let catalog_version: u32 = raw.trim().parse().with_context(|| {
                format!("catalog {META_TABLE}.schema_version {raw:?} is not a valid version number")
            })?;
            if catalog_version < MIN_COMPATIBLE_SCHEMA_VERSION {
                anyhow::bail!(
                    "DuckLake catalog schema v{catalog_version} is older than the minimum \
                     v{MIN_COMPATIBLE_SCHEMA_VERSION} canardstack {} supports; migrate the catalog \
                     or start from a fresh one",
                    env!("CARGO_PKG_VERSION"),
                );
            }
            if catalog_version > SCHEMA_VERSION {
                anyhow::bail!(
                    "DuckLake catalog schema v{catalog_version} was written by a newer canardstack \
                     (this binary {} writes v{SCHEMA_VERSION}); upgrade the binary to open it",
                    env!("CARGO_PKG_VERSION"),
                );
            }
            // Within the window: compatible. Leave schema_version as-is — only an
            // explicit (future) migration advances the stored generation.
        }
    }

    set_meta_if_changed(
        conn,
        prefix,
        "canardstack_version",
        env!("CARGO_PKG_VERSION"),
    )?;
    set_meta_if_changed(
        conn,
        prefix,
        "otlp2records_schema_fingerprint",
        &otlp2records_schema_fingerprint(),
    )?;
    Ok(())
}

fn read_meta_value(conn: &Connection, prefix: &str, key: &str) -> Result<Option<String>> {
    let sql = format!("SELECT value FROM {prefix}{META_TABLE} WHERE key = '{key}'");
    let mut stmt = conn.prepare(&sql)?;
    let mut rows = stmt.query([])?;
    match rows.next()? {
        Some(row) => Ok(Some(row.get::<_, String>(0)?)),
        None => Ok(None),
    }
}

fn set_meta_if_changed(conn: &Connection, prefix: &str, key: &str, value: &str) -> Result<()> {
    if read_meta_value(conn, prefix, key)?.as_deref() == Some(value) {
        return Ok(());
    }
    // Single DuckLake transaction so the row is never left deleted-without-insert.
    conn.execute_batch(&format!(
        "BEGIN TRANSACTION; \
         DELETE FROM {prefix}{META_TABLE} WHERE key = '{key}'; \
         INSERT INTO {prefix}{META_TABLE} (key, value) VALUES ('{key}', '{value}'); \
         COMMIT;"
    ))
    .with_context(|| format!("write {META_TABLE}.{key}"))?;
    Ok(())
}

/// Stable fingerprint of the otlp2records output schema this binary links
/// against, recorded as provenance so an operator can see which transform-schema
/// generation wrote a catalog. FNV-1a over a canonical rendering of
/// `otlp2records::schema_defs()`, so the value is stable across Rust
/// versions/platforms (unlike `std`'s `DefaultHasher`).
fn otlp2records_schema_fingerprint() -> String {
    const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
    let mut hash = FNV_OFFSET;
    for def in otlp2records::schema_defs() {
        hash = fnv1a(hash, def.name.as_bytes());
        for field in def.fields {
            hash = fnv1a(hash, field.name.as_bytes());
            hash = fnv1a(hash, field.field_type.as_str().as_bytes());
            hash = fnv1a(hash, &[u8::from(field.required)]);
        }
    }
    format!("fnv1a64:{hash:016x}")
}

fn fnv1a(seed: u64, bytes: &[u8]) -> u64 {
    const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;
    let mut hash = seed;
    for &byte in bytes {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash
}

#[cfg(test)]
mod tests {
    use super::*;
    use duckdb::Connection;
    use otlp2records::{schema_def, FieldType};

    /// The version guard: a fresh catalog is stamped at the current version and a
    /// reopen is idempotent; a catalog older than the minimum or newer than this
    /// binary fails boot closed; provenance rows are recorded. Runs against an
    /// in-memory DuckDB (no DuckLake extension needed) since `canardstack_meta`
    /// is a plain table.
    #[test]
    fn schema_version_guard_stamps_and_fails_closed() {
        let conn = Connection::open_in_memory().unwrap();
        let prefix = "";

        // Fresh catalog: adopts the current schema version + provenance.
        enforce_schema_version_on(&conn, prefix).unwrap();
        assert_eq!(
            read_meta_value(&conn, prefix, "schema_version")
                .unwrap()
                .as_deref(),
            Some(SCHEMA_VERSION.to_string().as_str())
        );
        assert_eq!(
            read_meta_value(&conn, prefix, "canardstack_version")
                .unwrap()
                .as_deref(),
            Some(env!("CARGO_PKG_VERSION"))
        );
        assert!(
            read_meta_value(&conn, prefix, "otlp2records_schema_fingerprint")
                .unwrap()
                .is_some()
        );

        // Reopen with the same binary: compatible, idempotent.
        enforce_schema_version_on(&conn, prefix).unwrap();

        // Catalog written by a newer binary: fail closed.
        set_meta_if_changed(
            &conn,
            prefix,
            "schema_version",
            &(SCHEMA_VERSION + 1).to_string(),
        )
        .unwrap();
        let err = enforce_schema_version_on(&conn, prefix).unwrap_err();
        assert!(
            err.to_string().contains("newer canardstack"),
            "unexpected error: {err}"
        );

        // Catalog older than the minimum supported: fail closed.
        set_meta_if_changed(
            &conn,
            prefix,
            "schema_version",
            &MIN_COMPATIBLE_SCHEMA_VERSION.saturating_sub(1).to_string(),
        )
        .unwrap();
        let err = enforce_schema_version_on(&conn, prefix).unwrap_err();
        assert!(
            err.to_string().contains("older than the minimum"),
            "unexpected error: {err}"
        );
    }

    /// Columns `storage_duckdb_batch` synthesizes locally instead of copying from
    /// the otlp2records output batch, so they are exempt from the otlp2records
    /// alignment check below. See `crate::storage::arrow::storage_duckdb_batch`.
    const SYNTHESIZED_COLUMNS: &[(&str, &str)] =
        &[("ingested_at", "TIMESTAMP"), ("source_format", "VARCHAR")];

    /// Map a storage signal to the otlp2records schema it is built from.
    fn otlp2records_schema_name(signal: StorageSignal) -> &'static str {
        match signal {
            StorageSignal::Logs => "logs",
            StorageSignal::Spans => "spans",
            StorageSignal::MetricGauge => "gauge",
            StorageSignal::MetricSum => "sum",
        }
    }

    /// The DuckDB column type canardstack declares to store an otlp2records field
    /// of the given type. JSON attribute fields are stored as VARCHAR (see the
    /// `schema` module doc).
    fn expected_duckdb_type(field_type: FieldType) -> &'static str {
        match field_type {
            FieldType::Timestamp => "TIMESTAMP",
            FieldType::Int64 => "BIGINT",
            FieldType::Int32 => "INTEGER",
            FieldType::Float64 => "DOUBLE",
            FieldType::Bool => "BOOLEAN",
            FieldType::String => "VARCHAR",
            FieldType::Json => "VARCHAR",
        }
    }

    /// Pin the implicit canardstack <-> otlp2records column contract. Every stored
    /// column that `storage_duckdb_batch` copies by name from the otlp2records
    /// output batch must exist in the matching `otlp2records::schema_def`, with a
    /// DuckDB type canardstack declares compatibly. An otlp2records upgrade that
    /// renames, drops, or retypes an emitted column trips THIS test (at
    /// `cargo test`) with a precise message, instead of silently failing every
    /// ingest at the `copy_arrow_column` name lookup. When it trips, bump the
    /// stored schema version and plan a catalog migration before upgrading.
    #[test]
    fn stored_columns_align_with_otlp2records_output() {
        for signal in StorageSignal::ALL {
            let schema_name = otlp2records_schema_name(signal);
            let def = schema_def(schema_name).unwrap_or_else(|| {
                panic!("otlp2records no longer defines a '{schema_name}' schema for {signal}")
            });
            for &(col, declared_ty) in table_columns(signal) {
                if let Some((_, synth_ty)) =
                    SYNTHESIZED_COLUMNS.iter().find(|(name, _)| *name == col)
                {
                    assert_eq!(
                        declared_ty, *synth_ty,
                        "{signal} synthesized column {col} is declared {declared_ty}, expected {synth_ty}"
                    );
                    continue;
                }
                let otlp_field = def
                    .fields
                    .iter()
                    .find(|f| f.name == col)
                    .unwrap_or_else(|| {
                        panic!(
                        "otlp2records '{schema_name}' no longer emits column '{col}' that {signal} \
                         stores; bump the stored schema version and migrate the catalog"
                    )
                    });
                let expected = expected_duckdb_type(otlp_field.field_type);
                assert_eq!(
                    declared_ty, expected,
                    "{signal} column {col}: otlp2records emits {:?} (-> {expected}), but the table \
                     declares {declared_ty}; bump the stored schema version and migrate the catalog",
                    otlp_field.field_type
                );
            }
        }
    }
}
