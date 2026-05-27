use super::arrow_write::timestamp_day;
use super::schema::{table_columns, table_timestamp_column};
use crate::signal::StorageSignal;
use anyhow::{Context, Result};
use arrow58::array as arrow58_array;
use arrow58::array::Array as _;
use arrow58::array::ArrayRef;
use arrow58::datatypes as arrow58_types;
use arrow58::record_batch::RecordBatch;
use chrono::Utc;
use std::collections::BTreeSet;
use std::sync::Arc;

/// Shape an `otlp2records::transform_*` output `RecordBatch` for the DuckDB
/// Arrow appender. Since canardstack v2 storage tracks the otlp2records 0.8.0
/// OTAP column names verbatim, the only work here is synthesizing the two
/// canardstack-owned book-keeping columns (`ingested_at`, `source_format`) and
/// passing every other column through unchanged. The output is ordered to
/// match [`table_columns`], which is the order the DuckDB appender expects.
pub(super) fn storage_duckdb_batch(
    storage_signal: StorageSignal,
    batch: &RecordBatch,
    source_format: &str,
) -> Result<RecordBatch> {
    let rows = batch.num_rows();
    let ingested_at = Utc::now().timestamp_micros();
    let mut fields = Vec::with_capacity(table_columns(storage_signal).len());
    let mut arrays = Vec::with_capacity(table_columns(storage_signal).len());

    for &(name, _) in table_columns(storage_signal) {
        let (field, array) = match name {
            "ingested_at" => (
                arrow58_types::Field::new(
                    name,
                    arrow58_types::DataType::Timestamp(arrow58_types::TimeUnit::Microsecond, None),
                    true,
                ),
                Arc::new(arrow58_array::TimestampMicrosecondArray::from(
                    (0..rows).map(|_| Some(ingested_at)).collect::<Vec<_>>(),
                )) as ArrayRef,
            ),
            "source_format" => (
                arrow58_types::Field::new(name, arrow58_types::DataType::Utf8, true),
                string_array_from_options((0..rows).map(|_| Some(source_format.to_string()))),
            ),
            _ => copy_arrow_column(batch, name)?,
        };
        fields.push(field);
        arrays.push(array);
    }

    RecordBatch::try_new(Arc::new(arrow58_types::Schema::new(fields)), arrays)
        .context("build storage RecordBatch")
}

pub(super) fn batch_timestamp_days(
    storage_signal: StorageSignal,
    batch: &RecordBatch,
) -> Result<Vec<String>> {
    let timestamps = timestamp_column(storage_signal, batch)?;
    let mut out = BTreeSet::new();
    for row in 0..timestamps.len() {
        if let Some(day) = timestamp_day(timestamps, row) {
            out.insert(day);
        }
    }
    Ok(out.into_iter().collect())
}

/// The per-signal `TimestampNanosecondArray` storage and freshness machinery
/// reads. v2 stores nanosecond-precision OTAP timestamps directly; logs and
/// metrics use the record's `time_unix_nano`, spans use `start_time_unix_nano`
/// (see [`table_timestamp_column`]).
pub(super) fn timestamp_column(
    storage_signal: StorageSignal,
    batch: &RecordBatch,
) -> Result<&arrow58_array::TimestampNanosecondArray> {
    let name = table_timestamp_column(storage_signal);
    let idx = batch
        .schema()
        .index_of(name)
        .with_context(|| format!("{storage_signal} batch has no '{name}' column"))?;
    batch
        .column(idx)
        .as_any()
        .downcast_ref::<arrow58_array::TimestampNanosecondArray>()
        .with_context(|| {
            format!("{storage_signal} '{name}' column is not TimestampNanosecondArray")
        })
}

pub(super) fn copy_arrow_column(
    batch: &RecordBatch,
    name: &str,
) -> Result<(arrow58_types::Field, ArrayRef)> {
    let schema = batch.schema();
    let idx = schema
        .index_of(name)
        .with_context(|| format!("otlp2records batch missing expected column '{name}'"))?;
    let field58 = schema.field(idx);
    let source = batch.column(idx);
    // The DuckDB Arrow appender maps Arrow `Duration` to DuckDB `INTERVAL`,
    // but canardstack stores `duration_time_unix_nano` as plain `BIGINT`
    // nanoseconds (no interval semantics, easier to aggregate). Reinterpret
    // the Duration array as Int64 — same i64 buffer, different DataType
    // wrapper — so the appender sees `BIGINT`.
    let (data_type, array): (arrow58_types::DataType, ArrayRef) = match field58.data_type() {
        arrow58_types::DataType::Duration(_) => {
            let dur = source
                .as_any()
                .downcast_ref::<arrow58_array::DurationNanosecondArray>()
                .with_context(|| {
                    format!("otlp2records '{name}' duration column is not DurationNanosecondArray")
                })?;
            let int64: arrow58_array::Int64Array = (0..dur.len())
                .map(|i| {
                    if dur.is_null(i) {
                        None
                    } else {
                        Some(dur.value(i))
                    }
                })
                .collect();
            (arrow58_types::DataType::Int64, Arc::new(int64) as ArrayRef)
        }
        other => (other.clone(), source.clone()),
    };
    let field = arrow58_types::Field::new(name, data_type, field58.is_nullable());
    Ok((field, array))
}

pub(super) fn string_array_from_options(
    values: impl IntoIterator<Item = Option<String>>,
) -> ArrayRef {
    let iter = values.into_iter();
    let (_, upper) = iter.size_hint();
    let mut builder = arrow58_array::StringBuilder::with_capacity(upper.unwrap_or(0), 0);
    for value in iter {
        if let Some(value) = value {
            builder.append_value(value);
        } else {
            builder.append_null();
        }
    }
    Arc::new(builder.finish())
}
