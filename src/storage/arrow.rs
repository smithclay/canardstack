use super::immutable::timestamp_day;
use super::schema::table_columns;
use crate::ingest::StorageSignal;
use anyhow::{Context, Result};
use arrow58::array as arrow58_array;
use arrow58::array::Array as _;
use arrow58::array::ArrayRef;
use arrow58::datatypes as arrow58_types;
use arrow58::record_batch::RecordBatch;
use chrono::Utc;
use std::collections::BTreeSet;
use std::sync::Arc;

pub(super) fn storage_duckdb_batch(
    table: StorageSignal,
    batch: &RecordBatch,
    source_format: &str,
) -> Result<RecordBatch> {
    let rows = batch.num_rows();
    let ingested_at = Utc::now().timestamp_micros();
    let mut fields = Vec::with_capacity(table_columns(table).len());
    let mut arrays = Vec::with_capacity(table_columns(table).len());

    for &(name, _) in table_columns(table) {
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

pub(super) fn batch_timestamp_days(batch: &RecordBatch) -> Result<Vec<String>> {
    let timestamps = timestamp_column(batch)?;
    let mut out = BTreeSet::new();
    for row in 0..timestamps.len() {
        if let Some(day) = timestamp_day(timestamps, row) {
            out.insert(day);
        }
    }
    Ok(out.into_iter().collect())
}

pub(super) fn timestamp_column(
    batch: &RecordBatch,
) -> Result<&arrow58_array::TimestampMicrosecondArray> {
    let idx = batch.schema().index_of("timestamp")?;
    batch
        .column(idx)
        .as_any()
        .downcast_ref::<arrow58_array::TimestampMicrosecondArray>()
        .context("timestamp column is not TimestampMicrosecondArray")
}

pub(super) fn copy_arrow_column(
    batch: &RecordBatch,
    name: &str,
) -> Result<(arrow58_types::Field, ArrayRef)> {
    let schema = batch.schema();
    let idx = schema.index_of(name)?;
    let field58 = schema.field(idx);
    let field = arrow58_types::Field::new(name, field58.data_type().clone(), field58.is_nullable());
    Ok((field, batch.column(idx).clone()))
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
