use crate::config::Config;
use crate::ingest::Signal;
use crate::sql::{escape_value, quote as sql_quote};
use crate::LockExt;
use anyhow::{Context, Result};
use arrow58::array as arrow58_array;
use arrow58::array::Array as _;
use arrow58::array::ArrayRef;
use arrow58::compute::{concat_batches, take};
use arrow58::datatypes as arrow58_types;
use arrow58::record_batch::RecordBatch;
use chrono::{DateTime, NaiveDate, Timelike, Utc};
use duckdb::Connection;
use otlp2records::output::write_parquet;
use serde::Serialize;
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File};
use std::io::BufWriter;
use std::path::Path;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::{Duration, Instant};

const DUCKDB_THREADS: usize = 1;
static IMMUTABLE_SEGMENT_COUNTER: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Debug, Serialize)]
pub struct StorageHealth {
    pub healthy: bool,
    pub mode: String,
    pub ducklake_catalog: String,
    pub ducklake_available: bool,
    pub ducklake_required: bool,
    pub postgres_catalog_configured: bool,
    pub last_error: Option<String>,
    pub capabilities: StorageCapabilities,
    pub freshness_watermarks: Value,
    pub logical_rows: Value,
    pub ducklake_storage_layout: Value,
    pub physical_bytes: u64,
}

#[derive(Clone, Debug, Serialize)]
pub struct StorageProbe {
    pub healthy: bool,
    pub mode: String,
    pub ducklake_available: bool,
    pub ducklake_required: bool,
    pub last_error: Option<String>,
}

impl StorageProbe {
    pub fn is_ready(&self) -> bool {
        self.healthy && (!self.ducklake_required || self.ducklake_available)
    }
}

impl StorageHealth {
    pub fn is_ready(&self) -> bool {
        self.healthy && (!self.ducklake_required || self.ducklake_available)
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct StorageCapabilities {
    pub insert: bool,
    pub query: bool,
    pub inlined_flush: bool,
    pub snapshot_expiration: bool,
    pub cleanup_old_files: bool,
    pub merge_adjacent_files: bool,
    pub whole_day_retention: bool,
}

pub struct Storage {
    /// Write-side connection. Held for inserts, DDL, and DuckLake maintenance.
    /// Reader path never touches this mutex — that decoupling keeps /healthz,
    /// /metrics, and queries responsive while a flush is in flight.
    writer: Mutex<Connection>,
    /// Cloned from `writer` at startup after ATTACH + DDL; shares the
    /// underlying Database (attached schemas survive `try_clone`). Source of
    /// per-query clones in `with_query_conn`.
    reader: Mutex<Connection>,
    target_prefix: String,
    mode: String,
    catalog_name: String,
    ducklake_available: bool,
    postgres_catalog_configured: bool,
    local_storage_dir: PathBuf,
    ducklake_required: bool,
    ducklake_managed_maintenance: bool,
    immutable_segment_target_bytes: usize,
    immutable_segment_max_age: Duration,
    immutable_buffers: Mutex<BTreeMap<Signal, ImmutableSegmentBuffer>>,
    write_memory_limit: String,
    last_error: Mutex<Option<String>>,
    /// Cache-invalidation token for discovery metadata. Bumped only after a
    /// committed `metadata_summary` change (refresh or retention); discovery
    /// caches in `Metadata` key entries on this value and drop them on a bump.
    metadata_generation: AtomicU64,
    /// Signal/event-date buckets whose `metadata_summary` rows are stale after
    /// a committed insert. Drained by the `metadata_refresh` scheduler job so
    /// the day-partition re-aggregation stays off the ingest commit path.
    dirty_metadata: Mutex<BTreeMap<Signal, BTreeSet<String>>>,
}

pub struct RetentionPolicy {
    pub logs_days: i64,
    pub spans_days: i64,
    pub metrics_days: i64,
}

/// Typed timeout from `with_query_conn`. Downcastable so callers can classify
/// the 503 as `query_timeout` without substring-matching the error message.
#[derive(Debug)]
pub struct QueryTimeoutError {
    pub timeout: Duration,
}

impl std::fmt::Display for QueryTimeoutError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "query timeout after {}ms", self.timeout.as_millis())
    }
}

impl std::error::Error for QueryTimeoutError {}

/// Storage insert failed after one or more smaller transactions may already
/// have committed. Callers use `committed_rows` to avoid retrying rows that
/// DuckDB has already accepted.
#[derive(Debug)]
pub struct InsertRecordsError {
    pub table: Signal,
    pub committed_rows: usize,
    pub attempted_rows: usize,
    pub source: anyhow::Error,
}

impl std::fmt::Display for InsertRecordsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "insert failed for {} after committing {}/{} row(s): {}",
            self.table, self.committed_rows, self.attempted_rows, self.source
        )
    }
}

impl std::error::Error for InsertRecordsError {}

pub struct ArrowBatchInsert<'a> {
    pub table: Signal,
    pub batch: &'a RecordBatch,
    pub source_format: &'a str,
}

#[derive(Clone, Debug)]
pub struct ArrowBatchInsertTiming {
    pub table: Signal,
    pub phase: &'static str,
    pub rows: usize,
    pub seconds: f64,
}

#[derive(Clone, Debug)]
pub struct ArrowBatchInsertResult {
    pub rows: usize,
    pub timings: Vec<ArrowBatchInsertTiming>,
}

struct PreparedArrowBatch {
    table: Signal,
    batch: RecordBatch,
    rows: usize,
    event_dates: Vec<String>,
}

#[derive(Clone)]
struct ImmutableSegmentBuffer {
    batches: Vec<RecordBatch>,
    rows: usize,
    bytes: usize,
    event_dates: BTreeSet<String>,
    opened_at: Instant,
}

struct ImmutableSealResult {
    rows: usize,
    files: usize,
    timings: Vec<ArrowBatchInsertTiming>,
    affected: BTreeMap<Signal, BTreeSet<String>>,
}

impl ImmutableSegmentBuffer {
    fn new(now: Instant) -> Self {
        Self {
            batches: Vec::new(),
            rows: 0,
            bytes: 0,
            event_dates: BTreeSet::new(),
            opened_at: now,
        }
    }

    fn push(&mut self, prepared: PreparedArrowBatch) {
        self.rows += prepared.rows;
        self.bytes += prepared.batch.get_array_memory_size().max(prepared.rows);
        self.event_dates.extend(prepared.event_dates);
        self.batches.push(prepared.batch);
    }

    fn append_buffer(&mut self, mut other: ImmutableSegmentBuffer) {
        self.rows += other.rows;
        self.bytes += other.bytes;
        self.event_dates.append(&mut other.event_dates);
        if other.opened_at < self.opened_at {
            self.opened_at = other.opened_at;
        }
        self.batches.append(&mut other.batches);
    }

    fn should_seal(&self, target_bytes: usize, max_age: Duration, now: Instant) -> bool {
        self.rows > 0
            && (self.bytes >= target_bytes || now.duration_since(self.opened_at) >= max_age)
    }

    fn record_batch(&self, table: Signal) -> Result<RecordBatch> {
        match self.batches.as_slice() {
            [] => anyhow::bail!("immutable {table} buffer is empty"),
            [batch] => Ok(batch.clone()),
            batches => {
                let schema = batches[0].schema();
                let refs = batches.iter().collect::<Vec<_>>();
                concat_batches(&schema, refs)
                    .with_context(|| format!("coalesce immutable {table} segment buffer"))
            }
        }
    }
}

impl Storage {
    pub fn open(config: &Config) -> Result<Self> {
        if let Some(parent) = config.duckdb_path.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::create_dir_all(&config.local_storage_dir)?;

        let writer = Connection::open(&config.duckdb_path)
            .with_context(|| format!("open DuckDB file {}", config.duckdb_path.display()))?;
        configure_base_connection(&writer)?;
        configure_write_connection(&writer, &config.duckdb_write_memory_limit)?;

        attach_ducklake_connection(
            &writer,
            config.postgres_dsn.as_deref(),
            config.ducklake_attach_uri.as_deref(),
            &config.duckdb_path,
            &config.local_storage_dir,
            config.duckdb_extension_dir.as_deref(),
            config.ducklake_data_inlining_row_limit,
        )
        .context(
            "DuckLake attach failed. Fix the catalog config (URI, token, network, or extension path) and restart.",
        )?;
        let target_prefix = "canardlake.".to_string();
        let plan = ducklake_attach_plan(config)?;
        let mode = format!("{}_immutable_segments", plan.mode);
        let ducklake_managed_maintenance = plan.managed_maintenance;

        create_tables_on(&writer, &target_prefix)?;

        // Reader must be cloned AFTER attach + create_tables so the new
        // session inherits the catalog.
        let reader = writer
            .try_clone()
            .context("clone writer connection for reader pool")?;
        configure_base_connection(&reader)?;

        Ok(Self {
            writer: Mutex::new(writer),
            reader: Mutex::new(reader),
            target_prefix,
            mode,
            catalog_name: "canardlake".to_string(),
            ducklake_available: true,
            postgres_catalog_configured: config.postgres_dsn.is_some(),
            local_storage_dir: config.local_storage_dir.clone(),
            ducklake_required: true,
            ducklake_managed_maintenance,
            immutable_segment_target_bytes: config.immutable_segment_target_bytes,
            immutable_segment_max_age: config.immutable_segment_max_age,
            immutable_buffers: Mutex::new(BTreeMap::new()),
            write_memory_limit: config.duckdb_write_memory_limit.clone(),
            last_error: Mutex::new(None),
            metadata_generation: AtomicU64::new(0),
            dirty_metadata: Mutex::new(BTreeMap::new()),
        })
    }

    pub fn healthy(&self) -> bool {
        match self.check_health_target() {
            Ok(()) => {
                *self.last_error.lock_or_poisoned() = None;
                true
            }
            Err(err) => {
                *self.last_error.lock_or_poisoned() = Some(err.to_string());
                false
            }
        }
    }

    pub fn accepts_memory_ingest(&self) -> bool {
        self.ducklake_available
    }

    pub fn health(&self) -> StorageHealth {
        StorageHealth {
            healthy: self.healthy(),
            mode: self.mode.clone(),
            ducklake_catalog: self.catalog_name.clone(),
            ducklake_available: self.ducklake_available,
            ducklake_required: self.ducklake_required,
            postgres_catalog_configured: self.postgres_catalog_configured,
            last_error: self.last_error.lock_or_poisoned().clone(),
            capabilities: StorageCapabilities {
                insert: true,
                query: true,
                inlined_flush: self.ducklake_managed_maintenance,
                snapshot_expiration: self.ducklake_managed_maintenance,
                cleanup_old_files: self.ducklake_managed_maintenance,
                merge_adjacent_files: false,
                whole_day_retention: true,
            },
            freshness_watermarks: self
                .freshness_watermarks()
                .unwrap_or_else(|err| json!({"error": err.to_string()})),
            logical_rows: self
                .logical_rows()
                .unwrap_or_else(|err| json!({"error": err.to_string()})),
            ducklake_storage_layout: self
                .ducklake_storage_layout()
                .unwrap_or_else(|err| json!({"error": err.to_string()})),
            physical_bytes: dir_size(&self.local_storage_dir).unwrap_or(0),
        }
    }

    pub fn probe(&self) -> StorageProbe {
        StorageProbe {
            healthy: self.healthy(),
            mode: self.mode.clone(),
            ducklake_available: self.ducklake_available,
            ducklake_required: self.ducklake_required,
            last_error: self.last_error.lock_or_poisoned().clone(),
        }
    }

    pub fn metadata_generation(&self) -> u64 {
        self.metadata_generation.load(Ordering::SeqCst)
    }

    fn mark_metadata_dirty(&self, affected: BTreeMap<Signal, BTreeSet<String>>) {
        merge_dirty_metadata(&mut self.dirty_metadata.lock_or_poisoned(), affected);
    }

    /// Re-aggregate `metadata_summary` for every signal/date bucket dirtied by
    /// a committed insert. Runs on the `metadata_refresh` scheduler job so the
    /// full day-partition scan stays off the ingest commit path. On failure the
    /// drained buckets are re-queued so the next tick retries them — committed
    /// telemetry must not stay invisible to the discovery APIs.
    pub fn refresh_metadata(&self) -> Result<usize> {
        let affected = std::mem::take(&mut *self.dirty_metadata.lock_or_poisoned());
        if affected.is_empty() {
            return Ok(0);
        }
        let conn = self.writer.lock_or_poisoned();
        configure_write_connection(&conn, &self.write_memory_limit)?;
        match refresh_metadata_summaries_on(&conn, &self.target_prefix, &affected) {
            Ok(buckets) => {
                self.metadata_generation.fetch_add(1, Ordering::SeqCst);
                Ok(buckets)
            }
            Err(err) => {
                self.mark_metadata_dirty(affected);
                Err(err)
            }
        }
    }

    pub fn insert_arrow_records(
        &self,
        table: Signal,
        batch: &RecordBatch,
        source_format: &str,
    ) -> Result<usize> {
        let result = self.insert_arrow_batches(&[ArrowBatchInsert {
            table,
            batch,
            source_format,
        }])?;
        Ok(result.rows)
    }

    pub fn insert_arrow_batches(
        &self,
        batches: &[ArrowBatchInsert<'_>],
    ) -> Result<ArrowBatchInsertResult> {
        let mut prepared = Vec::new();
        let mut prepare_timings = Vec::new();
        let mut attempted_rows = 0;
        for batch in batches {
            if batch.batch.num_rows() == 0 {
                continue;
            }
            let rows = batch.batch.num_rows();
            let prepare_started = Instant::now();
            let prepared_batch =
                storage_duckdb_batch(batch.table, batch.batch, batch.source_format)?;
            let event_dates = batch_event_dates(&prepared_batch)?;
            let prepare_seconds = prepare_started.elapsed().as_secs_f64();
            attempted_rows += rows;
            prepared.push(PreparedArrowBatch {
                table: batch.table,
                batch: prepared_batch,
                rows,
                event_dates,
            });
            prepare_timings.push(ArrowBatchInsertTiming {
                table: batch.table,
                phase: "storage_prepare",
                rows,
                seconds: prepare_seconds,
            });
        }

        if prepared.is_empty() {
            return Ok(ArrowBatchInsertResult {
                rows: 0,
                timings: Vec::new(),
            });
        }

        self.insert_immutable_segments(prepared, prepare_timings, attempted_rows)
    }

    fn insert_immutable_segments(
        &self,
        prepared: Vec<PreparedArrowBatch>,
        prepare_timings: Vec<ArrowBatchInsertTiming>,
        attempted_rows: usize,
    ) -> Result<ArrowBatchInsertResult> {
        if !self.ducklake_available {
            anyhow::bail!("immutable segment ingest requires DuckLake storage");
        }

        let error_table = prepared
            .first()
            .map(|batch| batch.table)
            .unwrap_or(Signal::Logs);
        let mut timings = prepare_timings;

        {
            let mut buffers = self.immutable_buffers.lock_or_poisoned();
            let started = Instant::now();
            for batch in prepared {
                buffers
                    .entry(batch.table)
                    .or_insert_with(|| ImmutableSegmentBuffer::new(started))
                    .push(batch);
            }
            timings.push(ArrowBatchInsertTiming {
                table: error_table,
                phase: "storage_buffer",
                rows: attempted_rows,
                seconds: started.elapsed().as_secs_f64(),
            });
        }
        *self.last_error.lock_or_poisoned() = None;
        Ok(ArrowBatchInsertResult {
            rows: attempted_rows,
            timings,
        })
    }

    pub fn flush_immutable_segments(&self, force: bool) -> Result<Value> {
        let mut to_seal = BTreeMap::new();
        let no_seal_snapshot;
        {
            let mut buffers = self.immutable_buffers.lock_or_poisoned();
            let now = Instant::now();
            let tables_to_seal = buffers
                .iter()
                .filter_map(|(table, buffer)| {
                    (force
                        || buffer.should_seal(
                            self.immutable_segment_target_bytes,
                            self.immutable_segment_max_age,
                            now,
                        ))
                    .then_some(*table)
                })
                .collect::<Vec<_>>();

            if tables_to_seal.is_empty() {
                no_seal_snapshot = immutable_buffer_snapshot(&buffers);
                return Ok(json!({
                    "supported": true,
                    "force": force,
                    "sealed_files": 0,
                    "sealed_rows": 0,
                    "timings": [],
                    "active_buffers": no_seal_snapshot,
                }));
            }

            for table in tables_to_seal {
                if let Some(buffer) = buffers.remove(&table) {
                    to_seal.insert(table, buffer);
                }
            }
        }
        let seal_result = match self.seal_immutable_buffers(&to_seal) {
            Ok(result) => result,
            Err(err) => {
                self.restore_immutable_buffers(to_seal);
                return Err(err);
            }
        };
        let active_buffers = immutable_buffer_snapshot(&self.immutable_buffers.lock_or_poisoned());
        *self.last_error.lock_or_poisoned() = None;
        self.mark_metadata_dirty(seal_result.affected);

        Ok(json!({
            "supported": true,
            "force": force,
            "sealed_files": seal_result.files,
            "sealed_rows": seal_result.rows,
            "timings": immutable_timing_snapshot(&seal_result.timings),
            "active_buffers": active_buffers,
        }))
    }

    fn seal_immutable_buffers(
        &self,
        buffers: &BTreeMap<Signal, ImmutableSegmentBuffer>,
    ) -> Result<ImmutableSealResult> {
        let mut timings = Vec::new();
        let mut sealed = Vec::with_capacity(buffers.len());
        let mut affected = BTreeMap::new();

        for (&table, buffer) in buffers {
            let batch = buffer.record_batch(table)?;
            let started = Instant::now();
            let segments = split_batch_by_immutable_partition(&batch)?
                .into_iter()
                .map(|(partition, batch)| {
                    write_immutable_segment(&self.local_storage_dir, table, partition, &batch)
                })
                .collect::<Result<Vec<_>>>()?;
            sealed.extend(segments);
            timings.push(ArrowBatchInsertTiming {
                table,
                phase: "storage_parquet_write",
                rows: buffer.rows,
                seconds: started.elapsed().as_secs_f64(),
            });
            affected.insert(table, buffer.event_dates.clone());
        }

        let conn = self.writer.lock_or_poisoned();
        configure_write_connection(&conn, &self.write_memory_limit)?;
        let register_result = (|| -> Result<()> {
            conn.execute_batch("BEGIN TRANSACTION;")?;
            for segment in &sealed {
                let started = Instant::now();
                register_ducklake_data_file(
                    &conn,
                    &self.catalog_name,
                    segment.table,
                    &segment.path,
                )?;
                timings.push(ArrowBatchInsertTiming {
                    table: segment.table,
                    phase: "storage_insert",
                    rows: segment.rows,
                    seconds: started.elapsed().as_secs_f64(),
                });
            }
            let commit_started = Instant::now();
            conn.execute_batch("COMMIT;")?;
            distribute_commit_seconds(&mut timings, commit_started.elapsed().as_secs_f64());
            Ok(())
        })();

        if let Err(err) = register_result {
            let _ = conn.execute_batch("ROLLBACK;");
            return Err(err);
        }

        Ok(ImmutableSealResult {
            rows: sealed.iter().map(|segment| segment.rows).sum(),
            files: sealed.len(),
            timings,
            affected,
        })
    }

    fn restore_immutable_buffers(&self, detached: BTreeMap<Signal, ImmutableSegmentBuffer>) {
        let mut buffers = self.immutable_buffers.lock_or_poisoned();
        for (table, mut detached_buffer) in detached {
            if let Some(current) = buffers.remove(&table) {
                detached_buffer.append_buffer(current);
            }
            buffers.insert(table, detached_buffer);
        }
    }

    pub fn ducklake_storage_layout(&self) -> Result<Value> {
        if !self.ducklake_available {
            return Ok(json!({"supported": false, "reason": "ducklake is not attached"}));
        }
        self.with_conn(|conn, _| {
            let tables = self.ducklake_storage_layout_on(conn)?;
            Ok(json!({"supported": true, "tables": tables}))
        })
    }

    fn ducklake_storage_layout_on(&self, conn: &Connection) -> Result<Value> {
        let metadata_prefix = ducklake_metadata_prefix(&self.catalog_name);
        let mut tables = serde_json::Map::new();
        let sql = format!(
            "\
            SELECT t.table_id, t.table_name, count(f.data_file_id), coalesce(sum(f.record_count), 0) \
            FROM {metadata_prefix}ducklake_table t \
            LEFT JOIN {metadata_prefix}ducklake_data_file f \
              ON f.table_id = t.table_id AND f.end_snapshot IS NULL \
            WHERE t.end_snapshot IS NULL \
            GROUP BY t.table_id, t.table_name"
        );
        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map([], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, i64>(2)?,
                row.get::<_, i64>(3)?,
            ))
        })?;
        for row in rows {
            let (table_id, table_name, parquet_files, parquet_rows) = row?;
            tables.insert(
                table_name,
                json!({
                    "table_id": table_id,
                    "parquet_files": parquet_files,
                    "parquet_rows": parquet_rows,
                    "inlined_rows": 0
                }),
            );
        }

        let sql = format!(
            "SELECT table_id, table_name FROM {metadata_prefix}ducklake_inlined_data_tables ORDER BY table_id"
        );
        let mut stmt = conn.prepare(&sql)?;
        let inlined = stmt.query_map([], |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
        })?;
        for row in inlined {
            let (table_id, inlined_table) = row?;
            let count_sql = format!(
                "SELECT count(*) FROM {metadata_prefix}{} WHERE end_snapshot IS NULL",
                quote_ident(&inlined_table)
            );
            let inlined_rows: i64 = conn.query_row(&count_sql, [], |row| row.get(0))?;
            for value in tables.values_mut() {
                if value.get("table_id").and_then(Value::as_i64) == Some(table_id) {
                    value["inlined_rows"] = json!(inlined_rows);
                    break;
                }
            }
        }
        Ok(Value::Object(tables))
    }

    /// Read-side connection access for SELECT-only paths. Do not call into
    /// write paths from inside the closure.
    pub fn with_conn<T>(&self, f: impl FnOnce(&Connection, &str) -> Result<T>) -> Result<T> {
        let conn = self.reader.lock_or_poisoned();
        f(&conn, &self.target_prefix)
    }

    pub fn with_query_conn<T>(
        &self,
        memory_limit: &str,
        timeout: Duration,
        f: impl FnOnce(&Connection, &str) -> Result<T>,
    ) -> Result<T> {
        let conn = self.open_scoped_query_connection()?;
        conn.execute_batch(&format!(
            "SET memory_limit = '{}';",
            escape_value(memory_limit)
        ))?;

        let interrupt = conn.interrupt_handle();
        let state = Arc::new((Mutex::new(false), Condvar::new()));
        let timer_state = state.clone();
        let timer = thread::spawn(move || {
            let (done, cvar) = &*timer_state;
            let done = done.lock_or_poisoned();
            let (done, wait) = cvar.wait_timeout(done, timeout).unwrap();
            if !*done && wait.timed_out() {
                interrupt.interrupt();
                return true;
            }
            false
        });

        let result = f(&conn, &self.target_prefix);
        let (done, cvar) = &*state;
        *done.lock_or_poisoned() = true;
        cvar.notify_one();
        // Timer panic ⇒ "not timed out": trust the query result over a panicked timer.
        let timed_out = match timer.join() {
            Ok(fired) => fired,
            Err(_) => {
                crate::log_event("warn", "query_timer_panicked", &[]);
                false
            }
        };
        // Timer fired ⇒ surface a typed timeout even if the query raced to Ok.
        if timed_out {
            return Err(anyhow::Error::new(QueryTimeoutError { timeout }));
        }
        result
    }

    fn open_scoped_query_connection(&self) -> Result<Connection> {
        // Clone from `reader`, not `writer`: queries don't block on flushes.
        // Attached schemas + extensions are inherited; only per-conn PRAGMAs
        // are reapplied here.
        let cloned = {
            let parent = self.reader.lock_or_poisoned();
            parent.try_clone().context("clone DuckDB connection")?
        };
        configure_base_connection(&cloned)?;
        Ok(cloned)
    }

    pub fn flush_inlined_data(&self, table: Option<&str>) -> Result<Value> {
        if !self.ducklake_managed_maintenance {
            return Ok(
                json!({"supported": false, "reason": "ducklake maintenance is not managed by this process"}),
            );
        }
        let conn = self.writer.lock_or_poisoned();
        let sql = match table {
            Some(t) => format!(
                "SELECT * FROM ducklake_flush_inlined_data('{}', table_name => '{}')",
                self.catalog_name,
                escape_value(t)
            ),
            None => format!(
                "SELECT * FROM ducklake_flush_inlined_data('{}')",
                self.catalog_name
            ),
        };
        conn.execute_batch(&sql)?;
        Ok(json!({"supported": true, "status": "ok"}))
    }

    pub fn compaction_decision(&self, table: Option<&str>, _min_files: usize) -> Result<Value> {
        if !self.ducklake_managed_maintenance {
            return Ok(
                json!({"supported": false, "reason": "ducklake maintenance is not managed by this process"}),
            );
        }
        Ok(json!({
            "supported": true,
            "status": "disabled",
            "should_compact": false,
            "table": table,
            "reason": "immutable_segments"
        }))
    }

    pub fn merge_adjacent_files(&self, table: Option<&str>) -> Result<Value> {
        if !self.ducklake_managed_maintenance {
            return Ok(
                json!({"supported": false, "reason": "ducklake maintenance is not managed by this process"}),
            );
        }
        Ok(json!({
            "supported": true,
            "status": "disabled",
            "table": table,
            "reason": "immutable_segments"
        }))
    }

    pub fn cleanup_old_files(&self, dry_run: bool) -> Result<Value> {
        if !self.ducklake_managed_maintenance {
            return Ok(
                json!({"supported": false, "reason": "ducklake maintenance is not managed by this process"}),
            );
        }
        self.writer.lock_or_poisoned().execute_batch(&format!(
            "SELECT * FROM ducklake_cleanup_old_files('{}', dry_run => {})",
            self.catalog_name, dry_run
        ))?;
        Ok(json!({"supported": true, "status": "ok", "dry_run": dry_run}))
    }

    pub fn expire_snapshots(&self, older_than_days: i64) -> Result<Value> {
        if !self.ducklake_managed_maintenance {
            return Ok(
                json!({"supported": false, "reason": "ducklake maintenance is not managed by this process"}),
            );
        }
        let older_than = (Utc::now() - chrono::Duration::days(older_than_days)).to_rfc3339();
        self.writer.lock_or_poisoned().execute_batch(&format!(
            "SELECT * FROM ducklake_expire_snapshots('{}', older_than => TIMESTAMPTZ '{}')",
            self.catalog_name,
            older_than.replace('\'', "''")
        ))?;
        Ok(json!({"supported": true, "status": "ok", "older_than": older_than}))
    }

    pub fn enforce_retention(&self, policy: &RetentionPolicy, dry_run: bool) -> Result<Value> {
        let conn = self.writer.lock_or_poisoned();
        let mut results = Vec::new();
        let mut metadata_deleted_total = 0_i64;
        for target in [
            ("logs", policy.logs_days, "event_date"),
            ("spans", policy.spans_days, "event_date"),
            ("metric_gauge", policy.metrics_days, "event_date"),
            ("metric_sum", policy.metrics_days, "event_date"),
        ] {
            let (table, retention_days, date_column) = target;
            let cutoff = (Utc::now() - chrono::Duration::days(retention_days))
                .format("%Y-%m-%d")
                .to_string();
            let full_table = format!("{}{}", self.target_prefix, table);
            let predicate = if date_column == "event_date" {
                format!("{date_column} < DATE {}", sql_quote(&cutoff))
            } else {
                format!(
                    "{date_column} < TIMESTAMP {}",
                    sql_quote(&format!("{cutoff} 00:00:00"))
                )
            };
            let count_sql = format!("SELECT count(*) FROM {full_table} WHERE {predicate}");
            let matching_rows: i64 = conn.query_row(&count_sql, [], |row| row.get(0))?;
            let deleted_rows = if dry_run || matching_rows == 0 {
                0
            } else {
                let delete_sql = format!("DELETE FROM {full_table} WHERE {predicate}");
                conn.execute(&delete_sql, [])? as i64
            };
            let metadata_predicate = format!(
                "signal = {} AND event_date < DATE {}",
                sql_quote(table),
                sql_quote(&cutoff)
            );
            let metadata_count_sql = format!(
                "SELECT count(*) FROM {prefix}metadata_summary WHERE {metadata_predicate}",
                prefix = self.target_prefix
            );
            let matching_metadata_rows: i64 =
                conn.query_row(&metadata_count_sql, [], |row| row.get(0))?;
            let deleted_metadata_rows = if dry_run || matching_metadata_rows == 0 {
                0
            } else {
                let delete_sql = format!(
                    "DELETE FROM {prefix}metadata_summary WHERE {metadata_predicate}",
                    prefix = self.target_prefix
                );
                conn.execute(&delete_sql, [])? as i64
            };
            metadata_deleted_total += deleted_metadata_rows;
            results.push(json!({
                "table": table,
                "retention_days": retention_days,
                "cutoff_date": cutoff,
                "matching_rows": matching_rows,
                "deleted_rows": deleted_rows,
                "matching_metadata_rows": matching_metadata_rows,
                "deleted_metadata_rows": deleted_metadata_rows
            }));
        }
        if metadata_deleted_total > 0 {
            self.metadata_generation.fetch_add(1, Ordering::SeqCst);
        }
        Ok(json!({"dry_run": dry_run, "tables": results}))
    }

    pub fn freshness_watermarks(&self) -> Result<Value> {
        self.with_conn(|conn, prefix| {
            let mut map = serde_json::Map::new();
            for table in ["logs", "spans", "metric_gauge", "metric_sum"] {
                let sql = format!(
                    "SELECT max(timestamp)::VARCHAR, epoch(max(timestamp)), max(ingested_at)::VARCHAR, epoch(max(ingested_at)) FROM {prefix}{table}"
                );
                let (
                    event_watermark,
                    event_watermark_epoch,
                    ingest_watermark,
                    ingest_watermark_epoch,
                ): (Option<String>, Option<f64>, Option<String>, Option<f64>) =
                    conn.query_row(&sql, [], |row| {
                        Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
                    })?;
                let ingest_lag_seconds = ingest_watermark_epoch
                    .map(|epoch| Utc::now().timestamp_millis() as f64 / 1000.0 - epoch);
                let event_lag_seconds = event_watermark_epoch
                    .map(|epoch| Utc::now().timestamp_millis() as f64 / 1000.0 - epoch);
                map.insert(
                    table.to_string(),
                    json!({
                        "timestamp": event_watermark,
                        "epoch_seconds": event_watermark_epoch,
                        "event_lag_seconds": event_lag_seconds,
                        "ingested_at": ingest_watermark,
                        "ingested_at_epoch_seconds": ingest_watermark_epoch,
                        "lag_seconds": ingest_lag_seconds
                    }),
                );
            }
            Ok(Value::Object(map))
        })
    }

    pub fn logical_rows(&self) -> Result<Value> {
        self.with_conn(|conn, prefix| {
            let mut map = serde_json::Map::new();
            for table in ["logs", "spans", "metric_gauge", "metric_sum"] {
                let sql = format!("SELECT count(*) FROM {prefix}{table}");
                let rows: i64 = conn.query_row(&sql, [], |row| row.get(0))?;
                map.insert(table.to_string(), json!(rows));
            }
            Ok(Value::Object(map))
        })
    }

    fn check_health_target(&self) -> Result<()> {
        // Reader, not writer — a stuck flush must not hang /healthz.
        let conn = self.reader.lock_or_poisoned();
        conn.query_row("SELECT 1", [], |_| Ok(()))?;
        let sql = format!("SELECT * FROM {}logs LIMIT 0", self.target_prefix);
        let _stmt = conn.prepare(&sql)?;
        Ok(())
    }
}

fn create_tables_on(conn: &Connection, prefix: &str) -> Result<()> {
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
    Ok(())
}

pub fn install_ducklake_extension(extension_dir: Option<&Path>) -> Result<()> {
    let conn = Connection::open_in_memory()?;
    configure_extension_directory(&conn, extension_dir)?;
    conn.execute_batch("INSTALL ducklake; LOAD ducklake;")?;
    Ok(())
}

fn promoted_from_attr_json(raw: &str, attr_key: &str) -> Option<String> {
    let parsed: Value = serde_json::from_str(raw).ok()?;
    parsed.get(attr_key).map(|v| {
        if let Some(s) = v.as_str() {
            s.to_string()
        } else {
            v.to_string()
        }
    })
}

fn promoted_int_from_attr_json(raw: &str, attr_key: &str) -> Option<i32> {
    let parsed: Value = serde_json::from_str(raw).ok()?;
    parsed
        .get(attr_key)
        .and_then(|v| v.as_i64().or_else(|| v.as_str()?.parse().ok()))
        .and_then(|v| i32::try_from(v).ok())
}

fn sql_path(path: &Path) -> String {
    path.to_string_lossy().replace('\'', "''")
}

fn attach_ducklake_connection(
    conn: &Connection,
    postgres_dsn: Option<&str>,
    attach_uri: Option<&str>,
    duckdb_path: &Path,
    local_storage_dir: &Path,
    extension_dir: Option<&Path>,
    data_inlining_row_limit: usize,
) -> Result<()> {
    configure_extension_directory(conn, extension_dir)?;
    let plan = build_ducklake_attach_plan(
        postgres_dsn,
        attach_uri,
        duckdb_path,
        local_storage_dir,
        data_inlining_row_limit,
    )?;

    if plan.needs_motherduck && conn.execute_batch("LOAD md;").is_err() {
        conn.execute_batch("INSTALL md; LOAD md;")?;
    }
    if plan.needs_ducklake && conn.execute_batch("LOAD ducklake;").is_err() {
        conn.execute_batch("INSTALL ducklake; LOAD ducklake;")?;
    }
    if plan.needs_postgres {
        conn.execute_batch("INSTALL postgres; LOAD postgres;")?;
    }
    conn.execute_batch(&plan.sql)?;
    Ok(())
}

#[derive(Clone, Debug)]
struct DuckLakeAttachPlan {
    sql: String,
    mode: &'static str,
    needs_ducklake: bool,
    needs_motherduck: bool,
    needs_postgres: bool,
    managed_maintenance: bool,
}

fn ducklake_attach_plan(config: &Config) -> Result<DuckLakeAttachPlan> {
    build_ducklake_attach_plan(
        config.postgres_dsn.as_deref(),
        config.ducklake_attach_uri.as_deref(),
        &config.duckdb_path,
        &config.local_storage_dir,
        config.ducklake_data_inlining_row_limit,
    )
}

fn build_ducklake_attach_plan(
    postgres_dsn: Option<&str>,
    attach_uri: Option<&str>,
    duckdb_path: &Path,
    local_storage_dir: &Path,
    data_inlining_row_limit: usize,
) -> Result<DuckLakeAttachPlan> {
    if postgres_dsn.is_some() && attach_uri.is_some() {
        anyhow::bail!(
            "set only one of CANARDSTACK_POSTGRES_DSN or CANARDSTACK_DUCKLAKE_ATTACH_URI"
        );
    }

    if let Some(uri) = attach_uri {
        let uri = uri.trim();
        if uri.is_empty() {
            anyhow::bail!("CANARDSTACK_DUCKLAKE_ATTACH_URI must not be empty");
        }
        if uri.to_ascii_uppercase().starts_with("ATTACH ") {
            anyhow::bail!(
                "CANARDSTACK_DUCKLAKE_ATTACH_URI must be the URI only, not an ATTACH statement"
            );
        }
        let is_motherduck = uri.starts_with("md:");
        let is_ducklake = uri.starts_with("ducklake:");
        if !is_motherduck && !is_ducklake {
            anyhow::bail!(
                "CANARDSTACK_DUCKLAKE_ATTACH_URI must be an md: or ducklake: URI because immutable ingest registers data files with DuckLake"
            );
        }
        let attach_options = if is_ducklake {
            format!(" (DATA_INLINING_ROW_LIMIT {data_inlining_row_limit})")
        } else {
            String::new()
        };
        return Ok(DuckLakeAttachPlan {
            sql: format!(
                "ATTACH '{}' AS canardlake{}; USE canardlake;",
                uri.replace('\'', "''"),
                attach_options
            ),
            mode: if is_motherduck {
                "ducklake_motherduck_remote"
            } else {
                "ducklake_custom_uri"
            },
            needs_ducklake: is_ducklake,
            needs_motherduck: is_motherduck,
            needs_postgres: false,
            managed_maintenance: is_ducklake,
        });
    }

    let data_path = sql_path(local_storage_dir);
    if let Some(dsn) = postgres_dsn {
        return Ok(DuckLakeAttachPlan {
            sql: format!(
                "ATTACH 'ducklake:postgres:{}' AS canardlake (DATA_PATH '{}', DATA_INLINING_ROW_LIMIT {}); USE canardlake;",
                dsn.replace('\'', "''"),
                data_path,
                data_inlining_row_limit
            ),
            mode: "ducklake_postgres_catalog",
            needs_ducklake: true,
            needs_motherduck: false,
            needs_postgres: true,
            managed_maintenance: true,
        });
    }

    let metadata = duckdb_path
        .parent()
        .unwrap_or_else(|| std::path::Path::new("."))
        .join("canardstack.ducklake");
    Ok(DuckLakeAttachPlan {
        sql: format!(
            "ATTACH 'ducklake:{}' AS canardlake (DATA_PATH '{}', DATA_INLINING_ROW_LIMIT {}); USE canardlake;",
            sql_path(&metadata),
            data_path,
            data_inlining_row_limit
        ),
        mode: "ducklake_duckdb_catalog",
        needs_ducklake: true,
        needs_motherduck: false,
        needs_postgres: false,
        managed_maintenance: true,
    })
}

fn configure_extension_directory(conn: &Connection, extension_dir: Option<&Path>) -> Result<()> {
    if let Some(path) = extension_dir {
        fs::create_dir_all(path)?;
        conn.execute_batch(&format!("SET extension_directory = '{}';", sql_path(path)))?;
    }
    Ok(())
}

fn configure_base_connection(conn: &Connection) -> Result<()> {
    conn.execute_batch(&format!(
        "SET preserve_insertion_order=false;\nPRAGMA threads={DUCKDB_THREADS};"
    ))?;
    Ok(())
}

fn configure_write_connection(conn: &Connection, memory_limit: &str) -> Result<()> {
    conn.execute_batch(&format!(
        "SET memory_limit = '{}';",
        escape_value(memory_limit)
    ))?;
    Ok(())
}

fn merge_dirty_metadata(
    dst: &mut BTreeMap<Signal, BTreeSet<String>>,
    src: BTreeMap<Signal, BTreeSet<String>>,
) {
    for (signal, dates) in src {
        dst.entry(signal).or_default().extend(dates);
    }
}

fn refresh_metadata_summaries_on(
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

fn metadata_refresh_sql(prefix: &str, signal: Signal, date: &str) -> Result<String> {
    let selects = match signal {
        Signal::Logs => logs_metadata_sql(prefix, date),
        Signal::Spans => spans_metadata_sql(prefix, date),
        Signal::MetricGauge => metric_metadata_sql(prefix, Signal::MetricGauge, date, "gauge"),
        Signal::MetricSum => metric_metadata_sql(prefix, Signal::MetricSum, date, "counter"),
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

fn logs_metadata_sql(prefix: &str, date: &str) -> Vec<String> {
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
            date,
            "label_value",
            name,
            column,
        ));
    }
    sql.push(format!(
        "\
        SELECT 'logs', event_date, 'series', 'stream', NULL, NULL, NULL, NULL, \
               service_name, deployment_environment, severity_text, \
               count(*), min(timestamp), max(timestamp) \
        FROM {prefix}logs \
        WHERE event_date = DATE {} \
        GROUP BY event_date, service_name, deployment_environment, severity_text",
        sql_quote(date)
    ));
    sql
}

fn spans_metadata_sql(prefix: &str, date: &str) -> Vec<String> {
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
            date,
            "tag_value",
            name,
            column,
        ));
    }
    sql
}

fn metric_metadata_sql(prefix: &str, signal: Signal, date: &str, metric_type: &str) -> Vec<String> {
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
            date,
            "label_value",
            name,
            column,
        ));
    }
    sql.push(format!(
        "\
        SELECT {}, event_date, 'series', metric_name, NULL, {}, NULL, NULL, \
               service_name, deployment_environment, NULL, \
               count(*), min(timestamp), max(timestamp) \
        FROM {prefix}{table} \
        WHERE event_date = DATE {} AND metric_name IS NOT NULL AND metric_name <> '' \
        GROUP BY event_date, metric_name, service_name, deployment_environment",
        sql_quote(signal),
        sql_quote(metric_type),
        sql_quote(date)
    ));
    sql.push(format!(
        "\
        SELECT {}, event_date, 'metric_metadata', metric_name, NULL, {}, \
               max(coalesce(metric_unit, '')), max(coalesce(metric_description, '')), \
               NULL, NULL, NULL, count(*), min(timestamp), max(timestamp) \
        FROM {prefix}{table} \
        WHERE event_date = DATE {} AND metric_name IS NOT NULL AND metric_name <> '' \
        GROUP BY event_date, metric_name",
        sql_quote(signal),
        sql_quote(metric_type),
        sql_quote(date)
    ));
    sql
}

fn label_value_insert_sql(
    prefix: &str,
    signal: &str,
    table: &str,
    date: &str,
    kind: &str,
    name: &str,
    column: &str,
) -> String {
    format!(
        "\
        SELECT {}, event_date, {}, {}, {column}::VARCHAR, NULL, NULL, NULL, \
               NULL, NULL, NULL, count(*), min(timestamp), max(timestamp) \
        FROM {prefix}{table} \
        WHERE event_date = DATE {} AND {column} IS NOT NULL AND {column}::VARCHAR <> '' \
        GROUP BY event_date, {column}",
        sql_quote(signal),
        sql_quote(kind),
        sql_quote(name),
        sql_quote(date)
    )
}

fn metadata_summary_columns() -> &'static str {
    "signal, event_date, kind, name, value, metric_type, metric_unit, \
     metric_description, service_name, deployment_environment, severity_text, \
     row_count, first_seen, last_seen"
}

fn distribute_commit_seconds(timings: &mut [ArrowBatchInsertTiming], commit_seconds: f64) {
    if timings.is_empty() || commit_seconds <= 0.0 {
        return;
    }
    let insert_timings = timings
        .iter_mut()
        .filter(|timing| timing.phase == "storage_insert")
        .collect::<Vec<_>>();
    if insert_timings.is_empty() {
        return;
    }
    let total_rows: usize = insert_timings.iter().map(|timing| timing.rows).sum();
    if total_rows == 0 {
        let each = commit_seconds / insert_timings.len() as f64;
        for timing in insert_timings {
            timing.seconds += each;
        }
        return;
    }
    for timing in insert_timings {
        timing.seconds += commit_seconds * timing.rows as f64 / total_rows as f64;
    }
}

fn quote_ident(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}

fn ducklake_metadata_prefix(catalog_name: &str) -> String {
    format!(
        "{}.",
        quote_ident(&format!("__ducklake_metadata_{catalog_name}"))
    )
}

struct SealedSegment {
    table: Signal,
    path: PathBuf,
    rows: usize,
}

fn write_immutable_segment(
    storage_dir: &Path,
    table: Signal,
    partition: ImmutableSegmentPartition,
    batch: &RecordBatch,
) -> Result<SealedSegment> {
    let final_path = immutable_segment_path(storage_dir, table, partition)?;
    let parent = final_path
        .parent()
        .context("immutable segment path has no parent")?;
    fs::create_dir_all(parent)
        .with_context(|| format!("create immutable segment directory {}", parent.display()))?;

    let tmp_path = final_path.with_extension("parquet.tmp");
    let mut file = BufWriter::new(
        File::create(&tmp_path)
            .with_context(|| format!("create immutable segment {}", tmp_path.display()))?,
    );
    write_parquet(batch, &mut file, None).context("encode immutable segment parquet")?;
    let file = file
        .into_inner()
        .context("flush immutable segment writer before seal")?;
    file.sync_all()
        .with_context(|| format!("fsync immutable segment {}", tmp_path.display()))?;
    drop(file);
    fs::rename(&tmp_path, &final_path).with_context(|| {
        format!(
            "seal immutable segment {} -> {}",
            tmp_path.display(),
            final_path.display()
        )
    })?;

    Ok(SealedSegment {
        table,
        path: final_path,
        rows: batch.num_rows(),
    })
}

fn immutable_segment_path(
    storage_dir: &Path,
    table: Signal,
    partition: ImmutableSegmentPartition,
) -> Result<PathBuf> {
    let sequence = IMMUTABLE_SEGMENT_COUNTER.fetch_add(1, Ordering::SeqCst);
    let suffix = format!("{}-{sequence}.parquet", Utc::now().timestamp_micros());
    Ok(storage_dir
        .join("main")
        .join(table.as_str())
        .join(format!("event_date={}", partition.event_date))
        .join(format!("hour={:02}", partition.hour))
        .join(suffix))
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct ImmutableSegmentPartition {
    event_date: String,
    hour: u32,
}

fn split_batch_by_immutable_partition(
    batch: &RecordBatch,
) -> Result<Vec<(ImmutableSegmentPartition, RecordBatch)>> {
    if batch.num_rows() == 0 {
        return Ok(Vec::new());
    }

    let mut rows_by_partition: BTreeMap<ImmutableSegmentPartition, Vec<u32>> = BTreeMap::new();
    let dates = event_date_column(batch)?;
    let timestamps = timestamp_column(batch)?;
    for row in 0..batch.num_rows() {
        rows_by_partition
            .entry(immutable_row_partition(dates, timestamps, row))
            .or_default()
            .push(row as u32);
    }

    if rows_by_partition.len() == 1 {
        let partition = rows_by_partition
            .into_keys()
            .next()
            .expect("partition exists for non-empty batch");
        return Ok(vec![(partition, batch.clone())]);
    }

    let schema = batch.schema();
    rows_by_partition
        .into_iter()
        .map(|(partition, rows)| {
            let indices = arrow58_array::UInt32Array::from(rows);
            let columns = batch
                .columns()
                .iter()
                .map(|column| take(column.as_ref(), &indices, None))
                .collect::<arrow58::error::Result<Vec<_>>>()?;
            let batch = RecordBatch::try_new(schema.clone(), columns)
                .context("build immutable partition RecordBatch")?;
            Ok((partition, batch))
        })
        .collect()
}

fn immutable_row_partition(
    dates: &arrow58_array::Date32Array,
    timestamps: &arrow58_array::TimestampMicrosecondArray,
    row: usize,
) -> ImmutableSegmentPartition {
    let now = Utc::now();
    ImmutableSegmentPartition {
        event_date: date32_value(dates, row).unwrap_or_else(|| now.date_naive().to_string()),
        hour: timestamp_hour(timestamps, row).unwrap_or_else(|| now.hour()),
    }
}

fn date32_value(dates: &arrow58_array::Date32Array, row: usize) -> Option<String> {
    if dates.is_null(row) {
        return None;
    }
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).expect("valid Unix epoch date");
    Some((epoch + chrono::Duration::days(dates.value(row) as i64)).to_string())
}

fn timestamp_hour(
    timestamps: &arrow58_array::TimestampMicrosecondArray,
    row: usize,
) -> Option<u32> {
    if timestamps.is_null(row) {
        return None;
    }
    let micros = timestamps.value(row);
    let secs = micros.div_euclid(1_000_000);
    let nanos = micros.rem_euclid(1_000_000) as u32 * 1_000;
    DateTime::<Utc>::from_timestamp(secs, nanos).map(|timestamp| timestamp.hour())
}

fn register_ducklake_data_file(
    conn: &Connection,
    catalog_name: &str,
    table: Signal,
    path: &Path,
) -> Result<()> {
    let sql = format!(
        "CALL ducklake_add_data_files({}, {}, {}, schema = 'main')",
        sql_quote(catalog_name),
        sql_quote(table.as_str()),
        sql_quote(&path.to_string_lossy())
    );
    conn.execute_batch(&sql)
        .with_context(|| format!("register immutable segment {}", path.display()))?;
    Ok(())
}

fn immutable_buffer_snapshot(buffers: &BTreeMap<Signal, ImmutableSegmentBuffer>) -> Value {
    let mut map = serde_json::Map::new();
    for (table, buffer) in buffers {
        map.insert(
            table.as_str().to_string(),
            json!({
                "rows": buffer.rows,
                "bytes": buffer.bytes,
                "age_seconds": buffer.opened_at.elapsed().as_secs_f64(),
            }),
        );
    }
    Value::Object(map)
}

fn immutable_timing_snapshot(timings: &[ArrowBatchInsertTiming]) -> Value {
    Value::Array(
        timings
            .iter()
            .map(|timing| {
                json!({
                    "table": timing.table.as_str(),
                    "phase": timing.phase,
                    "rows": timing.rows,
                    "seconds": timing.seconds,
                })
            })
            .collect(),
    )
}

fn storage_duckdb_batch(
    table: Signal,
    batch: &RecordBatch,
    source_format: &str,
) -> Result<RecordBatch> {
    let timestamp = timestamp_column(batch)?;
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
            "event_date" => (
                arrow58_types::Field::new(name, arrow58_types::DataType::Date32, true),
                Arc::new(arrow58_array::Date32Array::from(
                    (0..rows)
                        .map(|row| {
                            (!timestamp.is_null(row))
                                .then(|| timestamp.value(row).div_euclid(86_400_000_000) as i32)
                        })
                        .collect::<Vec<_>>(),
                )) as ArrayRef,
            ),
            "source_format" => (
                arrow58_types::Field::new(name, arrow58_types::DataType::Utf8, true),
                string_array_from_options((0..rows).map(|_| Some(source_format.to_string()))),
            ),
            "deployment_environment" if matches!(table, Signal::Logs | Signal::Spans) => (
                arrow58_types::Field::new(name, arrow58_types::DataType::Utf8, true),
                string_promoted_column(batch, "resource_attributes", "deployment.environment")?,
            ),
            "http_method" if table == Signal::Logs => (
                arrow58_types::Field::new(name, arrow58_types::DataType::Utf8, true),
                string_promoted_alt_column(
                    batch,
                    "log_attributes",
                    &["http.request.method", "http.method"],
                )?,
            ),
            "http_method" if table == Signal::Spans => (
                arrow58_types::Field::new(name, arrow58_types::DataType::Utf8, true),
                string_promoted_alt_column(
                    batch,
                    "span_attributes",
                    &["http.request.method", "http.method"],
                )?,
            ),
            "http_status_code" if table == Signal::Logs => (
                arrow58_types::Field::new(name, arrow58_types::DataType::Int32, true),
                int_promoted_alt_column(
                    batch,
                    "log_attributes",
                    &["http.response.status_code", "http.status_code"],
                )?,
            ),
            "http_status_code" if table == Signal::Spans => (
                arrow58_types::Field::new(name, arrow58_types::DataType::Int32, true),
                int_promoted_alt_column(
                    batch,
                    "span_attributes",
                    &["http.response.status_code", "http.status_code"],
                )?,
            ),
            "http_route" if table == Signal::Logs => (
                arrow58_types::Field::new(name, arrow58_types::DataType::Utf8, true),
                string_promoted_column(batch, "log_attributes", "http.route")?,
            ),
            "http_route" if table == Signal::Spans => (
                arrow58_types::Field::new(name, arrow58_types::DataType::Utf8, true),
                string_promoted_column(batch, "span_attributes", "http.route")?,
            ),
            "exception_type" if table == Signal::Logs => (
                arrow58_types::Field::new(name, arrow58_types::DataType::Utf8, true),
                string_promoted_column(batch, "log_attributes", "exception.type")?,
            ),
            "exception_type" if table == Signal::Spans => (
                arrow58_types::Field::new(name, arrow58_types::DataType::Utf8, true),
                string_promoted_column(batch, "span_attributes", "exception.type")?,
            ),
            "deployment_environment" => (
                arrow58_types::Field::new(name, arrow58_types::DataType::Utf8, true),
                string_promoted_column(batch, "resource_attributes", "deployment.environment")?,
            ),
            _ => copy_arrow_column(batch, name)?,
        };
        fields.push(field);
        arrays.push(array);
    }

    RecordBatch::try_new(Arc::new(arrow58_types::Schema::new(fields)), arrays)
        .context("build storage RecordBatch")
}

fn batch_event_dates(batch: &RecordBatch) -> Result<Vec<String>> {
    let dates = event_date_column(batch)?;
    let mut out = BTreeSet::new();
    for row in 0..dates.len() {
        if let Some(date) = date32_value(dates, row) {
            out.insert(date);
        }
    }
    Ok(out.into_iter().collect())
}

fn event_date_column(batch: &RecordBatch) -> Result<&arrow58_array::Date32Array> {
    let idx = batch.schema().index_of("event_date")?;
    batch
        .column(idx)
        .as_any()
        .downcast_ref::<arrow58_array::Date32Array>()
        .context("event_date column is not Date32Array")
}

fn timestamp_column(batch: &RecordBatch) -> Result<&arrow58_array::TimestampMicrosecondArray> {
    let idx = batch.schema().index_of("timestamp")?;
    batch
        .column(idx)
        .as_any()
        .downcast_ref::<arrow58_array::TimestampMicrosecondArray>()
        .context("timestamp column is not TimestampMicrosecondArray")
}

fn copy_arrow_column(batch: &RecordBatch, name: &str) -> Result<(arrow58_types::Field, ArrayRef)> {
    let schema = batch.schema();
    let idx = schema.index_of(name)?;
    let field58 = schema.field(idx);
    let field = arrow58_types::Field::new(name, field58.data_type().clone(), field58.is_nullable());
    Ok((field, batch.column(idx).clone()))
}

fn string_promoted_column(
    batch: &RecordBatch,
    attr_column: &str,
    attr_key: &str,
) -> Result<ArrayRef> {
    string_promoted_alt_column(batch, attr_column, &[attr_key])
}

fn string_promoted_alt_column(
    batch: &RecordBatch,
    attr_column: &str,
    attr_keys: &[&str],
) -> Result<ArrayRef> {
    let schema = batch.schema();
    let idx = schema.index_of(attr_column)?;
    let src = batch
        .column(idx)
        .as_any()
        .downcast_ref::<arrow58_array::StringArray>()
        .with_context(|| format!("{attr_column} column is not StringArray"))?;
    Ok(string_array_from_options((0..src.len()).map(|row| {
        if src.is_null(row) {
            None
        } else {
            attr_keys
                .iter()
                .find_map(|key| promoted_from_attr_json(src.value(row), key))
        }
    })))
}

fn int_promoted_alt_column(
    batch: &RecordBatch,
    attr_column: &str,
    attr_keys: &[&str],
) -> Result<ArrayRef> {
    let schema = batch.schema();
    let idx = schema.index_of(attr_column)?;
    let src = batch
        .column(idx)
        .as_any()
        .downcast_ref::<arrow58_array::StringArray>()
        .with_context(|| format!("{attr_column} column is not StringArray"))?;
    Ok(Arc::new(arrow58_array::Int32Array::from(
        (0..src.len())
            .map(|row| {
                if src.is_null(row) {
                    None
                } else {
                    attr_keys
                        .iter()
                        .find_map(|key| promoted_int_from_attr_json(src.value(row), key))
                }
            })
            .collect::<Vec<_>>(),
    )) as ArrayRef)
}

fn string_array_from_options(values: impl IntoIterator<Item = Option<String>>) -> ArrayRef {
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

fn dir_size(path: &Path) -> Result<u64> {
    if !path.exists() {
        return Ok(0);
    }
    let mut total = 0;
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        let metadata = entry.metadata()?;
        if metadata.is_dir() {
            total += dir_size(&entry.path())?;
        } else {
            total += metadata.len();
        }
    }
    Ok(total)
}

const LOGS_COLUMNS: &[(&str, &str)] = &[
    ("timestamp", "TIMESTAMP"),
    ("ingested_at", "TIMESTAMP"),
    ("event_date", "DATE"),
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
    ("deployment_environment", "VARCHAR"),
    ("http_method", "VARCHAR"),
    ("http_status_code", "INTEGER"),
    ("http_route", "VARCHAR"),
    ("exception_type", "VARCHAR"),
];

const SPANS_COLUMNS: &[(&str, &str)] = &[
    ("timestamp", "TIMESTAMP"),
    ("ingested_at", "TIMESTAMP"),
    ("event_date", "DATE"),
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
    ("deployment_environment", "VARCHAR"),
    ("http_method", "VARCHAR"),
    ("http_status_code", "INTEGER"),
    ("http_route", "VARCHAR"),
    ("exception_type", "VARCHAR"),
];

const METRIC_GAUGE_COLUMNS: &[(&str, &str)] = &[
    ("timestamp", "TIMESTAMP"),
    ("ingested_at", "TIMESTAMP"),
    ("event_date", "DATE"),
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
    ("deployment_environment", "VARCHAR"),
];

const METRIC_SUM_COLUMNS: &[(&str, &str)] = &[
    ("timestamp", "TIMESTAMP"),
    ("ingested_at", "TIMESTAMP"),
    ("event_date", "DATE"),
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
    ("deployment_environment", "VARCHAR"),
    ("aggregation_temporality", "INTEGER"),
    ("is_monotonic", "BOOLEAN"),
];

const METADATA_SUMMARY_COLUMNS: &[(&str, &str)] = &[
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

fn table_columns(table: Signal) -> &'static [(&'static str, &'static str)] {
    match table {
        Signal::Logs => LOGS_COLUMNS,
        Signal::Spans => SPANS_COLUMNS,
        Signal::MetricGauge => METRIC_GAUGE_COLUMNS,
        Signal::MetricSum => METRIC_SUM_COLUMNS,
    }
}

fn create_table_sql(prefix: &str, name: &str, cols: &[(&str, &str)]) -> String {
    let body = cols
        .iter()
        .map(|(col, ty)| format!("  {col} {ty}"))
        .collect::<Vec<_>>()
        .join(",\n");
    format!("CREATE TABLE IF NOT EXISTS {prefix}{name} (\n{body}\n);")
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn custom_ducklake_attach_uri_uses_ducklake_extension_and_canardlake_alias() {
        let dir = tempdir().unwrap();
        let plan = build_ducklake_attach_plan(
            None,
            Some("ducklake:md:test-ducklake"),
            &dir.path().join("canardstack.duckdb"),
            &dir.path().join("storage"),
            1_000,
        )
        .unwrap();

        assert_eq!(
            plan.sql,
            "ATTACH 'ducklake:md:test-ducklake' AS canardlake (DATA_INLINING_ROW_LIMIT 1000); USE canardlake;"
        );
        assert_eq!(plan.mode, "ducklake_custom_uri");
        assert!(plan.needs_ducklake);
        assert!(!plan.needs_postgres);
        assert!(plan.managed_maintenance);
    }

    #[test]
    fn motherduck_attach_uri_uses_md_extension_and_canardlake_alias() {
        let dir = tempdir().unwrap();
        let plan = build_ducklake_attach_plan(
            None,
            Some("md:test-ducklake"),
            &dir.path().join("canardstack.duckdb"),
            &dir.path().join("storage"),
            1_000,
        )
        .unwrap();

        assert_eq!(
            plan.sql,
            "ATTACH 'md:test-ducklake' AS canardlake; USE canardlake;"
        );
        assert_eq!(plan.mode, "ducklake_motherduck_remote");
        assert!(!plan.needs_ducklake);
        assert!(plan.needs_motherduck);
        assert!(!plan.needs_postgres);
        assert!(!plan.managed_maintenance);
    }

    #[test]
    fn custom_attach_uri_and_postgres_catalog_are_mutually_exclusive() {
        let dir = tempdir().unwrap();
        let err = build_ducklake_attach_plan(
            Some("dbname=ducklake_catalog host=localhost"),
            Some("ducklake:md:test-ducklake"),
            &dir.path().join("canardstack.duckdb"),
            &dir.path().join("storage"),
            1_000,
        )
        .unwrap_err();

        assert!(err.to_string().contains(
            "set only one of CANARDSTACK_POSTGRES_DSN or CANARDSTACK_DUCKLAKE_ATTACH_URI"
        ));
    }

    #[test]
    fn ducklake_attach_uses_configured_data_inlining_limit() {
        let dir = tempdir().unwrap();
        let plan = build_ducklake_attach_plan(
            None,
            None,
            &dir.path().join("canardstack.duckdb"),
            &dir.path().join("storage"),
            0,
        )
        .unwrap();

        assert!(plan.sql.contains("DATA_INLINING_ROW_LIMIT 0"));
    }

    #[test]
    fn custom_attach_uri_must_be_uri_not_attach_statement() {
        let dir = tempdir().unwrap();
        let err = build_ducklake_attach_plan(
            None,
            Some("ATTACH 'md:test-ducklake';"),
            &dir.path().join("canardstack.duckdb"),
            &dir.path().join("storage"),
            1_000,
        )
        .unwrap_err();

        assert!(err
            .to_string()
            .contains("must be the URI only, not an ATTACH statement"));
    }

    #[test]
    fn custom_attach_uri_must_be_md_or_ducklake_uri() {
        let dir = tempdir().unwrap();
        let err = build_ducklake_attach_plan(
            None,
            Some("sqlite:/tmp/not-ducklake.db"),
            &dir.path().join("canardstack.duckdb"),
            &dir.path().join("storage"),
            1_000,
        )
        .unwrap_err();

        assert!(err.to_string().contains("must be an md: or ducklake: URI"));
    }

    #[test]
    fn metadata_refresh_uses_one_insert_per_signal_bucket() {
        for (signal, select_count) in [
            (Signal::Logs, 8),
            (Signal::Spans, 6),
            (Signal::MetricGauge, 5),
            (Signal::MetricSum, 5),
        ] {
            let sql = metadata_refresh_sql("canardlake.", signal, "2026-05-16").unwrap();
            assert_eq!(
                sql.matches("INSERT INTO canardlake.metadata_summary")
                    .count(),
                1
            );
            assert_eq!(sql.matches("UNION ALL").count(), select_count - 1);
        }
    }

    #[test]
    fn immutable_segment_split_preserves_event_date_hour_partitions() {
        fn date32(year: i32, month: u32, day: u32) -> i32 {
            let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
            let date = NaiveDate::from_ymd_opt(year, month, day).unwrap();
            (date - epoch).num_days() as i32
        }

        fn timestamp_micros(year: i32, month: u32, day: u32, hour: u32) -> i64 {
            DateTime::<Utc>::from_naive_utc_and_offset(
                NaiveDate::from_ymd_opt(year, month, day)
                    .unwrap()
                    .and_hms_opt(hour, 0, 0)
                    .unwrap(),
                Utc,
            )
            .timestamp_micros()
        }

        let schema = Arc::new(arrow58_types::Schema::new(vec![
            arrow58_types::Field::new(
                "timestamp",
                arrow58_types::DataType::Timestamp(arrow58_types::TimeUnit::Microsecond, None),
                true,
            ),
            arrow58_types::Field::new("event_date", arrow58_types::DataType::Date32, true),
            arrow58_types::Field::new("value", arrow58_types::DataType::Int64, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(arrow58_array::TimestampMicrosecondArray::from(vec![
                    Some(timestamp_micros(2026, 5, 16, 0)),
                    Some(timestamp_micros(2026, 5, 16, 1)),
                    Some(timestamp_micros(2026, 5, 17, 0)),
                ])),
                Arc::new(arrow58_array::Date32Array::from(vec![
                    Some(date32(2026, 5, 16)),
                    Some(date32(2026, 5, 16)),
                    Some(date32(2026, 5, 17)),
                ])),
                Arc::new(arrow58_array::Int64Array::from(vec![1, 2, 3])),
            ],
        )
        .unwrap();

        let splits = split_batch_by_immutable_partition(&batch).unwrap();

        assert_eq!(splits.len(), 3);
        assert_eq!(splits[0].0.event_date, "2026-05-16");
        assert_eq!(splits[0].0.hour, 0);
        assert_eq!(splits[0].1.num_rows(), 1);
        assert_eq!(splits[1].0.event_date, "2026-05-16");
        assert_eq!(splits[1].0.hour, 1);
        assert_eq!(splits[1].1.num_rows(), 1);
        assert_eq!(splits[2].0.event_date, "2026-05-17");
        assert_eq!(splits[2].0.hour, 0);
        assert_eq!(splits[2].1.num_rows(), 1);
    }
}
