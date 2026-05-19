pub mod log;
pub mod plan;
pub mod prometheus;
pub mod trace;

use crate::config::Config;
use crate::db::sql::{
    logs_deployment_environment_expr, logs_http_method_expr, logs_http_route_expr,
    logs_http_status_code_expr, metrics_deployment_environment_expr, push_eq, push_eq_expr,
    push_eq_i_expr, quote as sql_quote, span_row, spans_deployment_environment_expr,
    spans_http_method_expr, spans_http_route_expr, spans_http_status_code_expr, time_predicate,
};
use crate::metrics::Timer;
use crate::query::plan::{
    FieldMatcher, LogPlan, MetricAggregation, MetricPlan, MetricSignal, SelectorPlan,
    SortDirection, TextFilter, TimeBounds, TracePlan, TraceSort,
};
use crate::storage::{DuckLakeLogCandidateWindow, QueryTimeoutError, Storage};
use crate::validation::{self, ApiError, ApiResult};
use chrono::{DateTime, Utc};
use duckdb::Row;
use serde_json::{json, Value};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

const INTERACTIVE_RANGE_SECS: i64 = 24 * 60 * 60;
const METRIC_RANGE_SECS: i64 = 30 * 24 * 60 * 60;
const CANDIDATE_WINDOW_BATCH_SIZE: usize = 1;
const MAX_PROGRESSIVE_CANDIDATE_FILES: usize = 10_000;
const LOG_COLUMNS: &[&str] = &[
    "service_name",
    "deployment_environment",
    "trace_id",
    "span_id",
    "http_route",
    "http_method",
    "severity_text",
];
const METRIC_COLUMNS: &[&str] = &["service_name", "deployment_environment"];
const TRACE_COLUMNS: &[&str] = &[
    "service_name",
    "span_name",
    "http_route",
    "status_code",
    "trace_id",
];

pub struct QueryEngine {
    interactive_active: AtomicUsize,
    background_active: AtomicUsize,
    interactive_limit: usize,
    background_limit: usize,
    interactive_timeout: Duration,
    background_timeout: Duration,
    interactive_memory_limit: String,
    background_memory_limit: String,
}

impl QueryEngine {
    pub fn new(config: &Config) -> Self {
        Self {
            interactive_active: AtomicUsize::new(0),
            background_active: AtomicUsize::new(0),
            interactive_limit: config.query_interactive.concurrency,
            background_limit: config.query_background.concurrency,
            interactive_timeout: Duration::from_secs(config.query_interactive.timeout_secs),
            background_timeout: Duration::from_secs(config.query_background.timeout_secs),
            interactive_memory_limit: config.query_interactive.memory_limit.clone(),
            background_memory_limit: config.query_background.memory_limit.clone(),
        }
    }

    pub fn health(&self) -> Value {
        json!({
            "interactive_active": self.interactive_active.load(Ordering::SeqCst),
            "interactive_limit": self.interactive_limit,
            "background_active": self.background_active.load(Ordering::SeqCst),
            "background_limit": self.background_limit,
            "interactive_timeout_seconds": self.interactive_timeout.as_secs(),
            "background_timeout_seconds": self.background_timeout.as_secs(),
            "memory_limit_interactive": self.interactive_memory_limit,
            "memory_limit_background": self.background_memory_limit
        })
    }

    pub fn interactive_limits(&self) -> QueryLimits {
        QueryLimits {
            timeout_ms: self.interactive_timeout.as_millis() as u64,
            memory_limit: self.interactive_memory_limit.clone(),
            concurrency_limit: self.interactive_limit,
            concurrency_active: self.interactive_active.load(Ordering::SeqCst),
        }
    }

    pub fn run_interactive<T>(
        &self,
        storage: &Storage,
        f: impl FnOnce(&duckdb::Connection, &str) -> anyhow::Result<T>,
    ) -> ApiResult<T> {
        let _guard = self.acquire(false)?;
        storage
            .with_query_conn(self.memory_limit(false), self.timeout(false), f)
            .map_err(storage_err)
    }

    pub fn execute_logs(&self, storage: &Storage, plan: &LogPlan) -> ApiResult<Value> {
        let _guard = self.acquire(false)?;
        let timer = Timer::start();
        let where_sql = log_where_sql(plan)?;
        let sql = log_select_sql(
            "{prefix}logs",
            &where_sql,
            plan.direction.sql(),
            plan.limit + 1,
        );
        let rows = storage
            .with_query_conn(
                self.memory_limit(false),
                self.timeout(false),
                |conn, prefix| {
                    let mut stmt = conn.prepare(&sql.replace("{prefix}", prefix))?;
                    let mapped = stmt.query_map([], log_row)?;
                    Ok(collect_rows(mapped)?)
                },
            )
            .map_err(storage_err)?;
        Ok(wrap_rows(rows, plan.limit, timer.elapsed_ms()))
    }

    pub fn execute_logs_progressive_window(
        &self,
        storage: &Storage,
        plan: &LogPlan,
    ) -> ApiResult<(Value, ProgressiveLogQueryReport)> {
        let _guard = self.acquire(false)?;
        let total_started = Instant::now();
        let plan_started = Instant::now();
        let candidate_plan = storage
            .ducklake_log_candidate_plan(
                plan.time_bounds.from,
                plan.time_bounds.to,
                MAX_PROGRESSIVE_CANDIDATE_FILES,
                CANDIDATE_WINDOW_BATCH_SIZE,
            )
            .map_err(storage_err)?;
        let candidate_plan_seconds = plan_started.elapsed().as_secs_f64();
        let execute_started = Instant::now();
        let where_sql = log_where_sql(plan)?;
        let (mut rows, files_scanned, batches_scanned, rows_scanned, bytes_scanned) =
            execute_logical_candidate_window(
                self,
                storage,
                plan,
                &where_sql,
                &candidate_plan.windows,
            )?;

        sort_log_rows(&mut rows, plan.direction);
        let candidate_execute_seconds = execute_started.elapsed().as_secs_f64();
        let truncated = rows.len() > plan.limit;
        let result_rows = rows.len().min(plan.limit);
        let query_duration_ms = total_started.elapsed().as_millis();
        let result = wrap_rows(rows, plan.limit, query_duration_ms);
        Ok((
            result,
            ProgressiveLogQueryReport {
                candidate_files: candidate_plan.candidate_files,
                candidate_rows: candidate_plan.candidate_rows.max(0),
                candidate_bytes: candidate_plan.candidate_bytes.max(0),
                batch_size: CANDIDATE_WINDOW_BATCH_SIZE,
                files_scanned,
                batches_scanned,
                rows_scanned,
                bytes_scanned,
                result_rows,
                truncated,
                candidate_plan_seconds,
                candidate_execute_seconds,
                query_duration_ms,
            },
        ))
    }

    pub fn explain_logs_progressive_window(
        &self,
        storage: &Storage,
        plan: &LogPlan,
        analyze: bool,
    ) -> ApiResult<Value> {
        let _guard = self.acquire(false)?;
        let plan_started = Instant::now();
        let candidate_plan = storage
            .ducklake_log_candidate_plan(
                plan.time_bounds.from,
                plan.time_bounds.to,
                MAX_PROGRESSIVE_CANDIDATE_FILES,
                CANDIDATE_WINDOW_BATCH_SIZE,
            )
            .map_err(storage_err)?;
        let candidate_plan_seconds = plan_started.elapsed().as_secs_f64();
        let selected = candidate_plan.windows.first();
        let lower_bound = selected.and_then(|window| window.timestamp_lower_bound.clone());
        let where_sql = log_where_sql(plan)?;

        let full_sql = log_select_sql(
            "{prefix}logs",
            &where_sql,
            plan.direction.sql(),
            plan.limit + 1,
        );
        let mut window_where = where_sql.clone();
        if let Some(lower_bound) = &lower_bound {
            window_where.push(format!("timestamp >= TIMESTAMP {}", sql_quote(lower_bound)));
        }
        let window_sql = log_select_sql(
            "{prefix}logs",
            &window_where,
            plan.direction.sql(),
            plan.limit + 1,
        );

        let (full_plan, progressive_plan) = storage
            .with_query_conn(
                self.memory_limit(false),
                self.timeout(false),
                |conn, prefix| {
                    let full = explain_query(conn, &full_sql.replace("{prefix}", prefix), analyze)?;
                    let progressive =
                        explain_query(conn, &window_sql.replace("{prefix}", prefix), analyze)?;
                    Ok((full, progressive))
                },
            )
            .map_err(storage_err)?;

        Ok(json!({
            "source": "duckdb_explain",
            "execution_engine": "duckdb_ducklake_logical_table",
            "analyze": analyze,
            "query_shape": "loki_query_range",
            "candidate_window": {
                "mode": "logical_window",
                "batch_size": CANDIDATE_WINDOW_BATCH_SIZE,
                "candidate_files": candidate_plan.candidate_files,
                "candidate_rows": candidate_plan.candidate_rows.max(0),
                "candidate_bytes": candidate_plan.candidate_bytes.max(0),
                "selected_files": selected.map(|window| window.files_scanned).unwrap_or(0),
                "selected_rows": selected.map(|window| window.rows_scanned).unwrap_or(0),
                "selected_bytes": selected.map(|window| window.bytes_scanned).unwrap_or(0),
                "timestamp_lower_bound": lower_bound,
                "candidate_plan_seconds": candidate_plan_seconds
            },
            "full_logical_query": {
                "sql": full_sql.replace("{prefix}", "main."),
                "plan": full_plan
            },
            "progressive_logical_query": {
                "sql": window_sql.replace("{prefix}", "main."),
                "plan": progressive_plan
            }
        }))
    }

    pub fn span_search(&self, storage: &Storage, req: &Value) -> ApiResult<Value> {
        let _guard = self.acquire(false)?;
        let timer = Timer::start();
        let (from, to) = parse_range(req, INTERACTIVE_RANGE_SECS)?;
        let limit = validation::parse_limit(req.get("limit"), 200, 1000)?;
        let mut where_sql = vec![time_predicate(from, to)];
        push_eq(&mut where_sql, "service_name", req.get("service_name"));
        push_eq_expr(
            &mut where_sql,
            &spans_deployment_environment_expr(),
            req.get("deployment_environment"),
        );
        push_eq(&mut where_sql, "span_name", req.get("span_name"));
        push_eq(&mut where_sql, "trace_id", req.get("trace_id"));
        push_eq_i(&mut where_sql, "status_code", req.get("status_code"));
        push_eq_expr(
            &mut where_sql,
            &spans_http_method_expr(),
            req.get("http_method"),
        );
        push_eq_i_expr(
            &mut where_sql,
            &spans_http_status_code_expr(),
            req.get("http_status_code"),
        );
        if let Some(ms) = req.get("min_duration_ms").and_then(Value::as_i64) {
            where_sql.push(format!("duration >= {ms}"));
        }
        let sort = if req.get("sort").and_then(Value::as_str) == Some("duration_desc") {
            "duration DESC"
        } else {
            "timestamp DESC"
        };
        let sql = format!(
            "SELECT timestamp::VARCHAR, trace_id, span_id, parent_span_id, service_name, span_name, duration, status_code, {} AS http_method, {} AS http_status_code FROM {{prefix}}spans WHERE {} ORDER BY {} LIMIT {}",
            spans_http_method_expr(),
            spans_http_status_code_expr(),
            where_sql.join(" AND "),
            sort,
            limit + 1
        );
        let rows = storage
            .with_query_conn(
                self.memory_limit(false),
                self.timeout(false),
                |conn, prefix| {
                    let mut stmt = conn.prepare(&sql.replace("{prefix}", prefix))?;
                    let mapped = stmt.query_map([], span_row)?;
                    Ok(collect_rows(mapped)?)
                },
            )
            .map_err(storage_err)?;
        Ok(wrap_rows(rows, limit, timer.elapsed_ms()))
    }

    pub fn execute_trace_search(&self, storage: &Storage, plan: &TracePlan) -> ApiResult<Value> {
        let TracePlan {
            selector,
            time_bounds,
            limit,
            sort,
        } = plan;
        let _guard = self.acquire(false)?;
        let timer = Timer::start();
        let mut where_sql = vec![time_predicate(time_bounds.from, time_bounds.to)];
        for matcher in &selector.matchers {
            push_matcher(
                &mut where_sql,
                matcher,
                trace_label_expr,
                "unsupported_selector",
            )?;
        }
        let order = match sort {
            TraceSort::DurationDesc => "7 DESC",
            TraceSort::TimestampDesc => "3 DESC",
        };
        let sql = format!(
            "SELECT trace_id, min(timestamp)::VARCHAR, max(timestamp)::VARCHAR, max(service_name), max(span_name), count(*), max(duration) FROM {{prefix}}spans WHERE {} GROUP BY trace_id ORDER BY {order} LIMIT {}",
            where_sql.join(" AND "),
            limit + 1
        );
        let rows = storage
            .with_query_conn(
                self.memory_limit(false),
                self.timeout(false),
                |conn, prefix| {
                    let mut stmt = conn.prepare(&sql.replace("{prefix}", prefix))?;
                    let rows = stmt.query_map([], |row| {
                        Ok(json!({
                            "trace_id": row.get::<_, Option<String>>(0)?.unwrap_or_default(),
                            "start_time": row.get::<_, Option<String>>(1)?,
                            "end_time": row.get::<_, Option<String>>(2)?,
                            "service_name": row.get::<_, Option<String>>(3)?,
                            "span_name": row.get::<_, Option<String>>(4)?,
                            "matched_spans": row.get::<_, i64>(5)?,
                            "duration": row.get::<_, Option<i64>>(6)?.unwrap_or(0),
                        }))
                    })?;
                    Ok(collect_rows(rows)?)
                },
            )
            .map_err(storage_err)?;
        Ok(wrap_rows(rows, *limit, timer.elapsed_ms()))
    }

    pub fn execute_metric(&self, storage: &Storage, plan: &MetricPlan) -> ApiResult<Value> {
        let _guard = self.acquire(false)?;
        let timer = Timer::start();
        let table = plan.signal.table();
        let agg_sql = match plan.aggregation {
            MetricAggregation::Avg => "avg(value)",
            MetricAggregation::Min => "min(value)",
            MetricAggregation::Max => "max(value)",
            MetricAggregation::Sum => "sum(value)",
            MetricAggregation::Count => "count(*)",
            MetricAggregation::Rate if plan.signal == MetricSignal::Sum => {
                "case when epoch(max(timestamp)-min(timestamp)) > 0 then (max(value)-min(value))/epoch(max(timestamp)-min(timestamp)) else null end"
            }
            MetricAggregation::Rate => {
                return Err(ApiError::new(
                    400,
                    "unsupported_aggregation",
                    "unsupported aggregation",
                ))
            }
        };
        let step = plan.step_seconds.clamp(1, 86_400);
        let metric_name =
            plan.selector.resource.as_deref().ok_or_else(|| {
                ApiError::new(400, "missing_metric_name", "metric_name is required")
            })?;
        let mut where_sql = vec![
            time_predicate(plan.time_bounds.from, plan.time_bounds.to),
            format!("metric_name = {}", sql_quote(metric_name)),
        ];
        for matcher in &plan.selector.matchers {
            push_matcher(
                &mut where_sql,
                matcher,
                metric_label_expr,
                "unsupported_promql",
            )?;
        }
        let group_by = &plan.group_by;
        let group_selects = group_by
            .iter()
            .filter_map(|label| metric_label_expr(label).map(|expr| (label.as_str(), expr)))
            .collect::<Vec<_>>();
        let group_select_clause = if group_selects.is_empty() {
            String::new()
        } else {
            group_selects
                .iter()
                .map(|(label, expr)| format!(", {expr} AS {label}"))
                .collect::<String>()
        };
        let group_clause = if group_selects.is_empty() {
            String::new()
        } else {
            group_selects
                .iter()
                .map(|(_, expr)| format!(", {expr}"))
                .collect::<String>()
        };
        let sql = format!(
            "SELECT to_timestamp(floor(epoch(timestamp)/{step})*{step})::VARCHAR AS bucket{group_select_clause}, {agg_sql} AS value FROM {{prefix}}{table} WHERE {} GROUP BY bucket{group_clause} ORDER BY bucket {} LIMIT {}",
            where_sql.join(" AND "),
            plan.order.sql(),
            plan.limit + 1
        );
        let rows = storage
            .with_query_conn(
                self.memory_limit(false),
                self.timeout(false),
                |conn, prefix| {
                    let mut stmt = conn.prepare(&sql.replace("{prefix}", prefix))?;
                    let mapped = stmt.query_map([], |row| {
                        let mut value = serde_json::Map::new();
                        value.insert("timestamp".to_string(), json!(row.get::<_, String>(0)?));
                        for (idx, column) in group_by.iter().enumerate() {
                            value.insert(
                                (*column).to_string(),
                                json!(row.get::<_, Option<String>>(idx + 1)?),
                            );
                        }
                        value.insert(
                            "value".to_string(),
                            json!(row.get::<_, Option<f64>>(group_by.len() + 1)?),
                        );
                        Ok(Value::Object(value))
                    })?;
                    Ok(collect_rows(mapped)?)
                },
            )
            .map_err(storage_err)?;
        Ok(wrap_rows(rows, plan.limit, timer.elapsed_ms()))
    }

    pub fn metric_query(&self, storage: &Storage, req: &Value) -> ApiResult<Value> {
        let (from, to) = parse_range(req, METRIC_RANGE_SECS)?;
        let limit = validation::parse_limit(req.get("limit"), 5000, 5000)?;
        let metric_name = req
            .get("metric_name")
            .and_then(Value::as_str)
            .ok_or_else(|| ApiError::new(400, "missing_metric_name", "metric_name is required"))?;
        let signal = req.get("signal").and_then(Value::as_str).unwrap_or("gauge");
        let signal = MetricSignal::parse(signal)?;
        let aggregation = req
            .get("aggregation")
            .and_then(Value::as_str)
            .unwrap_or("avg");
        let aggregation = MetricAggregation::parse(aggregation)?;
        let step = req
            .get("step_seconds")
            .and_then(Value::as_i64)
            .unwrap_or(60)
            .clamp(1, 86_400);
        let mut matchers = Vec::new();
        if let Some(filters) = req.get("filters").and_then(Value::as_object) {
            push_req_matcher(
                &mut matchers,
                "deployment_environment",
                filters.get("deployment_environment"),
            );
            push_req_matcher(&mut matchers, "service_name", filters.get("service_name"));
        }
        let group_by = req
            .get("group_by")
            .and_then(Value::as_array)
            .map(|values| {
                values
                    .iter()
                    .filter_map(Value::as_str)
                    .filter(|name| matches!(*name, "service_name" | "deployment_environment"))
                    .map(str::to_string)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        let plan = MetricPlan {
            selector: SelectorPlan {
                resource: Some(metric_name.to_string()),
                matchers,
                text_filters: Vec::new(),
            },
            time_bounds: TimeBounds { from, to },
            signal,
            aggregation,
            group_by,
            step_seconds: step,
            limit,
            order: SortDirection::from_order(req.get("order").and_then(Value::as_str)),
        };
        self.execute_metric(storage, &plan)
    }

    fn acquire(&self, background: bool) -> ApiResult<QueryGuard<'_>> {
        let (active, limit) = if background {
            (&self.background_active, self.background_limit)
        } else {
            (&self.interactive_active, self.interactive_limit)
        };
        let prev = active.fetch_add(1, Ordering::SeqCst);
        if prev >= limit {
            active.fetch_sub(1, Ordering::SeqCst);
            // 1s ≈ one query window; longer back-off punishes Grafana panels.
            return Err(ApiError::new(
                429,
                "query_concurrency_exhausted",
                "query concurrency limit is exhausted",
            )
            .with_retry_after(1));
        }
        Ok(QueryGuard { active })
    }

    fn memory_limit(&self, background: bool) -> &str {
        if background {
            &self.background_memory_limit
        } else {
            &self.interactive_memory_limit
        }
    }

    fn timeout(&self, background: bool) -> Duration {
        if background {
            self.background_timeout
        } else {
            self.interactive_timeout
        }
    }
}

#[derive(Clone, Debug)]
pub struct QueryLimits {
    pub timeout_ms: u64,
    pub memory_limit: String,
    pub concurrency_limit: usize,
    pub concurrency_active: usize,
}

#[derive(Clone, Debug)]
pub struct ProgressiveLogQueryReport {
    pub candidate_files: usize,
    pub candidate_rows: i64,
    pub candidate_bytes: i64,
    pub batch_size: usize,
    pub files_scanned: usize,
    pub batches_scanned: usize,
    pub rows_scanned: i64,
    pub bytes_scanned: i64,
    pub result_rows: usize,
    pub truncated: bool,
    pub candidate_plan_seconds: f64,
    pub candidate_execute_seconds: f64,
    pub query_duration_ms: u128,
}

struct QueryGuard<'a> {
    active: &'a AtomicUsize,
}

impl Drop for QueryGuard<'_> {
    fn drop(&mut self) {
        self.active.fetch_sub(1, Ordering::SeqCst);
    }
}

fn log_row(row: &Row<'_>) -> duckdb::Result<Value> {
    Ok(json!({
        "timestamp": row.get::<_, String>(0)?,
        "ingested_at": row.get::<_, String>(1)?,
        "trace_id": row.get::<_, Option<String>>(2)?,
        "span_id": row.get::<_, Option<String>>(3)?,
        "service_name": row.get::<_, Option<String>>(4)?,
        "severity_text": row.get::<_, Option<String>>(5)?,
        "body": row.get::<_, Option<String>>(6)?,
        "deployment_environment": row.get::<_, Option<String>>(7)?,
        "http_method": row.get::<_, Option<String>>(8)?,
        "http_status_code": row.get::<_, Option<i32>>(9)?,
        "http_route": row.get::<_, Option<String>>(10)?,
    }))
}

fn log_where_sql(plan: &LogPlan) -> ApiResult<Vec<String>> {
    let mut where_sql = vec![time_predicate(plan.time_bounds.from, plan.time_bounds.to)];
    for matcher in &plan.selector.matchers {
        push_matcher(
            &mut where_sql,
            matcher,
            log_label_expr,
            "unsupported_selector",
        )?;
    }
    for filter in &plan.selector.text_filters {
        match filter {
            TextFilter::BodyContains(text) => where_sql.push(text_terms("body", text)?),
        }
    }
    Ok(where_sql)
}

fn log_select_sql(source: &str, where_sql: &[String], direction: &str, limit: usize) -> String {
    format!(
        "SELECT timestamp::VARCHAR, ingested_at::VARCHAR, trace_id, span_id, service_name, severity_text, body, {} AS deployment_environment, {} AS http_method, {} AS http_status_code, {} AS http_route FROM {source} WHERE {} ORDER BY timestamp {direction} LIMIT {limit}",
        logs_deployment_environment_expr(),
        logs_http_method_expr(),
        logs_http_status_code_expr(),
        logs_http_route_expr(),
        where_sql.join(" AND ")
    )
}

type CandidateWindowRows = (Vec<Value>, usize, usize, i64, i64);

fn execute_logical_candidate_window(
    engine: &QueryEngine,
    storage: &Storage,
    plan: &LogPlan,
    where_sql: &[String],
    windows: &[DuckLakeLogCandidateWindow],
) -> ApiResult<CandidateWindowRows> {
    let mut rows = Vec::new();
    let mut files_scanned = 0usize;
    let mut batches_scanned = 0usize;
    let mut rows_scanned = 0i64;
    let mut bytes_scanned = 0i64;

    storage
        .with_query_conn(
            engine.memory_limit(false),
            engine.timeout(false),
            |conn, prefix| {
                for window in windows {
                    let mut window_where = where_sql.to_vec();
                    if let Some(lower_bound) = &window.timestamp_lower_bound {
                        window_where
                            .push(format!("timestamp >= TIMESTAMP {}", sql_quote(lower_bound)));
                    }
                    let source = format!("{prefix}logs");
                    let sql = log_select_sql(
                        &source,
                        &window_where,
                        plan.direction.sql(),
                        plan.limit + 1,
                    );
                    let mut stmt = conn.prepare(&sql)?;
                    let mapped = stmt.query_map([], log_row)?;
                    rows = collect_rows(mapped)?;
                    batches_scanned += 1;
                    files_scanned = window.files_scanned;
                    rows_scanned = window.rows_scanned;
                    bytes_scanned = window.bytes_scanned;
                    if rows.len() >= plan.limit {
                        break;
                    }
                }
                Ok(())
            },
        )
        .map_err(storage_err)?;

    Ok((
        rows,
        files_scanned,
        batches_scanned,
        rows_scanned,
        bytes_scanned,
    ))
}

fn sort_log_rows(rows: &mut [Value], direction: SortDirection) {
    rows.sort_by(|left, right| {
        let left_timestamp = left.get("timestamp").and_then(Value::as_str).unwrap_or("");
        let right_timestamp = right.get("timestamp").and_then(Value::as_str).unwrap_or("");
        if direction.is_forward() {
            left_timestamp.cmp(right_timestamp)
        } else {
            right_timestamp.cmp(left_timestamp)
        }
    });
}

fn collect_rows(
    iter: duckdb::MappedRows<'_, impl FnMut(&Row<'_>) -> duckdb::Result<Value>>,
) -> Result<Vec<Value>, duckdb::Error> {
    iter.collect()
}

fn wrap_rows(mut rows: Vec<Value>, limit: usize, query_duration_ms: u128) -> Value {
    let truncated = rows.len() > limit;
    rows.truncate(limit);
    json!({
        "rows": rows,
        "next_cursor": null,
        "applied_limit": limit,
        "truncated": truncated,
        "query_duration_ms": query_duration_ms
    })
}

fn explain_query(
    conn: &duckdb::Connection,
    sql: &str,
    analyze: bool,
) -> anyhow::Result<Vec<Value>> {
    let explain = if analyze {
        format!("EXPLAIN ANALYZE {sql}")
    } else {
        format!("EXPLAIN {sql}")
    };
    let mut stmt = conn.prepare(&explain)?;
    let rows = stmt.query_map([], |row| {
        Ok(json!({
            "key": row.get::<_, String>(0)?,
            "value": row.get::<_, String>(1)?,
        }))
    })?;
    Ok(rows.collect::<Result<Vec<_>, _>>()?)
}

fn parse_range(req: &Value, max_secs: i64) -> ApiResult<(DateTime<Utc>, DateTime<Utc>)> {
    let from = validation::parse_required_time(req.get("from"), "from")?;
    let to = validation::parse_required_time(req.get("to"), "to")?;
    validation::validate_range(from, to, max_secs)?;
    Ok((from, to))
}

fn push_eq_i(where_sql: &mut Vec<String>, column: &str, value: Option<&Value>) {
    if let Some(v) = value.and_then(Value::as_i64) {
        where_sql.push(format!("{column} = {v}"));
    }
}

fn push_req_matcher(matchers: &mut Vec<FieldMatcher>, field: &str, value: Option<&Value>) {
    if let Some(value) = value.and_then(Value::as_str).filter(|v| !v.is_empty()) {
        matchers.push(FieldMatcher {
            field: field.to_string(),
            value: value.to_string(),
        });
    }
}

fn push_matcher(
    where_sql: &mut Vec<String>,
    matcher: &FieldMatcher,
    label_expr: fn(&str) -> Option<String>,
    reason: &'static str,
) -> ApiResult<()> {
    let expr = label_expr(&matcher.field).ok_or_else(|| {
        ApiError::new(
            400,
            reason,
            format!("unsupported label {} in v0 selector", matcher.field),
        )
    })?;
    where_sql.push(format!("{expr} = {}", sql_quote(&matcher.value)));
    Ok(())
}

fn log_label_expr(label: &str) -> Option<String> {
    if !LOG_COLUMNS.contains(&label) {
        return None;
    }
    Some(match label {
        "deployment_environment" => logs_deployment_environment_expr(),
        "http_route" => logs_http_route_expr(),
        "http_method" => logs_http_method_expr(),
        direct => direct.to_string(),
    })
}

fn metric_label_expr(label: &str) -> Option<String> {
    if !METRIC_COLUMNS.contains(&label) {
        return None;
    }
    Some(match label {
        "deployment_environment" => metrics_deployment_environment_expr(),
        direct => direct.to_string(),
    })
}

fn trace_label_expr(label: &str) -> Option<String> {
    if !TRACE_COLUMNS.contains(&label) {
        return None;
    }
    Some(match label {
        "http_route" => spans_http_route_expr(),
        direct => direct.to_string(),
    })
}

fn text_terms(column: &str, query: &str) -> ApiResult<String> {
    let mut clauses = Vec::new();
    let upper = query.to_ascii_uppercase();
    if upper.contains('(') || upper.contains(')') {
        return Err(ApiError::new(
            400,
            "unsupported_query",
            "parentheses are not implemented in v0 text search",
        ));
    }
    let op = if upper.contains(" OR ") {
        " OR "
    } else {
        " AND "
    };
    for term in query.split(op) {
        let term = term.trim();
        if !term.is_empty() {
            clauses.push(format!(
                "{column} ILIKE {}",
                sql_quote(&format!("%{term}%"))
            ));
        }
    }
    if clauses.is_empty() {
        Ok("TRUE".to_string())
    } else {
        Ok(format!("({})", clauses.join(op)))
    }
}

fn storage_err(err: anyhow::Error) -> ApiError {
    // Use a typed downcast rather than substring-matching on the error
    // message — that was a brittle contract between this layer and storage.rs.
    if err.downcast_ref::<QueryTimeoutError>().is_some() {
        return ApiError::new(503, "query_timeout", err.to_string());
    }
    ApiError::new(503, "query_storage_unavailable", err.to_string())
}
