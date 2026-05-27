pub mod log;
pub mod plan;
pub mod prometheus;
pub mod trace;

use crate::config::Config;
use crate::db::sql::{quote as sql_quote, time_predicate};
use crate::metrics::Timer;
use crate::query::plan::{
    FieldMatcher, LogPlan, MatchOp, MetricAggregation, MetricPlan, MetricSignal, TextFilter,
    TracePlan, TraceSort,
};
use crate::semantic_labels::{self, LabelScope};
use crate::storage::{QueryTimeoutError, Storage};
use crate::validation::{ApiError, ApiResult};
use duckdb::Row;
use serde_json::{json, Value};
use std::time::Duration;

pub struct QueryEngine {
    interactive_timeout: Duration,
    interactive_memory_limit: String,
}

impl QueryEngine {
    pub fn new(config: &Config) -> Self {
        Self {
            interactive_timeout: Duration::from_secs(
                config.operator.query_interactive.timeout_secs,
            ),
            interactive_memory_limit: config.operator.query_interactive.memory_limit.clone(),
        }
    }

    pub fn health(&self) -> Value {
        json!({
            "interactive_timeout_seconds": self.interactive_timeout.as_secs(),
            "memory_limit_interactive": self.interactive_memory_limit
        })
    }

    pub fn run_interactive<T>(
        &self,
        storage: &Storage,
        f: impl FnOnce(&duckdb::Connection, &str) -> anyhow::Result<T>,
    ) -> ApiResult<T> {
        storage
            .with_query_conn(&self.interactive_memory_limit, self.interactive_timeout, f)
            .map_err(storage_err)
    }

    pub fn execute_logs(&self, storage: &Storage, plan: &LogPlan) -> ApiResult<Value> {
        let timer = Timer::start();
        let where_sql = log_where_sql(plan)?;
        let projected = semantic_labels::projected_labels(LabelScope::Logs);
        let sql = log_select_sql(
            "{prefix}logs",
            &projected,
            &where_sql,
            plan.direction.sql(),
            plan.limit + 1,
        );
        let rows = storage
            .with_query_conn(
                &self.interactive_memory_limit,
                self.interactive_timeout,
                |conn, prefix| {
                    let mut stmt = conn.prepare(&sql.replace("{prefix}", prefix))?;
                    let mapped = stmt.query_map([], |row| log_row(row, &projected))?;
                    Ok(collect_rows(mapped)?)
                },
            )
            .map_err(storage_err)?;
        Ok(wrap_rows(rows, plan.limit, timer.elapsed_ms()))
    }

    pub fn execute_trace_search(&self, storage: &Storage, plan: &TracePlan) -> ApiResult<Value> {
        let TracePlan {
            selector,
            time_bounds,
            limit,
            sort,
        } = plan;
        let timer = Timer::start();
        let mut where_sql = vec![time_predicate(
            "start_time_unix_nano",
            time_bounds.from,
            time_bounds.to,
        )];
        for matcher in &selector.matchers {
            push_matcher(
                &mut where_sql,
                matcher,
                LabelScope::Spans,
                "unsupported_selector",
            )?;
        }
        let order = match sort {
            TraceSort::DurationDesc => "7 DESC",
            TraceSort::TimestampDesc => "3 DESC",
        };
        // v2 OTAP: `trace_id` is BLOB → hex VARCHAR for output; `timestamp` →
        // `start_time_unix_nano`; `span_name` → `name`; `duration` →
        // `duration_time_unix_nano` (BIGINT nanoseconds, downstream `tempo_*`
        // converts to ms).
        let sql = format!(
            "SELECT lower(hex(trace_id)), min(start_time_unix_nano)::VARCHAR, max(start_time_unix_nano)::VARCHAR, max(service_name), max(name), count(*), max(duration_time_unix_nano) FROM {{prefix}}spans WHERE {} GROUP BY trace_id ORDER BY {order} LIMIT {}",
            where_sql.join(" AND "),
            limit + 1
        );
        let rows = storage
            .with_query_conn(
                &self.interactive_memory_limit,
                self.interactive_timeout,
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
        let timer = Timer::start();
        let table = plan.signal.table();
        // v2 gauge/sum tables store both `int_value BIGINT` and
        // `double_value DOUBLE`; coalesce to a single DOUBLE for aggregation
        // so downstream value handling is uniform. The double channel wins
        // when both are populated, matching OTLP semantics.
        let value_expr = "coalesce(double_value, int_value::DOUBLE)";
        let agg_sql: String = match plan.aggregation {
            MetricAggregation::Avg => format!("avg({value_expr})"),
            MetricAggregation::Min => format!("min({value_expr})"),
            MetricAggregation::Max => format!("max({value_expr})"),
            MetricAggregation::Sum => format!("sum({value_expr})"),
            MetricAggregation::Count => "count(*)".to_string(),
            MetricAggregation::Rate if plan.signal == MetricSignal::Sum => {
                format!("case when epoch(max(time_unix_nano)-min(time_unix_nano)) > 0 then (max({value_expr})-min({value_expr}))/epoch(max(time_unix_nano)-min(time_unix_nano)) else null end")
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
            time_predicate("time_unix_nano", plan.time_bounds.from, plan.time_bounds.to),
            format!("name = {}", sql_quote(metric_name)),
        ];
        for matcher in &plan.selector.matchers {
            push_matcher(
                &mut where_sql,
                matcher,
                LabelScope::Metrics,
                "unsupported_promql",
            )?;
        }
        let group_by = &plan.group_by;
        let group_selects = group_by
            .iter()
            .filter_map(|label| {
                semantic_labels::label_expr(LabelScope::Metrics, label)
                    .map(|expr| (label.as_str(), expr))
            })
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
            "SELECT to_timestamp(floor(epoch(time_unix_nano)/{step})*{step})::VARCHAR AS bucket{group_select_clause}, {agg_sql} AS value FROM {{prefix}}{table} WHERE {} GROUP BY bucket{group_clause} ORDER BY bucket {} LIMIT {}",
            where_sql.join(" AND "),
            plan.order.sql(),
            plan.limit + 1
        );
        let rows = storage
            .with_query_conn(
                &self.interactive_memory_limit,
                self.interactive_timeout,
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
}

fn log_row(row: &Row<'_>, projected: &[(&str, String)]) -> duckdb::Result<Value> {
    let mut obj = serde_json::Map::new();
    obj.insert(
        "timestamp".to_string(),
        json!(row.get::<_, String>("timestamp")?),
    );
    obj.insert(
        "ingested_at".to_string(),
        json!(row.get::<_, String>("ingested_at")?),
    );
    obj.insert(
        "body".to_string(),
        json!(row.get::<_, Option<String>>("body")?),
    );
    for (name, _) in projected {
        obj.insert(
            (*name).to_string(),
            json!(row.get::<_, Option<String>>(*name)?),
        );
    }
    Ok(Value::Object(obj))
}

fn log_where_sql(plan: &LogPlan) -> ApiResult<Vec<String>> {
    let mut where_sql = vec![time_predicate(
        "time_unix_nano",
        plan.time_bounds.from,
        plan.time_bounds.to,
    )];
    for matcher in &plan.selector.matchers {
        push_matcher(
            &mut where_sql,
            matcher,
            LabelScope::Logs,
            "unsupported_selector",
        )?;
    }
    for filter in &plan.selector.text_filters {
        match filter {
            TextFilter::BodyContains(text) => where_sql.push(text_terms("body", text)?),
            TextFilter::BodyRegex(pattern) => where_sql.push(format!(
                "regexp_matches(coalesce(body, ''), {})",
                sql_quote(pattern)
            )),
        }
    }
    Ok(where_sql)
}

fn log_select_sql(
    source: &str,
    projected: &[(&str, String)],
    where_sql: &[String],
    direction: &str,
    limit: usize,
) -> String {
    let mut columns = vec![
        // Alias the v2 `time_unix_nano` storage column back to the stable
        // `timestamp` output column the log row reader pulls by name.
        "time_unix_nano::VARCHAR AS timestamp".to_string(),
        "ingested_at::VARCHAR AS ingested_at".to_string(),
        "body".to_string(),
    ];
    // Project every registry label as VARCHAR so the row reader can pull each
    // column back by name with one uniform type; NULLs are preserved.
    for (name, expr) in projected {
        columns.push(format!("cast({expr} AS VARCHAR) AS {name}"));
    }
    format!(
        "SELECT {} FROM {source} WHERE {} ORDER BY time_unix_nano {direction} LIMIT {limit}",
        columns.join(", "),
        where_sql.join(" AND ")
    )
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

fn push_matcher(
    where_sql: &mut Vec<String>,
    matcher: &FieldMatcher,
    scope: LabelScope,
    reason: &'static str,
) -> ApiResult<()> {
    let expr = semantic_labels::label_expr(scope, &matcher.field).ok_or_else(|| {
        ApiError::new(
            400,
            reason,
            format!("unsupported label {} in v0 selector", matcher.field),
        )
    })?;
    let value_expr = format!("coalesce(cast({expr} AS VARCHAR), '')");
    match matcher.op {
        MatchOp::Eq => where_sql.push(format!("{value_expr} = {}", sql_quote(&matcher.value))),
        MatchOp::NotEq => where_sql.push(format!("{value_expr} != {}", sql_quote(&matcher.value))),
        MatchOp::Regex if matcher.value == ".*" => {}
        MatchOp::Regex => where_sql.push(format!(
            "regexp_matches({value_expr}, {})",
            sql_quote(&matcher.value)
        )),
        MatchOp::NotRegex if matcher.value == ".*" => where_sql.push("FALSE".to_string()),
        MatchOp::NotRegex => where_sql.push(format!(
            "NOT regexp_matches({value_expr}, {})",
            sql_quote(&matcher.value)
        )),
    }
    Ok(())
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
