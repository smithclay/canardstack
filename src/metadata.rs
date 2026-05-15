use crate::query::QueryEngine;
use crate::sql::quote as sql_quote;
use crate::storage::Storage;
use crate::validation::ApiResult;
use crate::LockExt;
use chrono::{DateTime, Utc};
use serde_json::{json, Map, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Mutex;

const MAX_CACHE_ENTRIES: usize = 128;
const MAX_CACHE_VALUE_BYTES: usize = 128 * 1024;
const DISCOVERY_LIMIT: usize = 1000;

#[derive(Default)]
pub struct Metadata {
    cache: Mutex<DiscoveryCache>,
}

impl Metadata {
    pub fn new() -> Self {
        Self {
            cache: Mutex::new(DiscoveryCache::default()),
        }
    }

    pub fn cache_entries(&self) -> usize {
        self.cache.lock_or_poisoned().entries.len()
    }

    pub fn prometheus_label_values(
        &self,
        queries: &QueryEngine,
        storage: &Storage,
        name: &str,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
    ) -> ApiResult<Vec<String>> {
        if !matches!(name, "__name__" | "service_name" | "deployment_environment") {
            return Ok(Vec::new());
        }
        let key = CacheKey::window(Api::Prometheus, Discovery::LabelValue, name, from, to);
        let value = self.cached(storage, key, || {
            label_values(
                queries,
                storage,
                &["metric_gauge", "metric_sum"],
                "label_value",
                name,
                from,
                to,
            )
            .map(|values| json!(values))
        })?;
        Ok(string_array(value))
    }

    pub fn prometheus_series(
        &self,
        queries: &QueryEngine,
        storage: &Storage,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
    ) -> ApiResult<Value> {
        let key = CacheKey::window(Api::Prometheus, Discovery::Series, "", from, to);
        self.cached(storage, key, || {
            let mut out = Vec::new();
            queries.run_interactive(storage, |conn, prefix| {
                let sql = format!(
                    "\
                    SELECT name, service_name, deployment_environment, sum(row_count) AS rows \
                    FROM {prefix}metadata_summary \
                    WHERE kind = 'series' \
                      AND signal IN ('metric_gauge', 'metric_sum') \
                      AND {} \
                    GROUP BY 1,2,3 \
                    ORDER BY rows DESC, name, service_name, deployment_environment \
                    LIMIT {DISCOVERY_LIMIT}",
                    metadata_time_predicate(from, to)
                );
                let mut stmt = conn.prepare(&sql)?;
                let rows = stmt.query_map([], |row| {
                    let mut labels = Map::new();
                    labels.insert("__name__".to_string(), json!(row.get::<_, String>(0)?));
                    insert_opt(
                        &mut labels,
                        "service_name",
                        row.get::<_, Option<String>>(1)?,
                    );
                    insert_opt(
                        &mut labels,
                        "deployment_environment",
                        row.get::<_, Option<String>>(2)?,
                    );
                    Ok(Value::Object(labels))
                })?;
                for row in rows {
                    out.push(row?);
                }
                Ok(())
            })?;
            Ok(json!(out))
        })
    }

    pub fn prometheus_metric_metadata(
        &self,
        queries: &QueryEngine,
        storage: &Storage,
    ) -> ApiResult<Value> {
        let key = CacheKey::static_key(Api::Prometheus, Discovery::MetricMetadata, "");
        self.cached(storage, key, || {
            let mut metadata = Map::new();
            queries.run_interactive(storage, |conn, prefix| {
                let sql = format!(
                    "\
                    SELECT name, metric_type, \
                           max(coalesce(metric_unit, '')) AS unit, \
                           max(coalesce(metric_description, '')) AS help, \
                           max(last_seen) AS last_seen \
                    FROM {prefix}metadata_summary \
                    WHERE kind = 'metric_metadata' \
                      AND signal IN ('metric_gauge', 'metric_sum') \
                    GROUP BY 1,2 \
                    ORDER BY last_seen DESC, name, metric_type \
                    LIMIT {DISCOVERY_LIMIT}"
                );
                let mut stmt = conn.prepare(&sql)?;
                let rows = stmt.query_map([], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                    ))
                })?;
                for row in rows {
                    let (name, metric_type, unit, help) = row?;
                    metadata
                        .entry(name)
                        .or_insert_with(|| json!([]))
                        .as_array_mut()
                        .expect("metadata entry should be an array")
                        .push(json!({"type": metric_type, "unit": unit, "help": help}));
                }
                Ok(())
            })?;
            Ok(Value::Object(metadata))
        })
    }

    pub fn loki_label_values(
        &self,
        queries: &QueryEngine,
        storage: &Storage,
        name: &str,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
    ) -> ApiResult<Vec<String>> {
        if !matches!(
            name,
            "service_name"
                | "deployment_environment"
                | "severity_text"
                | "trace_id"
                | "span_id"
                | "http_route"
                | "http_method"
        ) {
            return Ok(Vec::new());
        }
        let key = CacheKey::window(Api::Loki, Discovery::LabelValue, name, from, to);
        let value = self.cached(storage, key, || {
            label_values(queries, storage, &["logs"], "label_value", name, from, to)
                .map(|values| json!(values))
        })?;
        Ok(string_array(value))
    }

    pub fn loki_series(
        &self,
        queries: &QueryEngine,
        storage: &Storage,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
    ) -> ApiResult<Value> {
        let key = CacheKey::window(Api::Loki, Discovery::Series, "", from, to);
        self.cached(storage, key, || {
            let mut out = Vec::new();
            queries.run_interactive(storage, |conn, prefix| {
                let sql = format!(
                    "\
                    SELECT service_name, deployment_environment, severity_text, sum(row_count) AS rows \
                    FROM {prefix}metadata_summary \
                    WHERE kind = 'series' \
                      AND signal = 'logs' \
                      AND {} \
                    GROUP BY 1,2,3 \
                    ORDER BY rows DESC, service_name, deployment_environment, severity_text \
                    LIMIT {DISCOVERY_LIMIT}",
                    metadata_time_predicate(from, to)
                );
                let mut stmt = conn.prepare(&sql)?;
                let rows = stmt.query_map([], |row| {
                    let mut labels = Map::new();
                    insert_opt(&mut labels, "service_name", row.get::<_, Option<String>>(0)?);
                    insert_opt(
                        &mut labels,
                        "deployment_environment",
                        row.get::<_, Option<String>>(1)?,
                    );
                    insert_opt(&mut labels, "severity_text", row.get::<_, Option<String>>(2)?);
                    Ok(Value::Object(labels))
                })?;
                for row in rows {
                    out.push(row?);
                }
                Ok(())
            })?;
            Ok(json!(out))
        })
    }

    pub fn tempo_tag_values(
        &self,
        queries: &QueryEngine,
        storage: &Storage,
        tag: &str,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
    ) -> ApiResult<Vec<String>> {
        let Some(name) = tempo_tag_name(tag) else {
            return Ok(Vec::new());
        };
        let key = CacheKey::window(Api::Tempo, Discovery::TagValue, name, from, to);
        let value = self.cached(storage, key, || {
            label_values(queries, storage, &["spans"], "tag_value", name, from, to)
                .map(|values| json!(values))
        })?;
        Ok(string_array(value))
    }

    fn cached(
        &self,
        storage: &Storage,
        key: CacheKey,
        build: impl FnOnce() -> ApiResult<Value>,
    ) -> ApiResult<Value> {
        let generation = storage.metadata_generation();
        if let Some(value) = self.cache.lock_or_poisoned().get(&key, generation) {
            return Ok(value);
        }

        let value = build()?;
        self.cache
            .lock_or_poisoned()
            .insert(key, generation, value.clone());
        Ok(value)
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum Api {
    Prometheus,
    Loki,
    Tempo,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum Discovery {
    LabelValue,
    Series,
    MetricMetadata,
    TagValue,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum Scope {
    /// Not time-scoped; invalidated only by a metadata generation bump.
    Static,
    Window {
        start: String,
        end: String,
    },
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct CacheKey {
    api: Api,
    kind: Discovery,
    name: String,
    scope: Scope,
}

impl CacheKey {
    fn window(
        api: Api,
        kind: Discovery,
        name: &str,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
    ) -> Self {
        Self {
            api,
            kind,
            name: name.to_string(),
            scope: Scope::Window {
                start: cache_time(from),
                end: cache_time(to),
            },
        }
    }

    fn static_key(api: Api, kind: Discovery, name: &str) -> Self {
        Self {
            api,
            kind,
            name: name.to_string(),
            scope: Scope::Static,
        }
    }
}

#[derive(Clone)]
struct CacheEntry {
    generation: u64,
    /// Monotonic insertion counter; the smallest `seq` is the oldest entry.
    seq: u64,
    value: Value,
}

#[derive(Default)]
struct DiscoveryCache {
    entries: BTreeMap<CacheKey, CacheEntry>,
    next_seq: u64,
}

impl DiscoveryCache {
    fn get(&self, key: &CacheKey, generation: u64) -> Option<Value> {
        self.entries
            .get(key)
            .and_then(|entry| (entry.generation == generation).then(|| entry.value.clone()))
    }

    fn insert(&mut self, key: CacheKey, generation: u64, value: Value) {
        if value.to_string().len() > MAX_CACHE_VALUE_BYTES {
            return;
        }
        if self.entries.len() >= MAX_CACHE_ENTRIES && !self.entries.contains_key(&key) {
            // Evict the oldest entry by insertion order (FIFO).
            if let Some(oldest) = self
                .entries
                .iter()
                .min_by_key(|(_, entry)| entry.seq)
                .map(|(key, _)| key.clone())
            {
                self.entries.remove(&oldest);
            }
        }
        let seq = self.next_seq;
        self.next_seq += 1;
        self.entries.insert(
            key,
            CacheEntry {
                generation,
                seq,
                value,
            },
        );
    }
}

fn label_values(
    queries: &QueryEngine,
    storage: &Storage,
    signals: &[&str],
    kind: &str,
    name: &str,
    from: DateTime<Utc>,
    to: DateTime<Utc>,
) -> ApiResult<Vec<String>> {
    let signal_list = signals
        .iter()
        .map(|signal| sql_quote(signal))
        .collect::<Vec<_>>()
        .join(", ");
    let mut values = BTreeSet::new();
    queries.run_interactive(storage, |conn, prefix| {
        let sql = format!(
            "\
            SELECT value, sum(row_count) AS rows \
            FROM {prefix}metadata_summary \
            WHERE kind = {} \
              AND signal IN ({signal_list}) \
              AND name = {} \
              AND value IS NOT NULL \
              AND {} \
            GROUP BY 1 \
            ORDER BY value \
            LIMIT {DISCOVERY_LIMIT}",
            sql_quote(kind),
            sql_quote(name),
            metadata_time_predicate(from, to)
        );
        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map([], |row| row.get::<_, Option<String>>(0))?;
        for row in rows {
            if let Some(value) = row? {
                values.insert(value);
            }
        }
        Ok(())
    })?;
    Ok(values.into_iter().collect())
}

fn metadata_time_predicate(from: DateTime<Utc>, to: DateTime<Utc>) -> String {
    format!(
        "first_seen < TIMESTAMP {} AND last_seen >= TIMESTAMP {}",
        sql_quote(&to.format("%Y-%m-%d %H:%M:%S%.3f").to_string()),
        sql_quote(&from.format("%Y-%m-%d %H:%M:%S%.3f").to_string())
    )
}

fn cache_time(time: DateTime<Utc>) -> String {
    time.format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string()
}

fn string_array(value: Value) -> Vec<String> {
    value
        .as_array()
        .map(|values| {
            values
                .iter()
                .filter_map(Value::as_str)
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_default()
}

fn insert_opt(labels: &mut Map<String, Value>, name: &str, value: Option<String>) {
    if let Some(value) = value.filter(|v| !v.is_empty()) {
        labels.insert(name.to_string(), json!(value));
    }
}

fn tempo_tag_name(tag: &str) -> Option<&'static str> {
    match tag {
        "service.name" | "service_name" | "service-name" => Some("service.name"),
        "name" | "span.name" | "span_name" | "span-name" => Some("span.name"),
        "http.route" | "http_route" | "http-route" => Some("http.route"),
        "status" => Some("status"),
        "status.code" | "status_code" | "status-code" => Some("status.code"),
        "traceID" | "trace_id" => Some("traceID"),
        _ => None,
    }
}
