mod loki;
mod params;
mod prometheus;
mod tempo;

pub use loki::{loki_label_values, loki_labels, loki_query, loki_query_range, loki_series};
pub use prometheus::{
    prometheus_label_values, prometheus_labels, prometheus_metadata, prometheus_query,
    prometheus_query_range, prometheus_series,
};
use serde_json::{json, Value};
pub use tempo::{tempo_search, tempo_tag_values, tempo_tags, tempo_trace, tempo_trace_proto};

use crate::validation::ApiError;

pub fn compat_error(err: ApiError) -> Value {
    json!({"status": "error", "errorType": err.reason, "error": err.message})
}
