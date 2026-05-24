use crate::ingest::OtlpRequestKind;
#[cfg(feature = "otlp2records-observer")]
use crate::metrics::{MetricName, Metrics};
use crate::signal::StorageSignal;
use crate::validation::{ApiError, ApiResult};
use arrow58::record_batch::RecordBatch;
use flate2::read::GzDecoder;
use otlp2records::{transform_logs, transform_metrics, transform_traces, InputFormat};
#[cfg(feature = "otlp2records-observer")]
use otlp2records::{
    transform_logs_with_observer, transform_metrics_with_observer, transform_traces_with_observer,
    TransformCounter, TransformCounterValue, TransformObserver, TransformPhase,
    TransformPhaseTiming,
};
use std::borrow::Cow;
#[cfg(feature = "otlp2records-observer")]
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::io::Read;

#[derive(Clone, Debug)]
pub struct Transformed {
    pub logs: Option<RecordBatch>,
    pub spans: Option<RecordBatch>,
    pub gauge: Option<RecordBatch>,
    pub sum: Option<RecordBatch>,
    pub source_format: &'static str,
    pub unsupported_histograms: usize,
}

impl Transformed {
    pub fn signal_batches(&self) -> [(StorageSignal, Option<&RecordBatch>); 4] {
        [
            (StorageSignal::Logs, self.logs.as_ref()),
            (StorageSignal::Spans, self.spans.as_ref()),
            (StorageSignal::MetricGauge, self.gauge.as_ref()),
            (StorageSignal::MetricSum, self.sum.as_ref()),
        ]
    }
}

pub fn decompress_if_needed<'a>(
    headers: &HashMap<String, String>,
    body: &'a [u8],
    max_decompressed_bytes: usize,
) -> ApiResult<Cow<'a, [u8]>> {
    match headers
        .get("content-encoding")
        .map(|v| v.to_ascii_lowercase())
    {
        None => Ok(Cow::Borrowed(body)),
        Some(v) if v == "identity" => Ok(Cow::Borrowed(body)),
        Some(v) if v == "gzip" => {
            let decoder = GzDecoder::new(body);
            let mut limited = decoder.take(max_decompressed_bytes as u64 + 1);
            let mut out = Vec::new();
            limited.read_to_end(&mut out).map_err(|e| {
                ApiError::new(400, "invalid_gzip", format!("gzip decode failed: {e}"))
            })?;
            if out.len() > max_decompressed_bytes {
                return Err(ApiError::new(
                    400,
                    "payload_too_large",
                    format!("decompressed payload exceeds max of {max_decompressed_bytes} bytes"),
                ));
            }
            Ok(Cow::Owned(out))
        }
        Some(v) => Err(ApiError::new(
            400,
            "unsupported_compression",
            format!("unsupported content-encoding {v}"),
        )),
    }
}

/// Parse depth is bounded by serde_json's default 128-level recursion limit
/// (asserted in the test module below) and `Config::max_body_bytes` upstream.
pub fn transform(
    route: OtlpRequestKind,
    headers: &HashMap<String, String>,
    body: &[u8],
) -> ApiResult<Transformed> {
    let format = input_format(headers);
    let source_format = source_format(format);

    match route {
        OtlpRequestKind::Logs => transform_logs(body, format)
            .map(|logs| Transformed {
                logs: Some(logs),
                spans: None,
                gauge: None,
                sum: None,
                source_format,
                unsupported_histograms: 0,
            })
            .map_err(|e| ApiError::new(400, "invalid_payload", e.to_string())),
        OtlpRequestKind::Traces => transform_traces(body, format)
            .map(|spans| Transformed {
                logs: None,
                spans: Some(spans),
                gauge: None,
                sum: None,
                source_format,
                unsupported_histograms: 0,
            })
            .map_err(|e| ApiError::new(400, "invalid_payload", e.to_string())),
        OtlpRequestKind::Metrics => {
            let batches = transform_metrics(body, format)
                .map_err(|e| ApiError::new(400, "invalid_payload", e.to_string()))?;
            let unsupported_histograms = batches
                .histogram
                .as_ref()
                .map(RecordBatch::num_rows)
                .unwrap_or(0)
                + batches
                    .exp_histogram
                    .as_ref()
                    .map(RecordBatch::num_rows)
                    .unwrap_or(0);
            Ok(Transformed {
                logs: None,
                spans: None,
                gauge: batches.gauge,
                sum: batches.sum,
                source_format,
                unsupported_histograms,
            })
        }
    }
}

#[cfg(feature = "otlp2records-observer")]
pub fn transform_observed(
    route: OtlpRequestKind,
    headers: &HashMap<String, String>,
    body: &[u8],
    metrics: &Metrics,
) -> ApiResult<Transformed> {
    let format = input_format(headers);
    let source_format = source_format(format);
    let mut observer = OtlpTransformMetrics::new(route);

    let result = match route {
        OtlpRequestKind::Logs => transform_logs_with_observer(body, format, &mut observer)
            .map(|logs| Transformed {
                logs: Some(logs),
                spans: None,
                gauge: None,
                sum: None,
                source_format,
                unsupported_histograms: 0,
            })
            .map_err(|e| ApiError::new(400, "invalid_payload", e.to_string())),
        OtlpRequestKind::Traces => transform_traces_with_observer(body, format, &mut observer)
            .map(|spans| Transformed {
                logs: None,
                spans: Some(spans),
                gauge: None,
                sum: None,
                source_format,
                unsupported_histograms: 0,
            })
            .map_err(|e| ApiError::new(400, "invalid_payload", e.to_string())),
        OtlpRequestKind::Metrics => transform_metrics_with_observer(body, format, &mut observer)
            .map(|batches| {
                let unsupported_histograms = batches
                    .histogram
                    .as_ref()
                    .map(RecordBatch::num_rows)
                    .unwrap_or(0)
                    + batches
                        .exp_histogram
                        .as_ref()
                        .map(RecordBatch::num_rows)
                        .unwrap_or(0);
                Transformed {
                    logs: None,
                    spans: None,
                    gauge: batches.gauge,
                    sum: batches.sum,
                    source_format,
                    unsupported_histograms,
                }
            })
            .map_err(|e| ApiError::new(400, "invalid_payload", e.to_string())),
    };
    observer.emit(metrics);
    result
}

#[cfg(feature = "otlp2records-observer")]
struct OtlpTransformMetrics {
    route: OtlpRequestKind,
    phase_totals: BTreeMap<&'static str, PhaseTotal>,
    counters: BTreeMap<&'static str, u64>,
}

#[cfg(feature = "otlp2records-observer")]
#[derive(Default)]
struct PhaseTotal {
    count: u64,
    sum_seconds: f64,
}

#[cfg(feature = "otlp2records-observer")]
impl OtlpTransformMetrics {
    fn new(route: OtlpRequestKind) -> Self {
        Self {
            route,
            phase_totals: BTreeMap::new(),
            counters: BTreeMap::new(),
        }
    }

    fn emit(&self, metrics: &Metrics) {
        let request_kind = self.route.as_str();
        for (phase, total) in &self.phase_totals {
            metrics.observe_request_phase_seconds_n(
                request_kind,
                phase,
                total.count,
                total.sum_seconds,
            );
        }
        for (counter, value) in &self.counters {
            metrics.inc(
                MetricName::Otlp2recordsTransformEventsTotal,
                &[("request_kind", request_kind), ("event", counter)],
                *value,
            );
        }
    }
}

#[cfg(feature = "otlp2records-observer")]
impl TransformObserver for OtlpTransformMetrics {
    fn on_phase(&mut self, timing: TransformPhaseTiming) {
        let total = self
            .phase_totals
            .entry(phase_name(timing.phase))
            .or_default();
        total.count += 1;
        total.sum_seconds += timing.elapsed.as_secs_f64();
    }

    fn on_counter(&mut self, counter: TransformCounterValue) {
        *self
            .counters
            .entry(counter_name(counter.counter))
            .or_default() += counter.value;
    }
}

fn source_format(format: InputFormat) -> &'static str {
    match format {
        InputFormat::Json | InputFormat::Jsonl => "otlp_json",
        _ => "otlp_proto",
    }
}

#[cfg(feature = "otlp2records-observer")]
fn phase_name(phase: TransformPhase) -> &'static str {
    match phase {
        TransformPhase::ProtobufDecode => "otlp2records_protobuf_decode",
        TransformPhase::JsonDecode => "otlp2records_json_decode",
        TransformPhase::JsonlDecode => "otlp2records_jsonl_decode",
        TransformPhase::RowCount => "otlp2records_row_count",
        TransformPhase::BuilderInit => "otlp2records_builder_init",
        TransformPhase::ResourceLogsBuild => "otlp2records_resource_logs_build",
        TransformPhase::ResourceSpansBuild => "otlp2records_resource_spans_build",
        TransformPhase::ResourceContextBuild => "otlp2records_resource_context_build",
        TransformPhase::ResourceAttributesJson => "otlp2records_resource_attributes_json",
        TransformPhase::ScopeLogsBuild => "otlp2records_scope_logs_build",
        TransformPhase::ScopeSpansBuild => "otlp2records_scope_spans_build",
        TransformPhase::ScopeContextBuild => "otlp2records_scope_context_build",
        TransformPhase::ScopeAttributesJson => "otlp2records_scope_attributes_json",
        TransformPhase::LogRecordBuild => "otlp2records_log_record_build",
        TransformPhase::SpanBuild => "otlp2records_span_build",
        TransformPhase::ArrowAppend => "otlp2records_arrow_append",
        TransformPhase::BodyAppend => "otlp2records_body_append",
        TransformPhase::ResourceAttributesAppend => "otlp2records_resource_attributes_append",
        TransformPhase::ScopeAttributesAppend => "otlp2records_scope_attributes_append",
        TransformPhase::LogAttributesJson => "otlp2records_log_attributes_json",
        TransformPhase::SpanAttributesJson => "otlp2records_span_attributes_json",
        TransformPhase::MetricAttributesJson => "otlp2records_metric_attributes_json",
        TransformPhase::EventsJson => "otlp2records_events_json",
        TransformPhase::LinksJson => "otlp2records_links_json",
        TransformPhase::ExemplarsJson => "otlp2records_exemplars_json",
        TransformPhase::MetricArrayJson => "otlp2records_metric_array_json",
        TransformPhase::MetricsCapacity => "otlp2records_metrics_capacity",
        TransformPhase::ArrowFinalize => "otlp2records_arrow_finalize",
    }
}

#[cfg(feature = "otlp2records-observer")]
fn counter_name(counter: TransformCounter) -> &'static str {
    match counter {
        TransformCounter::OutputRows => "output_rows",
        TransformCounter::ResourceContextDuplicateHit => "resource_context_duplicate_hit",
        TransformCounter::ResourceContextDuplicateMiss => "resource_context_duplicate_miss",
        TransformCounter::ScopeContextDuplicateHit => "scope_context_duplicate_hit",
        TransformCounter::ScopeContextDuplicateMiss => "scope_context_duplicate_miss",
        TransformCounter::ResourceAttributesRowCopies => "resource_attributes_row_copies",
        TransformCounter::ResourceAttributesRowCopyBytes => "resource_attributes_row_copy_bytes",
        TransformCounter::ScopeAttributesRowCopies => "scope_attributes_row_copies",
        TransformCounter::ScopeAttributesRowCopyBytes => "scope_attributes_row_copy_bytes",
    }
}

fn input_format(headers: &HashMap<String, String>) -> InputFormat {
    let content_type = headers
        .get("content-type")
        .map(|v| v.split(';').next().unwrap_or(v).trim());
    InputFormat::from_content_type(content_type)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deeply_nested_json_is_rejected_by_default_recursion_limit() {
        // serde_json's default 128-level cap must reject this before transform runs.
        let depth = 200;
        let payload = "[".repeat(depth).into_bytes();
        let mut bytes = payload;
        bytes.extend(std::iter::repeat_n(b']', depth));
        let mut headers = HashMap::new();
        headers.insert("content-type".to_string(), "application/json".to_string());
        let result = transform(OtlpRequestKind::Logs, &headers, &bytes);
        assert!(result.is_err(), "depth={depth} should be rejected");
    }
}
