use crate::ingest::Signal;
#[cfg(feature = "transform-split-instrumentation")]
use crate::metrics::Metrics;
use crate::validation::{ApiError, ApiResult};
use arrow58::record_batch::RecordBatch;
use flate2::read::GzDecoder;
#[cfg(feature = "transform-split-instrumentation")]
use opentelemetry_proto::tonic::collector::{
    logs::v1::ExportLogsServiceRequest, metrics::v1::ExportMetricsServiceRequest,
    trace::v1::ExportTraceServiceRequest,
};
use otlp2records::{transform_logs, transform_metrics, transform_traces, InputFormat};
#[cfg(feature = "transform-split-instrumentation")]
use otlp2records::{
    transform_logs_decoded_for_bench, transform_metrics_decoded_for_bench,
    transform_traces_decoded_for_bench,
};
#[cfg(feature = "transform-split-instrumentation")]
use prost::Message;
use std::collections::HashMap;
use std::io::Read;
#[cfg(feature = "transform-split-instrumentation")]
use std::time::Instant;

#[derive(Clone, Debug)]
pub struct Transformed {
    pub logs: Option<RecordBatch>,
    pub spans: Option<RecordBatch>,
    pub gauge: Option<RecordBatch>,
    pub sum: Option<RecordBatch>,
    pub source_format: &'static str,
    pub unsupported_histograms: usize,
}

pub fn decompress_if_needed(
    headers: &HashMap<String, String>,
    body: &[u8],
    max_decompressed_bytes: usize,
) -> ApiResult<Vec<u8>> {
    match headers
        .get("content-encoding")
        .map(|v| v.to_ascii_lowercase())
    {
        None => Ok(body.to_vec()),
        Some(v) if v == "identity" => Ok(body.to_vec()),
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
            Ok(out)
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
    signal: Signal,
    headers: &HashMap<String, String>,
    body: &[u8],
) -> ApiResult<Transformed> {
    let format = input_format(headers);
    let source_format = source_format(format);

    match signal {
        Signal::Logs => transform_logs(body, format)
            .map(|logs| Transformed {
                logs: Some(logs),
                spans: None,
                gauge: None,
                sum: None,
                source_format,
                unsupported_histograms: 0,
            })
            .map_err(|e| ApiError::new(400, "invalid_payload", e.to_string())),
        Signal::Spans => transform_traces(body, format)
            .map(|spans| Transformed {
                logs: None,
                spans: Some(spans),
                gauge: None,
                sum: None,
                source_format,
                unsupported_histograms: 0,
            })
            .map_err(|e| ApiError::new(400, "invalid_payload", e.to_string())),
        Signal::MetricGauge | Signal::MetricSum => {
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

#[cfg(feature = "transform-split-instrumentation")]
pub fn transform_observed(
    signal: Signal,
    headers: &HashMap<String, String>,
    body: &[u8],
    metrics: &Metrics,
) -> ApiResult<Transformed> {
    let started = Instant::now();
    let format = input_format(headers);
    let source_format = source_format(format);
    metrics.observe_phase_seconds(
        signal.as_str(),
        "otlp_input_format",
        None,
        started.elapsed().as_secs_f64(),
    );

    if format != InputFormat::Protobuf {
        let started = Instant::now();
        let transformed = transform(signal, headers, body);
        metrics.observe_phase_seconds(
            signal.as_str(),
            "otlp2records_transform_blackbox",
            None,
            started.elapsed().as_secs_f64(),
        );
        return transformed;
    }

    match signal {
        Signal::Logs => {
            let request = decode_protobuf::<ExportLogsServiceRequest>(signal, body, metrics)?;
            let logs = build_arrow(signal, metrics, || {
                transform_logs_decoded_for_bench(request, body.len())
            })?;
            build_transformed(signal, metrics, || Transformed {
                logs: Some(logs),
                spans: None,
                gauge: None,
                sum: None,
                source_format,
                unsupported_histograms: 0,
            })
        }
        Signal::Spans => {
            let request = decode_protobuf::<ExportTraceServiceRequest>(signal, body, metrics)?;
            let spans = build_arrow(signal, metrics, || {
                transform_traces_decoded_for_bench(request, body.len())
            })?;
            build_transformed(signal, metrics, || Transformed {
                logs: None,
                spans: Some(spans),
                gauge: None,
                sum: None,
                source_format,
                unsupported_histograms: 0,
            })
        }
        Signal::MetricGauge | Signal::MetricSum => {
            let request = decode_protobuf::<ExportMetricsServiceRequest>(signal, body, metrics)?;
            let batches = build_arrow(signal, metrics, || {
                transform_metrics_decoded_for_bench(request)
            })?;
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
            build_transformed(signal, metrics, || Transformed {
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

fn source_format(format: InputFormat) -> &'static str {
    match format {
        InputFormat::Json | InputFormat::Jsonl => "otlp_json",
        _ => "otlp_proto",
    }
}

#[cfg(feature = "transform-split-instrumentation")]
fn decode_protobuf<T>(signal: Signal, body: &[u8], metrics: &Metrics) -> ApiResult<T>
where
    T: Message + Default,
{
    let started = Instant::now();
    let decoded = T::decode(body).map_err(|e| ApiError::new(400, "invalid_payload", e.to_string()));
    metrics.observe_phase_seconds(
        signal.as_str(),
        "otlp_protobuf_decode",
        None,
        started.elapsed().as_secs_f64(),
    );
    decoded
}

#[cfg(feature = "transform-split-instrumentation")]
fn build_arrow<T>(
    signal: Signal,
    metrics: &Metrics,
    build: impl FnOnce() -> otlp2records::Result<T>,
) -> ApiResult<T> {
    let started = Instant::now();
    let built = build().map_err(|e| ApiError::new(400, "invalid_payload", e.to_string()));
    metrics.observe_phase_seconds(
        signal.as_str(),
        "otlp2records_arrow_build",
        None,
        started.elapsed().as_secs_f64(),
    );
    built
}

#[cfg(feature = "transform-split-instrumentation")]
fn build_transformed(
    signal: Signal,
    metrics: &Metrics,
    build: impl FnOnce() -> Transformed,
) -> ApiResult<Transformed> {
    let started = Instant::now();
    let transformed = build();
    metrics.observe_phase_seconds(
        signal.as_str(),
        "otlp_transformed_build",
        None,
        started.elapsed().as_secs_f64(),
    );
    Ok(transformed)
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
        let result = transform(Signal::Logs, &headers, &bytes);
        assert!(result.is_err(), "depth={depth} should be rejected");
    }
}
