use crate::ingest::Signal;
use crate::validation::{ApiError, ApiResult};
use flate2::read::GzDecoder;
use otlp2records::{
    transform_logs_json, transform_metrics_json, transform_traces_json, InputFormat,
};
use serde_json::Value;
use std::collections::HashMap;
use std::io::Read;

#[derive(Clone, Debug)]
pub struct Transformed {
    pub logs: Vec<Value>,
    pub spans: Vec<Value>,
    pub gauge: Vec<Value>,
    pub sum: Vec<Value>,
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
    let source_format = match format {
        InputFormat::Json | InputFormat::Jsonl => "otlp_json",
        _ => "otlp_proto",
    };

    match signal {
        Signal::Logs => transform_logs_json(body, format)
            .map(|logs| Transformed {
                logs,
                spans: vec![],
                gauge: vec![],
                sum: vec![],
                source_format,
                unsupported_histograms: 0,
            })
            .map_err(|e| ApiError::new(400, "invalid_payload", e.to_string())),
        Signal::Spans => transform_traces_json(body, format)
            .map(|spans| Transformed {
                logs: vec![],
                spans,
                gauge: vec![],
                sum: vec![],
                source_format,
                unsupported_histograms: 0,
            })
            .map_err(|e| ApiError::new(400, "invalid_payload", e.to_string())),
        Signal::MetricGauge | Signal::MetricSum => {
            let batches = transform_metrics_json(body, format)
                .map_err(|e| ApiError::new(400, "invalid_payload", e.to_string()))?;
            Ok(Transformed {
                logs: vec![],
                spans: vec![],
                gauge: batches.gauge,
                sum: batches.sum,
                source_format,
                unsupported_histograms: batches.histogram.len() + batches.exp_histogram.len(),
            })
        }
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
        let result = transform(Signal::Logs, &headers, &bytes);
        assert!(result.is_err(), "depth={depth} should be rejected");
    }
}
