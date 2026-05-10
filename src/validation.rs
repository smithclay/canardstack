use crate::config::Config;
use crate::ingest::Signal;
use chrono::{DateTime, Utc};
use serde_json::{json, Value};
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct ApiError {
    pub status: u16,
    pub reason: &'static str,
    pub message: String,
    /// Emits `Retry-After: <seconds>` on the wire. Only meaningful on 429/503.
    pub retry_after_seconds: Option<u32>,
}

impl ApiError {
    pub fn new(status: u16, reason: &'static str, message: impl Into<String>) -> Self {
        Self {
            status,
            reason,
            message: message.into(),
            retry_after_seconds: None,
        }
    }

    pub fn with_retry_after(mut self, seconds: u32) -> Self {
        self.retry_after_seconds = Some(seconds);
        self
    }

    pub fn body(&self) -> Value {
        json!({"error": self.reason, "message": self.message})
    }
}

pub type ApiResult<T> = Result<T, ApiError>;

pub fn validate_api_key(
    headers: &HashMap<String, String>,
    config: &Config,
    admin: bool,
) -> ApiResult<()> {
    let expected = if admin {
        &config.admin_api_key
    } else {
        &config.api_key
    };
    // Defense-in-depth: even if Config::validate() didn't run, never accept an
    // empty configured key. An empty `expected` would otherwise match the
    // strip_prefix("Bearer ") on a bare "Authorization: Bearer " header.
    if expected.is_empty() {
        return Err(ApiError::new(
            403,
            "bad_api_key",
            "supplied API key is not authorized",
        ));
    }
    let supplied = headers
        .get("authorization")
        .and_then(|v| v.strip_prefix("Bearer ").map(str::to_string))
        .or_else(|| headers.get("x-api-key").cloned());

    match supplied {
        None => Err(ApiError::new(
            401,
            "missing_api_key",
            "missing Authorization bearer token or x-api-key",
        )),
        Some(value) if constant_time_eq(value.as_bytes(), expected.as_bytes()) => Ok(()),
        Some(_) => Err(ApiError::new(
            403,
            "bad_api_key",
            "supplied API key is not authorized",
        )),
    }
}

/// Compare two byte slices in time independent of the matching prefix length.
/// `String::eq` short-circuits on first mismatch and would leak a network
/// timing oracle. The length compare is allowed to short-circuit — key length
/// is not secret.
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff: u8 = 0;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

pub fn validate_content_type(headers: &HashMap<String, String>) -> ApiResult<()> {
    let Some(value) = headers.get("content-type") else {
        return Err(ApiError::new(
            400,
            "missing_content_type",
            "content-type is required",
        ));
    };
    let value = value
        .split(';')
        .next()
        .unwrap_or(value)
        .trim()
        .to_ascii_lowercase();
    let accepted = matches!(
        value.as_str(),
        "application/json"
            | "application/otlp+json"
            | "application/x-protobuf"
            | "application/protobuf"
            | "application/otlp"
    );
    if accepted {
        Ok(())
    } else {
        Err(ApiError::new(
            400,
            "unsupported_content_type",
            format!("unsupported content-type {value}"),
        ))
    }
}

pub fn validate_body_size(body_len: usize, config: &Config) -> ApiResult<()> {
    if body_len > config.max_body_bytes {
        Err(ApiError::new(
            400,
            "payload_too_large",
            format!(
                "payload has {body_len} bytes; max is {}",
                config.max_body_bytes
            ),
        ))
    } else {
        Ok(())
    }
}

pub fn parse_required_time(value: Option<&Value>, field: &'static str) -> ApiResult<DateTime<Utc>> {
    let raw = value
        .and_then(Value::as_str)
        .ok_or_else(|| ApiError::new(400, "missing_time_range", format!("{field} is required")))?;
    DateTime::parse_from_rfc3339(raw)
        .map(|dt| dt.with_timezone(&Utc))
        .map_err(|_| {
            ApiError::new(
                400,
                "invalid_time_range",
                format!("{field} must be RFC3339"),
            )
        })
}

pub fn validate_range(from: DateTime<Utc>, to: DateTime<Utc>, max_secs: i64) -> ApiResult<()> {
    if to <= from {
        return Err(ApiError::new(
            400,
            "invalid_time_range",
            "to must be after from",
        ));
    }
    let span = (to - from).num_seconds();
    if span > max_secs {
        return Err(ApiError::new(
            400,
            "range_too_large",
            format!("time range is {span}s; max is {max_secs}s"),
        ));
    }
    Ok(())
}

pub fn parse_limit(value: Option<&Value>, default: usize, max: usize) -> ApiResult<usize> {
    let limit = value
        .and_then(Value::as_u64)
        .map(|v| v as usize)
        .unwrap_or(default);
    if limit == 0 || limit > max {
        Err(ApiError::new(
            400,
            "limit_too_large",
            format!("limit must be between 1 and {max}"),
        ))
    } else {
        Ok(limit)
    }
}

pub fn validate_timestamp_skew(
    records: &[Value],
    signal: Signal,
    config: &Config,
) -> ApiResult<()> {
    let now_ms = Utc::now().timestamp_millis();
    let min_ms = now_ms - config.late_accept_secs * 1000;
    let max_ms = now_ms + config.future_accept_secs * 1000;
    for record in records {
        let Some(ts) = record_timestamp_ms(record) else {
            return Err(ApiError::new(
                400,
                "invalid_timestamp",
                format!("{signal} timestamp is required and must be parseable"),
            ));
        };
        if ts <= 0 {
            return Err(ApiError::new(
                400,
                "invalid_timestamp",
                format!("{signal} timestamp is required and must be positive"),
            ));
        }
        if ts < min_ms {
            return Err(ApiError::new(
                400,
                "timestamp_too_old",
                format!("{signal} timestamp is outside late-arrival window"),
            ));
        }
        if ts > max_ms {
            return Err(ApiError::new(
                400,
                "timestamp_in_future",
                format!("{signal} timestamp is outside future-skew window"),
            ));
        }
    }
    Ok(())
}

pub fn record_timestamp_ms(record: &Value) -> Option<i64> {
    let value = record.get("timestamp")?;
    if let Some(n) = value.as_i64() {
        return Some(if n > 100_000_000_000_000 { n / 1000 } else { n });
    }
    if let Some(s) = value.as_str() {
        if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
            return Some(dt.timestamp_millis());
        }
        return s
            .parse::<i64>()
            .ok()
            .map(|n| if n > 100_000_000_000_000 { n / 1000 } else { n });
    }
    None
}
