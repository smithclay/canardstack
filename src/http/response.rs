use crate::validation::ApiError;
use serde_json::{json, Value};
use std::io::Write;
use std::net::TcpStream;

pub struct HttpResponse {
    status: u16,
    content_type: String,
    body: Vec<u8>,
    retry_after_seconds: Option<u32>,
}

impl HttpResponse {
    pub fn status(&self) -> u16 {
        self.status
    }

    pub fn json_body(&self) -> Value {
        serde_json::from_slice(&self.body)
            .unwrap_or_else(|_| json!({"raw": String::from_utf8_lossy(&self.body)}))
    }

    pub fn text_body(&self) -> String {
        String::from_utf8_lossy(&self.body).to_string()
    }

    pub fn body(&self) -> &[u8] {
        &self.body
    }

    pub fn json(status: u16, value: Value) -> Self {
        Self {
            status,
            content_type: "application/json".to_string(),
            body: serde_json::to_vec(&value).unwrap(),
            retry_after_seconds: None,
        }
    }

    pub fn html(status: u16, value: String) -> Self {
        Self {
            status,
            content_type: "text/html; charset=utf-8".to_string(),
            body: value.into_bytes(),
            retry_after_seconds: None,
        }
    }

    pub fn text(status: u16, content_type: &str, value: String) -> Self {
        Self {
            status,
            content_type: content_type.to_string(),
            body: value.into_bytes(),
            retry_after_seconds: None,
        }
    }

    pub fn bytes(status: u16, content_type: &str, body: Vec<u8>) -> Self {
        Self {
            status,
            content_type: content_type.to_string(),
            body,
            retry_after_seconds: None,
        }
    }

    pub fn from_api_error(err: &ApiError) -> Self {
        let mut response = Self::json(err.status, err.body());
        response.retry_after_seconds = err.retry_after_seconds;
        response
    }

    pub fn with_retry_after(mut self, seconds: u32) -> Self {
        self.retry_after_seconds = Some(seconds);
        self
    }
}

pub(super) fn write_response(stream: &mut TcpStream, response: HttpResponse) -> anyhow::Result<()> {
    write_response_with_connection(stream, response, false)
}

pub(super) fn write_response_with_connection(
    stream: &mut TcpStream,
    response: HttpResponse,
    keep_alive: bool,
) -> anyhow::Result<()> {
    let reason = match response.status {
        200 => "OK",
        202 => "Accepted",
        400 => "Bad Request",
        401 => "Unauthorized",
        403 => "Forbidden",
        404 => "Not Found",
        429 => "Too Many Requests",
        503 => "Service Unavailable",
        _ => "OK",
    };
    // Assemble the whole response (status line, headers, body) into one buffer
    // and flush it with a single write_all, instead of a syscall per header
    // line plus one per body. With TCP_NODELAY set on the socket this lets the
    // response go out in one packet rather than dribbling.
    let mut out = Vec::with_capacity(response.body.len() + 128);
    write!(
        out,
        "HTTP/1.1 {} {}\r\ncontent-type: {}\r\ncontent-length: {}\r\nconnection: {}\r\n",
        response.status,
        reason,
        response.content_type,
        response.body.len(),
        if keep_alive { "keep-alive" } else { "close" }
    )?;
    if let Some(seconds) = response.retry_after_seconds {
        write!(out, "retry-after: {seconds}\r\n")?;
    }
    out.extend_from_slice(b"\r\n");
    out.extend_from_slice(&response.body);
    stream.write_all(&out)?;
    Ok(())
}
