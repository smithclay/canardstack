use anyhow::{bail, Context, Result};
use serde_json::Value;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::Duration;

pub fn run(url: Option<String>) -> Result<()> {
    let url = url.unwrap_or_else(|| "http://127.0.0.1:4318/healthz".to_string());
    let response = get(&url)?;
    if response.status != 200 {
        bail!(
            "healthcheck failed: HTTP {} body={}",
            response.status,
            response.body
        );
    }
    let body: Value = serde_json::from_str(&response.body)
        .with_context(|| format!("healthcheck returned malformed JSON: {}", response.body))?;
    if body.get("status").and_then(Value::as_str) != Some("ok") {
        bail!("healthcheck status was not ok: {body}");
    }
    Ok(())
}

struct Response {
    status: u16,
    body: String,
}

fn get(url: &str) -> Result<Response> {
    let target = HttpTarget::parse(url)?;
    let mut stream = TcpStream::connect((target.host.as_str(), target.port))
        .with_context(|| format!("connect to {url}"))?;
    stream.set_read_timeout(Some(Duration::from_secs(5)))?;
    stream.set_write_timeout(Some(Duration::from_secs(5)))?;
    write!(
        stream,
        "GET {} HTTP/1.1\r\nhost: {}\r\nconnection: close\r\n\r\n",
        target.path, target.host
    )?;
    read_response(stream)
}

fn read_response(mut stream: TcpStream) -> Result<Response> {
    let mut bytes = Vec::new();
    stream.read_to_end(&mut bytes)?;
    let raw = String::from_utf8_lossy(&bytes);
    let (head, body) = raw
        .split_once("\r\n\r\n")
        .ok_or_else(|| anyhow::anyhow!("malformed HTTP response"))?;
    let status = head
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|raw| raw.parse::<u16>().ok())
        .ok_or_else(|| anyhow::anyhow!("malformed HTTP status line"))?;
    Ok(Response {
        status,
        body: body.to_string(),
    })
}

struct HttpTarget {
    host: String,
    port: u16,
    path: String,
}

impl HttpTarget {
    fn parse(url: &str) -> Result<Self> {
        let rest = url
            .strip_prefix("http://")
            .ok_or_else(|| anyhow::anyhow!("only http:// URLs are supported"))?;
        let (authority, path) = rest.split_once('/').unwrap_or((rest, "healthz"));
        let (host, port) = authority.split_once(':').unwrap_or((authority, "80"));
        Ok(Self {
            host: host.to_string(),
            port: port.parse().context("parse URL port")?,
            path: format!("/{path}"),
        })
    }
}
