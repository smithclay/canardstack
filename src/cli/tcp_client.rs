use anyhow::{Context, Result};
use serde_json::Value;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::Duration;

pub struct Client {
    host: String,
    port: u16,
}

pub struct Response {
    pub status: u16,
    pub body: String,
}

impl Client {
    pub fn new(base_url: &str) -> Result<Self> {
        let rest = base_url
            .strip_prefix("http://")
            .ok_or_else(|| anyhow::anyhow!("only http:// URLs are supported"))?;
        let authority = rest.trim_end_matches('/');
        let (host, port) = authority.split_once(':').unwrap_or((authority, "80"));
        Ok(Self {
            host: host.to_string(),
            port: port.parse().context("parse base URL port")?,
        })
    }

    pub fn get(&self, path: &str, bearer: Option<&str>) -> Result<Response> {
        self.request("GET", path, bearer, None)
    }

    pub fn post_json(&self, path: &str, bearer: Option<&str>, body: Value) -> Result<Response> {
        self.request("POST", path, bearer, Some(body.to_string()))
    }

    fn request(
        &self,
        method: &str,
        path: &str,
        bearer: Option<&str>,
        body: Option<String>,
    ) -> Result<Response> {
        let mut stream = TcpStream::connect((self.host.as_str(), self.port))
            .with_context(|| format!("connect to http://{}:{}", self.host, self.port))?;
        stream.set_read_timeout(Some(Duration::from_secs(15)))?;
        stream.set_write_timeout(Some(Duration::from_secs(15)))?;
        let body = body.unwrap_or_default();
        write!(
            stream,
            "{method} {path} HTTP/1.1\r\nhost: {}\r\naccept: application/json\r\ncontent-length: {}\r\nconnection: close\r\n",
            self.host,
            body.len()
        )?;
        if let Some(token) = bearer {
            write!(stream, "authorization: Bearer {token}\r\n")?;
        }
        if method == "POST" {
            write!(stream, "content-type: application/json\r\n")?;
        }
        write!(stream, "\r\n{body}")?;
        read_response(stream)
    }
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

pub fn parse_json(response: &Response, context: &str) -> Result<Value> {
    serde_json::from_str(&response.body)
        .with_context(|| format!("{context} returned malformed JSON: {}", response.body))
}

pub fn ensure_status(response: &Response, expected: u16, context: &str) -> Result<()> {
    if response.status != expected {
        anyhow::bail!(
            "{context} expected HTTP {expected}, got {} with body {}",
            response.status,
            response.body
        );
    }
    Ok(())
}
