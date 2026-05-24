use crate::cli::smoke::{log_fixture, metric_fixture, trace_fixture};
use crate::cli::tcp_client::{ensure_status, parse_json, Client};
use crate::Config;
use anyhow::{bail, Result};
use chrono::{Duration as ChronoDuration, Utc};
use serde_json::{json, Value};
use std::thread;
use std::time::{Duration, Instant};

const TRACE_ID: &str = "11111111111111111111111111111111";

pub fn run(args: impl Iterator<Item = String>) -> Result<()> {
    let mut base_url = "http://127.0.0.1:4318".to_string();
    let mut mode = Mode::IngestAndVerify;
    for arg in args {
        match arg.as_str() {
            "--verify-only" => mode = Mode::VerifyOnly,
            "--expect-empty" => mode = Mode::ExpectEmpty,
            _ if arg.starts_with("http://") => base_url = arg,
            _ => bail!(
                "unknown smoke-http argument {arg}; use [http://host:port] [--verify-only|--expect-empty]"
            ),
        }
    }

    let config = Config::from_env()?;
    let api_key = config.operator.api_key;
    let admin_key = config.operator.admin_api_key;
    let client = Client::new(&base_url)?;

    ensure_service_healthy(&client)?;
    ensure_ducklake_attached(&client, &admin_key)?;

    let now = Utc::now();
    let from = (now - ChronoDuration::minutes(60)).to_rfc3339();
    let to = (now + ChronoDuration::minutes(10)).to_rfc3339();

    if mode == Mode::IngestAndVerify {
        let nanos = now
            .timestamp_nanos_opt()
            .unwrap_or(now.timestamp_millis() * 1_000_000);
        ingest_fixture(&client, &api_key, "/v1/logs", log_fixture(nanos))?;
        ingest_fixture(&client, &api_key, "/v1/traces", trace_fixture(nanos))?;
        ingest_fixture(&client, &api_key, "/v1/metrics", metric_fixture(nanos))?;
        let seal = client.post_json("/api/admin/maintenance/seal", Some(&admin_key), json!({}))?;
        ensure_status(&seal, 200, "admin seal")?;
    }

    if mode == Mode::ExpectEmpty {
        verify_empty(&client, &api_key, &from, &to)?;
        println!("canardstack docker smoke: reset state is empty");
        return Ok(());
    }

    verify_fixture_evidence(&client, &api_key, &from, &to)?;
    println!(
        "canardstack docker smoke: verified ingest, DuckLake attach, compatibility query endpoints, fixture evidence, and metrics"
    );
    Ok(())
}

#[derive(Copy, Clone, Eq, PartialEq)]
enum Mode {
    IngestAndVerify,
    VerifyOnly,
    ExpectEmpty,
}

fn ensure_service_healthy(client: &Client) -> Result<()> {
    let response = client.get("/healthz", None)?;
    ensure_status(&response, 200, "service health")?;
    let body = parse_json(&response, "service health")?;
    if body.get("status").and_then(Value::as_str) != Some("ok") {
        bail!("service is not healthy: {body}");
    }
    Ok(())
}

fn ensure_ducklake_attached(client: &Client, admin_key: &str) -> Result<()> {
    let response = client.get("/api/admin/health/storage", Some(admin_key))?;
    ensure_status(&response, 200, "storage health")?;
    let body = parse_json(&response, "storage health")?;
    let mode = body.get("mode").and_then(Value::as_str).unwrap_or_default();
    if !mode.starts_with("ducklake_") {
        bail!("expected a DuckLake-backed storage mode, got storage health {body}");
    }
    Ok(())
}

fn ingest_fixture(client: &Client, api_key: &str, path: &str, body: Value) -> Result<()> {
    let response = client.post_json(path, Some(api_key), body)?;
    ensure_status(&response, 202, path)?;
    let body = parse_json(&response, path)?;
    if body.get("accepted").and_then(Value::as_bool) == Some(false) {
        bail!("{path} did not accept fixture payload: {body}");
    }
    Ok(())
}

fn verify_fixture_evidence(client: &Client, api_key: &str, from: &str, to: &str) -> Result<()> {
    let prom_labels = client.get(
        &format!("/api/v1/labels?start={}&end={}", enc(from), enc(to)),
        Some(api_key),
    )?;
    ensure_status(&prom_labels, 200, "Prometheus labels")?;
    ensure_text(
        &parse_json(&prom_labels, "Prometheus labels")?,
        "__name__",
        "Prometheus labels did not include metric name",
    )?;

    let prom = client.get(
        &format!(
            "/api/v1/query_range?query={}&start={}&end={}&step=60",
            enc("avg by (service_name) (smoke.gauge)"),
            enc(from),
            enc(to)
        ),
        Some(api_key),
    )?;
    ensure_status(&prom, 200, "Prometheus query_range")?;
    let prom_body = parse_json(&prom, "Prometheus query_range")?;
    ensure_success(&prom_body, "Prometheus query_range")?;
    ensure_text(
        &prom_body,
        "42",
        "Prometheus query did not return gauge value",
    )?;
    ensure_text(
        &prom_body,
        "payments",
        "Prometheus query did not return grouped smoke services",
    )?;

    let loki = client.get(
        &format!(
            "/loki/api/v1/query_range?query={}&start={}&end={}&limit=10",
            enc("{service_name=\"checkout\"} |= \"smoke\""),
            enc(from),
            enc(to)
        ),
        Some(api_key),
    )?;
    ensure_status(&loki, 200, "Loki query_range")?;
    let loki_body = parse_json(&loki, "Loki query_range")?;
    ensure_success(&loki_body, "Loki query_range")?;
    ensure_text(
        &loki_body,
        "smoke payment timeout",
        "Loki query did not find fixture log",
    )?;

    ensure_loki_route_label_value(client, api_key, from, to)?;

    let tempo_search = client.get(
        &format!(
            "/api/search?start={}&end={}&service.name=checkout&limit=10",
            enc(from),
            enc(to)
        ),
        Some(api_key),
    )?;
    ensure_status(&tempo_search, 200, "Tempo search")?;
    ensure_text(
        &parse_json(&tempo_search, "Tempo search")?,
        TRACE_ID,
        "Tempo search did not find trace",
    )?;

    let tempo_trace = client.get(&format!("/api/v2/traces/{TRACE_ID}"), Some(api_key))?;
    ensure_status(&tempo_trace, 200, "Tempo trace")?;
    ensure_text(
        &parse_json(&tempo_trace, "Tempo trace")?,
        "GET /smoke",
        "Tempo trace lookup did not find fixture span",
    )?;

    let prom_series = client.get(
        &format!("/api/v1/series?start={}&end={}", enc(from), enc(to)),
        Some(api_key),
    )?;
    ensure_status(&prom_series, 200, "Prometheus series")?;
    ensure_text(
        &parse_json(&prom_series, "Prometheus series")?,
        "smoke.gauge",
        "Prometheus series did not include gauge",
    )?;
    ensure_text(
        &parse_json(&prom_series, "Prometheus series")?,
        "smoke.sum",
        "Prometheus series did not include sum",
    )?;

    Ok(())
}

fn ensure_loki_route_label_value(
    client: &Client,
    api_key: &str,
    from: &str,
    to: &str,
) -> Result<()> {
    let path = format!(
        "/loki/api/v1/label/http_route/values?start={}&end={}",
        enc(from),
        enc(to)
    );
    let deadline = Instant::now() + Duration::from_secs(45);
    let mut last = json!({"data":[],"status":"success"});
    while Instant::now() <= deadline {
        let response = client.get(&path, Some(api_key))?;
        ensure_status(&response, 200, "Loki label values")?;
        let body = parse_json(&response, "Loki label values")?;
        if body.to_string().contains("/smoke") {
            return Ok(());
        }
        last = body;
        thread::sleep(Duration::from_secs(1));
    }
    bail!(
        "Loki label values did not include route after metadata refresh wait: {}",
        last
    )
}

fn verify_empty(client: &Client, api_key: &str, _from: &str, _to: &str) -> Result<()> {
    let trace = client.get(&format!("/api/v2/traces/{TRACE_ID}"), Some(api_key))?;
    ensure_status(&trace, 200, "Tempo trace")?;
    let trace = parse_json(&trace, "Tempo trace")?;
    if trace.to_string().contains(TRACE_ID) {
        bail!("expected reset state to have no fixture trace spans, got {trace}");
    }
    Ok(())
}

fn ensure_success(value: &Value, label: &str) -> Result<()> {
    if value.get("status").and_then(Value::as_str) != Some("success") {
        bail!("{label} did not return success envelope: {value}");
    }
    Ok(())
}

fn enc(value: &str) -> String {
    value
        .bytes()
        .flat_map(|b| match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                vec![b as char]
            }
            b' ' => vec!['+'],
            other => format!("%{other:02X}").chars().collect(),
        })
        .collect()
}

fn ensure_text(value: &Value, needle: &str, message: &str) -> Result<()> {
    if !value.to_string().contains(needle) {
        bail!("{message}: {value}");
    }
    Ok(())
}
