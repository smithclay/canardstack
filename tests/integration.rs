use canardstack::{http, AppState, Config};
use serde_json::Value;
use std::collections::HashMap;
use tempfile::tempdir;

fn app() -> (tempfile::TempDir, AppState) {
    let dir = tempdir().unwrap();
    let mut config = Config::test(dir.path().join("canardstack.duckdb"));
    config.operator.local_storage_dir = dir.path().join("storage");
    (dir, AppState::new(config).unwrap())
}

fn app_with_metric_without_summary() -> (tempfile::TempDir, AppState) {
    let dir = tempdir().unwrap();
    let mut config = Config::test(dir.path().join("canardstack.duckdb"));
    config.operator.local_storage_dir = dir.path().join("storage");
    let state = AppState::new_with_storage_hook_for_tests(config, |storage| {
        storage
            .with_conn(|conn, prefix| {
                conn.execute_batch(&format!(
                    "\
                    INSERT INTO {prefix}otlp_metrics_sum \
                      (time_unix_nano, start_time_unix_nano, name, description, unit, int_value, \
                       double_value, service_name, resource_attributes, metric_attributes, \
                       aggregation_temporality, is_monotonic) \
                    VALUES \
                      (current_timestamp::TIMESTAMP_NS, current_timestamp::TIMESTAMP_NS, \
                       'demo.payment.transactions', 'payment transactions', '1', 7, NULL, \
                       'payment', '{{\"deployment.environment\":\"demo\"}}', '{{}}', 2, true);"
                ))?;
                Ok(())
            })
            .unwrap();
    })
    .unwrap();
    (dir, state)
}

fn api_headers() -> HashMap<String, String> {
    HashMap::from([("authorization".to_string(), "Bearer test-key".to_string())])
}

#[test]
fn healthz_reports_storage_readiness() {
    let (_dir, state) = app();

    let response = http::route(
        "GET",
        "/healthz",
        &HashMap::new(),
        &HashMap::new(),
        &[],
        &state,
    );

    assert_eq!(response.status(), 200, "{}", response.json_body());
    assert_eq!(response.json_body()["status"], "ok");
    assert!(response.json_body()["storage"]["healthy"]
        .as_bool()
        .unwrap());
}

#[test]
fn otlp_ingest_routes_are_not_served() {
    let (_dir, state) = app();

    for path in ["/v1/logs", "/v1/traces", "/v1/metrics"] {
        let response = http::route(
            "POST",
            path,
            &HashMap::new(),
            &api_headers(),
            br#"{}"#,
            &state,
        );
        assert_eq!(response.status(), 404, "{path}: {}", response.json_body());
    }
}

#[test]
fn buildinfo_query_route_still_works() {
    let (_dir, state) = app();

    let response = http::route(
        "GET",
        "/api/status/buildinfo",
        &HashMap::new(),
        &api_headers(),
        &[],
        &state,
    );

    assert_eq!(response.status(), 200, "{}", response.json_body());
    assert!(matches!(response.json_body(), Value::Object(_)));
    assert_eq!(response.json_body()["revision"], "canardstack");
}

#[test]
fn prometheus_metric_discovery_falls_back_to_raw_metrics() {
    let (_dir, state) = app_with_metric_without_summary();
    let params = HashMap::new();

    let labels = http::route(
        "GET",
        "/api/v1/label/__name__/values",
        &params,
        &api_headers(),
        &[],
        &state,
    );
    assert_eq!(labels.status(), 200, "{}", labels.json_body());
    assert_eq!(
        labels.json_body()["data"],
        serde_json::json!(["demo.payment.transactions"])
    );

    let series = http::route(
        "GET",
        "/api/v1/series",
        &params,
        &api_headers(),
        &[],
        &state,
    );
    assert_eq!(series.status(), 200, "{}", series.json_body());
    assert_eq!(
        series.json_body()["data"],
        serde_json::json!([{
            "__name__": "demo.payment.transactions",
            "deployment_environment": "demo",
            "service_name": "payment"
        }])
    );
}
