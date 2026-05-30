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
