use serde_json::{json, Value};

pub fn log_fixture(now_nanos: i64) -> Value {
    json!({
        "resourceLogs": [{
            "resource": {"attributes": [
                {"key": "service.name", "value": {"stringValue": "checkout"}},
                {"key": "deployment.environment", "value": {"stringValue": "dev"}}
            ]},
            "scopeLogs": [{
                "scope": {"name": "smoke", "version": "1"},
                "logRecords": [{
                    "timeUnixNano": now_nanos.to_string(),
                    "observedTimeUnixNano": now_nanos.to_string(),
                    "severityNumber": 17,
                    "severityText": "ERROR",
                    "traceId": "11111111111111111111111111111111",
                    "spanId": "2222222222222222",
                    "body": {"stringValue": "smoke payment timeout"},
                    "attributes": [
                        {"key": "http.route", "value": {"stringValue": "/smoke"}},
                        {"key": "exception.type", "value": {"stringValue": "SmokeTimeout"}}
                    ]
                }]
            }]
        }]
    })
}

pub fn trace_fixture(now_nanos: i64) -> Value {
    json!({
        "resourceSpans": [{
            "resource": {"attributes": [
                {"key": "service.name", "value": {"stringValue": "checkout"}},
                {"key": "deployment.environment", "value": {"stringValue": "dev"}}
            ]},
            "scopeSpans": [{
                "scope": {"name": "smoke", "version": "1"},
                "spans": [{
                    "traceId": "11111111111111111111111111111111",
                    "spanId": "2222222222222222",
                    "parentSpanId": "",
                    "name": "GET /smoke",
                    "kind": 2,
                    "startTimeUnixNano": now_nanos.to_string(),
                    "endTimeUnixNano": (now_nanos + 25_000_000).to_string(),
                    "status": {"code": 2, "message": "smoke timeout"},
                    "attributes": [
                        {"key": "http.request.method", "value": {"stringValue": "GET"}},
                        {"key": "http.response.status_code", "value": {"intValue": "500"}},
                        {"key": "http.route", "value": {"stringValue": "/smoke"}},
                        {"key": "exception.type", "value": {"stringValue": "SmokeTimeout"}}
                    ]
                }]
            }]
        }]
    })
}

pub fn metric_fixture(now_nanos: i64) -> Value {
    json!({
        "resourceMetrics": [{
            "resource": {"attributes": [
                {"key": "service.name", "value": {"stringValue": "checkout"}},
                {"key": "deployment.environment", "value": {"stringValue": "dev"}}
            ]},
            "scopeMetrics": [{
                "scope": {"name": "smoke", "version": "1"},
                "metrics": [{
                    "name": "smoke.gauge",
                    "description": "smoke gauge",
                    "unit": "1",
                    "gauge": {"dataPoints": [{
                        "timeUnixNano": now_nanos.to_string(),
                        "asDouble": 42.0,
                        "attributes": [{"key": "route", "value": {"stringValue": "/smoke"}}]
                    }]}
                }, {
                    "name": "smoke.sum",
                    "sum": {"aggregationTemporality": 2, "isMonotonic": true, "dataPoints": [{
                        "timeUnixNano": now_nanos.to_string(),
                        "asInt": "7"
                    }]}
                }]
            }]
        }]
    })
}
