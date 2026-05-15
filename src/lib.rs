pub mod app;
pub mod cli;
pub mod compat;
pub mod config;
pub mod http;
pub mod ingest;
pub mod log_query;
pub mod maintenance;
pub mod memory;
pub mod metrics;
pub mod otlp;
pub mod promql;
pub mod query;
pub mod query_plan;
pub mod sql;
pub mod storage;
pub mod trace_query;
pub mod ui;
pub mod validation;

pub use app::AppState;
pub use config::Config;
pub use maintenance::Scheduler;

/// Logfmt-style structured event to stderr. Values containing whitespace,
/// `=`, or `"` are quoted with `\"` escaping.
pub fn log_event(level: &str, event: &str, fields: &[(&str, &str)]) {
    use std::fmt::Write;
    let mut buf = format!("level={level} event={event}");
    for (k, v) in fields {
        if v.is_empty() || v.chars().any(|c| c.is_whitespace() || c == '=' || c == '"') {
            let escaped = v.replace('\\', "\\\\").replace('"', "\\\"");
            let _ = write!(&mut buf, " {k}=\"{escaped}\"");
        } else {
            let _ = write!(&mut buf, " {k}={v}");
        }
    }
    eprintln!("{buf}");
}

/// Lock a `Mutex`, proceeding past `PoisonError` rather than re-panicking.
/// Stops a single poisoned lock from cascading every request into a panic.
pub trait LockExt<T> {
    fn lock_or_poisoned(&self) -> std::sync::MutexGuard<'_, T>;
}

impl<T> LockExt<T> for std::sync::Mutex<T> {
    fn lock_or_poisoned(&self) -> std::sync::MutexGuard<'_, T> {
        self.lock().unwrap_or_else(|e| e.into_inner())
    }
}
