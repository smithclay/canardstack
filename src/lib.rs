pub mod app;
pub mod cli;
pub mod compat;
pub mod config;
mod db;
pub mod http;
pub mod ingest;
pub mod log_query;
pub mod logging;
pub mod maintenance;
pub mod memory;
pub mod metadata;
pub mod metrics;
pub mod otlp;
pub mod promql;
pub mod query;
pub mod query_plan;
mod runtime;
pub mod sql;
pub mod storage;
pub mod trace_query;
pub mod validation;

pub use app::AppState;
pub use config::Config;
pub use logging::init_logging;
pub use maintenance::Scheduler;

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
