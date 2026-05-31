pub mod admission_control;
pub mod app;
pub mod cli;
pub mod compat;
pub mod config;
mod db;
pub mod http;
pub mod logging;
pub mod metadata;
pub mod metrics;
pub mod query;
pub(crate) mod semantic_labels;
pub mod signal;
pub mod storage;
#[cfg(feature = "tls")]
pub mod tls;
pub mod validation;

pub use app::AppState;
pub use config::Config;
pub use logging::init_logging;

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
