use super::ducklake::configure_base_connection;
use super::{QueryTimeoutError, Storage};
use crate::db::sql::escape_value;
use crate::LockExt;
use anyhow::{Context, Result};
use duckdb::Connection;
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::Duration;

impl Storage {
    /// Read-side connection access for SELECT-only paths. Do not call into
    /// write paths from inside the closure.
    pub fn with_conn<T>(&self, f: impl FnOnce(&Connection, &str) -> Result<T>) -> Result<T> {
        let conn = self.reader.lock_or_poisoned();
        f(&conn, &self.target_prefix)
    }

    pub fn with_query_conn<T>(
        &self,
        memory_limit: &str,
        timeout: Duration,
        f: impl FnOnce(&Connection, &str) -> Result<T>,
    ) -> Result<T> {
        let conn = self.open_scoped_query_connection()?;
        conn.execute_batch(&format!(
            "SET memory_limit = '{}';",
            escape_value(memory_limit)
        ))?;

        let interrupt = conn.interrupt_handle();
        let state = Arc::new((Mutex::new(false), Condvar::new()));
        let timer_state = state.clone();
        let timer = thread::spawn(move || {
            let (done, cvar) = &*timer_state;
            let done = done.lock_or_poisoned();
            let (done, wait) = cvar.wait_timeout(done, timeout).unwrap();
            if !*done && wait.timed_out() {
                interrupt.interrupt();
                return true;
            }
            false
        });

        let result = f(&conn, &self.target_prefix);
        let (done, cvar) = &*state;
        *done.lock_or_poisoned() = true;
        cvar.notify_one();
        // Timer panic ⇒ "not timed out": trust the query result over a panicked timer.
        let timed_out = match timer.join() {
            Ok(fired) => fired,
            Err(_) => {
                crate::log_event("warn", "query_timer_panicked", &[]);
                false
            }
        };
        // Timer fired ⇒ surface a typed timeout even if the query raced to Ok.
        if timed_out {
            return Err(anyhow::Error::new(QueryTimeoutError { timeout }));
        }
        result
    }

    fn open_scoped_query_connection(&self) -> Result<Connection> {
        // Clone from `reader`, not `writer`: queries don't block on flushes.
        // Attached schemas + extensions are inherited; only per-conn PRAGMAs
        // are reapplied here.
        let cloned = {
            let parent = self.reader.lock_or_poisoned();
            parent.try_clone().context("clone DuckDB connection")?
        };
        configure_base_connection(&cloned)?;
        Ok(cloned)
    }
}
