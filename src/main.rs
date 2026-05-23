use canardstack::cli::{healthcheck, smoke, smoke_http};
use canardstack::config::ServeRole;
use canardstack::{http, init_logging, storage, AppState, Config, Scheduler};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

static SHUTDOWN_REQUESTED: AtomicBool = AtomicBool::new(false);

fn main() -> anyhow::Result<()> {
    init_logging();
    let mut args = std::env::args().skip(1);
    match args.next().as_deref() {
        Some("smoke") => smoke::run(),
        Some("smoke-http") => smoke_http::run(args),
        Some("healthcheck") => healthcheck::run(args.next()),
        Some("install-ducklake-extension") => {
            let config = Config::from_env()?;
            storage::install_ducklake_extension(config.duckdb_extension_dir.as_deref())
        }
        Some("serve") | None => {
            install_shutdown_signal_handlers();
            let role = parse_serve_role(args)?;
            let mut config = Config::from_env()?;
            config.serve_role = role;
            config.validate()?;
            let state = Arc::new(AppState::new(config)?);
            if !state.config.scheduler_enabled {
                tracing::warn!(
                    event = "scheduler_disabled",
                    "scheduler disabled; ingest workers still accept and buffer requests, but the scheduler is the single seal driver, so buffered rows will not be sealed to DuckLake (and maintenance will not run) until it is enabled or an admin seal is issued"
                );
            }
            let _scheduler = state
                .config
                .scheduler_enabled
                .then_some(())
                .filter(|_| state.config.serve_role.runs_scheduler())
                .map(|_| Scheduler::spawn(state.clone()));
            http::serve_until(state, &SHUTDOWN_REQUESTED)
        }
        Some(other) => anyhow::bail!("unknown command {other}; use serve, smoke, smoke-http, healthcheck, or install-ducklake-extension"),
    }
}

fn parse_serve_role(mut args: impl Iterator<Item = String>) -> anyhow::Result<ServeRole> {
    let mut role = ServeRole::All;
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--role" => {
                let value = args
                    .next()
                    .ok_or_else(|| anyhow::anyhow!("--role requires all, ingest, or query"))?;
                role = ServeRole::parse(&value)?;
            }
            "--role=all" => role = ServeRole::All,
            "--role=ingest" => role = ServeRole::Ingest,
            "--role=query" => role = ServeRole::Query,
            "--help" | "-h" => {
                anyhow::bail!("usage: canardstack serve [--role all|ingest|query]");
            }
            other => {
                anyhow::bail!(
                    "unknown serve option {other}; usage: canardstack serve [--role all|ingest|query]"
                );
            }
        }
    }
    Ok(role)
}

extern "C" fn request_shutdown(_signal: i32) {
    SHUTDOWN_REQUESTED.store(true, Ordering::SeqCst);
}

#[cfg(unix)]
fn install_shutdown_signal_handlers() {
    const SIGINT: i32 = 2;
    const SIGTERM: i32 = 15;
    unsafe extern "C" {
        fn signal(signal: i32, handler: extern "C" fn(i32)) -> extern "C" fn(i32);
    }
    unsafe {
        let _ = signal(SIGINT, request_shutdown);
        let _ = signal(SIGTERM, request_shutdown);
    }
}

#[cfg(not(unix))]
fn install_shutdown_signal_handlers() {}
