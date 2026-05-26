use canardstack::cli::{healthcheck, serve_catalog, smoke, smoke_http};
use canardstack::config::ServeRole;
#[cfg(feature = "tls")]
use canardstack::config::TlsMode;
use canardstack::{http, init_logging, storage, AppState, Config, Scheduler};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
#[cfg(feature = "tls")]
use std::thread;

static SHUTDOWN_REQUESTED: AtomicBool = AtomicBool::new(false);

fn main() -> anyhow::Result<()> {
    init_logging();
    let mut args = std::env::args().skip(1);
    match args.next().as_deref() {
        Some("--version") | Some("-V") => {
            println!("canardstack {}", env!("CARGO_PKG_VERSION"));
            Ok(())
        }
        Some("smoke") => smoke::run(),
        Some("smoke-http") => smoke_http::run(args),
        Some("healthcheck") => healthcheck::run(args.next()),
        Some("install-ducklake-extension") => {
            let config = Config::from_env()?;
            storage::install_ducklake_extension(config.operator.duckdb_extension_dir.as_deref())
        }
        Some("serve-catalog") => {
            install_shutdown_signal_handlers();
            serve_catalog::run(args, &SHUTDOWN_REQUESTED)
        }
        Some("serve") | None => {
            install_shutdown_signal_handlers();
            let role = parse_serve_role(args)?;
            let mut config = Config::from_env()?;
            config.operator.serve_role = role;
            config.validate()?;
            #[cfg(feature = "tls")]
            let tls_frontend = prepare_serve_tls(&mut config)?;
            #[cfg(not(feature = "tls"))]
            if config.operator.tls.enabled {
                anyhow::bail!("CANARDSTACK_TLS_ENABLED=true requires a build with --features tls");
            }
            #[cfg(not(feature = "grpc"))]
            if config.operator.grpc.enabled {
                anyhow::bail!(
                    "CANARDSTACK_GRPC_ENABLED=true requires a build with --features grpc"
                );
            }
            let state = Arc::new(AppState::new(config)?);
            if !state.config.operator.scheduler_enabled {
                tracing::warn!(
                    event = "scheduler_disabled",
                    "scheduler disabled; ingest workers still accept and buffer requests, but the scheduler is the single seal driver, so buffered rows will not be flushed to DuckLake (and maintenance will not run) until it is enabled or an admin seal is issued"
                );
            }
            let _scheduler = state
                .config
                .operator.scheduler_enabled
                .then_some(())
                .filter(|_| state.config.operator.serve_role.runs_scheduler())
                .map(|_| Scheduler::spawn(state.clone()));
            #[cfg(feature = "tls")]
            if let Some((public, backend, identity)) = tls_frontend {
                thread::spawn(move || {
                    if let Err(err) = canardstack::tls::run_tls_terminator(
                        &public,
                        backend,
                        identity,
                        "serve",
                        &SHUTDOWN_REQUESTED,
                    ) {
                        tracing::error!(event = "serve_tls_terminator_failed", error = %err);
                    }
                });
            }
            #[cfg(feature = "grpc")]
            let _grpc_server = if state.config.operator.grpc.enabled {
                Some(canardstack::grpc::spawn(state.clone(), &SHUTDOWN_REQUESTED)?)
            } else {
                None
            };
            http::serve_until(state, &SHUTDOWN_REQUESTED)
        }
        Some(other) => anyhow::bail!("unknown command {other}; use --version, serve, serve-catalog, smoke, smoke-http, healthcheck, or install-ducklake-extension"),
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

#[cfg(feature = "tls")]
fn prepare_serve_tls(
    config: &mut Config,
) -> anyhow::Result<Option<(String, String, canardstack::tls::TlsIdentity)>> {
    if !config.operator.tls.enabled {
        return Ok(None);
    }
    let public = config.operator.bind.clone();
    let backend = config.operator.tls.backend_bind.clone();
    let identity = match config.operator.tls.mode {
        TlsMode::File => canardstack::tls::TlsIdentity::File {
            cert_file: config
                .operator
                .tls
                .cert_file
                .clone()
                .expect("validated TLS cert_file"),
            key_file: config
                .operator
                .tls
                .key_file
                .clone()
                .expect("validated TLS key_file"),
        },
        TlsMode::EphemeralSelfSigned => {
            tracing::warn!(
                event = "serve_tls_ephemeral_self_signed",
                "serve TLS is using an ephemeral self-signed certificate; clients must trust it explicitly or skip verification"
            );
            canardstack::tls::TlsIdentity::EphemeralSelfSigned {
                subject_alt_names: vec!["localhost".to_string()],
            }
        }
    };
    config.operator.bind = backend.clone();
    Ok(Some((public, backend, identity)))
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
