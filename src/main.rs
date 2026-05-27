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
        Some("healthcheck") => healthcheck::run(args),
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
            let Some(serve_options) = parse_serve_options(args)? else {
                return Ok(());
            };
            let mut config = Config::from_env()?;
            config.operator.serve_role = serve_options.role;
            if serve_options.local_catalog_enabled {
                config.operator.local_catalog_enabled = true;
            }
            if let Some(listen) = serve_options.listen {
                config.operator.bind = listen;
            }
            if let Some(listen) = serve_options.local_catalog_listen {
                // Supplying the catalog listener is shorthand for enabling the
                // local catalog mode; otherwise the address would be ignored.
                config.operator.local_catalog_enabled = true;
                config.operator.local_catalog_listen = listen;
            }
            config.validate()?;
            let _local_catalog = prepare_local_catalog(&mut config)?;
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

struct ServeOptions {
    role: ServeRole,
    listen: Option<String>,
    local_catalog_enabled: bool,
    local_catalog_listen: Option<String>,
}

fn parse_serve_options(
    mut args: impl Iterator<Item = String>,
) -> anyhow::Result<Option<ServeOptions>> {
    let mut role = ServeRole::All;
    let mut listen = None;
    let mut local_catalog_enabled = false;
    let mut local_catalog_listen = None;
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--role" => {
                let value = args
                    .next()
                    .ok_or_else(|| anyhow::anyhow!("--role requires all, ingest, or query"))?;
                role = ServeRole::parse(&value)?;
            }
            "--listen" => {
                listen = Some(
                    args.next()
                        .ok_or_else(|| anyhow::anyhow!("{arg} requires an address"))?,
                );
            }
            "--local-catalog" => local_catalog_enabled = true,
            "--catalog-listen" => {
                let listen = args
                    .next()
                    .ok_or_else(|| anyhow::anyhow!("--catalog-listen requires an address"))?;
                local_catalog_listen = Some(listen);
            }
            "--help" | "-h" => {
                println!(
                    "usage: canardstack serve [--role all|ingest|query] [--listen addr] [--local-catalog] [--catalog-listen addr]"
                );
                return Ok(None);
            }
            other => {
                if let Some(value) = other.strip_prefix("--role=") {
                    role = ServeRole::parse(value)?;
                    continue;
                }
                if let Some(value) = other.strip_prefix("--listen=") {
                    listen = Some(value.to_string());
                    continue;
                }
                if let Some(value) = other.strip_prefix("--catalog-listen=") {
                    local_catalog_listen = Some(value.to_string());
                    continue;
                }
                anyhow::bail!(
                    "unknown serve option {other}; usage: canardstack serve [--role all|ingest|query] [--listen addr] [--local-catalog] [--catalog-listen addr]"
                );
            }
        }
    }
    Ok(Some(ServeOptions {
        role,
        listen,
        local_catalog_enabled,
        local_catalog_listen,
    }))
}

fn prepare_local_catalog(
    config: &mut Config,
) -> anyhow::Result<Option<serve_catalog::QuackCatalogEndpoint>> {
    if !config.operator.local_catalog_enabled {
        return Ok(None);
    }
    let listen = config.operator.local_catalog_listen.clone();
    let data_path = config
        .operator
        .ducklake_data_path
        .clone()
        .unwrap_or_else(|| {
            config
                .operator
                .local_storage_dir
                .to_string_lossy()
                .into_owned()
        });
    // Start the loopback Quack catalog endpoint in this process; it owns the
    // sole `Database` handle on the catalog file. Redirect the app's DuckLake
    // ATTACH at that loopback URI so the writer talks Quack to it instead of
    // opening the catalog file directly -- this is the same trick the
    // production `serve-catalog` topology uses, just without a process boundary.
    let endpoint = serve_catalog::start_local_catalog_endpoint(config, &listen)?;
    config.operator.ducklake_attach_uri = Some(format!("ducklake:quack:{}", endpoint.listen()));
    config.operator.ducklake_data_path = Some(data_path.clone());
    config.operator.ducklake_catalog_path = None;
    // Loopback never leaves the process, so the insecure-TLS escape hatch is
    // irrelevant here; reset it so a stale env value can't enable verification
    // bypass on the now-unused TLS path.
    config.operator.ducklake_quack_insecure_tls = false;
    tracing::info!(
        event = "local_catalog_ready",
        attach_uri = %config.operator.ducklake_attach_uri.as_deref().unwrap_or(""),
        data_path = %data_path,
        "local DuckLake catalog is available over Quack for DuckDB clients"
    );
    Ok(Some(endpoint))
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_serve_options_enables_local_catalog_enabled_with_listen() {
        let options = parse_serve_options(
            [
                "--role=query",
                "--local-catalog",
                "--catalog-listen=127.0.0.1:9495",
            ]
            .into_iter()
            .map(str::to_string),
        )
        .unwrap()
        .unwrap();

        assert_eq!(options.role, ServeRole::Query);
        assert!(options.listen.is_none());
        assert!(options.local_catalog_enabled);
        assert_eq!(
            options.local_catalog_listen.as_deref(),
            Some("127.0.0.1:9495")
        );
    }

    #[test]
    fn parse_serve_options_accepts_listen() {
        let options = parse_serve_options(
            ["--listen", "127.0.0.1:4320"]
                .into_iter()
                .map(str::to_string),
        )
        .unwrap();
        let options = options.unwrap();
        assert_eq!(options.listen.as_deref(), Some("127.0.0.1:4320"));

        let options =
            parse_serve_options(["--listen=127.0.0.1:4321"].into_iter().map(str::to_string))
                .unwrap()
                .unwrap();
        assert_eq!(options.listen.as_deref(), Some("127.0.0.1:4321"));
    }

    #[test]
    fn parse_serve_options_accepts_role_equals() {
        let options = parse_serve_options(["--role=query"].into_iter().map(str::to_string))
            .unwrap()
            .unwrap();
        assert_eq!(options.role, ServeRole::Query);
    }

    #[test]
    fn parse_serve_options_help_is_ok_none() {
        assert!(
            parse_serve_options(["--help"].into_iter().map(str::to_string))
                .unwrap()
                .is_none()
        );
    }
}
