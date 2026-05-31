use canardstack::cli::healthcheck;
#[cfg(feature = "tls")]
use canardstack::config::TlsMode;
use canardstack::{http, init_logging, AppState, Config};
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
        Some("healthcheck") => healthcheck::run(args),
        Some("serve") | None => {
            install_shutdown_signal_handlers();
            let Some(serve_options) = parse_serve_options(args)? else {
                return Ok(());
            };
            let mut config = Config::from_env()?;
            if let Some(listen) = serve_options.listen {
                config.operator.bind = listen;
            }
            config.validate()?;
            #[cfg(feature = "tls")]
            let tls_frontend = prepare_serve_tls(&mut config)?;
            #[cfg(not(feature = "tls"))]
            if config.operator.tls.enabled {
                anyhow::bail!("CANARDSTACK_TLS_ENABLED=true requires a build with --features tls");
            }
            let state = Arc::new(AppState::new(config)?);
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
            http::serve_until(state, &SHUTDOWN_REQUESTED)
        }
        Some(other) => {
            anyhow::bail!("unknown command {other}; use --version, serve, or healthcheck")
        }
    }
}

struct ServeOptions {
    listen: Option<String>,
}

fn parse_serve_options(
    mut args: impl Iterator<Item = String>,
) -> anyhow::Result<Option<ServeOptions>> {
    let mut listen = None;
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--listen" => {
                listen = Some(
                    args.next()
                        .ok_or_else(|| anyhow::anyhow!("{arg} requires an address"))?,
                );
            }
            "--help" | "-h" => {
                println!("usage: canardstack serve [--listen addr]");
                return Ok(None);
            }
            other => {
                if let Some(value) = other.strip_prefix("--listen=") {
                    listen = Some(value.to_string());
                    continue;
                }
                anyhow::bail!(
                    "unknown serve option {other}; usage: canardstack serve [--listen addr]"
                );
            }
        }
    }
    Ok(Some(ServeOptions { listen }))
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
    fn parse_serve_options_accepts_listen_equals() {
        let options =
            parse_serve_options(["--listen=127.0.0.1:9495"].into_iter().map(str::to_string))
                .unwrap()
                .unwrap();

        assert_eq!(options.listen.as_deref(), Some("127.0.0.1:9495"));
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
    fn parse_serve_options_help_is_ok_none() {
        assert!(
            parse_serve_options(["--help"].into_iter().map(str::to_string))
                .unwrap()
                .is_none()
        );
    }
}
