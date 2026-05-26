use crate::AppState;
use anyhow::{Context, Result};
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::thread;

mod otlp;
mod server;
mod status;

pub fn spawn(
    state: Arc<AppState>,
    shutdown: &'static AtomicBool,
) -> Result<thread::JoinHandle<()>> {
    let bind = state.config.operator.grpc.bind.parse().with_context(|| {
        format!(
            "CANARDSTACK_GRPC_BIND must be a socket address, got {}",
            state.config.operator.grpc.bind
        )
    })?;
    thread::Builder::new()
        .name("canardstack-grpc".to_string())
        .spawn(move || {
            let runtime = match tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .thread_name("canardstack-grpc-runtime")
                .build()
            {
                Ok(runtime) => runtime,
                Err(err) => {
                    tracing::error!(event = "grpc_runtime_failed", error = %err);
                    return;
                }
            };
            if let Err(err) = runtime.block_on(server::serve(state, bind, shutdown)) {
                tracing::error!(event = "grpc_server_failed", error = %err);
            }
        })
        .context("failed to spawn gRPC server thread")
}
