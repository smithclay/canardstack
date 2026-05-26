use crate::AppState;
use anyhow::Result;
use opentelemetry_proto::tonic::collector::logs::v1::logs_service_server::LogsServiceServer;
use opentelemetry_proto::tonic::collector::metrics::v1::metrics_service_server::MetricsServiceServer;
use opentelemetry_proto::tonic::collector::trace::v1::trace_service_server::TraceServiceServer;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tonic::codec::CompressionEncoding;
use tonic::transport::Server;

use super::otlp::OtlpGrpcService;

pub(crate) async fn serve(
    state: Arc<AppState>,
    bind: SocketAddr,
    shutdown: &'static AtomicBool,
) -> Result<()> {
    let service = OtlpGrpcService::new(Arc::clone(&state));
    let max_body_bytes = state.config.operator.grpc.max_body_bytes;
    tracing::info!(
        event = "grpc_server_listening",
        bind = %bind,
        max_body_bytes,
        "serving OTLP/gRPC"
    );
    Server::builder()
        .add_service(otlp_logs_service(service.clone(), max_body_bytes))
        .add_service(otlp_trace_service(service.clone(), max_body_bytes))
        .add_service(otlp_metrics_service(service, max_body_bytes))
        .serve_with_shutdown(bind, wait_for_shutdown(shutdown))
        .await?;
    Ok(())
}

fn otlp_logs_service(
    service: OtlpGrpcService,
    max_body_bytes: usize,
) -> LogsServiceServer<OtlpGrpcService> {
    LogsServiceServer::new(service)
        .accept_compressed(CompressionEncoding::Gzip)
        .max_decoding_message_size(max_body_bytes)
}

fn otlp_trace_service(
    service: OtlpGrpcService,
    max_body_bytes: usize,
) -> TraceServiceServer<OtlpGrpcService> {
    TraceServiceServer::new(service)
        .accept_compressed(CompressionEncoding::Gzip)
        .max_decoding_message_size(max_body_bytes)
}

fn otlp_metrics_service(
    service: OtlpGrpcService,
    max_body_bytes: usize,
) -> MetricsServiceServer<OtlpGrpcService> {
    MetricsServiceServer::new(service)
        .accept_compressed(CompressionEncoding::Gzip)
        .max_decoding_message_size(max_body_bytes)
}

async fn wait_for_shutdown(shutdown: &'static AtomicBool) {
    while !shutdown.load(Ordering::SeqCst) {
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}
