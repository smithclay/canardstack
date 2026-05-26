use crate::ingest::OtlpRequestKind;
use crate::validation;
use crate::AppState;
use prost::Message;
use std::collections::HashMap;
use std::sync::Arc;
use tonic::metadata::MetadataMap;
use tonic::{Request, Response, Status};

use super::status;

use opentelemetry_proto::tonic::collector::logs::v1::logs_service_server::LogsService;
use opentelemetry_proto::tonic::collector::logs::v1::{
    ExportLogsServiceRequest, ExportLogsServiceResponse,
};
use opentelemetry_proto::tonic::collector::metrics::v1::metrics_service_server::MetricsService;
use opentelemetry_proto::tonic::collector::metrics::v1::{
    ExportMetricsServiceRequest, ExportMetricsServiceResponse,
};
use opentelemetry_proto::tonic::collector::trace::v1::trace_service_server::TraceService;
use opentelemetry_proto::tonic::collector::trace::v1::{
    ExportTraceServiceRequest, ExportTraceServiceResponse,
};

#[derive(Clone)]
pub(crate) struct OtlpGrpcService {
    state: Arc<AppState>,
}

impl OtlpGrpcService {
    pub(crate) fn new(state: Arc<AppState>) -> Self {
        Self { state }
    }

    async fn export<M>(&self, route: OtlpRequestKind, request: Request<M>) -> Result<(), Status>
    where
        M: Message + Send + 'static,
    {
        let headers = metadata_headers(request.metadata())?;
        let state = Arc::clone(&self.state);
        let body = request.into_inner().encode_to_vec();
        tokio::task::spawn_blocking(move || ingest(route, headers, body, state))
            .await
            .map_err(|err| Status::internal(format!("gRPC ingest task failed: {err}")))?
    }
}

#[tonic::async_trait]
impl LogsService for OtlpGrpcService {
    async fn export(
        &self,
        request: Request<ExportLogsServiceRequest>,
    ) -> Result<Response<ExportLogsServiceResponse>, Status> {
        self.export(OtlpRequestKind::Logs, request).await?;
        Ok(Response::new(ExportLogsServiceResponse {
            partial_success: None,
        }))
    }
}

#[tonic::async_trait]
impl TraceService for OtlpGrpcService {
    async fn export(
        &self,
        request: Request<ExportTraceServiceRequest>,
    ) -> Result<Response<ExportTraceServiceResponse>, Status> {
        self.export(OtlpRequestKind::Traces, request).await?;
        Ok(Response::new(ExportTraceServiceResponse {
            partial_success: None,
        }))
    }
}

#[tonic::async_trait]
impl MetricsService for OtlpGrpcService {
    async fn export(
        &self,
        request: Request<ExportMetricsServiceRequest>,
    ) -> Result<Response<ExportMetricsServiceResponse>, Status> {
        self.export(OtlpRequestKind::Metrics, request).await?;
        Ok(Response::new(ExportMetricsServiceResponse {
            partial_success: None,
        }))
    }
}

fn ingest(
    route: OtlpRequestKind,
    headers: HashMap<String, String>,
    body: Vec<u8>,
    state: Arc<AppState>,
) -> Result<(), Status> {
    if !state.config.operator.serve_role.accepts_ingest() {
        return Err(Status::not_found(
            "gRPC ingest routes are disabled for this serve role",
        ));
    }
    validation::validate_api_key(&headers, &state.config, false).map_err(status::from_api_error)?;
    state
        .ingestor
        .ingest(
            route,
            &headers,
            body,
            &state.storage,
            &state.admission,
            state.metrics.clone(),
        )
        .map(|_| ())
        .map_err(status::from_api_error)
}

fn metadata_headers(metadata: &MetadataMap) -> Result<HashMap<String, String>, Status> {
    let mut headers = HashMap::new();
    for key in ["authorization", "x-api-key"] {
        if let Some(value) = metadata.get(key) {
            let value = value
                .to_str()
                .map_err(|_| Status::unauthenticated(format!("{key} metadata is not ASCII")))?;
            headers.insert(key.to_string(), value.to_string());
        }
    }
    headers.insert(
        "content-type".to_string(),
        "application/x-protobuf".to_string(),
    );
    Ok(headers)
}
