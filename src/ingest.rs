use std::net::SocketAddr;

use anyhow::{Context, Result};
use axum::{
    Router,
    body::Bytes,
    extract::State,
    http::{HeaderMap, HeaderValue, StatusCode},
    response::IntoResponse,
    routing::post,
};
use opentelemetry_proto::tonic::collector::{
    logs::v1::{
        ExportLogsServiceRequest, ExportLogsServiceResponse,
        logs_service_server::{LogsService, LogsServiceServer},
    },
    metrics::v1::{
        ExportMetricsServiceRequest, ExportMetricsServiceResponse,
        metrics_service_server::{MetricsService, MetricsServiceServer},
    },
    trace::v1::{
        ExportTraceServiceRequest, ExportTraceServiceResponse,
        trace_service_server::{TraceService, TraceServiceServer},
    },
};
use prost::Message;
use tokio::sync::watch;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{Request, Response, Status, transport::Server};

use crate::store::Store;

#[derive(Clone)]
struct IngestState {
    store: Store,
}

pub async fn serve(
    http_bind: &str,
    grpc_bind: &str,
    store: Store,
    shutdown: watch::Receiver<bool>,
) -> Result<()> {
    let http_addr: SocketAddr = http_bind
        .parse()
        .with_context(|| format!("invalid HTTP bind addr {http_bind}"))?;
    let grpc_addr: SocketAddr = grpc_bind
        .parse()
        .with_context(|| format!("invalid gRPC bind addr {grpc_bind}"))?;
    let state = IngestState { store };

    let http_listener = tokio::net::TcpListener::bind(http_addr).await?;
    let grpc_listener = tokio::net::TcpListener::bind(grpc_addr).await?;

    tokio::try_join!(
        serve_http_listener(http_listener, state.clone(), shutdown.clone()),
        serve_grpc_listener(grpc_listener, state, shutdown),
    )?;
    Ok(())
}

async fn serve_http_listener(
    listener: tokio::net::TcpListener,
    state: IngestState,
    shutdown: watch::Receiver<bool>,
) -> Result<()> {
    let app = Router::new()
        .route("/v1/traces", post(export_traces))
        .route("/v1/logs", post(export_logs))
        .route("/v1/metrics", post(export_metrics))
        .with_state(state);

    axum::serve(listener, app)
        .with_graceful_shutdown(wait_for_shutdown(shutdown))
        .await?;
    Ok(())
}

async fn serve_grpc_listener(
    listener: tokio::net::TcpListener,
    state: IngestState,
    shutdown: watch::Receiver<bool>,
) -> Result<()> {
    let incoming = TcpListenerStream::new(listener);

    Server::builder()
        .add_service(TraceServiceServer::new(state.clone()))
        .add_service(LogsServiceServer::new(state.clone()))
        .add_service(MetricsServiceServer::new(state))
        .serve_with_incoming_shutdown(incoming, wait_for_shutdown(shutdown))
        .await?;
    Ok(())
}

async fn export_traces(State(state): State<IngestState>, body: Bytes) -> impl IntoResponse {
    decode_and_handle::<ExportTraceServiceRequest, _, _>(body, move |request| {
        state
            .store
            .ingest_traces(request)
            .map(|_| ExportTraceServiceResponse::default())
    })
}

async fn export_logs(State(state): State<IngestState>, body: Bytes) -> impl IntoResponse {
    decode_and_handle::<ExportLogsServiceRequest, _, _>(body, move |request| {
        state
            .store
            .ingest_logs(request)
            .map(|_| ExportLogsServiceResponse::default())
    })
}

async fn export_metrics(State(state): State<IngestState>, body: Bytes) -> impl IntoResponse {
    decode_and_handle::<ExportMetricsServiceRequest, _, _>(body, move |request| {
        state
            .store
            .ingest_metrics(request)
            .map(|_| ExportMetricsServiceResponse::default())
    })
}

fn decode_and_handle<Req, Resp, F>(body: Bytes, handler: F) -> impl IntoResponse
where
    Req: Message + Default,
    Resp: Message + Default,
    F: FnOnce(Req) -> Result<Resp>,
{
    match Req::decode(body) {
        Ok(request) => match handler(request) {
            Ok(response) => {
                let mut headers = HeaderMap::new();
                headers.insert(
                    axum::http::header::CONTENT_TYPE,
                    HeaderValue::from_static("application/x-protobuf"),
                );
                (StatusCode::OK, headers, response.encode_to_vec()).into_response()
            }
            Err(err) => (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()).into_response(),
        },
        Err(err) => (StatusCode::BAD_REQUEST, err.to_string()).into_response(),
    }
}

async fn wait_for_shutdown(mut shutdown: watch::Receiver<bool>) {
    while shutdown.changed().await.is_ok() {
        if *shutdown.borrow() {
            break;
        }
    }
}

fn internal_status(err: anyhow::Error) -> Status {
    Status::internal(err.to_string())
}

#[tonic::async_trait]
impl TraceService for IngestState {
    async fn export(
        &self,
        request: Request<ExportTraceServiceRequest>,
    ) -> std::result::Result<Response<ExportTraceServiceResponse>, Status> {
        self.store
            .ingest_traces(request.into_inner())
            .map(|_| Response::new(ExportTraceServiceResponse::default()))
            .map_err(internal_status)
    }
}

#[tonic::async_trait]
impl LogsService for IngestState {
    async fn export(
        &self,
        request: Request<ExportLogsServiceRequest>,
    ) -> std::result::Result<Response<ExportLogsServiceResponse>, Status> {
        self.store
            .ingest_logs(request.into_inner())
            .map(|_| Response::new(ExportLogsServiceResponse::default()))
            .map_err(internal_status)
    }
}

#[tonic::async_trait]
impl MetricsService for IngestState {
    async fn export(
        &self,
        request: Request<ExportMetricsServiceRequest>,
    ) -> std::result::Result<Response<ExportMetricsServiceResponse>, Status> {
        self.store
            .ingest_metrics(request.into_inner())
            .map(|_| Response::new(ExportMetricsServiceResponse::default()))
            .map_err(internal_status)
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    use axum::http::StatusCode;
    use axum::{Router, body::Body, http::Request, routing::post};
    use opentelemetry_proto::tonic::{
        collector::trace::v1::{
            ExportTraceServiceRequest, trace_service_client::TraceServiceClient,
        },
        common::v1::{AnyValue, InstrumentationScope, KeyValue, any_value},
        resource::v1::Resource,
        trace::v1::{ResourceSpans, ScopeSpans, Span, Status},
    };
    use prost::Message;
    use tempfile::tempdir;
    use tonic::transport::Channel;
    use tower::ServiceExt;

    use crate::store::Store;

    use super::{IngestState, export_traces, serve_grpc_listener};

    #[tokio::test]
    async fn traces_endpoint_accepts_otlp_protobuf() {
        let now = now_nanos() as u64;
        let tempdir = tempdir().unwrap();
        let store = Store::open(&tempdir.path().join("ottyel.db"), 24, 1000).unwrap();
        let app = Router::new()
            .route("/v1/traces", post(export_traces))
            .with_state(IngestState {
                store: store.clone(),
            });

        let payload = trace_export_request(now).encode_to_vec();

        let response = app
            .oneshot(
                Request::post("/v1/traces")
                    .header("content-type", "application/x-protobuf")
                    .body(Body::from(payload))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(store.counts(None).unwrap().0, 1);
    }

    #[tokio::test]
    async fn grpc_traces_ingest_through_otlp_service() {
        let now = now_nanos() as u64;
        let tempdir = tempdir().unwrap();
        let store = Store::open(&tempdir.path().join("ottyel.db"), 24, 1000).unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let server = tokio::spawn(serve_grpc_listener(
            listener,
            IngestState {
                store: store.clone(),
            },
            shutdown_rx,
        ));

        let endpoint = format!("http://{addr}");
        let mut client = connect_trace_client(&endpoint).await;
        client.export(trace_export_request(now)).await.unwrap();

        let _ = shutdown_tx.send(true);
        server.await.unwrap().unwrap();

        assert_eq!(store.counts(None).unwrap().0, 1);
    }

    async fn connect_trace_client(endpoint: &str) -> TraceServiceClient<Channel> {
        for _ in 0..10 {
            if let Ok(client) = TraceServiceClient::connect(endpoint.to_string()).await {
                return client;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        TraceServiceClient::connect(endpoint.to_string())
            .await
            .unwrap()
    }

    fn trace_export_request(now: u64) -> ExportTraceServiceRequest {
        ExportTraceServiceRequest {
            resource_spans: vec![ResourceSpans {
                resource: Some(Resource {
                    attributes: vec![KeyValue {
                        key: "service.name".to_string(),
                        value: Some(AnyValue {
                            value: Some(any_value::Value::StringValue("api".to_string())),
                        }),
                    }],
                    dropped_attributes_count: 0,
                    entity_refs: Vec::new(),
                }),
                schema_url: String::new(),
                scope_spans: vec![ScopeSpans {
                    scope: Some(InstrumentationScope::default()),
                    schema_url: String::new(),
                    spans: vec![Span {
                        trace_id: vec![1; 16],
                        span_id: vec![2; 8],
                        parent_span_id: vec![],
                        trace_state: String::new(),
                        name: "request".to_string(),
                        kind: 1,
                        start_time_unix_nano: now,
                        end_time_unix_nano: now + 10,
                        attributes: vec![],
                        dropped_attributes_count: 0,
                        events: vec![],
                        dropped_events_count: 0,
                        links: vec![],
                        dropped_links_count: 0,
                        status: Some(Status {
                            message: String::new(),
                            code: 1,
                        }),
                        flags: 0,
                    }],
                }],
            }],
        }
    }

    fn now_nanos() -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos() as i64
    }
}
