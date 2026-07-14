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

use crate::store::{AsyncWriteReceipt, Store, StoreWriteError};

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
    decode_and_handle::<ExportTraceServiceRequest, ExportTraceServiceResponse, _>(
        body,
        move |request| state.store.try_ingest_traces(request),
    )
    .await
}

async fn export_logs(State(state): State<IngestState>, body: Bytes) -> impl IntoResponse {
    decode_and_handle::<ExportLogsServiceRequest, ExportLogsServiceResponse, _>(
        body,
        move |request| state.store.try_ingest_logs(request),
    )
    .await
}

async fn export_metrics(State(state): State<IngestState>, body: Bytes) -> impl IntoResponse {
    decode_and_handle::<ExportMetricsServiceRequest, ExportMetricsServiceResponse, _>(
        body,
        move |request| state.store.try_ingest_metrics(request),
    )
    .await
}

async fn decode_and_handle<Req, Resp, F>(body: Bytes, handler: F) -> axum::response::Response
where
    Req: Message + Default,
    Resp: Message + Default,
    F: FnOnce(Req) -> Result<AsyncWriteReceipt<usize>>,
{
    match Req::decode(body) {
        Ok(request) => match handler(request) {
            Ok(receipt) => match receipt.wait().await {
                Ok(_) => {
                    let response = Resp::default();
                    let mut headers = HeaderMap::new();
                    headers.insert(
                        axum::http::header::CONTENT_TYPE,
                        HeaderValue::from_static("application/x-protobuf"),
                    );
                    (StatusCode::OK, headers, response.encode_to_vec()).into_response()
                }
                Err(err) => store_http_error(err),
            },
            Err(err) => store_http_error(err),
        },
        Err(err) => (StatusCode::BAD_REQUEST, err.to_string()).into_response(),
    }
}

fn store_http_error(err: anyhow::Error) -> axum::response::Response {
    let status = match err.downcast_ref::<StoreWriteError>() {
        Some(
            StoreWriteError::Overloaded
            | StoreWriteError::Unavailable
            | StoreWriteError::OutcomeUnknown,
        ) => StatusCode::SERVICE_UNAVAILABLE,
        None => StatusCode::INTERNAL_SERVER_ERROR,
    };
    (status, err.to_string()).into_response()
}

fn store_status(err: anyhow::Error) -> Status {
    match err.downcast_ref::<StoreWriteError>() {
        Some(
            StoreWriteError::Overloaded
            | StoreWriteError::Unavailable
            | StoreWriteError::OutcomeUnknown,
        ) => Status::unavailable(err.to_string()),
        None => Status::internal(err.to_string()),
    }
}

async fn wait_for_shutdown(mut shutdown: watch::Receiver<bool>) {
    while shutdown.changed().await.is_ok() {
        if *shutdown.borrow() {
            break;
        }
    }
}

#[tonic::async_trait]
impl TraceService for IngestState {
    async fn export(
        &self,
        request: Request<ExportTraceServiceRequest>,
    ) -> std::result::Result<Response<ExportTraceServiceResponse>, Status> {
        self.store
            .try_ingest_traces(request.into_inner())
            .map_err(store_status)?
            .wait()
            .await
            .map(|_| Response::new(ExportTraceServiceResponse::default()))
            .map_err(store_status)
    }
}

#[tonic::async_trait]
impl LogsService for IngestState {
    async fn export(
        &self,
        request: Request<ExportLogsServiceRequest>,
    ) -> std::result::Result<Response<ExportLogsServiceResponse>, Status> {
        self.store
            .try_ingest_logs(request.into_inner())
            .map_err(store_status)?
            .wait()
            .await
            .map(|_| Response::new(ExportLogsServiceResponse::default()))
            .map_err(store_status)
    }
}

#[tonic::async_trait]
impl MetricsService for IngestState {
    async fn export(
        &self,
        request: Request<ExportMetricsServiceRequest>,
    ) -> std::result::Result<Response<ExportMetricsServiceResponse>, Status> {
        self.store
            .try_ingest_metrics(request.into_inner())
            .map_err(store_status)?
            .wait()
            .await
            .map(|_| Response::new(ExportMetricsServiceResponse::default()))
            .map_err(store_status)
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::mpsc,
        task::Poll,
        thread,
        time::{Duration, Instant, SystemTime, UNIX_EPOCH},
    };

    use anyhow::anyhow;
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
    use tonic::{Code, transport::Channel};
    use tower::ServiceExt;

    use crate::store::{Store, StoreWriteError};

    use super::{IngestState, export_traces, serve_grpc_listener, store_http_error, store_status};

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

    #[test]
    fn writer_lifecycle_errors_are_retryable_and_operation_errors_are_internal() {
        for error in [
            StoreWriteError::Overloaded,
            StoreWriteError::Unavailable,
            StoreWriteError::OutcomeUnknown,
        ] {
            assert_eq!(
                store_http_error(anyhow!(error)).status(),
                StatusCode::SERVICE_UNAVAILABLE
            );
            assert_eq!(store_status(anyhow!(error)).code(), Code::Unavailable);
        }

        assert_eq!(
            store_http_error(anyhow!("sqlite operation failed")).status(),
            StatusCode::INTERNAL_SERVER_ERROR
        );
        assert_eq!(
            store_status(anyhow!("sqlite operation failed")).code(),
            Code::Internal
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn http_handler_yields_while_an_admitted_write_waits_for_the_owner() {
        let now = now_nanos() as u64;
        let tempdir = tempdir().unwrap();
        let store = Store::open(&tempdir.path().join("ottyel.db"), 24, 1000).unwrap();
        let app = Router::new()
            .route("/v1/traces", post(export_traces))
            .with_state(IngestState {
                store: store.clone(),
            });
        let (entered_sender, entered_receiver) = mpsc::channel();
        let (release_sender, release_receiver) = mpsc::channel();
        let blocking_store = store.clone();
        let blocker = thread::spawn(move || {
            blocking_store.execute_write_for_test(move |_| {
                entered_sender.send(()).unwrap();
                release_receiver.recv().unwrap();
                Ok(())
            })
        });
        entered_receiver
            .recv_timeout(Duration::from_secs(2))
            .unwrap();

        let request = Request::post("/v1/traces")
            .header("content-type", "application/x-protobuf")
            .body(Body::from(trace_export_request(now).encode_to_vec()))
            .unwrap();
        let mut response_future = Box::pin(app.oneshot(request));
        let watchdog_release = release_sender.clone();
        let (cancel_sender, cancel_receiver) = mpsc::channel();
        let watchdog = thread::spawn(move || {
            if cancel_receiver
                .recv_timeout(Duration::from_millis(500))
                .is_err()
            {
                let _ = watchdog_release.send(());
            }
        });
        let started = Instant::now();
        let first_poll = futures::poll!(response_future.as_mut());
        let poll_duration = started.elapsed();

        if matches!(first_poll, Poll::Ready(_)) {
            let _ = cancel_sender.send(());
            let _ = release_sender.send(());
            watchdog.join().unwrap();
            blocker.join().unwrap().unwrap();
            if poll_duration >= Duration::from_millis(250) {
                panic!("HTTP handler blocked the current-thread runtime until SQLite completed");
            }
            panic!("HTTP handler completed before the gated writer was released");
        }
        assert!(poll_duration < Duration::from_millis(250));

        cancel_sender.send(()).unwrap();
        release_sender.send(()).unwrap();
        let response = tokio::time::timeout(Duration::from_secs(2), response_future)
            .await
            .unwrap()
            .unwrap();
        watchdog.join().unwrap();
        blocker.join().unwrap().unwrap();
        assert_eq!(response.status(), StatusCode::OK);
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
