mod grpc;
mod http;
mod policy;
mod preflight;

use std::{net::SocketAddr, sync::Arc};

use anyhow::{Context, Result};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, watch};
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{Status, service::interceptor::InterceptedService, transport::Server};

use crate::store::{AsyncWriteReceipt, Store, StoreWriteError};

pub use policy::IngestLimits;

#[derive(Clone)]
struct IngestState {
    store: Store,
    limits: Arc<IngestLimits>,
    admission: Arc<Semaphore>,
}

impl IngestState {
    fn new(store: Store, limits: IngestLimits) -> Self {
        let max_in_flight = limits.max_in_flight;
        Self {
            store,
            limits: Arc::new(limits),
            admission: Arc::new(Semaphore::new(max_in_flight)),
        }
    }
}

pub async fn serve(
    http_bind: &str,
    grpc_bind: &str,
    store: Store,
    limits: IngestLimits,
    shutdown: watch::Receiver<bool>,
) -> Result<()> {
    let http_addr: SocketAddr = http_bind
        .parse()
        .with_context(|| format!("invalid HTTP bind addr {http_bind}"))?;
    let grpc_addr: SocketAddr = grpc_bind
        .parse()
        .with_context(|| format!("invalid gRPC bind addr {grpc_bind}"))?;
    let state = IngestState::new(store, limits);

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
    axum::serve(listener, http::router(state))
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
    let message_limit = state.limits.grpc_message_bytes();
    let request_timeout = state.limits.request_timeout;

    let traces =
        grpc::server::OtlpService::<grpc::server::Traces>::new(state.clone(), message_limit);
    let traces =
        InterceptedService::new(traces, grpc::admission_interceptor(state.admission.clone()));
    let traces = grpc::NormalizeTonicSizeError::new(traces);

    let logs = grpc::server::OtlpService::<grpc::server::Logs>::new(state.clone(), message_limit);
    let logs = InterceptedService::new(logs, grpc::admission_interceptor(state.admission.clone()));
    let logs = grpc::NormalizeTonicSizeError::new(logs);

    let metrics =
        grpc::server::OtlpService::<grpc::server::Metrics>::new(state.clone(), message_limit);
    let metrics = InterceptedService::new(
        metrics,
        grpc::admission_interceptor(state.admission.clone()),
    );
    let metrics = grpc::NormalizeTonicSizeError::new(metrics);

    Server::builder()
        .timeout(request_timeout)
        .add_service(traces)
        .add_service(logs)
        .add_service(metrics)
        .serve_with_incoming_shutdown(incoming, wait_for_shutdown(shutdown))
        .await?;
    Ok(())
}

fn store_status(err: anyhow::Error) -> Status {
    match err.downcast_ref::<StoreWriteError>() {
        Some(StoreWriteError::TooLarge { .. }) => Status::resource_exhausted(err.to_string()),
        Some(
            StoreWriteError::Overloaded
            | StoreWriteError::Unavailable
            | StoreWriteError::OutcomeUnknown,
        ) => Status::unavailable(err.to_string()),
        None => Status::internal(err.to_string()),
    }
}

async fn wait_for_write(
    receipt: AsyncWriteReceipt<usize>,
    permit: OwnedSemaphorePermit,
) -> anyhow::Result<()> {
    // The detached waiter preserves both the write acknowledgement and admission permit if the
    // transport request is cancelled or times out after SQLite has accepted the operation.
    let waiter = tokio::spawn(async move {
        let result = receipt.wait().await.map(|_| ());
        drop(permit);
        result
    });
    waiter.await.map_err(anyhow::Error::from)?
}

async fn wait_for_shutdown(mut shutdown: watch::Receiver<bool>) {
    while shutdown.changed().await.is_ok() {
        if *shutdown.borrow() {
            break;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        num::NonZeroUsize,
        sync::mpsc,
        task::Poll,
        thread,
        time::{Duration, Instant, SystemTime, UNIX_EPOCH},
    };

    use anyhow::anyhow;
    use axum::http::StatusCode;
    use axum::{body::Body, http::Request};
    use opentelemetry_proto::tonic::{
        collector::{
            logs::v1::{ExportLogsServiceRequest, logs_service_client::LogsServiceClient},
            metrics::v1::{
                ExportMetricsServiceRequest, metrics_service_client::MetricsServiceClient,
            },
            trace::v1::{ExportTraceServiceRequest, trace_service_client::TraceServiceClient},
        },
        common::v1::{AnyValue, InstrumentationScope, KeyValue, any_value},
        logs::v1::{LogRecord, ResourceLogs, ScopeLogs},
        metrics::v1::{Gauge, Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics, metric},
        resource::v1::Resource,
        trace::v1::{ResourceSpans, ScopeSpans, Span, Status},
    };
    use prost::Message;
    use tempfile::tempdir;
    use tonic::{Code, codec::CompressionEncoding, transport::Channel};
    use tower::ServiceExt;

    use crate::store::{Store, StoreWriteError, WriterLimitDimension, WriterLimits};

    use super::{
        IngestLimits, IngestState, http as ingest_http, serve_grpc_listener, store_status,
    };

    #[tokio::test]
    async fn traces_endpoint_accepts_otlp_protobuf() {
        let now = now_nanos() as u64;
        let tempdir = tempdir().unwrap();
        let store = Store::open(&tempdir.path().join("ottyel.db"), 24, 1000).unwrap();
        let app = ingest_http::router(IngestState::new(store.clone(), IngestLimits::default()));

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
    async fn http_maps_writer_weight_rejection_to_code_8_without_writing() {
        use http_body_util::BodyExt;

        let now = now_nanos() as u64;
        let tempdir = tempdir().unwrap();
        let store = Store::open_with_writer_limits(
            &tempdir.path().join("ottyel.db"),
            24,
            1000,
            WriterLimits::new(NonZeroUsize::new(1).unwrap(), NonZeroUsize::new(1).unwrap()),
        )
        .unwrap();
        let app = ingest_http::router(IngestState::new(store.clone(), IngestLimits::default()));
        let response = app
            .oneshot(
                Request::post("/v1/traces")
                    .header("content-type", "application/x-protobuf")
                    .body(Body::from(trace_export_request(now).encode_to_vec()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(ingest_http::decode_rpc_status(&body).0, 8);
        assert_eq!(store.counts(None).unwrap().0, 0);
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
            IngestState::new(store.clone(), IngestLimits::default()),
            shutdown_rx,
        ));

        let endpoint = format!("http://{addr}");
        let mut client = connect_trace_client(&endpoint).await;
        client.export(trace_export_request(now)).await.unwrap();

        let _ = shutdown_tx.send(true);
        server.await.unwrap().unwrap();

        assert_eq!(store.counts(None).unwrap().0, 1);
    }

    #[tokio::test]
    async fn grpc_maps_writer_weight_rejection_to_resource_exhausted() {
        let now = now_nanos() as u64;
        let tempdir = tempdir().unwrap();
        let store = Store::open_with_writer_limits(
            &tempdir.path().join("ottyel.db"),
            24,
            1000,
            WriterLimits::new(NonZeroUsize::new(1).unwrap(), NonZeroUsize::new(1).unwrap()),
        )
        .unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let server = tokio::spawn(serve_grpc_listener(
            listener,
            IngestState::new(store.clone(), IngestLimits::default()),
            shutdown_rx,
        ));
        let mut client = connect_trace_client(&format!("http://{addr}")).await;

        let error = client.export(trace_export_request(now)).await.unwrap_err();
        assert_eq!(error.code(), Code::ResourceExhausted);
        let _ = shutdown_tx.send(true);
        server.await.unwrap().unwrap();
        assert_eq!(store.counts(None).unwrap().0, 0);
    }

    #[tokio::test]
    async fn grpc_identity_and_gzip_ingest_succeed_for_all_three_signals() {
        for compressed in [false, true] {
            let now = now_nanos() as u64;
            let tempdir = tempdir().unwrap();
            let store = Store::open(&tempdir.path().join("ottyel.db"), 24, 1000).unwrap();
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
            let server = tokio::spawn(serve_grpc_listener(
                listener,
                IngestState::new(store.clone(), IngestLimits::default()),
                shutdown_rx,
            ));
            let channel = connect_channel(&format!("http://{addr}")).await;

            let traces = TraceServiceClient::new(channel.clone());
            let mut traces = if compressed {
                traces.send_compressed(CompressionEncoding::Gzip)
            } else {
                traces
            };
            traces.export(trace_export_request(now)).await.unwrap();

            let logs = LogsServiceClient::new(channel.clone());
            let mut logs = if compressed {
                logs.send_compressed(CompressionEncoding::Gzip)
            } else {
                logs
            };
            logs.export(log_export_request(now)).await.unwrap();

            let metrics = MetricsServiceClient::new(channel);
            let mut metrics = if compressed {
                metrics.send_compressed(CompressionEncoding::Gzip)
            } else {
                metrics
            };
            metrics.export(metric_export_request(now)).await.unwrap();

            let _ = shutdown_tx.send(true);
            server.await.unwrap().unwrap();
            let counts = store.counts(None).unwrap();
            assert_eq!((counts.0, counts.2, counts.3), (1, 1, 1));
        }
    }

    #[tokio::test]
    async fn tonic_plain_and_gzip_message_overflow_are_resource_exhausted() {
        let now = now_nanos() as u64;
        let tempdir = tempdir().unwrap();
        let store = Store::open(&tempdir.path().join("ottyel.db"), 24, 1000).unwrap();
        let limits = IngestLimits {
            max_wire_bytes: 128,
            max_decompressed_bytes: 4096,
            max_value_bytes: 10_000,
            ..IngestLimits::default()
        };
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let server = tokio::spawn(serve_grpc_listener(
            listener,
            IngestState::new(store.clone(), limits),
            shutdown_rx,
        ));
        let channel = connect_channel(&format!("http://{addr}")).await;
        let mut request = trace_export_request(now);
        request.resource_spans[0].scope_spans[0].spans[0].name = "x".repeat(1024);

        let plain = TraceServiceClient::new(channel.clone())
            .export(request.clone())
            .await
            .unwrap_err();
        assert_eq!(plain.code(), Code::ResourceExhausted);

        let compressed = TraceServiceClient::new(channel)
            .send_compressed(CompressionEncoding::Gzip)
            .export(request)
            .await
            .unwrap_err();
        assert_eq!(compressed.code(), Code::ResourceExhausted);

        let _ = shutdown_tx.send(true);
        server.await.unwrap().unwrap();
        assert_eq!(store.counts(None).unwrap().0, 0);
    }

    #[tokio::test]
    async fn grpc_policy_failure_is_atomic_and_capacity_failure_is_retryable() {
        let now = now_nanos() as u64;
        let tempdir = tempdir().unwrap();
        let store = Store::open(&tempdir.path().join("ottyel.db"), 24, 1000).unwrap();
        let limits = IngestLimits {
            max_in_flight: 1,
            max_value_bytes: 3,
            ..IngestLimits::default()
        };
        let state = IngestState::new(store.clone(), limits);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let server = tokio::spawn(serve_grpc_listener(listener, state.clone(), shutdown_rx));
        let channel = connect_channel(&format!("http://{addr}")).await;

        let policy = TraceServiceClient::new(channel.clone())
            .export(trace_export_request(now))
            .await
            .unwrap_err();
        assert_eq!(policy.code(), Code::ResourceExhausted);
        assert_eq!(store.counts(None).unwrap().0, 0);

        let held = state.admission.clone().acquire_owned().await.unwrap();
        let overload = TraceServiceClient::new(channel)
            .export(ExportTraceServiceRequest::default())
            .await
            .unwrap_err();
        assert_eq!(overload.code(), Code::Unavailable);
        drop(held);

        let _ = shutdown_tx.send(true);
        server.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn grpc_preflight_rejects_budget_prefixes_and_malformed_wire_for_every_signal() {
        use prost::bytes::Bytes;
        use tonic::codegen::http::uri::PathAndQuery;

        let tempdir = tempdir().unwrap();
        let store = Store::open(&tempdir.path().join("ottyel.db"), 24, 1000).unwrap();
        let limits = IngestLimits {
            max_structures: 1,
            ..IngestLimits::default()
        };
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let server = tokio::spawn(serve_grpc_listener(
            listener,
            IngestState::new(store.clone(), limits),
            shutdown_rx,
        ));
        let channel = connect_channel(&format!("http://{addr}")).await;
        let paths = [
            "/opentelemetry.proto.collector.trace.v1.TraceService/Export",
            "/opentelemetry.proto.collector.logs.v1.LogsService/Export",
            "/opentelemetry.proto.collector.metrics.v1.MetricsService/Export",
        ];

        for compressed in [false, true] {
            for path in paths {
                let client = tonic::client::Grpc::new(channel.clone());
                let mut client = if compressed {
                    client.send_compressed(CompressionEncoding::Gzip)
                } else {
                    client
                };
                client.ready().await.unwrap();
                let error = client
                    .unary(
                        tonic::Request::new(Bytes::from_static(&[0x0a, 0x00, 0x0a, 0x00, 0x0f])),
                        PathAndQuery::from_static(path),
                        super::grpc::server::RawClientCodec,
                    )
                    .await
                    .unwrap_err();
                assert_eq!(error.code(), Code::ResourceExhausted);
            }
        }

        for compressed in [false, true] {
            for path in paths {
                let client = tonic::client::Grpc::new(channel.clone());
                let mut client = if compressed {
                    client.send_compressed(CompressionEncoding::Gzip)
                } else {
                    client
                };
                client.ready().await.unwrap();
                let error = client
                    .unary(
                        tonic::Request::new(Bytes::from_static(&[0x0f])),
                        PathAndQuery::from_static(path),
                        super::grpc::server::RawClientCodec,
                    )
                    .await
                    .unwrap_err();
                assert_eq!(error.code(), Code::InvalidArgument);
            }
        }
        assert_eq!(store.counts(None).unwrap(), (0, 0, 0, 0, 0));

        let _ = shutdown_tx.send(true);
        server.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn grpc_unary_export_rejects_a_second_message_before_ingest() {
        use prost::bytes::Bytes;
        use tonic::codegen::http::uri::PathAndQuery;

        let now = now_nanos() as u64;
        for compressed in [false, true] {
            let tempdir = tempdir().unwrap();
            let store = Store::open(&tempdir.path().join("ottyel.db"), 24, 1000).unwrap();
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
            let server = tokio::spawn(serve_grpc_listener(
                listener,
                IngestState::new(store.clone(), IngestLimits::default()),
                shutdown_rx,
            ));
            let channel = connect_channel(&format!("http://{addr}")).await;
            let client = tonic::client::Grpc::new(channel);
            let mut client = if compressed {
                client.send_compressed(CompressionEncoding::Gzip)
            } else {
                client
            };
            client.ready().await.unwrap();
            let messages = tokio_stream::iter([
                Bytes::from(trace_export_request(now).encode_to_vec()),
                Bytes::new(),
            ]);
            let error = client
                .client_streaming(
                    tonic::Request::new(messages),
                    PathAndQuery::from_static(
                        "/opentelemetry.proto.collector.trace.v1.TraceService/Export",
                    ),
                    super::grpc::server::RawClientCodec,
                )
                .await
                .unwrap_err();
            assert_eq!(error.code(), Code::InvalidArgument);
            assert_eq!(store.counts(None).unwrap(), (0, 0, 0, 0, 0));

            let _ = shutdown_tx.send(true);
            server.await.unwrap().unwrap();
        }
    }

    #[tokio::test]
    async fn grpc_writer_timeout_is_retryable_and_retains_permit_until_commit() {
        let now = now_nanos() as u64;
        let tempdir = tempdir().unwrap();
        let store = Store::open(&tempdir.path().join("ottyel.db"), 24, 1000).unwrap();
        let limits = IngestLimits {
            max_in_flight: 1,
            request_timeout: Duration::from_millis(100),
            ..IngestLimits::default()
        };
        let state = IngestState::new(store.clone(), limits);
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

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let server = tokio::spawn(serve_grpc_listener(listener, state.clone(), shutdown_rx));
        let mut client = connect_trace_client(&format!("http://{addr}")).await;

        let first = tokio::time::timeout(
            Duration::from_secs(2),
            client.export(trace_export_request(now)),
        )
        .await;
        let permit_retained_after_timeout = state.admission.available_permits() == 0;

        release_sender.send(()).unwrap();
        blocker.join().unwrap().unwrap();
        for _ in 0..100 {
            if state.admission.available_permits() == 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let permit_released_after_commit = state.admission.available_permits() == 1;
        let committed_after_timeout = store.counts(None).unwrap().0;
        let recovered = client.export(ExportTraceServiceRequest::default()).await;

        let _ = shutdown_tx.send(true);
        server.await.unwrap().unwrap();

        let error = first
            .expect("Tonic server timeout did not bound the request")
            .unwrap_err();
        assert_eq!(error.code(), Code::Cancelled);
        assert!(matches!(
            error.code(),
            Code::Cancelled
                | Code::DeadlineExceeded
                | Code::Aborted
                | Code::OutOfRange
                | Code::Unavailable
                | Code::DataLoss
        ));
        assert!(permit_retained_after_timeout);
        assert!(permit_released_after_commit);
        assert_eq!(committed_after_timeout, 1);
        recovered.unwrap();
    }

    #[test]
    fn writer_lifecycle_errors_are_retryable_and_operation_errors_are_internal() {
        for error in [
            StoreWriteError::Overloaded,
            StoreWriteError::Unavailable,
            StoreWriteError::OutcomeUnknown,
        ] {
            assert_eq!(
                ingest_http::store_error(anyhow!(error)).status(),
                StatusCode::SERVICE_UNAVAILABLE
            );
            assert_eq!(store_status(anyhow!(error)).code(), Code::Unavailable);
        }

        assert_eq!(
            ingest_http::store_error(anyhow!("sqlite operation failed")).status(),
            StatusCode::INTERNAL_SERVER_ERROR
        );
        assert_eq!(
            store_status(anyhow!("sqlite operation failed")).code(),
            Code::Internal
        );
    }

    #[tokio::test]
    async fn writer_request_limit_errors_are_permanent_resource_exhaustion() {
        use http_body_util::BodyExt;

        let error = StoreWriteError::TooLarge {
            dimension: WriterLimitDimension::PrimaryRecords,
            requested: 2,
            limit: 1,
        };
        let response = ingest_http::store_error(anyhow!(error));
        assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(ingest_http::decode_rpc_status(&body).0, 8);
        assert_eq!(store_status(anyhow!(error)).code(), Code::ResourceExhausted);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn http_handler_yields_while_an_admitted_write_waits_for_the_owner() {
        let now = now_nanos() as u64;
        let tempdir = tempdir().unwrap();
        let store = Store::open(&tempdir.path().join("ottyel.db"), 24, 1000).unwrap();
        let app = ingest_http::router(IngestState::new(store.clone(), IngestLimits::default()));
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

    async fn connect_channel(endpoint: &str) -> Channel {
        for _ in 0..10 {
            if let Ok(channel) = Channel::from_shared(endpoint.to_string())
                .unwrap()
                .connect()
                .await
            {
                return channel;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        Channel::from_shared(endpoint.to_string())
            .unwrap()
            .connect()
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

    fn log_export_request(now: u64) -> ExportLogsServiceRequest {
        ExportLogsServiceRequest {
            resource_logs: vec![ResourceLogs {
                scope_logs: vec![ScopeLogs {
                    log_records: vec![LogRecord {
                        time_unix_nano: now,
                        body: Some(AnyValue {
                            value: Some(any_value::Value::StringValue("ok".into())),
                        }),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        }
    }

    fn metric_export_request(now: u64) -> ExportMetricsServiceRequest {
        ExportMetricsServiceRequest {
            resource_metrics: vec![ResourceMetrics {
                scope_metrics: vec![ScopeMetrics {
                    metrics: vec![Metric {
                        name: "requests".into(),
                        data: Some(metric::Data::Gauge(Gauge {
                            data_points: vec![NumberDataPoint {
                                time_unix_nano: now,
                                ..Default::default()
                            }],
                        })),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
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
