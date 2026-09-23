use std::time::{SystemTime, UNIX_EPOCH};

use axum::{
    body::Body,
    http::{Request, StatusCode, header},
};
use opentelemetry_proto::tonic::{
    collector::{
        logs::v1::{ExportLogsServiceRequest, logs_service_client::LogsServiceClient},
        trace::v1::{ExportTraceServiceRequest, trace_service_client::TraceServiceClient},
    },
    logs::v1::{LogRecord, ResourceLogs, ScopeLogs},
    trace::v1::{ResourceSpans, ScopeSpans, Span},
};
use prost::Message;
use tempfile::tempdir;
use tonic::Code;
use tower::ServiceExt;

use super::{Failure, IngestStats, LATENCY_BUCKET_BOUNDS, Signal, StreamCounters, Transport};
use crate::{
    ingest::{IngestLimits, IngestState, http, serve_grpc_listener},
    store::Store,
};

fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64
}

fn traces(spans: Vec<(Vec<u8>, Vec<u8>)>) -> ExportTraceServiceRequest {
    let now = now();
    ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            scope_spans: vec![ScopeSpans {
                spans: spans
                    .into_iter()
                    .map(|(trace_id, span_id)| Span {
                        trace_id,
                        span_id,
                        name: "request".to_string(),
                        start_time_unix_nano: now,
                        end_time_unix_nano: now + 1,
                        ..Span::default()
                    })
                    .collect(),
                ..ScopeSpans::default()
            }],
            ..ResourceSpans::default()
        }],
    }
}

fn logs() -> ExportLogsServiceRequest {
    ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            scope_logs: vec![ScopeLogs {
                log_records: vec![LogRecord {
                    time_unix_nano: now(),
                    ..LogRecord::default()
                }],
                ..ScopeLogs::default()
            }],
            ..ResourceLogs::default()
        }],
    }
}

#[test]
fn failures_classify_by_transport_status() {
    assert_eq!(
        [
            StatusCode::OK,
            StatusCode::BAD_REQUEST,
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            StatusCode::PAYLOAD_TOO_LARGE,
            StatusCode::SERVICE_UNAVAILABLE,
            StatusCode::GATEWAY_TIMEOUT,
            StatusCode::INTERNAL_SERVER_ERROR,
        ]
        .map(Failure::from_http),
        [
            None,
            Some(Failure::Invalid),
            Some(Failure::Invalid),
            Some(Failure::TooLarge),
            Some(Failure::Unavailable),
            Some(Failure::TimedOut),
            Some(Failure::Internal),
        ]
    );
    assert_eq!(
        [
            Code::Ok,
            Code::InvalidArgument,
            Code::ResourceExhausted,
            Code::Unavailable,
            Code::DeadlineExceeded,
            Code::Cancelled,
            Code::Internal,
        ]
        .map(Failure::from_grpc),
        [
            None,
            Some(Failure::Invalid),
            Some(Failure::TooLarge),
            Some(Failure::Unavailable),
            Some(Failure::TimedOut),
            Some(Failure::TimedOut),
            Some(Failure::Internal),
        ]
    );
}

#[test]
fn latency_p95_reports_the_bucket_bound_or_none_when_unbounded() {
    let mut counters = StreamCounters::default();
    assert_eq!(counters.latency_p95_bound(), None);

    counters.latency[0] = 94;
    counters.latency[3] = 1;
    assert_eq!(counters.latency_p95_bound(), Some(LATENCY_BUCKET_BOUNDS[0]));
    counters.latency[3] = 6;
    assert_eq!(counters.latency_p95_bound(), Some(LATENCY_BUCKET_BOUNDS[3]));

    let slow = StreamCounters {
        latency: [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1],
        ..StreamCounters::default()
    };
    assert_eq!(slow.latency_p95_bound(), None);
}

#[test]
fn window_counters_subtract_and_saturate() {
    let earlier = StreamCounters {
        requests_accepted: 2,
        records_accepted: 20,
        failures: [1, 0, 0, 0, 0],
        latency: [2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
        ..StreamCounters::default()
    };
    let later = StreamCounters {
        requests_accepted: 5,
        records_accepted: 50,
        records_rejected: 3,
        failures: [1, 0, 2, 0, 0],
        latency: [2, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0],
        ..StreamCounters::default()
    };

    assert_eq!(
        later.since(&earlier),
        StreamCounters {
            requests_accepted: 3,
            records_accepted: 30,
            records_rejected: 3,
            failures: [0, 0, 2, 0, 0],
            latency: [0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0],
            ..StreamCounters::default()
        }
    );
    assert_eq!(earlier.since(&later).records_accepted, 0);
}

#[test]
fn an_unresolved_export_counts_as_timed_out() {
    let stats = IngestStats::default();
    drop(stats.pending(Signal::Logs, Transport::Grpc));
    stats
        .pending(Signal::Logs, Transport::Grpc)
        .accepted(3, 1, true);

    let inner = stats.lock();
    let stream = inner.streams[1][1];
    assert_eq!(stream.failed(Failure::TimedOut), 1);
    assert_eq!(
        (
            stream.requests_accepted,
            stream.records_accepted,
            stream.records_rejected,
            stream.requests_with_warnings
        ),
        (1, 3, 1, 1)
    );
    assert_eq!(
        inner.last_failure.as_ref().map(|note| note.failure),
        Some(Failure::TimedOut)
    );
    assert!(inner.last_accepted.is_some());
}

#[tokio::test]
async fn http_records_accepted_rejected_and_failed_exports() {
    let directory = tempdir().unwrap();
    let store = Store::open(&directory.path().join("ottyel.db"), 24, 1000).unwrap();
    let state = IngestState::new(store, IngestLimits::default());
    let probe = state.probe();
    let app = http::router(state);
    let mixed = traces(vec![(vec![1; 16], vec![2; 8]), (vec![0; 16], vec![3; 8])]);

    let accepted = app
        .clone()
        .oneshot(
            Request::post("/v1/traces")
                .header(header::CONTENT_TYPE, "application/x-protobuf")
                .body(Body::from(mixed.encode_to_vec()))
                .unwrap(),
        )
        .await
        .unwrap();
    let invalid = app
        .oneshot(
            Request::post("/v1/traces")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(accepted.status(), StatusCode::OK);
    assert_eq!(invalid.status(), StatusCode::UNSUPPORTED_MEDIA_TYPE);

    let health = probe.sample();
    let stream = health.stream(Signal::Traces, Transport::Http);
    assert_eq!(
        (
            stream.requests_accepted,
            stream.records_accepted,
            stream.records_rejected
        ),
        (1, 1, 1)
    );
    assert_eq!(stream.failed(Failure::Invalid), 1);
    assert_eq!(stream.latency.iter().sum::<u64>(), 1);
    assert_eq!(health.totals().failed_requests(), 1);
    assert_eq!(
        health
            .last_failure
            .map(|note| (note.transport, note.failure)),
        Some((Transport::Http, Failure::Invalid))
    );
    assert_eq!((health.in_flight_requests, health.queued_records), (0, 0));
}

#[tokio::test]
async fn grpc_records_service_successes_and_pre_service_failures() {
    let directory = tempdir().unwrap();
    let store = Store::open(&directory.path().join("ottyel.db"), 24, 1000).unwrap();
    let limits = IngestLimits {
        max_in_flight: 1,
        max_wire_bytes: 256,
        ..IngestLimits::default()
    };
    let state = IngestState::new(store, limits);
    let probe = state.probe();
    let admission = state.admission.clone();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let (shutdown_sender, shutdown) = tokio::sync::watch::channel(false);
    let server = tokio::spawn(serve_grpc_listener(listener, state, shutdown));

    LogsServiceClient::connect(endpoint.clone())
        .await
        .unwrap()
        .export(logs())
        .await
        .unwrap();
    let mut traces_client = TraceServiceClient::connect(endpoint).await.unwrap();
    let mut oversized = traces(vec![(vec![1; 16], vec![2; 8])]);
    oversized.resource_spans[0].scope_spans[0].spans[0].name = "x".repeat(1024);
    let too_large = traces_client.export(oversized).await.unwrap_err();
    let held = admission.acquire_owned().await.unwrap();
    let saturated = traces_client
        .export(traces(vec![(vec![1; 16], vec![2; 8])]))
        .await
        .unwrap_err();
    drop(held);
    let _ = shutdown_sender.send(true);
    server.await.unwrap().unwrap();

    assert_eq!(too_large.code(), Code::ResourceExhausted);
    assert_eq!(saturated.code(), Code::Unavailable);
    let health = probe.sample();
    let logs = health.stream(Signal::Logs, Transport::Grpc);
    assert_eq!((logs.requests_accepted, logs.records_accepted), (1, 1));
    let traces = health.stream(Signal::Traces, Transport::Grpc);
    assert_eq!(
        (
            traces.failed(Failure::TooLarge),
            traces.failed(Failure::Unavailable),
            traces.failed_requests()
        ),
        (1, 1, 2)
    );
    assert_eq!(
        health.last_failure.map(|note| (note.signal, note.failure)),
        Some((Signal::Traces, Failure::Unavailable))
    );
}
