use std::time::{SystemTime, UNIX_EPOCH};

use axum::{
    body::Body,
    http::{Request, StatusCode, header},
};
use http_body_util::BodyExt;
use opentelemetry_proto::tonic::{
    collector::{
        logs::v1::{
            ExportLogsPartialSuccess, ExportLogsServiceRequest, ExportLogsServiceResponse,
            logs_service_client::LogsServiceClient,
        },
        metrics::v1::{
            ExportMetricsPartialSuccess, ExportMetricsServiceRequest, ExportMetricsServiceResponse,
            metrics_service_client::MetricsServiceClient,
        },
        trace::v1::{
            ExportTracePartialSuccess, ExportTraceServiceRequest, ExportTraceServiceResponse,
        },
    },
    logs::v1::{LogRecord, ResourceLogs, ScopeLogs},
    metrics::v1::{
        Gauge, Histogram, HistogramDataPoint, Metric, NumberDataPoint, ResourceMetrics,
        ScopeMetrics, metric,
    },
    trace::v1::{ResourceSpans, ScopeSpans, Span, span::Link},
};
use prost::Message;
use tempfile::tempdir;
use tower::ServiceExt;

use super::ScreenRecords;
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

fn span(trace_id: Vec<u8>, span_id: Vec<u8>, name: &str) -> Span {
    let now = now();
    Span {
        trace_id,
        span_id,
        name: name.to_string(),
        start_time_unix_nano: now,
        end_time_unix_nano: now + 1,
        ..Span::default()
    }
}

fn traces(spans: Vec<Span>) -> ExportTraceServiceRequest {
    ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            scope_spans: vec![ScopeSpans {
                spans,
                ..ScopeSpans::default()
            }],
            ..ResourceSpans::default()
        }],
    }
}

fn logs(records: Vec<LogRecord>) -> ExportLogsServiceRequest {
    ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            scope_logs: vec![ScopeLogs {
                log_records: records,
                ..ScopeLogs::default()
            }],
            ..ResourceLogs::default()
        }],
    }
}

fn metrics(metrics: Vec<Metric>) -> ExportMetricsServiceRequest {
    ExportMetricsServiceRequest {
        resource_metrics: vec![ResourceMetrics {
            scope_metrics: vec![ScopeMetrics {
                metrics,
                ..ScopeMetrics::default()
            }],
            ..ResourceMetrics::default()
        }],
    }
}

fn gauge(name: &str, points: usize) -> Metric {
    Metric {
        name: name.to_string(),
        data: Some(metric::Data::Gauge(Gauge {
            data_points: vec![
                NumberDataPoint {
                    time_unix_nano: now(),
                    ..NumberDataPoint::default()
                };
                points
            ],
        })),
        ..Metric::default()
    }
}

fn span_names(request: &ExportTraceServiceRequest) -> Vec<String> {
    request.resource_spans[0].scope_spans[0]
        .spans
        .iter()
        .map(|span| span.name.clone())
        .collect()
}

#[test]
fn a_clean_export_leaves_partial_success_unset() {
    let mut request = traces(vec![span(vec![1; 16], vec![2; 8], "ok")]);
    let original = request.clone();
    let report = request.screen();

    assert_eq!(request, original);
    assert_eq!(
        ExportTraceServiceRequest::response(&report),
        ExportTraceServiceResponse::default()
    );
}

#[test]
fn spans_with_invalid_identity_are_rejected_and_secondary_fields_normalized() {
    let mut zero_parent = span(vec![1; 16], vec![3; 8], "zero-parent");
    zero_parent.parent_span_id = vec![0; 8];
    let mut bad_link = span(vec![1; 16], vec![4; 8], "bad-link");
    bad_link.links = vec![
        Link {
            trace_id: vec![1; 16],
            span_id: vec![2; 8],
            ..Link::default()
        },
        Link {
            trace_id: vec![0; 16],
            span_id: vec![2; 8],
            ..Link::default()
        },
    ];
    let mut backwards = span(vec![1; 16], vec![5; 8], "backwards");
    backwards.end_time_unix_nano = backwards.start_time_unix_nano - 1;
    let mut bad_parent = span(vec![1; 16], vec![6; 8], "bad-parent");
    bad_parent.parent_span_id = vec![7; 4];

    let mut request = traces(vec![
        span(vec![1; 16], vec![2; 8], "valid"),
        span(vec![0; 16], vec![2; 8], "zero-trace"),
        span(vec![1; 15], vec![2; 8], "short-trace"),
        span(vec![1; 16], vec![0; 8], "zero-span"),
        bad_parent,
        zero_parent,
        bad_link,
        backwards,
    ]);
    let report = request.screen();

    assert_eq!(
        span_names(&request),
        vec!["valid", "zero-parent", "bad-link", "backwards"]
    );
    let spans = &request.resource_spans[0].scope_spans[0].spans;
    assert!(spans[1].parent_span_id.is_empty());
    assert_eq!(spans[2].links.len(), 1);
    assert_eq!(
        ExportTraceServiceRequest::response(&report),
        ExportTraceServiceResponse {
            partial_success: Some(ExportTracePartialSuccess {
                rejected_spans: 4,
                error_message: "2 span(s) rejected: trace_id is not 16 non-zero bytes; \
                    1 span(s) rejected: span_id is not 8 non-zero bytes; \
                    1 span(s) rejected: parent_span_id is neither empty nor 8 bytes; \
                    1 span(s) with an all-zero parent_span_id stored as roots; \
                    1 span link(s) dropped: trace_id or span_id is invalid; \
                    1 span(s) end before they start"
                    .to_string(),
            }),
        }
    );
}

#[test]
fn logs_with_invalid_ids_are_kept_without_association_as_a_warning() {
    let mut request = logs(vec![
        LogRecord {
            trace_id: vec![1; 16],
            span_id: vec![2; 8],
            ..LogRecord::default()
        },
        LogRecord {
            trace_id: vec![1; 15],
            span_id: vec![0; 8],
            ..LogRecord::default()
        },
        LogRecord::default(),
    ]);
    let report = request.screen();

    let records = &request.resource_logs[0].scope_logs[0].log_records;
    assert_eq!(
        records
            .iter()
            .map(|record| (record.trace_id.len(), record.span_id.len()))
            .collect::<Vec<_>>(),
        vec![(16, 8), (0, 0), (0, 0)]
    );
    assert_eq!(
        ExportLogsServiceRequest::response(&report),
        ExportLogsServiceResponse {
            partial_success: Some(ExportLogsPartialSuccess {
                rejected_log_records: 0,
                error_message: "1 log record(s) with an invalid trace_id stored without trace \
                    association; 1 log record(s) with an invalid span_id stored without span \
                    association"
                    .to_string(),
            }),
        }
    );
}

#[test]
fn unnamed_metrics_and_inconsistent_histogram_points_are_rejected_by_point() {
    let histogram = Metric {
        name: "latency".to_string(),
        data: Some(metric::Data::Histogram(Histogram {
            data_points: vec![
                HistogramDataPoint {
                    explicit_bounds: vec![1.0],
                    bucket_counts: vec![1, 2],
                    ..HistogramDataPoint::default()
                },
                HistogramDataPoint::default(),
                HistogramDataPoint {
                    explicit_bounds: vec![1.0, 2.0],
                    bucket_counts: vec![1, 2],
                    ..HistogramDataPoint::default()
                },
            ],
            ..Histogram::default()
        })),
        ..Metric::default()
    };
    let mut request = metrics(vec![gauge("", 2), gauge("requests", 1), histogram]);
    let report = request.screen();

    let kept = &request.resource_metrics[0].scope_metrics[0].metrics;
    assert_eq!(
        kept.iter()
            .map(|metric| metric.name.as_str())
            .collect::<Vec<_>>(),
        vec!["requests", "latency"]
    );
    let Some(metric::Data::Histogram(histogram)) = &kept[1].data else {
        panic!("histogram metric changed type");
    };
    assert_eq!(histogram.data_points.len(), 2);
    assert_eq!(report.rejected(), 3);
    assert_eq!(
        ExportMetricsServiceRequest::response(&report),
        ExportMetricsServiceResponse {
            partial_success: Some(ExportMetricsPartialSuccess {
                rejected_data_points: 3,
                error_message: "2 data point(s) rejected: metric name is empty; 1 data point(s) \
                    rejected: histogram bucket_counts length is not explicit_bounds + 1"
                    .to_string(),
            }),
        }
    );
}

#[tokio::test]
async fn http_commits_valid_spans_and_reports_rejected_siblings() {
    let directory = tempdir().unwrap();
    let store = Store::open(&directory.path().join("ottyel.db"), 24, 1000).unwrap();
    let app = http::router(IngestState::new(store.clone(), IngestLimits::default()));
    let request = traces(vec![
        span(vec![1; 16], vec![2; 8], "valid"),
        span(vec![0; 16], vec![3; 8], "invalid"),
    ]);

    let response = app
        .oneshot(
            Request::post("/v1/traces")
                .header(header::CONTENT_TYPE, "application/x-protobuf")
                .body(Body::from(request.encode_to_vec()))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = response.into_body().collect().await.unwrap().to_bytes();
    let decoded = ExportTraceServiceResponse::decode(body).unwrap();
    assert_eq!(
        decoded
            .partial_success
            .map(|partial| partial.rejected_spans),
        Some(1)
    );
    assert_eq!(store.counts(None).unwrap().0, 1);
}

#[tokio::test]
async fn grpc_reports_log_warnings_and_rejected_metric_points() {
    let directory = tempdir().unwrap();
    let store = Store::open(&directory.path().join("ottyel.db"), 24, 1000).unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let (shutdown_sender, shutdown) = tokio::sync::watch::channel(false);
    let server = tokio::spawn(serve_grpc_listener(
        listener,
        IngestState::new(store.clone(), IngestLimits::default()),
        shutdown,
    ));

    let log_response = LogsServiceClient::connect(endpoint.clone())
        .await
        .unwrap()
        .export(logs(vec![LogRecord {
            time_unix_nano: now(),
            trace_id: vec![9; 3],
            ..LogRecord::default()
        }]))
        .await
        .unwrap()
        .into_inner();
    let metric_response = MetricsServiceClient::connect(endpoint)
        .await
        .unwrap()
        .export(metrics(vec![gauge("", 2), gauge("requests", 1)]))
        .await
        .unwrap()
        .into_inner();
    let _ = shutdown_sender.send(true);
    server.await.unwrap().unwrap();

    assert_eq!(
        log_response.partial_success.map(|partial| (
            partial.rejected_log_records,
            partial.error_message.is_empty()
        )),
        Some((0, false))
    );
    assert_eq!(
        metric_response
            .partial_success
            .map(|partial| partial.rejected_data_points),
        Some(2)
    );
    let counts = store.counts(None).unwrap();
    assert_eq!((counts.2, counts.3), (1, 1));
}
