use std::{borrow::Cow, error::Error as _, io::Read};

use axum::{
    Router,
    body::{Body, to_bytes},
    extract::{Request, State},
    http::{HeaderMap, HeaderValue, StatusCode, header},
    response::{IntoResponse, Response},
    routing::post,
};
use flate2::read::MultiGzDecoder;
use http_body_util::LengthLimitError;
use opentelemetry_proto::tonic::collector::{
    logs::v1::{ExportLogsServiceRequest, ExportLogsServiceResponse},
    metrics::v1::{ExportMetricsServiceRequest, ExportMetricsServiceResponse},
    trace::v1::{ExportTraceServiceRequest, ExportTraceServiceResponse},
};
use prost::Message;
use tokio::sync::OwnedSemaphorePermit;

use crate::store::{AsyncWriteReceipt, StoreWriteError};

use super::{IngestState, policy::ValidateOtlp, wait_for_write};

const PROTOBUF_CONTENT_TYPE: &str = "application/x-protobuf";

#[derive(Clone, Copy)]
enum ContentEncoding {
    Identity,
    Gzip,
}

#[derive(Clone, PartialEq, Message)]
struct RpcStatus {
    #[prost(int32, tag = "1")]
    code: i32,
    #[prost(string, tag = "2")]
    message: String,
}

pub(super) fn router(state: IngestState) -> Router {
    Router::new()
        .route("/v1/traces", post(export_traces))
        .route("/v1/logs", post(export_logs))
        .route("/v1/metrics", post(export_metrics))
        .method_not_allowed_fallback(method_not_allowed)
        .fallback(not_found)
        .with_state(state)
}

async fn not_found() -> Response {
    protobuf_error(StatusCode::NOT_FOUND, 5, "OTLP endpoint not found")
}

async fn method_not_allowed() -> Response {
    protobuf_error(
        StatusCode::METHOD_NOT_ALLOWED,
        12,
        "OTLP endpoint does not support this method",
    )
}

async fn export_traces(State(state): State<IngestState>, request: Request) -> Response {
    handle::<ExportTraceServiceRequest, ExportTraceServiceResponse, _>(
        state,
        request,
        |state, request| state.store.try_ingest_traces(request),
    )
    .await
}

async fn export_logs(State(state): State<IngestState>, request: Request) -> Response {
    handle::<ExportLogsServiceRequest, ExportLogsServiceResponse, _>(
        state,
        request,
        |state, request| state.store.try_ingest_logs(request),
    )
    .await
}

async fn export_metrics(State(state): State<IngestState>, request: Request) -> Response {
    handle::<ExportMetricsServiceRequest, ExportMetricsServiceResponse, _>(
        state,
        request,
        |state, request| state.store.try_ingest_metrics(request),
    )
    .await
}

async fn handle<Req, Resp, F>(state: IngestState, request: Request, ingest: F) -> Response
where
    Req: Message + Default + ValidateOtlp,
    Resp: Message + Default,
    F: FnOnce(&IngestState, Req) -> anyhow::Result<AsyncWriteReceipt<usize>>,
{
    let permit = match state.admission.clone().try_acquire_owned() {
        Ok(permit) => permit,
        Err(_) => return protobuf_error(StatusCode::SERVICE_UNAVAILABLE, 14, "ingest at capacity"),
    };

    let request_timeout = state.limits.request_timeout;
    match tokio::time::timeout(
        request_timeout,
        handle_admitted::<Req, Resp, F>(state, request, ingest, permit),
    )
    .await
    {
        Ok(response) => response,
        Err(_) => protobuf_error(
            StatusCode::GATEWAY_TIMEOUT,
            4,
            "OTLP request exceeded the response deadline",
        ),
    }
}

async fn handle_admitted<Req, Resp, F>(
    state: IngestState,
    request: Request,
    ingest: F,
    permit: OwnedSemaphorePermit,
) -> Response
where
    Req: Message + Default + ValidateOtlp,
    Resp: Message + Default,
    F: FnOnce(&IngestState, Req) -> anyhow::Result<AsyncWriteReceipt<usize>>,
{
    let encoding = match request_encoding(request.headers()) {
        Ok(encoding) => encoding,
        Err(err) => return err.into_response(),
    };
    let wire_limit = state.limits.max_wire_bytes;
    let body = match to_bytes(request.into_body(), wire_limit).await {
        Ok(body) => body,
        Err(err)
            if err
                .source()
                .is_some_and(|source| source.is::<LengthLimitError>()) =>
        {
            return protobuf_error(
                StatusCode::PAYLOAD_TOO_LARGE,
                8,
                "OTLP request exceeds the HTTP wire-byte budget",
            );
        }
        Err(_) => {
            return protobuf_error(StatusCode::BAD_REQUEST, 3, "failed to read request body");
        }
    };

    let limits = state.limits.clone();
    let decoded = tokio::task::spawn_blocking(move || {
        let body = decode_content(body.as_ref(), encoding, limits.max_decompressed_bytes)?;
        let request = Req::decode(body.as_ref())
            .map_err(|_| HttpFailure::bad_request("request body is not valid OTLP protobuf"))?;
        request
            .validate(&limits)
            .map_err(|err| HttpFailure::too_large(err.to_string()))?;
        Ok::<_, HttpFailure>((request, permit))
    })
    .await;

    let (request, permit) = match decoded {
        Ok(Ok(decoded)) => decoded,
        Ok(Err(err)) => return err.into_response(),
        Err(_) => {
            return protobuf_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                13,
                "request decoder task failed",
            );
        }
    };

    let receipt = match ingest(&state, request) {
        Ok(receipt) => receipt,
        Err(err) => return store_error(err),
    };
    match wait_for_write(receipt, permit).await {
        Ok(()) => protobuf_response(StatusCode::OK, Resp::default().encode_to_vec()),
        Err(err) => store_error(err),
    }
}

fn request_encoding(headers: &HeaderMap) -> Result<ContentEncoding, HttpFailure> {
    let mut content_types = headers.get_all(header::CONTENT_TYPE).iter();
    let Some(content_type) = content_types.next() else {
        return Err(HttpFailure::new(
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            3,
            "content-type must be application/x-protobuf",
        ));
    };
    if content_types.next().is_some()
        || content_type
            .to_str()
            .ok()
            .and_then(|value| value.split(';').next())
            .is_none_or(|value| !value.trim().eq_ignore_ascii_case(PROTOBUF_CONTENT_TYPE))
    {
        return Err(HttpFailure::new(
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            3,
            "content-type must be application/x-protobuf",
        ));
    }

    let mut encodings = headers.get_all(header::CONTENT_ENCODING).iter();
    let encoding = match encodings.next() {
        None => ContentEncoding::Identity,
        Some(value) => match value.to_str().ok().map(str::trim) {
            Some(value) if value.eq_ignore_ascii_case("identity") => ContentEncoding::Identity,
            Some(value) if value.eq_ignore_ascii_case("gzip") => ContentEncoding::Gzip,
            _ => {
                return Err(HttpFailure::new(
                    StatusCode::UNSUPPORTED_MEDIA_TYPE,
                    12,
                    "content-encoding must be identity or gzip",
                ));
            }
        },
    };
    if encodings.next().is_some() {
        return Err(HttpFailure::new(
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            12,
            "only one content-encoding is supported",
        ));
    }
    Ok(encoding)
}

fn decode_content(
    body: &[u8],
    encoding: ContentEncoding,
    limit: usize,
) -> Result<Cow<'_, [u8]>, HttpFailure> {
    match encoding {
        ContentEncoding::Identity => {
            if body.len() > limit {
                return Err(HttpFailure::too_large(
                    "OTLP request exceeds the decompressed-byte budget",
                ));
            }
            Ok(Cow::Borrowed(body))
        }
        ContentEncoding::Gzip => {
            // RFC 1952 permits concatenated members. MultiGzDecoder accepts them and the
            // output budget applies to their combined decompressed bytes.
            let mut decoder = MultiGzDecoder::new(body);
            let mut output = Vec::with_capacity(body.len().min(limit));
            let mut chunk = [0_u8; 8192];
            loop {
                let read = decoder
                    .read(&mut chunk)
                    .map_err(|_| HttpFailure::bad_request("request body is not valid gzip data"))?;
                if read == 0 {
                    break;
                }
                if output.len().checked_add(read).is_none_or(|len| len > limit) {
                    return Err(HttpFailure::too_large(
                        "OTLP request exceeds the decompressed-byte budget",
                    ));
                }
                output.extend_from_slice(&chunk[..read]);
            }
            Ok(Cow::Owned(output))
        }
    }
}

pub(super) fn store_error(err: anyhow::Error) -> Response {
    match err.downcast_ref::<StoreWriteError>() {
        Some(
            StoreWriteError::Overloaded
            | StoreWriteError::Unavailable
            | StoreWriteError::OutcomeUnknown,
        ) => protobuf_error(StatusCode::SERVICE_UNAVAILABLE, 14, err.to_string()),
        None => protobuf_error(StatusCode::INTERNAL_SERVER_ERROR, 13, err.to_string()),
    }
}

fn protobuf_response(status: StatusCode, body: Vec<u8>) -> Response {
    let mut response = (status, Body::from(body)).into_response();
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static(PROTOBUF_CONTENT_TYPE),
    );
    response
}

fn protobuf_error(status: StatusCode, code: i32, message: impl Into<String>) -> Response {
    protobuf_response(
        status,
        RpcStatus {
            code,
            message: message.into(),
        }
        .encode_to_vec(),
    )
}

#[derive(Debug)]
struct HttpFailure {
    status: StatusCode,
    code: i32,
    message: String,
}

impl HttpFailure {
    fn new(status: StatusCode, code: i32, message: impl Into<String>) -> Self {
        Self {
            status,
            code,
            message: message.into(),
        }
    }

    fn bad_request(message: impl Into<String>) -> Self {
        Self::new(StatusCode::BAD_REQUEST, 3, message)
    }

    fn too_large(message: impl Into<String>) -> Self {
        Self::new(StatusCode::PAYLOAD_TOO_LARGE, 8, message)
    }

    fn into_response(self) -> Response {
        protobuf_error(self.status, self.code, self.message)
    }
}

#[cfg(test)]
pub(super) fn decode_rpc_status(body: &[u8]) -> (i32, String) {
    let status = RpcStatus::decode(body).unwrap();
    (status.code, status.message)
}

#[cfg(test)]
mod tests {
    use std::{
        convert::Infallible,
        io::Write,
        time::{Duration, SystemTime, UNIX_EPOCH},
    };

    use axum::{
        body::{Body, Bytes},
        http::{HeaderValue, Request, StatusCode, header},
    };
    use flate2::{Compression, write::GzEncoder};
    use http_body_util::BodyExt;
    use opentelemetry_proto::tonic::{
        collector::{
            logs::v1::ExportLogsServiceRequest, metrics::v1::ExportMetricsServiceRequest,
            trace::v1::ExportTraceServiceRequest,
        },
        logs::v1::{LogRecord, ResourceLogs, ScopeLogs},
        metrics::v1::{Gauge, Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics, metric},
        trace::v1::{ResourceSpans, ScopeSpans, Span},
    };
    use prost::Message;
    use tempfile::tempdir;
    use tower::ServiceExt;

    use crate::store::Store;

    use super::{
        ContentEncoding, PROTOBUF_CONTENT_TYPE, decode_content, decode_rpc_status, router,
    };
    use crate::ingest::{IngestLimits, IngestState};

    #[tokio::test]
    async fn identity_and_gzip_ingest_succeed_for_all_three_http_signals() {
        for compressed in [false, true] {
            let encoding = compressed.then_some("gzip");
            let tempdir = tempdir().unwrap();
            let store = Store::open(&tempdir.path().join("ottyel.db"), 24, 1000).unwrap();
            let app = router(IngestState::new(store.clone(), IngestLimits::default()));
            let cases = [
                ("/v1/traces", trace_request().encode_to_vec()),
                ("/v1/logs", log_request().encode_to_vec()),
                ("/v1/metrics", metric_request().encode_to_vec()),
            ];

            for (path, payload) in cases {
                let body = if compressed { gzip(&payload) } else { payload };
                let response = app
                    .clone()
                    .oneshot(protobuf_request(path, body, encoding))
                    .await
                    .unwrap();
                assert_eq!(response.status(), StatusCode::OK, "path: {path}");
                assert_eq!(
                    response.headers()[header::CONTENT_TYPE],
                    PROTOBUF_CONTENT_TYPE
                );
            }
            let counts = store.counts(None).unwrap();
            assert_eq!((counts.0, counts.2, counts.3), (1, 1, 1));
        }
    }

    #[tokio::test]
    async fn endpoint_failures_are_protobuf_with_permanent_status_codes() {
        let tempdir = tempdir().unwrap();
        let store = Store::open(&tempdir.path().join("ottyel.db"), 24, 1000).unwrap();
        let app = router(IngestState::new(store, IngestLimits::default()));
        let requests = [
            Request::post("/v1/traces").body(Body::empty()).unwrap(),
            protobuf_request("/v1/traces", b"not gzip".to_vec(), Some("gzip")),
            protobuf_request("/v1/traces", vec![0xff], None),
            protobuf_request("/v1/traces", Vec::new(), Some("br")),
        ];
        let expected = [
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            StatusCode::BAD_REQUEST,
            StatusCode::BAD_REQUEST,
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
        ];

        for (request, expected) in requests.into_iter().zip(expected) {
            let response = app.clone().oneshot(request).await.unwrap();
            assert_eq!(response.status(), expected);
            assert_eq!(
                response.headers()[header::CONTENT_TYPE],
                PROTOBUF_CONTENT_TYPE
            );
            let body = response.into_body().collect().await.unwrap().to_bytes();
            let (code, message) = decode_rpc_status(&body);
            assert!([3, 12].contains(&code));
            assert!(!message.is_empty());
        }
    }

    #[tokio::test]
    async fn routing_failures_are_protobuf_status_responses() {
        let tempdir = tempdir().unwrap();
        let store = Store::open(&tempdir.path().join("ottyel.db"), 24, 1000).unwrap();
        let app = router(IngestState::new(store, IngestLimits::default()));
        let cases = [
            (
                Request::get("/v1/traces").body(Body::empty()).unwrap(),
                StatusCode::METHOD_NOT_ALLOWED,
                12,
            ),
            (
                Request::post("/v1/unknown").body(Body::empty()).unwrap(),
                StatusCode::NOT_FOUND,
                5,
            ),
        ];

        for (request, expected_status, expected_code) in cases {
            let response = app.clone().oneshot(request).await.unwrap();
            assert_eq!(response.status(), expected_status);
            assert_eq!(
                response.headers()[header::CONTENT_TYPE],
                PROTOBUF_CONTENT_TYPE
            );
            let body = response.into_body().collect().await.unwrap().to_bytes();
            assert_eq!(decode_rpc_status(&body).0, expected_code);
        }
    }

    #[tokio::test]
    async fn stalled_body_times_out_and_releases_shared_capacity() {
        let tempdir = tempdir().unwrap();
        let store = Store::open(&tempdir.path().join("ottyel.db"), 24, 1000).unwrap();
        let limits = IngestLimits {
            max_in_flight: 1,
            request_timeout: Duration::from_millis(25),
            ..IngestLimits::default()
        };
        let app = router(IngestState::new(store.clone(), limits));
        let stalled_body =
            Body::from_stream(futures::stream::pending::<Result<Bytes, Infallible>>());
        let stalled_request = Request::post("/v1/traces")
            .header(header::CONTENT_TYPE, PROTOBUF_CONTENT_TYPE)
            .body(stalled_body)
            .unwrap();

        let response =
            tokio::time::timeout(Duration::from_secs(1), app.clone().oneshot(stalled_request))
                .await
                .unwrap()
                .unwrap();
        assert_eq!(response.status(), StatusCode::GATEWAY_TIMEOUT);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(decode_rpc_status(&body).0, 4);

        let response = app
            .oneshot(protobuf_request(
                "/v1/traces",
                trace_request().encode_to_vec(),
                None,
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(store.counts(None).unwrap().0, 1);
    }

    #[tokio::test]
    async fn wire_budget_accepts_exact_size_and_rejects_one_byte_over() {
        let tempdir = tempdir().unwrap();
        let store = Store::open(&tempdir.path().join("ottyel.db"), 24, 1000).unwrap();
        let limits = IngestLimits {
            max_wire_bytes: 3,
            ..IngestLimits::default()
        };
        let app = router(IngestState::new(store, limits));

        let exact = app
            .clone()
            .oneshot(protobuf_request("/v1/traces", vec![0; 3], None))
            .await
            .unwrap();
        assert_eq!(exact.status(), StatusCode::BAD_REQUEST);

        let over = app
            .oneshot(protobuf_request("/v1/traces", vec![0; 4], None))
            .await
            .unwrap();
        assert_eq!(over.status(), StatusCode::PAYLOAD_TOO_LARGE);
        let body = over.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(decode_rpc_status(&body).0, 8);
    }

    #[tokio::test]
    async fn gzip_decompressed_budget_failure_is_nonretryable_and_atomic() {
        let tempdir = tempdir().unwrap();
        let store = Store::open(&tempdir.path().join("ottyel.db"), 24, 1000).unwrap();
        let limits = IngestLimits {
            max_wire_bytes: 1024,
            max_decompressed_bytes: 10,
            ..IngestLimits::default()
        };
        let app = router(IngestState::new(store.clone(), limits));

        let response = app
            .oneshot(protobuf_request(
                "/v1/traces",
                gzip(&trace_request().encode_to_vec()),
                Some("gzip"),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(decode_rpc_status(&body).0, 8);
        assert_eq!(store.counts(None).unwrap().0, 0);
    }

    #[tokio::test]
    async fn a_late_policy_failure_is_atomic_and_does_not_reach_sqlite() {
        let tempdir = tempdir().unwrap();
        let store = Store::open(&tempdir.path().join("ottyel.db"), 24, 1000).unwrap();
        let limits = IngestLimits {
            max_value_bytes: 3,
            ..IngestLimits::default()
        };
        let app = router(IngestState::new(store.clone(), limits));
        let mut request = trace_request();
        request.resource_spans[0].scope_spans[0].spans.push(Span {
            name: "long".into(),
            span_id: vec![3],
            ..Default::default()
        });

        let response = app
            .oneshot(protobuf_request(
                "/v1/traces",
                request.encode_to_vec(),
                None,
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
        assert_eq!(store.counts(None).unwrap().0, 0);
    }

    #[tokio::test]
    async fn saturated_request_admission_is_retryable_without_reading_body() {
        let tempdir = tempdir().unwrap();
        let store = Store::open(&tempdir.path().join("ottyel.db"), 24, 1000).unwrap();
        let limits = IngestLimits {
            max_in_flight: 1,
            ..IngestLimits::default()
        };
        let state = IngestState::new(store, limits);
        let _held = state.admission.clone().acquire_owned().await.unwrap();
        let app = router(state);

        let response = app
            .oneshot(protobuf_request(
                "/v1/traces",
                trace_request().encode_to_vec(),
                None,
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(decode_rpc_status(&body).0, 14);
    }

    #[test]
    fn gzip_decoding_bounds_combined_members_and_rejects_corruption() {
        let member_a = gzip(b"abc");
        let member_b = gzip(b"def");
        let concatenated = [member_a, member_b].concat();
        assert_eq!(
            decode_content(&concatenated, ContentEncoding::Gzip, 6)
                .unwrap()
                .as_ref(),
            b"abcdef".as_slice()
        );
        assert!(decode_content(&concatenated, ContentEncoding::Gzip, 5).is_err());

        let mut truncated = gzip(b"abcdef");
        truncated.truncate(truncated.len() - 4);
        assert!(decode_content(&truncated, ContentEncoding::Gzip, 100).is_err());
        assert!(decode_content(b"not gzip", ContentEncoding::Gzip, 100).is_err());
    }

    fn protobuf_request(path: &str, body: Vec<u8>, encoding: Option<&str>) -> Request<Body> {
        let mut request = Request::post(path)
            .header(
                header::CONTENT_TYPE,
                "Application/X-Protobuf; charset=binary",
            )
            .body(Body::from(body))
            .unwrap();
        if let Some(encoding) = encoding {
            request.headers_mut().insert(
                header::CONTENT_ENCODING,
                HeaderValue::from_str(encoding).unwrap(),
            );
        }
        request
    }

    fn gzip(value: &[u8]) -> Vec<u8> {
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(value).unwrap();
        encoder.finish().unwrap()
    }

    fn trace_request() -> ExportTraceServiceRequest {
        let now = now_nanos();
        ExportTraceServiceRequest {
            resource_spans: vec![ResourceSpans {
                scope_spans: vec![ScopeSpans {
                    spans: vec![Span {
                        trace_id: vec![1],
                        span_id: vec![2],
                        name: "ok".into(),
                        start_time_unix_nano: now,
                        end_time_unix_nano: now + 1,
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        }
    }

    fn log_request() -> ExportLogsServiceRequest {
        let now = now_nanos();
        ExportLogsServiceRequest {
            resource_logs: vec![ResourceLogs {
                scope_logs: vec![ScopeLogs {
                    log_records: vec![LogRecord {
                        time_unix_nano: now,
                        body: Some(opentelemetry_proto::tonic::common::v1::AnyValue {
                            value: Some(
                                opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(
                                    "ok".into(),
                                ),
                            ),
                        }),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        }
    }

    fn metric_request() -> ExportMetricsServiceRequest {
        let now = now_nanos();
        ExportMetricsServiceRequest {
            resource_metrics: vec![ResourceMetrics {
                scope_metrics: vec![ScopeMetrics {
                    metrics: vec![Metric {
                        name: "m".into(),
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

    fn now_nanos() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64
    }
}
