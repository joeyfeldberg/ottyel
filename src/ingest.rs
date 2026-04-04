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
    logs::v1::{ExportLogsServiceRequest, ExportLogsServiceResponse},
    metrics::v1::{ExportMetricsServiceRequest, ExportMetricsServiceResponse},
    trace::v1::{ExportTraceServiceRequest, ExportTraceServiceResponse},
};
use prost::Message;
use tokio::sync::watch;

use crate::store::Store;

#[derive(Clone)]
struct IngestState {
    store: Store,
}

pub async fn serve(bind: &str, store: Store, shutdown: watch::Receiver<bool>) -> Result<()> {
    let addr: SocketAddr = bind
        .parse()
        .with_context(|| format!("invalid bind addr {bind}"))?;
    let state = IngestState { store };
    let listener = tokio::net::TcpListener::bind(addr).await?;
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

#[cfg(test)]
mod tests {
    use std::time::{SystemTime, UNIX_EPOCH};

    use axum::http::StatusCode;
    use axum::{Router, body::Body, http::Request, routing::post};
    use opentelemetry_proto::tonic::{
        collector::trace::v1::ExportTraceServiceRequest,
        common::v1::{AnyValue, InstrumentationScope, KeyValue, any_value},
        resource::v1::Resource,
        trace::v1::{ResourceSpans, ScopeSpans, Span, Status},
    };
    use prost::Message;
    use tempfile::tempdir;
    use tower::ServiceExt;

    use crate::store::Store;

    use super::{IngestState, export_traces};

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

        let payload = ExportTraceServiceRequest {
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
        .encode_to_vec();

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

    fn now_nanos() -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos() as i64
    }
}
