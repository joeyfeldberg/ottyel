use std::time::{SystemTime, UNIX_EPOCH};

use opentelemetry_proto::tonic::{
    collector::{
        logs::v1::ExportLogsServiceRequest, metrics::v1::ExportMetricsServiceRequest,
        trace::v1::ExportTraceServiceRequest,
    },
    common::v1::{AnyValue, InstrumentationScope, KeyValue, any_value},
    logs::v1::{LogRecord, ResourceLogs, ScopeLogs},
    metrics::v1::{
        Gauge, Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics, metric, number_data_point,
    },
    resource::v1::Resource,
    trace::v1::{ResourceSpans, ScopeSpans, Span, Status, span, span::Event, span::Link},
};
use tempfile::tempdir;

use crate::query::{LogCorrelationFilter, LogFilters, LogSeverityFilter};

use super::Store;

#[test]
fn store_ingests_all_three_signals() {
    let tempdir = tempdir().unwrap();
    let store = Store::open(&tempdir.path().join("ottyel.db"), 24, 1000).unwrap();
    let now = now_nanos();

    store.ingest_traces(trace_request(now)).unwrap();
    store.ingest_logs(log_request(now)).unwrap();
    store.ingest_metrics(metric_request(now)).unwrap();

    let (trace_count, _error_spans, log_count, metric_count, llm_count) =
        store.counts(None).unwrap();
    assert_eq!(trace_count, 1);
    assert_eq!(log_count, 3);
    assert_eq!(metric_count, 1);
    assert_eq!(llm_count, 1);

    let traces = store.recent_traces(None, false, 10, None, None).unwrap();
    assert_eq!(traces[0].trace_id, "0102030405060708090a0b0c0d0e0f10");
    let detail = store
        .trace_detail("0102030405060708090a0b0c0d0e0f10")
        .unwrap();
    assert_eq!(detail[0].events.len(), 1);
    assert_eq!(detail[0].events[0].name, "model.invoke");
    assert_eq!(detail[0].links.len(), 1);
    assert_eq!(detail[0].links[0].span_id, "0909090909090909");
    let llm = store.recent_llm(None, 10, None, None).unwrap();
    assert_eq!(llm[0].model, "gpt-5.4");
    assert_eq!(
        store
            .recent_traces(None, false, 10, None, Some("input.value"))
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        store
            .recent_logs(
                None,
                10,
                None,
                Some("completion finished"),
                &LogFilters::default(),
            )
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        store
            .recent_metrics(None, 10, None, Some("tokens.total"))
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        store
            .recent_llm(None, 10, None, Some("gpt-5.4"))
            .unwrap()
            .len(),
        1
    );

    let threshold_after_trace = now + 2_050_000;
    assert!(
        store
            .recent_traces(None, false, 10, Some(threshold_after_trace), None)
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        store
            .recent_logs(
                None,
                10,
                Some(threshold_after_trace),
                None,
                &LogFilters::default(),
            )
            .unwrap()
            .len(),
        0
    );
    assert_eq!(
        store
            .recent_metrics(None, 10, Some(threshold_after_trace), None)
            .unwrap()
            .len(),
        1
    );
    assert!(
        store
            .recent_llm(None, 10, Some(threshold_after_trace), None)
            .unwrap()
            .is_empty()
    );
}

#[test]
fn recent_logs_apply_severity_correlation_and_text_filters() {
    let tempdir = tempdir().unwrap();
    let store = Store::open(&tempdir.path().join("ottyel.db"), 24, 1000).unwrap();
    let now = now_nanos();

    store.ingest_logs(log_request(now)).unwrap();

    let error_logs = store
        .recent_logs(
            None,
            10,
            None,
            None,
            &LogFilters {
                severity: LogSeverityFilter::Error,
                ..LogFilters::default()
            },
        )
        .unwrap();
    assert_eq!(error_logs.len(), 1);
    assert_eq!(error_logs[0].severity, "ERROR");

    let span_linked = store
        .recent_logs(
            None,
            10,
            None,
            None,
            &LogFilters {
                correlation: LogCorrelationFilter::SpanLinked,
                ..LogFilters::default()
            },
        )
        .unwrap();
    assert_eq!(span_linked.len(), 1);
    assert!(!span_linked[0].span_id.is_empty());

    let uncorrelated = store
        .recent_logs(
            None,
            10,
            None,
            None,
            &LogFilters {
                correlation: LogCorrelationFilter::Uncorrelated,
                ..LogFilters::default()
            },
        )
        .unwrap();
    assert_eq!(uncorrelated.len(), 1);
    assert!(uncorrelated[0].trace_id.is_empty());

    let pane_text = store
        .recent_logs(
            None,
            10,
            None,
            None,
            &LogFilters {
                search_query: Some("validation".to_string()),
                ..LogFilters::default()
            },
        )
        .unwrap();
    assert_eq!(pane_text.len(), 1);
    assert!(pane_text[0].body.contains("validation"));
}

fn trace_request(now: i64) -> ExportTraceServiceRequest {
    let now = now as u64;
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
                    trace_id: vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16],
                    span_id: vec![1, 2, 3, 4, 5, 6, 7, 8],
                    parent_span_id: vec![],
                    trace_state: String::new(),
                    name: "chat.completion".to_string(),
                    kind: span::SpanKind::Server as i32,
                    start_time_unix_nano: now,
                    end_time_unix_nano: now + 2_000_000,
                    attributes: vec![
                        string_attr("llm.provider", "openai"),
                        string_attr("llm.model_name", "gpt-5.4"),
                        string_attr("input.value", "hello"),
                        string_attr("output.value", "world"),
                        int_attr("llm.token_count.prompt", 5),
                        int_attr("llm.token_count.completion", 7),
                    ],
                    dropped_attributes_count: 0,
                    events: vec![Event {
                        time_unix_nano: now + 1_000_000,
                        name: "model.invoke".to_string(),
                        attributes: vec![string_attr("event.phase", "request")],
                        dropped_attributes_count: 0,
                    }],
                    dropped_events_count: 0,
                    links: vec![Link {
                        trace_id: vec![7; 16],
                        span_id: vec![9; 8],
                        trace_state: "linked=true".to_string(),
                        attributes: vec![string_attr("link.kind", "retry")],
                        dropped_attributes_count: 0,
                        flags: 0,
                    }],
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

fn log_request(now: i64) -> ExportLogsServiceRequest {
    let now = now as u64;
    ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            resource: Some(Resource {
                attributes: vec![string_attr("service.name", "api")],
                dropped_attributes_count: 0,
                entity_refs: Vec::new(),
            }),
            schema_url: String::new(),
            scope_logs: vec![ScopeLogs {
                scope: Some(InstrumentationScope::default()),
                schema_url: String::new(),
                log_records: vec![
                    LogRecord {
                        time_unix_nano: now + 2_000_000,
                        observed_time_unix_nano: now + 2_000_100,
                        severity_number: 0,
                        severity_text: "INFO".to_string(),
                        body: Some(AnyValue {
                            value: Some(any_value::Value::StringValue(
                                "completion finished".to_string(),
                            )),
                        }),
                        attributes: vec![string_attr("phase", "completion")],
                        dropped_attributes_count: 0,
                        flags: 0,
                        trace_id: vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16],
                        span_id: vec![1, 2, 3, 4, 5, 6, 7, 8],
                        event_name: String::new(),
                    },
                    LogRecord {
                        time_unix_nano: now + 2_000_200,
                        observed_time_unix_nano: now + 2_000_250,
                        severity_number: 0,
                        severity_text: "ERROR".to_string(),
                        body: Some(AnyValue {
                            value: Some(any_value::Value::StringValue(
                                "validation failed".to_string(),
                            )),
                        }),
                        attributes: vec![string_attr("error.type", "validation")],
                        dropped_attributes_count: 0,
                        flags: 0,
                        trace_id: vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16],
                        span_id: vec![],
                        event_name: String::new(),
                    },
                    LogRecord {
                        time_unix_nano: now + 2_000_300,
                        observed_time_unix_nano: now + 2_000_350,
                        severity_number: 0,
                        severity_text: "DEBUG".to_string(),
                        body: Some(AnyValue {
                            value: Some(any_value::Value::StringValue(
                                "{\"message\":\"cache warm\",\"hit\":true}".to_string(),
                            )),
                        }),
                        attributes: vec![string_attr("cache.layer", "memory")],
                        dropped_attributes_count: 0,
                        flags: 0,
                        trace_id: vec![],
                        span_id: vec![],
                        event_name: String::new(),
                    },
                ],
            }],
        }],
    }
}

fn metric_request(now: i64) -> ExportMetricsServiceRequest {
    let now = now as u64;
    ExportMetricsServiceRequest {
        resource_metrics: vec![ResourceMetrics {
            resource: Some(Resource {
                attributes: vec![string_attr("service.name", "api")],
                dropped_attributes_count: 0,
                entity_refs: Vec::new(),
            }),
            schema_url: String::new(),
            scope_metrics: vec![ScopeMetrics {
                scope: Some(InstrumentationScope::default()),
                schema_url: String::new(),
                metrics: vec![Metric {
                    name: "tokens.total".to_string(),
                    description: String::new(),
                    unit: "1".to_string(),
                    metadata: vec![],
                    data: Some(metric::Data::Gauge(Gauge {
                        data_points: vec![NumberDataPoint {
                            attributes: vec![],
                            start_time_unix_nano: 0,
                            time_unix_nano: now + 2_500_000,
                            exemplars: vec![],
                            flags: 0,
                            value: Some(number_data_point::Value::AsInt(12)),
                        }],
                    })),
                }],
            }],
        }],
    }
}

fn string_attr(key: &str, value: &str) -> KeyValue {
    KeyValue {
        key: key.to_string(),
        value: Some(AnyValue {
            value: Some(any_value::Value::StringValue(value.to_string())),
        }),
    }
}

fn int_attr(key: &str, value: i64) -> KeyValue {
    KeyValue {
        key: key.to_string(),
        value: Some(AnyValue {
            value: Some(any_value::Value::IntValue(value)),
        }),
    }
}

fn now_nanos() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos() as i64
}
