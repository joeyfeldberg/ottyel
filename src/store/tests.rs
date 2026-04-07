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

use crate::{
    domain::{LlmRollupDimension, LlmTopCallKind},
    query::{LogCorrelationFilter, LogFilters, LogSeverityFilter, PageRequest},
};

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
    assert_eq!(llm[0].prompt_preview.as_deref(), Some("hello"));
    assert_eq!(llm[0].output_preview.as_deref(), Some("world"));
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

#[test]
fn cursor_pages_advance_without_rereading_rows() {
    let tempdir = tempdir().unwrap();
    let store = Store::open(&tempdir.path().join("ottyel.db"), 24, 1000).unwrap();
    let now = now_nanos();

    store.ingest_traces(trace_request(now)).unwrap();
    store
        .ingest_traces(trace_request_variant(
            now + 10_000_000,
            [2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17],
            [2, 3, 4, 5, 6, 7, 8, 9],
            "gpt-4o-mini",
            "hola",
            "mundo",
        ))
        .unwrap();
    store.ingest_logs(log_request(now)).unwrap();
    store.ingest_metrics(metric_request(now)).unwrap();
    store
        .ingest_metrics(metric_request_variant(now + 20_000_000, "tokens.total", 24))
        .unwrap();

    let trace_page_1 = store
        .recent_traces_page(None, false, &PageRequest::first(1), None, None)
        .unwrap();
    let trace_page_2 = store
        .recent_traces_page(
            None,
            false,
            &PageRequest {
                limit: 1,
                cursor: trace_page_1.next_cursor.clone(),
            },
            None,
            None,
        )
        .unwrap();
    assert_eq!(trace_page_1.items.len(), 1);
    assert_eq!(trace_page_2.items.len(), 1);
    assert_ne!(
        trace_page_1.items[0].trace_id,
        trace_page_2.items[0].trace_id
    );

    let log_page_1 = store
        .recent_logs_page(
            None,
            &PageRequest::first(1),
            None,
            None,
            &LogFilters::default(),
        )
        .unwrap();
    let log_page_2 = store
        .recent_logs_page(
            None,
            &PageRequest {
                limit: 1,
                cursor: log_page_1.next_cursor.clone(),
            },
            None,
            None,
            &LogFilters::default(),
        )
        .unwrap();
    assert_eq!(log_page_1.items.len(), 1);
    assert_eq!(log_page_2.items.len(), 1);
    assert_ne!(log_page_1.items[0].body, log_page_2.items[0].body);

    let metric_page_1 = store
        .recent_metrics_page(None, &PageRequest::first(1), None, None)
        .unwrap();
    let metric_page_2 = store
        .recent_metrics_page(
            None,
            &PageRequest {
                limit: 1,
                cursor: metric_page_1.next_cursor.clone(),
            },
            None,
            None,
        )
        .unwrap();
    assert_eq!(metric_page_1.items.len(), 1);
    assert_eq!(metric_page_2.items.len(), 1);
    assert_ne!(
        metric_page_1.items[0].timestamp_unix_nano,
        metric_page_2.items[0].timestamp_unix_nano
    );

    let llm_page_1 = store
        .recent_llm_page(None, &PageRequest::first(1), None, None)
        .unwrap();
    let llm_page_2 = store
        .recent_llm_page(
            None,
            &PageRequest {
                limit: 1,
                cursor: llm_page_1.next_cursor.clone(),
            },
            None,
            None,
        )
        .unwrap();
    assert_eq!(llm_page_1.items.len(), 1);
    assert_eq!(llm_page_2.items.len(), 1);
    assert_ne!(llm_page_1.items[0].span_id, llm_page_2.items[0].span_id);
}

#[test]
fn llm_rollups_group_tokens_latency_errors_and_cost() {
    let tempdir = tempdir().unwrap();
    let store = Store::open(&tempdir.path().join("ottyel.db"), 24, 1000).unwrap();
    let now = now_nanos();

    store.ingest_traces(trace_request(now)).unwrap();
    store
        .ingest_traces(trace_request_variant(
            now + 10_000_000,
            [2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17],
            [2, 3, 4, 5, 6, 7, 8, 9],
            "gpt-5.4",
            "hola",
            "mundo",
        ))
        .unwrap();
    store
        .ingest_traces(trace_request_variant(
            now + 20_000_000,
            [3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18],
            [3, 4, 5, 6, 7, 8, 9, 10],
            "gpt-4o-mini",
            "bonjour",
            "monde",
        ))
        .unwrap();

    let rollups = store.llm_rollups(None, None, None).unwrap();
    let gpt_54 = rollups
        .iter()
        .find(|item| item.dimension == LlmRollupDimension::Model && item.label == "gpt-5.4")
        .unwrap();

    assert_eq!(gpt_54.call_count, 2);
    assert_eq!(gpt_54.error_count, 0);
    assert_eq!(gpt_54.input_tokens, 10);
    assert_eq!(gpt_54.output_tokens, 14);
    assert_eq!(gpt_54.total_tokens, 24);
    assert_eq!(gpt_54.cost, Some(0.004));
    assert_eq!(gpt_54.avg_latency_ms, Some(2.0));

    let provider = rollups
        .iter()
        .find(|item| item.dimension == LlmRollupDimension::Provider && item.label == "openai")
        .unwrap();
    assert_eq!(provider.call_count, 3);
    assert_eq!(provider.total_tokens, 36);

    let comparisons = store.llm_model_comparisons(None, None, None).unwrap();
    assert_eq!(comparisons[0].model, "gpt-5.4");
    assert_eq!(comparisons[0].call_count, 2);
    assert_eq!(comparisons[0].total_tokens, 24);

    let top_calls = store.llm_top_calls(None, None, None).unwrap();
    assert!(
        top_calls
            .iter()
            .any(|call| call.kind == LlmTopCallKind::Tokens && call.model == "gpt-5.4")
    );
    assert!(
        top_calls
            .iter()
            .any(|call| call.kind == LlmTopCallKind::Cost && call.cost == Some(0.002))
    );
}

#[test]
fn llm_sessions_group_when_conversation_attrs_exist() {
    let tempdir = tempdir().unwrap();
    let store = Store::open(&tempdir.path().join("ottyel.db"), 24, 1000).unwrap();
    let now = now_nanos();

    store.ingest_traces(trace_request(now)).unwrap();
    store
        .ingest_traces(trace_request_variant(
            now + 10_000_000,
            [2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17],
            [2, 3, 4, 5, 6, 7, 8, 9],
            "gpt-4o-mini",
            "hola",
            "mundo",
        ))
        .unwrap();

    let sessions = store.llm_sessions(None, None, None).unwrap();
    assert_eq!(sessions.len(), 1);
    assert_eq!(sessions[0].correlation_kind, "conversation");
    assert_eq!(sessions[0].correlation_id, "conv-test");
    assert_eq!(sessions[0].call_count, 2);
    assert_eq!(sessions[0].model_count, 2);
    assert_eq!(sessions[0].provider_count, 1);
    assert_eq!(sessions[0].total_tokens, 24);
    assert_eq!(sessions[0].duration_ms, 12.0);
}

fn trace_request(now: i64) -> ExportTraceServiceRequest {
    trace_request_variant(
        now,
        [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16],
        [1, 2, 3, 4, 5, 6, 7, 8],
        "gpt-5.4",
        "hello",
        "world",
    )
}

fn trace_request_variant(
    now: i64,
    trace_id: [u8; 16],
    span_id: [u8; 8],
    model: &str,
    input: &str,
    output: &str,
) -> ExportTraceServiceRequest {
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
                    trace_id: trace_id.to_vec(),
                    span_id: span_id.to_vec(),
                    parent_span_id: vec![],
                    trace_state: String::new(),
                    name: "chat.completion".to_string(),
                    kind: span::SpanKind::Server as i32,
                    start_time_unix_nano: now,
                    end_time_unix_nano: now + 2_000_000,
                    attributes: vec![
                        string_attr("llm.provider", "openai"),
                        string_attr("llm.model_name", model),
                        string_attr("conversation.id", "conv-test"),
                        string_attr("input.value", input),
                        string_attr("output.value", output),
                        int_attr("llm.token_count.prompt", 5),
                        int_attr("llm.token_count.completion", 7),
                        double_attr("llm.cost.total", 0.002),
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
    metric_request_variant(now, "tokens.total", 12)
}

fn metric_request_variant(now: i64, metric_name: &str, value: i64) -> ExportMetricsServiceRequest {
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
                    name: metric_name.to_string(),
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
                            value: Some(number_data_point::Value::AsInt(value)),
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

fn double_attr(key: &str, value: f64) -> KeyValue {
    KeyValue {
        key: key.to_string(),
        value: Some(AnyValue {
            value: Some(any_value::Value::DoubleValue(value)),
        }),
    }
}

fn now_nanos() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos() as i64
}
