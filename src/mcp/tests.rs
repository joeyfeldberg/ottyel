use std::io::Write;

use opentelemetry_proto::tonic::{
    collector::{logs::v1::ExportLogsServiceRequest, trace::v1::ExportTraceServiceRequest},
    common::v1::{AnyValue, InstrumentationScope, KeyValue, any_value},
    logs::v1::{LogRecord, ResourceLogs, ScopeLogs},
    resource::v1::Resource,
    trace::v1::{ResourceSpans, ScopeSpans, Span, Status, span},
};
use serde_json::{Value, json};
use tempfile::NamedTempFile;

use super::{
    handle_request, protocol::JsonRpcRequest, resources::resource_templates_list_result,
    tools::tools_list_result,
};
use crate::{query::QueryService, store::Store};

#[test]
fn initialize_advertises_tools_and_resources() {
    let response = handle_request(
        &empty_query(),
        JsonRpcRequest {
            id: Some(json!(1)),
            method: "initialize".to_string(),
            params: json!({"protocolVersion": "2025-06-18"}),
        },
    )
    .unwrap();

    assert_eq!(response["result"]["protocolVersion"], "2025-06-18");
    assert!(response["result"]["capabilities"]["tools"].is_object());
    assert!(response["result"]["capabilities"]["resources"].is_object());
}

#[test]
fn tools_list_exposes_trace_and_llm_tools() {
    let result = tools_list_result();
    let names = result["tools"]
        .as_array()
        .unwrap()
        .iter()
        .map(|tool| tool["name"].as_str().unwrap())
        .collect::<Vec<_>>();

    assert!(names.contains(&"search_traces"));
    assert!(names.contains(&"get_trace"));
    assert!(names.contains(&"search_llm"));
    assert!(names.contains(&"get_llm_timeline"));
}

#[test]
fn resource_templates_list_exposes_entity_templates() {
    let result = resource_templates_list_result();
    let templates = result["resourceTemplates"]
        .as_array()
        .unwrap()
        .iter()
        .map(|template| template["uriTemplate"].as_str().unwrap())
        .collect::<Vec<_>>();

    assert!(templates.contains(&"ottyel://trace/{trace_id}"));
    assert!(templates.contains(&"ottyel://logs/{trace_id}"));
    assert!(templates.contains(&"ottyel://llm/{trace_id}/{span_id}/timeline"));
}

#[test]
fn trace_resource_template_reads_trace_detail() {
    let query = query_with_trace_and_logs();
    let response = handle_request(
        &query,
        JsonRpcRequest {
            id: Some(json!("trace-resource")),
            method: "resources/read".to_string(),
            params: json!({
                "uri": "ottyel://trace/02020202020202020202020202020202"
            }),
        },
    )
    .unwrap();
    let text = response["result"]["contents"][0]["text"].as_str().unwrap();
    let payload = serde_json::from_str::<Value>(text).unwrap();

    assert_eq!(payload["traceId"], "02020202020202020202020202020202");
    assert_eq!(payload["spans"].as_array().unwrap().len(), 1);
}

#[test]
fn logs_resource_template_reads_trace_logs() {
    let query = query_with_trace_and_logs();
    let response = handle_request(
        &query,
        JsonRpcRequest {
            id: Some(json!("logs-resource")),
            method: "resources/read".to_string(),
            params: json!({
                "uri": "ottyel://logs/01010101010101010101010101010101"
            }),
        },
    )
    .unwrap();
    let text = response["result"]["contents"][0]["text"].as_str().unwrap();
    let payload = serde_json::from_str::<Value>(text).unwrap();

    assert_eq!(payload["traceId"], "01010101010101010101010101010101");
    assert_eq!(payload["logs"].as_array().unwrap().len(), 2);
}

#[test]
fn search_traces_tool_returns_structured_content() {
    let query = empty_query();
    let response = handle_request(
        &query,
        JsonRpcRequest {
            id: Some(json!("tool-1")),
            method: "tools/call".to_string(),
            params: json!({
                "name": "search_traces",
                "arguments": {
                    "limit": 5,
                    "timeWindow": "24h"
                }
            }),
        },
    )
    .unwrap();

    assert_eq!(response["id"], "tool-1");
    assert!(response["result"]["structuredContent"]["traces"].is_array());
    assert_eq!(response["result"]["content"][0]["type"], "text");
}

#[test]
fn search_traces_accepts_returned_cursor() {
    let query = query_with_trace_and_logs();
    let first = call_tool(
        &query,
        "search_traces",
        json!({
            "limit": 1,
            "timeWindow": "24h",
        }),
    );
    let cursor = first["nextCursor"].clone();
    let second = call_tool(
        &query,
        "search_traces",
        json!({
            "limit": 1,
            "timeWindow": "24h",
            "cursor": cursor,
        }),
    );

    assert_eq!(first["traces"].as_array().unwrap().len(), 1);
    assert_eq!(second["traces"].as_array().unwrap().len(), 1);
    assert_ne!(
        first["traces"][0]["trace_id"],
        second["traces"][0]["trace_id"]
    );
}

#[test]
fn search_logs_accepts_returned_cursor() {
    let query = query_with_trace_and_logs();
    let first = call_tool(
        &query,
        "search_logs",
        json!({
            "limit": 1,
            "timeWindow": "24h",
        }),
    );
    let cursor = first["nextCursor"].clone();
    let second = call_tool(
        &query,
        "search_logs",
        json!({
            "limit": 1,
            "timeWindow": "24h",
            "cursor": cursor,
        }),
    );

    assert_eq!(first["logs"].as_array().unwrap().len(), 1);
    assert_eq!(second["logs"].as_array().unwrap().len(), 1);
    assert_ne!(first["logs"][0]["body"], second["logs"][0]["body"]);
}

fn empty_query() -> QueryService {
    let file = NamedTempFile::new().unwrap();
    let store = Store::open(file.path(), 24, 1000).unwrap();
    QueryService::new(store, 50)
}

fn query_with_trace_and_logs() -> QueryService {
    let file = NamedTempFile::new().unwrap();
    let store = Store::open(file.path(), 24, 1000).unwrap();
    let now = current_unix_nanos();
    store.ingest_traces(trace_request(now, 1)).unwrap();
    store
        .ingest_traces(trace_request(now + 10_000_000, 2))
        .unwrap();
    store.ingest_logs(log_request(now)).unwrap();
    QueryService::new(store, 50)
}

fn call_tool(query: &QueryService, name: &str, arguments: Value) -> Value {
    let response = handle_request(
        query,
        JsonRpcRequest {
            id: Some(json!("tool")),
            method: "tools/call".to_string(),
            params: json!({
                "name": name,
                "arguments": arguments,
            }),
        },
    )
    .unwrap();
    response["result"]["structuredContent"].clone()
}

#[test]
fn stdio_server_writes_one_response_per_request_line() {
    let mut file = NamedTempFile::new().unwrap();
    writeln!(file, r#"{{"jsonrpc":"2.0","id":1,"method":"tools/list"}}"#).unwrap();

    let request =
        serde_json::from_str::<JsonRpcRequest>(r#"{"jsonrpc":"2.0","id":1,"method":"tools/list"}"#)
            .unwrap();
    let response = handle_request(&empty_query(), request).unwrap();

    assert_eq!(response["jsonrpc"], "2.0");
    assert_eq!(response["id"], 1);
    assert!(response["result"]["tools"].is_array());
}

fn trace_request(now: i64, index: u8) -> ExportTraceServiceRequest {
    ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            resource: Some(Resource {
                attributes: vec![string_attr("service.name", "api")],
                dropped_attributes_count: 0,
                entity_refs: Vec::new(),
            }),
            schema_url: String::new(),
            scope_spans: vec![ScopeSpans {
                scope: Some(InstrumentationScope::default()),
                schema_url: String::new(),
                spans: vec![Span {
                    trace_id: vec![index; 16],
                    span_id: vec![index; 8],
                    parent_span_id: Vec::new(),
                    trace_state: String::new(),
                    name: format!("request.{index}"),
                    kind: span::SpanKind::Server as i32,
                    start_time_unix_nano: now as u64,
                    end_time_unix_nano: (now + 1_000_000) as u64,
                    attributes: Vec::new(),
                    dropped_attributes_count: 0,
                    events: Vec::new(),
                    dropped_events_count: 0,
                    links: Vec::new(),
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
                    log_record(now + 1_000_000, "first"),
                    log_record(now + 2_000_000, "second"),
                ],
            }],
        }],
    }
}

fn log_record(time_unix_nano: i64, body: &str) -> LogRecord {
    LogRecord {
        time_unix_nano: time_unix_nano as u64,
        observed_time_unix_nano: time_unix_nano as u64,
        severity_number: 0,
        severity_text: "INFO".to_string(),
        body: Some(AnyValue {
            value: Some(any_value::Value::StringValue(body.to_string())),
        }),
        attributes: Vec::new(),
        dropped_attributes_count: 0,
        flags: 0,
        trace_id: vec![1; 16],
        span_id: vec![1; 8],
        event_name: String::new(),
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

fn current_unix_nanos() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as i64
}
