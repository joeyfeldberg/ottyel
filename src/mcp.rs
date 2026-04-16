use std::io::{self, BufRead, Write};

use anyhow::{Context, Result, anyhow};
use serde_json::{Value, json};

use crate::query::{
    LogCorrelationFilter, LogFilters, LogSeverityFilter, PageRequest, QueryFilters, QueryService,
    TimeWindow,
};

const PROTOCOL_VERSION: &str = "2025-11-25";
const SERVER_NAME: &str = "ottyel";

pub fn serve_stdio(query: QueryService) -> Result<()> {
    let stdin = io::stdin();
    let mut stdout = io::stdout().lock();

    for line in stdin.lock().lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }

        let request = serde_json::from_str::<JsonRpcRequest>(&line)
            .with_context(|| "failed to decode MCP JSON-RPC message")?;
        if let Some(response) = handle_request(&query, request) {
            serde_json::to_writer(&mut stdout, &response)?;
            stdout.write_all(b"\n")?;
            stdout.flush()?;
        }
    }

    Ok(())
}

#[derive(Debug)]
struct JsonRpcRequest {
    id: Option<Value>,
    method: String,
    params: Value,
}

impl<'de> serde::Deserialize<'de> for JsonRpcRequest {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = Value::deserialize(deserializer)?;
        let id = value.get("id").cloned();
        let method = value
            .get("method")
            .and_then(Value::as_str)
            .ok_or_else(|| serde::de::Error::custom("missing method"))?
            .to_string();
        let params = value.get("params").cloned().unwrap_or(Value::Null);
        Ok(Self { id, method, params })
    }
}

fn handle_request(query: &QueryService, request: JsonRpcRequest) -> Option<Value> {
    let id = request.id?;
    let result = match request.method.as_str() {
        "initialize" => initialize_result(&request.params),
        "ping" => Ok(json!({})),
        "resources/list" => Ok(resources_list_result()),
        "resources/read" => resources_read_result(query, &request.params),
        "tools/list" => Ok(tools_list_result()),
        "tools/call" => tools_call_result(query, &request.params),
        "notifications/initialized" => return None,
        method => Err(anyhow!("unsupported MCP method: {method}")),
    };

    Some(match result {
        Ok(result) => json!({
            "jsonrpc": "2.0",
            "id": id,
            "result": result,
        }),
        Err(error) => json!({
            "jsonrpc": "2.0",
            "id": id,
            "error": {
                "code": -32603,
                "message": error.to_string(),
            },
        }),
    })
}

fn initialize_result(params: &Value) -> Result<Value> {
    let protocol_version = params
        .get("protocolVersion")
        .and_then(Value::as_str)
        .unwrap_or(PROTOCOL_VERSION);

    Ok(json!({
        "protocolVersion": protocol_version,
        "capabilities": {
            "resources": {
                "subscribe": false,
                "listChanged": false,
            },
            "tools": {
                "listChanged": false,
            },
        },
        "serverInfo": {
            "name": SERVER_NAME,
            "version": env!("CARGO_PKG_VERSION"),
        },
    }))
}

fn resources_list_result() -> Value {
    json!({
        "resources": [
            resource("ottyel://overview", "Overview", "Counts and currently available services"),
            resource("ottyel://traces/recent", "Recent Traces", "Recent trace summaries"),
            resource("ottyel://logs/recent", "Recent Logs", "Recent log records"),
            resource("ottyel://metrics/recent", "Recent Metrics", "Recent metric summaries"),
            resource("ottyel://llm/recent", "Recent LLM Calls", "Recent normalized LLM spans"),
            resource("ottyel://llm/rollups", "LLM Rollups", "LLM model, provider, and service rollups"),
        ],
    })
}

fn resource(uri: &str, name: &str, description: &str) -> Value {
    json!({
        "uri": uri,
        "name": name,
        "description": description,
        "mimeType": "application/json",
    })
}

fn resources_read_result(query: &QueryService, params: &Value) -> Result<Value> {
    let uri = required_str(params, "uri")?;
    let filters = QueryFilters::default();
    let payload = match uri {
        "ottyel://overview" => {
            let snapshot = query.snapshot(&filters)?;
            json!({
                "overview": snapshot.overview,
                "services": snapshot.services,
            })
        }
        "ottyel://traces/recent" => json!({
            "traces": query.traces_page(&filters, &PageRequest::first(50))?.items,
        }),
        "ottyel://logs/recent" => json!({
            "logs": query.logs_page(&filters, &PageRequest::first(50))?.items,
        }),
        "ottyel://metrics/recent" => json!({
            "metrics": query.metrics_page(&filters, &PageRequest::first(50))?.items,
        }),
        "ottyel://llm/recent" => json!({
            "llm": query.llm_page(&filters, &PageRequest::first(50))?.items,
        }),
        "ottyel://llm/rollups" => json!({
            "rollups": query.llm_rollups(&filters)?,
            "sessions": query.llm_sessions(&filters)?,
            "models": query.llm_model_comparisons(&filters)?,
            "top_calls": query.llm_top_calls(&filters)?,
        }),
        _ => return Err(anyhow!("unknown resource uri: {uri}")),
    };

    Ok(json!({
        "contents": [{
            "uri": uri,
            "mimeType": "application/json",
            "text": serde_json::to_string_pretty(&payload)?,
        }],
    }))
}

fn tools_list_result() -> Value {
    json!({
        "tools": [
            tool(
                "search_traces",
                "Search Traces",
                "Search recent trace summaries by service, text, error status, and time window.",
                json!({
                    "type": "object",
                    "properties": common_filter_properties({
                        let mut map = serde_json::Map::new();
                        map.insert("errorsOnly".to_string(), json!({"type": "boolean"}));
                        map
                    }),
                }),
            ),
            tool(
                "get_trace",
                "Get Trace",
                "Fetch full span detail for a trace ID.",
                json!({
                    "type": "object",
                    "properties": {
                        "traceId": {"type": "string"}
                    },
                    "required": ["traceId"],
                }),
            ),
            tool(
                "search_logs",
                "Search Logs",
                "Search recent logs by service, text, severity, correlation, trace ID, and span ID.",
                json!({
                    "type": "object",
                    "properties": common_filter_properties({
                        let mut map = serde_json::Map::new();
                        map.insert("severity".to_string(), json!({"type": "string", "enum": ["all", "error", "warn", "info", "debug"]}));
                        map.insert("correlation".to_string(), json!({"type": "string", "enum": ["all", "trace-linked", "span-linked", "uncorrelated"]}));
                        map.insert("traceId".to_string(), json!({"type": "string"}));
                        map.insert("spanId".to_string(), json!({"type": "string"}));
                        map
                    }),
                }),
            ),
            tool(
                "search_metrics",
                "Search Metrics",
                "Search recent metric summaries by service, text, and time window.",
                json!({
                    "type": "object",
                    "properties": common_filter_properties(serde_json::Map::new()),
                }),
            ),
            tool(
                "search_llm",
                "Search LLM Calls",
                "Search normalized LLM spans and include current aggregate sections.",
                json!({
                    "type": "object",
                    "properties": common_filter_properties(serde_json::Map::new()),
                }),
            ),
            tool(
                "get_llm_timeline",
                "Get LLM Timeline",
                "Fetch prompt/tool/output timeline steps for a selected LLM span.",
                json!({
                    "type": "object",
                    "properties": {
                        "traceId": {"type": "string"},
                        "spanId": {"type": "string"}
                    },
                    "required": ["traceId", "spanId"],
                }),
            ),
        ],
    })
}

fn common_filter_properties(extra: serde_json::Map<String, Value>) -> Value {
    let mut properties = serde_json::Map::from_iter([
        ("query".to_string(), json!({"type": "string"})),
        ("service".to_string(), json!({"type": "string"})),
        (
            "timeWindow".to_string(),
            json!({"type": "string", "enum": ["15m", "1h", "6h", "24h"]}),
        ),
        (
            "limit".to_string(),
            json!({"type": "integer", "minimum": 1, "maximum": 500}),
        ),
    ]);
    properties.extend(extra);
    Value::Object(properties)
}

fn tool(name: &str, title: &str, description: &str, input_schema: Value) -> Value {
    json!({
        "name": name,
        "title": title,
        "description": description,
        "inputSchema": input_schema,
    })
}

fn tools_call_result(query: &QueryService, params: &Value) -> Result<Value> {
    let name = required_str(params, "name")?;
    let arguments = params.get("arguments").unwrap_or(&Value::Null);
    let payload = match name {
        "search_traces" => {
            let filters = filters_from_args(arguments)?;
            let page = query.traces_page(&filters, &PageRequest::first(limit(arguments)))?;
            json!({"traces": page.items, "nextCursor": page.next_cursor})
        }
        "get_trace" => {
            let trace_id = required_str(arguments, "traceId")?;
            json!({"traceId": trace_id, "spans": query.trace_detail(trace_id)?})
        }
        "search_logs" => {
            let filters = filters_from_args(arguments)?;
            let page = query.logs_page(&filters, &PageRequest::first(limit(arguments)))?;
            json!({"logs": page.items, "nextCursor": page.next_cursor})
        }
        "search_metrics" => {
            let filters = filters_from_args(arguments)?;
            let page = query.metrics_page(&filters, &PageRequest::first(limit(arguments)))?;
            json!({"metrics": page.items, "nextCursor": page.next_cursor})
        }
        "search_llm" => {
            let filters = filters_from_args(arguments)?;
            let page = query.llm_page(&filters, &PageRequest::first(limit(arguments)))?;
            json!({
                "llm": page.items,
                "nextCursor": page.next_cursor,
                "rollups": query.llm_rollups(&filters)?,
                "sessions": query.llm_sessions(&filters)?,
                "models": query.llm_model_comparisons(&filters)?,
                "topCalls": query.llm_top_calls(&filters)?,
            })
        }
        "get_llm_timeline" => {
            let trace_id = required_str(arguments, "traceId")?;
            let span_id = required_str(arguments, "spanId")?;
            json!({
                "traceId": trace_id,
                "spanId": span_id,
                "timeline": query.llm_timeline(trace_id, span_id)?,
            })
        }
        _ => return Err(anyhow!("unknown MCP tool: {name}")),
    };

    Ok(tool_result(payload)?)
}

fn tool_result(payload: Value) -> Result<Value> {
    Ok(json!({
        "content": [{
            "type": "text",
            "text": serde_json::to_string_pretty(&payload)?,
        }],
        "structuredContent": payload,
    }))
}

fn filters_from_args(args: &Value) -> Result<QueryFilters> {
    let mut filters = QueryFilters::default();
    filters.service = optional_string(args, "service");
    filters.search_query = optional_string(args, "query");
    filters.errors_only = args
        .get("errorsOnly")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    filters.time_window = args
        .get("timeWindow")
        .and_then(Value::as_str)
        .map(parse_time_window)
        .transpose()?
        .unwrap_or(TimeWindow::TwentyFourHours);
    filters.log_filters = LogFilters {
        severity: args
            .get("severity")
            .and_then(Value::as_str)
            .map(parse_log_severity)
            .transpose()?
            .unwrap_or_default(),
        correlation: args
            .get("correlation")
            .and_then(Value::as_str)
            .map(parse_log_correlation)
            .transpose()?
            .unwrap_or_default(),
        search_query: optional_string(args, "query"),
        pinned_trace_id: optional_string(args, "traceId"),
        pinned_span_id: optional_string(args, "spanId"),
    };
    Ok(filters)
}

fn required_str<'a>(value: &'a Value, key: &str) -> Result<&'a str> {
    value
        .get(key)
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("missing required string argument: {key}"))
}

fn optional_string(value: &Value, key: &str) -> Option<String> {
    value
        .get(key)
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .map(str::to_string)
}

fn limit(args: &Value) -> usize {
    args.get("limit")
        .and_then(Value::as_u64)
        .unwrap_or(50)
        .clamp(1, 500) as usize
}

fn parse_time_window(value: &str) -> Result<TimeWindow> {
    match value {
        "15m" => Ok(TimeWindow::FifteenMinutes),
        "1h" => Ok(TimeWindow::OneHour),
        "6h" => Ok(TimeWindow::SixHours),
        "24h" => Ok(TimeWindow::TwentyFourHours),
        _ => Err(anyhow!("unsupported timeWindow: {value}")),
    }
}

fn parse_log_severity(value: &str) -> Result<LogSeverityFilter> {
    match value {
        "all" => Ok(LogSeverityFilter::All),
        "error" => Ok(LogSeverityFilter::Error),
        "warn" => Ok(LogSeverityFilter::Warn),
        "info" => Ok(LogSeverityFilter::Info),
        "debug" => Ok(LogSeverityFilter::Debug),
        _ => Err(anyhow!("unsupported severity: {value}")),
    }
}

fn parse_log_correlation(value: &str) -> Result<LogCorrelationFilter> {
    match value {
        "all" => Ok(LogCorrelationFilter::All),
        "trace-linked" => Ok(LogCorrelationFilter::TraceLinked),
        "span-linked" => Ok(LogCorrelationFilter::SpanLinked),
        "uncorrelated" => Ok(LogCorrelationFilter::Uncorrelated),
        _ => Err(anyhow!("unsupported correlation: {value}")),
    }
}

impl Default for QueryFilters {
    fn default() -> Self {
        Self {
            service: None,
            errors_only: false,
            time_window: TimeWindow::TwentyFourHours,
            search_query: None,
            log_filters: LogFilters::default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use tempfile::NamedTempFile;

    use super::*;
    use crate::store::Store;

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

    fn empty_query() -> QueryService {
        let file = NamedTempFile::new().unwrap();
        let store = Store::open(file.path(), 24, 1000).unwrap();
        QueryService::new(store, 50)
    }

    #[test]
    fn stdio_server_writes_one_response_per_request_line() {
        let mut file = NamedTempFile::new().unwrap();
        writeln!(file, r#"{{"jsonrpc":"2.0","id":1,"method":"tools/list"}}"#).unwrap();

        let request = serde_json::from_str::<JsonRpcRequest>(
            r#"{"jsonrpc":"2.0","id":1,"method":"tools/list"}"#,
        )
        .unwrap();
        let response = handle_request(&empty_query(), request).unwrap();

        assert_eq!(response["jsonrpc"], "2.0");
        assert_eq!(response["id"], 1);
        assert!(response["result"]["tools"].is_array());
    }
}
