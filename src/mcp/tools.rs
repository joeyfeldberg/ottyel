use anyhow::Context;
use serde_json::{Value, json};

use crate::query::{
    LlmCursor, LogCorrelationFilter, LogCursor, LogFilters, LogSeverityFilter, MetricCursor,
    PageRequest, QueryFilters, QueryService, TimeWindow, TraceCursor,
};

use super::common::{optional_string, required_str};
use super::protocol::McpError;

pub(super) fn tools_list_result() -> Value {
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
        (
            "cursor".to_string(),
            json!({
                "type": "object",
                "description": "Opaque cursor returned as nextCursor by the previous call for the same tool and filters."
            }),
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

pub(super) fn tools_call_result(query: &QueryService, params: &Value) -> Result<Value, McpError> {
    let name = required_str(params, "name")?;
    let arguments = params.get("arguments").unwrap_or(&Value::Null);
    let payload = match name {
        "search_traces" => {
            let filters = filters_from_args(arguments)?;
            let page = query
                .traces_page(
                    &filters,
                    &PageRequest {
                        limit: limit(arguments),
                        cursor: parse_cursor::<TraceCursor>(arguments)?,
                    },
                )
                .map_err(internal_error)?;
            json!({"traces": page.items, "nextCursor": page.next_cursor})
        }
        "get_trace" => {
            let trace_id = required_str(arguments, "traceId")?;
            json!({"traceId": trace_id, "spans": query.trace_detail(trace_id).map_err(internal_error)?})
        }
        "search_logs" => {
            let filters = filters_from_args(arguments)?;
            let page = query
                .logs_page(
                    &filters,
                    &PageRequest {
                        limit: limit(arguments),
                        cursor: parse_cursor::<LogCursor>(arguments)?,
                    },
                )
                .map_err(internal_error)?;
            json!({"logs": page.items, "nextCursor": page.next_cursor})
        }
        "search_metrics" => {
            let filters = filters_from_args(arguments)?;
            let page = query
                .metrics_page(
                    &filters,
                    &PageRequest {
                        limit: limit(arguments),
                        cursor: parse_cursor::<MetricCursor>(arguments)?,
                    },
                )
                .map_err(internal_error)?;
            json!({"metrics": page.items, "nextCursor": page.next_cursor})
        }
        "search_llm" => {
            let filters = filters_from_args(arguments)?;
            let page = query
                .llm_page(
                    &filters,
                    &PageRequest {
                        limit: limit(arguments),
                        cursor: parse_cursor::<LlmCursor>(arguments)?,
                    },
                )
                .map_err(internal_error)?;
            json!({
                "llm": page.items,
                "nextCursor": page.next_cursor,
                "rollups": query.llm_rollups(&filters).map_err(internal_error)?,
                "sessions": query.llm_sessions(&filters).map_err(internal_error)?,
                "models": query.llm_model_comparisons(&filters).map_err(internal_error)?,
                "topCalls": query.llm_top_calls(&filters).map_err(internal_error)?,
            })
        }
        "get_llm_timeline" => {
            let trace_id = required_str(arguments, "traceId")?;
            let span_id = required_str(arguments, "spanId")?;
            json!({
                "traceId": trace_id,
                "spanId": span_id,
                "timeline": query.llm_timeline(trace_id, span_id).map_err(internal_error)?,
            })
        }
        _ => {
            return Err(McpError::method_not_found(format!(
                "unknown MCP tool: {name}"
            )));
        }
    };

    tool_result(payload)
}

fn tool_result(payload: Value) -> Result<Value, McpError> {
    Ok(json!({
        "content": [{
            "type": "text",
            "text": serde_json::to_string_pretty(&payload).map_err(internal_error)?,
        }],
        "structuredContent": payload,
    }))
}

fn filters_from_args(args: &Value) -> Result<QueryFilters, McpError> {
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

fn limit(args: &Value) -> usize {
    args.get("limit")
        .and_then(Value::as_u64)
        .unwrap_or(50)
        .clamp(1, 500) as usize
}

fn parse_cursor<C>(args: &Value) -> Result<Option<C>, McpError>
where
    C: serde::de::DeserializeOwned,
{
    match args.get("cursor") {
        Some(Value::Null) | None => Ok(None),
        Some(value) => Ok(Some(
            serde_json::from_value(value.clone())
                .with_context(|| "cursor shape does not match this MCP tool")
                .map_err(|error| McpError::invalid_params(error.to_string()))?,
        )),
    }
}

fn parse_time_window(value: &str) -> Result<TimeWindow, McpError> {
    match value {
        "15m" => Ok(TimeWindow::FifteenMinutes),
        "1h" => Ok(TimeWindow::OneHour),
        "6h" => Ok(TimeWindow::SixHours),
        "24h" => Ok(TimeWindow::TwentyFourHours),
        _ => Err(McpError::invalid_params(format!(
            "unsupported timeWindow: {value}"
        ))),
    }
}

fn parse_log_severity(value: &str) -> Result<LogSeverityFilter, McpError> {
    match value {
        "all" => Ok(LogSeverityFilter::All),
        "error" => Ok(LogSeverityFilter::Error),
        "warn" => Ok(LogSeverityFilter::Warn),
        "info" => Ok(LogSeverityFilter::Info),
        "debug" => Ok(LogSeverityFilter::Debug),
        _ => Err(McpError::invalid_params(format!(
            "unsupported severity: {value}"
        ))),
    }
}

fn parse_log_correlation(value: &str) -> Result<LogCorrelationFilter, McpError> {
    match value {
        "all" => Ok(LogCorrelationFilter::All),
        "trace-linked" => Ok(LogCorrelationFilter::TraceLinked),
        "span-linked" => Ok(LogCorrelationFilter::SpanLinked),
        "uncorrelated" => Ok(LogCorrelationFilter::Uncorrelated),
        _ => Err(McpError::invalid_params(format!(
            "unsupported correlation: {value}"
        ))),
    }
}

fn internal_error(error: impl std::fmt::Display) -> McpError {
    McpError::internal(error.to_string())
}
