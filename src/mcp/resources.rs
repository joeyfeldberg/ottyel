use anyhow::{Result, anyhow};
use serde_json::{Value, json};

use crate::query::{LogFilters, PageRequest, QueryFilters, QueryService};

use super::common::required_str;

pub(super) fn resources_list_result() -> Value {
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

pub(super) fn resource_templates_list_result() -> Value {
    json!({
        "resourceTemplates": [
            resource_template(
                "ottyel://trace/{trace_id}",
                "Trace Detail",
                "Full span detail for a trace ID",
            ),
            resource_template(
                "ottyel://logs/{trace_id}",
                "Trace Logs",
                "Logs correlated with a trace ID",
            ),
            resource_template(
                "ottyel://llm/{trace_id}/{span_id}/timeline",
                "LLM Timeline",
                "Prompt, tool, and output timeline for an LLM span",
            ),
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

fn resource_template(uri_template: &str, name: &str, description: &str) -> Value {
    json!({
        "uriTemplate": uri_template,
        "name": name,
        "description": description,
        "mimeType": "application/json",
    })
}

pub(super) fn resources_read_result(query: &QueryService, params: &Value) -> Result<Value> {
    let uri = required_str(params, "uri")?;
    let filters = QueryFilters::default();
    let payload = if let Some(trace_id) = uri.strip_prefix("ottyel://trace/") {
        json!({
            "traceId": trace_id,
            "spans": query.trace_detail(trace_id)?,
        })
    } else if let Some(trace_id) = uri.strip_prefix("ottyel://logs/") {
        let mut filters = QueryFilters::default();
        filters.log_filters = LogFilters {
            pinned_trace_id: Some(trace_id.to_string()),
            ..LogFilters::default()
        };
        json!({
            "traceId": trace_id,
            "logs": query.logs_page(&filters, &PageRequest::first(100))?.items,
        })
    } else if let Some((trace_id, span_id)) = parse_llm_timeline_uri(uri) {
        json!({
            "traceId": trace_id,
            "spanId": span_id,
            "timeline": query.llm_timeline(trace_id, span_id)?,
        })
    } else {
        match uri {
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
        }
    };

    Ok(json!({
        "contents": [{
            "uri": uri,
            "mimeType": "application/json",
            "text": serde_json::to_string_pretty(&payload)?,
        }],
    }))
}

fn parse_llm_timeline_uri(uri: &str) -> Option<(&str, &str)> {
    let rest = uri.strip_prefix("ottyel://llm/")?;
    let (trace_id, suffix) = rest.split_once('/')?;
    let span_id = suffix.strip_suffix("/timeline")?;
    if trace_id.is_empty() || span_id.is_empty() || span_id.contains('/') {
        return None;
    }
    Some((trace_id, span_id))
}
