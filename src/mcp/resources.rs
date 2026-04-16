use anyhow::{Result, anyhow};
use serde_json::{Value, json};

use crate::query::{PageRequest, QueryFilters, QueryService};

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

fn resource(uri: &str, name: &str, description: &str) -> Value {
    json!({
        "uri": uri,
        "name": name,
        "description": description,
        "mimeType": "application/json",
    })
}

pub(super) fn resources_read_result(query: &QueryService, params: &Value) -> Result<Value> {
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
