mod common;
mod protocol;
mod resources;
mod tools;

#[cfg(test)]
mod tests;

use std::io::{self, BufRead, Write};

use anyhow::{Context, Result};
use serde_json::{Value, json};

use crate::query::QueryService;

use protocol::{JsonRpcRequest, McpError, initialize_result};
use resources::{resource_templates_list_result, resources_list_result, resources_read_result};
use tools::{tools_call_result, tools_list_result};

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

fn handle_request(query: &QueryService, request: JsonRpcRequest) -> Option<Value> {
    let id = request.id?;
    let result = match request.method.as_str() {
        "initialize" => initialize_result(&request.params)
            .map_err(|error| McpError::internal(error.to_string())),
        "ping" => Ok(json!({})),
        "resources/list" => Ok(resources_list_result()),
        "resources/templates/list" => Ok(resource_templates_list_result()),
        "resources/read" => resources_read_result(query, &request.params),
        "tools/list" => Ok(tools_list_result()),
        "tools/call" => tools_call_result(query, &request.params),
        "notifications/initialized" => return None,
        method => Err(McpError::method_not_found(format!(
            "unsupported MCP method: {method}"
        ))),
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
                "code": error.code,
                "message": error.message,
            },
        }),
    })
}
