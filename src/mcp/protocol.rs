use std::fmt;

use anyhow::Result;
use serde_json::{Value, json};

const PROTOCOL_VERSION: &str = "2025-11-25";
const SERVER_NAME: &str = "ottyel";

#[derive(Debug)]
pub(super) struct JsonRpcRequest {
    pub id: Option<Value>,
    pub method: String,
    pub params: Value,
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

pub(super) fn initialize_result(params: &Value) -> Result<Value> {
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

#[derive(Debug)]
pub(super) struct McpError {
    pub code: i64,
    pub message: String,
}

impl McpError {
    pub fn method_not_found(message: impl Into<String>) -> Self {
        Self {
            code: -32601,
            message: message.into(),
        }
    }

    pub fn invalid_params(message: impl Into<String>) -> Self {
        Self {
            code: -32602,
            message: message.into(),
        }
    }

    pub fn internal(message: impl Into<String>) -> Self {
        Self {
            code: -32603,
            message: message.into(),
        }
    }
}

impl fmt::Display for McpError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for McpError {}
