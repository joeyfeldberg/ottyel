use std::time::{SystemTime, UNIX_EPOCH};

use opentelemetry_proto::tonic::{
    common::v1::{AnyValue, any_value},
    logs::v1::LogRecord,
    metrics::v1::number_data_point::Value as NumberValue,
    resource::v1::Resource,
    trace::v1::status::StatusCode,
};
use serde_json::Value;

use crate::{
    domain::{AttributeMap, attributes_to_map},
    query::{LogCorrelationFilter, LogSeverityFilter},
};

pub(super) fn resource_to_map(resource: Option<&Resource>) -> AttributeMap {
    resource
        .map(|resource| attributes_to_map(&resource.attributes))
        .unwrap_or_default()
}

pub(super) fn span_kind_name(kind: i32) -> String {
    match kind {
        1 => "INTERNAL",
        2 => "SERVER",
        3 => "CLIENT",
        4 => "PRODUCER",
        5 => "CONSUMER",
        _ => "UNSPECIFIED",
    }
    .to_string()
}

pub(super) fn status_code_name(
    status: Option<&opentelemetry_proto::tonic::trace::v1::Status>,
) -> String {
    match status.map(|status| status.code) {
        Some(code) if code == StatusCode::Ok as i32 => "STATUS_CODE_OK",
        Some(code) if code == StatusCode::Error as i32 => "STATUS_CODE_ERROR",
        _ => "STATUS_CODE_UNSET",
    }
    .to_string()
}

pub(super) fn log_time_unix_nano(log: &LogRecord) -> i64 {
    let observed = log.observed_time_unix_nano as i64;
    let timestamp = log.time_unix_nano as i64;
    timestamp.max(observed)
}

pub(super) fn number_value(value: Option<&NumberValue>) -> Option<f64> {
    match value {
        Some(NumberValue::AsDouble(number)) => Some(*number),
        Some(NumberValue::AsInt(number)) => Some(*number as f64),
        None => None,
    }
}

pub(super) fn format_metric_summary(kind: &str, value: Option<f64>) -> String {
    match value {
        Some(number) => format!("{kind}={number:.3}"),
        None => kind.to_string(),
    }
}

pub(super) fn hex_bytes(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

pub(super) fn any_value_text(value: Option<&AnyValue>) -> String {
    match value.and_then(|inner| inner.value.as_ref()) {
        Some(any_value::Value::StringValue(text)) => text.clone(),
        Some(any_value::Value::BoolValue(value)) => value.to_string(),
        Some(any_value::Value::IntValue(value)) => value.to_string(),
        Some(any_value::Value::DoubleValue(value)) => value.to_string(),
        Some(any_value::Value::BytesValue(value)) => hex_bytes(value),
        Some(any_value::Value::KvlistValue(value)) => serde_json::to_string(
            &value
                .values
                .iter()
                .map(|item| {
                    (
                        item.key.clone(),
                        crate::domain::any_value_to_json(item.value.as_ref()),
                    )
                })
                .collect::<serde_json::Map<String, Value>>(),
        )
        .unwrap_or_default(),
        Some(any_value::Value::ArrayValue(value)) => serde_json::to_string(
            &value
                .values
                .iter()
                .map(|item| crate::domain::any_value_to_json(Some(item)))
                .collect::<Vec<Value>>(),
        )
        .unwrap_or_default(),
        None => String::new(),
    }
}

pub(super) fn now_unix_nanos() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as i64
}

pub(super) fn escape_sql(value: &str) -> String {
    value.replace('\'', "''")
}

pub(super) fn threshold_clause(column: &str, threshold_unix_nano: Option<i64>) -> String {
    threshold_unix_nano
        .map(|threshold| format!(" WHERE {column} >= {threshold}"))
        .unwrap_or_default()
}

pub(super) fn and_threshold_clause(column: &str, threshold_unix_nano: Option<i64>) -> String {
    threshold_unix_nano
        .map(|threshold| format!(" AND {column} >= {threshold}"))
        .unwrap_or_default()
}

pub(super) fn like_pattern(value: &str) -> String {
    format!("%{}%", escape_like(value))
}

fn escape_like(value: &str) -> String {
    escape_sql(value)
        .replace('\\', "\\\\")
        .replace('%', "\\%")
        .replace('_', "\\_")
}

pub(super) fn log_severity_clause(filter: LogSeverityFilter) -> Option<String> {
    match filter {
        LogSeverityFilter::All => None,
        LogSeverityFilter::Error => {
            Some("(UPPER(severity) LIKE 'ERROR%' OR UPPER(severity) LIKE 'FATAL%')".to_string())
        }
        LogSeverityFilter::Warn => Some("UPPER(severity) LIKE 'WARN%'".to_string()),
        LogSeverityFilter::Info => Some("UPPER(severity) LIKE 'INFO%'".to_string()),
        LogSeverityFilter::Debug => {
            Some("(UPPER(severity) LIKE 'DEBUG%' OR UPPER(severity) LIKE 'TRACE%')".to_string())
        }
    }
}

pub(super) fn log_correlation_clause(filter: LogCorrelationFilter) -> Option<String> {
    match filter {
        LogCorrelationFilter::All => None,
        LogCorrelationFilter::TraceLinked => Some("trace_id != ''".to_string()),
        LogCorrelationFilter::SpanLinked => Some("span_id != ''".to_string()),
        LogCorrelationFilter::Uncorrelated => Some("trace_id = '' AND span_id = ''".to_string()),
    }
}
