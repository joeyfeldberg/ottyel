use std::collections::BTreeMap;

use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue, any_value};
use serde::{Deserialize, Serialize};
use serde_json::Value;

pub type AttributeMap = BTreeMap<String, Value>;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct LlmAttributes {
    pub provider: Option<String>,
    pub model: Option<String>,
    pub operation: Option<String>,
    pub span_kind: Option<String>,
    pub prompt_preview: Option<String>,
    pub output_preview: Option<String>,
    pub tool_name: Option<String>,
    pub tool_args: Option<String>,
    pub input_tokens: Option<u64>,
    pub output_tokens: Option<u64>,
    pub total_tokens: Option<u64>,
    pub cost: Option<f64>,
    pub latency_ms: Option<f64>,
    pub status: Option<String>,
}

impl LlmAttributes {
    pub fn is_present(&self) -> bool {
        self.provider.is_some()
            || self.model.is_some()
            || self.operation.is_some()
            || self.span_kind.is_some()
            || self.prompt_preview.is_some()
            || self.output_preview.is_some()
            || self.tool_name.is_some()
            || self.input_tokens.is_some()
            || self.output_tokens.is_some()
            || self.total_tokens.is_some()
            || self.cost.is_some()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TraceSummary {
    pub trace_id: String,
    pub service_name: String,
    pub root_name: String,
    pub span_count: i64,
    pub error_count: i64,
    pub duration_ms: f64,
    pub started_at_unix_nano: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpanEventDetail {
    pub name: String,
    pub timestamp_unix_nano: i64,
    pub attributes: AttributeMap,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpanLinkDetail {
    pub trace_id: String,
    pub span_id: String,
    pub trace_state: String,
    pub attributes: AttributeMap,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpanDetail {
    pub trace_id: String,
    pub span_id: String,
    pub parent_span_id: String,
    pub service_name: String,
    pub span_name: String,
    pub span_kind: String,
    pub status_code: String,
    pub start_time_unix_nano: i64,
    pub end_time_unix_nano: i64,
    pub duration_ms: f64,
    pub resource_attributes: AttributeMap,
    pub attributes: AttributeMap,
    pub events: Vec<SpanEventDetail>,
    pub links: Vec<SpanLinkDetail>,
    pub llm: Option<LlmAttributes>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogSummary {
    pub service_name: String,
    pub timestamp_unix_nano: i64,
    pub severity: String,
    pub body: String,
    pub trace_id: String,
    pub span_id: String,
    pub resource_attributes: AttributeMap,
    pub attributes: AttributeMap,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricSummary {
    pub service_name: String,
    pub metric_name: String,
    pub instrument_kind: String,
    pub timestamp_unix_nano: i64,
    pub value: Option<f64>,
    pub summary: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LlmSummary {
    pub trace_id: String,
    pub span_id: String,
    pub service_name: String,
    pub provider: String,
    pub model: String,
    pub operation: String,
    pub span_kind: Option<String>,
    pub prompt_preview: Option<String>,
    pub output_preview: Option<String>,
    pub tool_name: Option<String>,
    pub tool_args: Option<String>,
    pub input_tokens: Option<u64>,
    pub output_tokens: Option<u64>,
    pub total_tokens: Option<u64>,
    pub cost: Option<f64>,
    pub latency_ms: Option<f64>,
    pub status: String,
    pub raw_json: Value,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
pub enum LlmRollupDimension {
    Model,
    Provider,
    Service,
}

impl LlmRollupDimension {
    pub fn label(self) -> &'static str {
        match self {
            Self::Model => "model",
            Self::Provider => "provider",
            Self::Service => "service",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LlmRollup {
    pub dimension: LlmRollupDimension,
    pub label: String,
    pub call_count: usize,
    pub error_count: usize,
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub total_tokens: u64,
    pub cost: Option<f64>,
    pub avg_latency_ms: Option<f64>,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, Deserialize)]
pub enum LlmTimelineKind {
    Prompt,
    Step,
    Tool,
    Output,
}

impl LlmTimelineKind {
    pub fn label(self) -> &'static str {
        match self {
            Self::Prompt => "prompt",
            Self::Step => "step",
            Self::Tool => "tool",
            Self::Output => "output",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LlmTimelineItem {
    pub kind: LlmTimelineKind,
    pub label: String,
    pub detail: Option<String>,
    pub offset_ms: f64,
    pub duration_ms: Option<f64>,
    pub status: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OverviewStats {
    pub service_count: usize,
    pub trace_count: usize,
    pub error_span_count: usize,
    pub log_count: usize,
    pub metric_count: usize,
    pub llm_count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DashboardSnapshot {
    pub services: Vec<String>,
    pub overview: OverviewStats,
    pub traces: Vec<TraceSummary>,
    pub selected_trace: Vec<SpanDetail>,
    pub logs: Vec<LogSummary>,
    pub metrics: Vec<MetricSummary>,
    pub llm: Vec<LlmSummary>,
    pub llm_rollups: Vec<LlmRollup>,
    pub selected_llm_timeline: Vec<LlmTimelineItem>,
}

pub fn attributes_to_map(attributes: &[KeyValue]) -> AttributeMap {
    attributes
        .iter()
        .map(|item| (item.key.clone(), any_value_to_json(item.value.as_ref())))
        .collect()
}

pub fn any_value_to_json(value: Option<&AnyValue>) -> Value {
    match value.and_then(|inner| inner.value.as_ref()) {
        Some(any_value::Value::StringValue(text)) => Value::String(text.clone()),
        Some(any_value::Value::BoolValue(flag)) => Value::Bool(*flag),
        Some(any_value::Value::IntValue(number)) => Value::Number((*number).into()),
        Some(any_value::Value::DoubleValue(number)) => {
            serde_json::Number::from_f64(*number).map_or(Value::Null, Value::Number)
        }
        Some(any_value::Value::BytesValue(bytes)) => Value::String(hex::encode(bytes)),
        Some(any_value::Value::ArrayValue(array)) => Value::Array(
            array
                .values
                .iter()
                .map(|entry| any_value_to_json(Some(entry)))
                .collect(),
        ),
        Some(any_value::Value::KvlistValue(list)) => {
            let mut obj = serde_json::Map::new();
            for entry in &list.values {
                obj.insert(entry.key.clone(), any_value_to_json(entry.value.as_ref()));
            }
            Value::Object(obj)
        }
        None => Value::Null,
    }
}

pub fn extract_service_name(resource_attrs: &AttributeMap) -> String {
    resource_attrs
        .get("service.name")
        .and_then(Value::as_str)
        .unwrap_or("unknown-service")
        .to_string()
}

pub fn extract_llm_attributes(
    attrs: &AttributeMap,
    status_code: Option<&str>,
    duration_ms: Option<f64>,
) -> Option<LlmAttributes> {
    let provider = first_string(
        attrs,
        &[
            "llm.provider",
            "gen_ai.provider.name",
            "gen_ai.system",
            "openinference.provider",
        ],
    );
    let model = first_string(
        attrs,
        &[
            "llm.model_name",
            "gen_ai.request.model",
            "gen_ai.response.model",
            "openinference.model",
        ],
    );
    let operation = first_string(
        attrs,
        &[
            "openinference.span.kind",
            "llm.operation",
            "gen_ai.operation.name",
        ],
    );
    let span_kind = first_string(attrs, &["openinference.span.kind", "llm.span.kind"]);
    let prompt_preview = first_string(
        attrs,
        &[
            "input.value",
            "llm.prompt",
            "llm.prompts.0.content",
            "gen_ai.prompt.0.content",
        ],
    )
    .or_else(|| message_preview(attrs, &["gen_ai.input.messages"]))
    .or_else(|| first_json_preview(attrs, &["gen_ai.system_instructions"]));
    let output_preview = first_string(
        attrs,
        &[
            "output.value",
            "llm.response",
            "llm.completions.0.content",
            "gen_ai.completion.0.content",
        ],
    )
    .or_else(|| message_preview(attrs, &["gen_ai.output.messages"]));
    let tool_name = first_string(attrs, &["tool.name", "llm.tool.name"])
        .or_else(|| first_tool_name(attrs, &["gen_ai.tool.definitions"]))
        .or_else(|| first_tool_name(attrs, &["gen_ai.input.messages", "gen_ai.output.messages"]));
    let tool_args = first_string(attrs, &["tool.arguments", "llm.tool.arguments"])
        .or_else(|| first_tool_args(attrs, &["gen_ai.input.messages", "gen_ai.output.messages"]));
    let input_tokens = first_u64(
        attrs,
        &[
            "llm.token_count.prompt",
            "gen_ai.usage.input_tokens",
            "usage.prompt_tokens",
        ],
    );
    let output_tokens = first_u64(
        attrs,
        &[
            "llm.token_count.completion",
            "gen_ai.usage.output_tokens",
            "usage.completion_tokens",
        ],
    );
    let total_tokens = first_u64(
        attrs,
        &[
            "llm.token_count.total",
            "gen_ai.usage.total_tokens",
            "usage.total_tokens",
        ],
    )
    .or_else(|| match (input_tokens, output_tokens) {
        (Some(input), Some(output)) => Some(input + output),
        _ => None,
    });
    let cost = first_f64(
        attrs,
        &[
            "llm.cost.total",
            "gen_ai.usage.cost",
            "usage.cost",
            "cost.total",
        ],
    );

    let llm = LlmAttributes {
        provider,
        model,
        operation,
        span_kind,
        prompt_preview,
        output_preview,
        tool_name,
        tool_args,
        input_tokens,
        output_tokens,
        total_tokens,
        cost,
        latency_ms: duration_ms,
        status: status_code.map(ToString::to_string),
    };

    llm.is_present().then_some(llm)
}

fn first_string(attrs: &AttributeMap, keys: &[&str]) -> Option<String> {
    keys.iter().find_map(|key| {
        attrs.get(*key).and_then(|value| match value {
            Value::String(text) if !text.is_empty() => Some(text.clone()),
            Value::Number(number) => Some(number.to_string()),
            _ => None,
        })
    })
}

fn first_u64(attrs: &AttributeMap, keys: &[&str]) -> Option<u64> {
    keys.iter().find_map(|key| {
        attrs.get(*key).and_then(|value| match value {
            Value::Number(number) => number.as_u64(),
            Value::String(text) => text.parse().ok(),
            _ => None,
        })
    })
}

fn first_f64(attrs: &AttributeMap, keys: &[&str]) -> Option<f64> {
    keys.iter().find_map(|key| {
        attrs.get(*key).and_then(|value| match value {
            Value::Number(number) => number.as_f64(),
            Value::String(text) => text.parse().ok(),
            _ => None,
        })
    })
}

fn first_json_preview(attrs: &AttributeMap, keys: &[&str]) -> Option<String> {
    keys.iter()
        .find_map(|key| attrs.get(*key).and_then(value_preview))
}

fn message_preview(attrs: &AttributeMap, keys: &[&str]) -> Option<String> {
    keys.iter()
        .find_map(|key| attrs.get(*key).and_then(messages_preview))
}

fn messages_preview(value: &Value) -> Option<String> {
    match value {
        Value::Array(items) => {
            let previews = items
                .iter()
                .filter_map(message_content_preview)
                .collect::<Vec<_>>();
            if previews.is_empty() {
                None
            } else {
                Some(previews.join("\n\n"))
            }
        }
        _ => message_content_preview(value),
    }
}

fn message_content_preview(value: &Value) -> Option<String> {
    match value {
        Value::String(text) if !text.is_empty() => Some(text.clone()),
        Value::Object(object) => {
            if let Some(content) = object.get("content").and_then(content_preview) {
                return Some(content);
            }
            if let Some(input_text) = object.get("input_text").and_then(value_preview) {
                return Some(input_text);
            }
            if let Some(output_text) = object.get("output_text").and_then(value_preview) {
                return Some(output_text);
            }
            if let Some(text) = object.get("text").and_then(value_preview) {
                return Some(text);
            }
            if let Some(arguments) = object.get("arguments").and_then(value_preview) {
                return Some(arguments);
            }
            None
        }
        _ => value_preview(value),
    }
}

fn content_preview(value: &Value) -> Option<String> {
    match value {
        Value::Array(items) => {
            let previews = items.iter().filter_map(part_preview).collect::<Vec<_>>();
            if previews.is_empty() {
                None
            } else {
                Some(previews.join("\n"))
            }
        }
        _ => value_preview(value),
    }
}

fn part_preview(value: &Value) -> Option<String> {
    match value {
        Value::String(text) if !text.is_empty() => Some(text.clone()),
        Value::Object(object) => object
            .get("text")
            .and_then(value_preview)
            .or_else(|| object.get("content").and_then(value_preview))
            .or_else(|| object.get("output_text").and_then(value_preview))
            .or_else(|| object.get("input_text").and_then(value_preview))
            .or_else(|| object.get("arguments").and_then(value_preview)),
        _ => value_preview(value),
    }
}

fn first_tool_name(attrs: &AttributeMap, keys: &[&str]) -> Option<String> {
    keys.iter()
        .find_map(|key| attrs.get(*key).and_then(tool_name_from_value))
}

fn first_tool_args(attrs: &AttributeMap, keys: &[&str]) -> Option<String> {
    keys.iter()
        .find_map(|key| attrs.get(*key).and_then(tool_args_from_value))
}

fn tool_name_from_value(value: &Value) -> Option<String> {
    match value {
        Value::Array(items) => items.iter().find_map(tool_name_from_value),
        Value::Object(object) => object
            .get("name")
            .and_then(value_preview)
            .or_else(|| object.get("tool_name").and_then(value_preview))
            .or_else(|| object.get("function").and_then(tool_name_from_value))
            .or_else(|| object.get("tool_calls").and_then(tool_name_from_value)),
        _ => None,
    }
}

fn tool_args_from_value(value: &Value) -> Option<String> {
    match value {
        Value::Array(items) => items.iter().find_map(tool_args_from_value),
        Value::Object(object) => object
            .get("arguments")
            .and_then(value_preview)
            .or_else(|| object.get("args").and_then(value_preview))
            .or_else(|| object.get("input").and_then(value_preview))
            .or_else(|| object.get("function").and_then(tool_args_from_value))
            .or_else(|| object.get("tool_calls").and_then(tool_args_from_value)),
        _ => None,
    }
}

fn value_preview(value: &Value) -> Option<String> {
    match value {
        Value::Null => None,
        Value::String(text) if text.is_empty() => None,
        Value::String(text) => Some(text.clone()),
        Value::Number(number) => Some(number.to_string()),
        Value::Bool(flag) => Some(flag.to_string()),
        Value::Array(_) | Value::Object(_) => serde_json::to_string_pretty(value).ok(),
    }
}

pub fn truncate(text: &str, max_chars: usize) -> String {
    if text.chars().count() <= max_chars {
        return text.to_string();
    }
    let truncated: String = text.chars().take(max_chars.saturating_sub(1)).collect();
    format!("{truncated}…")
}

pub fn project_llm_timeline(spans: &[SpanDetail], llm_span_id: &str) -> Vec<LlmTimelineItem> {
    let Some(llm_span) = spans.iter().find(|span| span.span_id == llm_span_id) else {
        return Vec::new();
    };

    let mut timeline = Vec::new();
    if let Some(prompt) = llm_span
        .llm
        .as_ref()
        .and_then(|llm| llm.prompt_preview.as_deref())
        .filter(|prompt| !prompt.is_empty())
    {
        timeline.push(LlmTimelineItem {
            kind: LlmTimelineKind::Prompt,
            label: "input".to_string(),
            detail: Some(prompt.to_string()),
            offset_ms: 0.0,
            duration_ms: None,
            status: None,
        });
    }

    let descendants = llm_descendants(spans, llm_span_id);
    let mut rendered_tool = false;
    for span in descendants {
        let tool_name = span_tool_name(span);
        let kind = if tool_name.is_some() {
            rendered_tool = true;
            LlmTimelineKind::Tool
        } else {
            LlmTimelineKind::Step
        };
        timeline.push(LlmTimelineItem {
            kind,
            label: tool_name.unwrap_or_else(|| span.span_name.clone()),
            detail: span_tool_detail(span),
            offset_ms: span_offset_ms(span, llm_span),
            duration_ms: Some(span.duration_ms.max(0.0)),
            status: Some(span.status_code.clone()),
        });
    }

    if !rendered_tool
        && let Some(llm) = &llm_span.llm
        && let Some(tool_name) = llm.tool_name.as_ref().filter(|name| !name.is_empty())
    {
        timeline.push(LlmTimelineItem {
            kind: LlmTimelineKind::Tool,
            label: tool_name.clone(),
            detail: llm.tool_args.clone(),
            offset_ms: (llm_span.duration_ms.max(1.0) * 0.5).min(llm_span.duration_ms.max(0.0)),
            duration_ms: None,
            status: None,
        });
    }

    if let Some(output) = llm_span
        .llm
        .as_ref()
        .and_then(|llm| llm.output_preview.as_deref())
        .filter(|output| !output.is_empty())
    {
        timeline.push(LlmTimelineItem {
            kind: LlmTimelineKind::Output,
            label: "output".to_string(),
            detail: Some(output.to_string()),
            offset_ms: llm_span.duration_ms.max(0.0),
            duration_ms: None,
            status: None,
        });
    }

    timeline.sort_by(|left, right| {
        left.offset_ms
            .partial_cmp(&right.offset_ms)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| left.kind.label().cmp(right.kind.label()))
    });
    timeline
}

fn llm_descendants<'a>(spans: &'a [SpanDetail], llm_span_id: &str) -> Vec<&'a SpanDetail> {
    let mut descendants = Vec::new();
    let mut frontier = vec![llm_span_id];

    while let Some(parent_id) = frontier.pop() {
        for span in spans.iter().filter(|span| span.parent_span_id == parent_id) {
            frontier.push(span.span_id.as_str());
            descendants.push(span);
        }
    }

    descendants.sort_by(|left, right| {
        left.start_time_unix_nano
            .cmp(&right.start_time_unix_nano)
            .then(left.span_name.cmp(&right.span_name))
    });
    descendants
}

fn span_offset_ms(span: &SpanDetail, llm_span: &SpanDetail) -> f64 {
    ((span.start_time_unix_nano - llm_span.start_time_unix_nano) as f64 / 1_000_000.0).max(0.0)
}

fn span_tool_name(span: &SpanDetail) -> Option<String> {
    span.attributes
        .get("tool.name")
        .and_then(Value::as_str)
        .filter(|name| !name.is_empty())
        .map(ToString::to_string)
        .or_else(|| {
            span.llm
                .as_ref()
                .and_then(|llm| llm.tool_name.clone())
                .filter(|name| !name.is_empty())
        })
        .or_else(|| {
            span.span_name
                .contains("tool")
                .then(|| span.span_name.clone())
        })
}

fn span_tool_detail(span: &SpanDetail) -> Option<String> {
    span.attributes
        .get("tool.arguments")
        .and_then(value_preview)
        .or_else(|| span.llm.as_ref().and_then(|llm| llm.tool_args.clone()))
}

mod hex {
    pub fn encode(bytes: &[u8]) -> String {
        bytes.iter().map(|byte| format!("{byte:02x}")).collect()
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{
        AttributeMap, LlmAttributes, SpanDetail, extract_llm_attributes, project_llm_timeline,
    };

    #[test]
    fn llm_attributes_normalize_openinference_keys() {
        let attrs = AttributeMap::from([
            ("llm.provider".to_string(), json!("openai")),
            ("llm.model_name".to_string(), json!("gpt-5.4")),
            ("llm.token_count.prompt".to_string(), json!(11)),
            ("llm.token_count.completion".to_string(), json!(7)),
            ("input.value".to_string(), json!("hello")),
            ("output.value".to_string(), json!("world")),
        ]);

        let llm = extract_llm_attributes(&attrs, Some("STATUS_CODE_OK"), Some(42.5)).unwrap();

        assert_eq!(llm.provider.as_deref(), Some("openai"));
        assert_eq!(llm.model.as_deref(), Some("gpt-5.4"));
        assert_eq!(llm.input_tokens, Some(11));
        assert_eq!(llm.output_tokens, Some(7));
        assert_eq!(llm.total_tokens, Some(18));
        assert_eq!(llm.prompt_preview.as_deref(), Some("hello"));
        assert_eq!(llm.output_preview.as_deref(), Some("world"));
        assert_eq!(llm.latency_ms, Some(42.5));
    }

    #[test]
    fn llm_attributes_normalize_gen_ai_message_arrays() {
        let attrs = AttributeMap::from([
            ("gen_ai.provider.name".to_string(), json!("openai")),
            ("gen_ai.request.model".to_string(), json!("gpt-4o-mini")),
            ("gen_ai.operation.name".to_string(), json!("chat")),
            ("gen_ai.usage.input_tokens".to_string(), json!(1184)),
            ("gen_ai.usage.output_tokens".to_string(), json!(34)),
            (
                "gen_ai.system_instructions".to_string(),
                json!("You are a useful assistant."),
            ),
            (
                "gen_ai.input.messages".to_string(),
                json!([
                    {
                        "role": "system",
                        "content": [{"type": "text", "text": "You are a useful assistant."}]
                    },
                    {
                        "role": "user",
                        "content": [{"type": "text", "text": "Summarize this ticket"}]
                    }
                ]),
            ),
            (
                "gen_ai.output.messages".to_string(),
                json!([
                    {
                        "role": "assistant",
                        "content": [{"type": "text", "text": "Here is the summary."}],
                        "tool_calls": [
                            {
                                "name": "lookup_customer",
                                "arguments": {"customer_id": "123"}
                            }
                        ]
                    }
                ]),
            ),
            (
                "gen_ai.tool.definitions".to_string(),
                json!([
                    {
                        "name": "lookup_customer",
                        "description": "Fetch a customer by id"
                    }
                ]),
            ),
        ]);

        let llm = extract_llm_attributes(&attrs, Some("STATUS_CODE_UNSET"), Some(1609.3)).unwrap();

        assert_eq!(llm.provider.as_deref(), Some("openai"));
        assert_eq!(llm.model.as_deref(), Some("gpt-4o-mini"));
        assert_eq!(llm.operation.as_deref(), Some("chat"));
        assert_eq!(llm.input_tokens, Some(1184));
        assert_eq!(llm.output_tokens, Some(34));
        assert_eq!(llm.total_tokens, Some(1218));
        assert!(
            llm.prompt_preview
                .as_deref()
                .is_some_and(|value| value.contains("Summarize this ticket"))
        );
        assert_eq!(llm.output_preview.as_deref(), Some("Here is the summary."));
        assert_eq!(llm.tool_name.as_deref(), Some("lookup_customer"));
        assert!(
            llm.tool_args
                .as_deref()
                .is_some_and(|value| value.contains("customer_id"))
        );
        assert_eq!(llm.latency_ms, Some(1609.3));
    }

    #[test]
    fn project_llm_timeline_includes_prompt_tool_and_output() {
        let spans = vec![
            SpanDetail {
                trace_id: "trace-1".to_string(),
                span_id: "llm".to_string(),
                parent_span_id: "root".to_string(),
                service_name: "api".to_string(),
                span_name: "chat.completion".to_string(),
                span_kind: "INTERNAL".to_string(),
                status_code: "STATUS_CODE_OK".to_string(),
                start_time_unix_nano: 1_000,
                end_time_unix_nano: 51_000_000,
                duration_ms: 51.0,
                resource_attributes: Default::default(),
                attributes: Default::default(),
                events: Vec::new(),
                links: Vec::new(),
                llm: Some(LlmAttributes {
                    prompt_preview: Some("hello".to_string()),
                    output_preview: Some("world".to_string()),
                    tool_name: Some("lookup_customer".to_string()),
                    tool_args: Some("{\"id\":\"123\"}".to_string()),
                    ..LlmAttributes::default()
                }),
            },
            SpanDetail {
                trace_id: "trace-1".to_string(),
                span_id: "tool".to_string(),
                parent_span_id: "llm".to_string(),
                service_name: "api".to_string(),
                span_name: "tool.lookup_customer".to_string(),
                span_kind: "INTERNAL".to_string(),
                status_code: "STATUS_CODE_OK".to_string(),
                start_time_unix_nano: 11_000_000,
                end_time_unix_nano: 21_000_000,
                duration_ms: 10.0,
                resource_attributes: Default::default(),
                attributes: AttributeMap::from([
                    ("tool.name".to_string(), json!("lookup_customer")),
                    ("tool.arguments".to_string(), json!("{\"id\":\"123\"}")),
                ]),
                events: Vec::new(),
                links: Vec::new(),
                llm: None,
            },
        ];

        let timeline = project_llm_timeline(&spans, "llm");

        assert_eq!(timeline.len(), 3);
        assert_eq!(timeline[0].kind.label(), "prompt");
        assert_eq!(timeline[1].kind.label(), "tool");
        assert_eq!(timeline[1].label, "lookup_customer");
        assert_eq!(timeline[2].kind.label(), "output");
    }
}
