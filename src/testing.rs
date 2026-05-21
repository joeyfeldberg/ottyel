use std::collections::BTreeMap;

use serde_json::json;

use crate::domain::{
    AttributeMap, DashboardSnapshot, LlmAttributes, LlmModelComparison, LlmRollup,
    LlmRollupDimension, LlmSessionSummary, LlmSummary, LlmTimelineItem, LlmTimelineKind,
    LogSummary, MetricSummary, OverviewStats, SpanDetail, SpanEventDetail, SpanLinkDetail,
    TraceSummary,
};

const BASE_NANOS: i64 = 1_700_000_000_000_000_000;

pub(crate) fn dashboard_snapshot() -> DashboardSnapshot {
    let selected_trace = trace_spans();
    DashboardSnapshot {
        services: vec![
            "checkout-api".to_string(),
            "catalog-worker".to_string(),
            "llm-gateway".to_string(),
        ],
        overview: OverviewStats {
            service_count: 3,
            trace_count: 6,
            error_span_count: 1,
            log_count: 4,
            metric_count: 5,
            llm_count: 3,
        },
        traces: vec![
            TraceSummary {
                trace_id: "019e2d0a6fb7e5337b121ed7562519cb".to_string(),
                service_name: "checkout-api".to_string(),
                root_name: "Checkout request".to_string(),
                span_count: 8,
                error_count: 1,
                duration_ms: 1294.6,
                started_at_unix_nano: BASE_NANOS,
            },
            TraceSummary {
                trace_id: "019e2d0a6fb7e5337b121ed7562519cc".to_string(),
                service_name: "catalog-worker".to_string(),
                root_name: "Sync product catalog".to_string(),
                span_count: 5,
                error_count: 0,
                duration_ms: 842.1,
                started_at_unix_nano: BASE_NANOS - 10_000_000_000,
            },
            TraceSummary {
                trace_id: "019e2d0a6fb7e5337b121ed7562519cd".to_string(),
                service_name: "llm-gateway".to_string(),
                root_name: "Prompt: Recommend Add-ons".to_string(),
                span_count: 4,
                error_count: 0,
                duration_ms: 612.4,
                started_at_unix_nano: BASE_NANOS - 20_000_000_000,
            },
        ],
        selected_trace,
        logs: logs(),
        metrics: metrics(),
        llm: llm_summaries(),
        llm_rollups: llm_rollups(),
        llm_sessions: llm_sessions(),
        llm_model_comparisons: llm_model_comparisons(),
        llm_top_calls: Vec::new(),
        selected_llm_timeline: llm_timeline(),
    }
}

fn trace_spans() -> Vec<SpanDetail> {
    vec![
        span(
            "root",
            "",
            "Checkout request",
            "SERVER",
            0,
            1_294_600_000,
            attrs([
                ("http.method", json!("POST")),
                ("http.route", json!("/checkout")),
            ]),
        ),
        span(
            "auth",
            "root",
            "Authenticate customer",
            "INTERNAL",
            30_000_000,
            180_000_000,
            attrs([("customer.segment", json!("premium"))]),
        ),
        span(
            "inventory",
            "root",
            "Reserve inventory",
            "CLIENT",
            210_000_000,
            640_000_000,
            attrs([("warehouse", json!("us-east-1"))]),
        ),
        span(
            "llm",
            "root",
            "Prompt: Recommend Add-ons",
            "INTERNAL",
            680_000_000,
            1_180_000_000,
            attrs([("module", json!("recommendations"))]),
        )
        .with_llm(),
        span(
            "payment",
            "root",
            "Charge payment",
            "CLIENT",
            1_205_000_000,
            1_294_000_000,
            attrs([("payment.provider", json!("stripe"))]),
        )
        .with_error(),
    ]
}

fn logs() -> Vec<LogSummary> {
    vec![
        log(
            "checkout-api",
            "INFO",
            "checkout started",
            "root",
            BASE_NANOS,
        ),
        log(
            "checkout-api",
            "ERROR",
            r#"{"message":"payment declined","reason":"card_check_failed"}"#,
            "payment",
            BASE_NANOS + 1_294_000_000,
        ),
        log(
            "catalog-worker",
            "WARN",
            "catalog lag above target",
            "",
            BASE_NANOS - 9_000_000_000,
        ),
    ]
}

fn metrics() -> Vec<MetricSummary> {
    [18.0, 23.0, 41.0, 37.0, 29.0]
        .into_iter()
        .enumerate()
        .map(|(index, value)| MetricSummary {
            service_name: "checkout-api".to_string(),
            metric_name: "checkout.queue.depth".to_string(),
            instrument_kind: "gauge".to_string(),
            timestamp_unix_nano: BASE_NANOS + index as i64 * 1_000_000_000,
            value: Some(value),
            summary: format!("gauge={value}"),
        })
        .collect()
}

fn llm_summaries() -> Vec<LlmSummary> {
    vec![
        LlmSummary {
            trace_id: "019e2d0a6fb7e5337b121ed7562519cb".to_string(),
            span_id: "llm".to_string(),
            started_at_unix_nano: BASE_NANOS + 680_000_000,
            service_name: "checkout-api".to_string(),
            span_name: "Prompt: Recommend Add-ons".to_string(),
            provider: "openai".to_string(),
            model: "gpt-4.1-mini".to_string(),
            operation: "chat".to_string(),
            span_kind: Some("INTERNAL".to_string()),
            session_id: Some("checkout-session-42".to_string()),
            conversation_id: Some("customer-thread-42".to_string()),
            prompt_preview: Some(
                "Recommend two add-ons for a premium checkout customer.".to_string(),
            ),
            output_preview: Some("Offer expedited shipping and warranty coverage.".to_string()),
            tool_name: None,
            tool_args: None,
            input_tokens: Some(920),
            output_tokens: Some(88),
            total_tokens: Some(1008),
            cost: Some(0.0042),
            latency_ms: Some(500.0),
            status: "STATUS_CODE_OK".to_string(),
            raw_json: json!({}),
        },
        LlmSummary {
            trace_id: "019e2d0a6fb7e5337b121ed7562519cd".to_string(),
            span_id: "llm-2".to_string(),
            started_at_unix_nano: BASE_NANOS - 20_000_000_000,
            service_name: "llm-gateway".to_string(),
            span_name: "Prompt: Summarize Cart".to_string(),
            provider: "openai".to_string(),
            model: "gpt-4o-mini".to_string(),
            operation: "chat".to_string(),
            span_kind: Some("INTERNAL".to_string()),
            session_id: None,
            conversation_id: None,
            prompt_preview: Some("Summarize cart risks.".to_string()),
            output_preview: Some("Cart has one delayed item.".to_string()),
            tool_name: None,
            tool_args: None,
            input_tokens: Some(320),
            output_tokens: Some(40),
            total_tokens: Some(360),
            cost: Some(0.0011),
            latency_ms: Some(612.4),
            status: "STATUS_CODE_OK".to_string(),
            raw_json: json!({}),
        },
    ]
}

fn llm_rollups() -> Vec<LlmRollup> {
    vec![
        LlmRollup {
            dimension: LlmRollupDimension::Model,
            label: "gpt-4.1-mini".to_string(),
            call_count: 1,
            error_count: 0,
            input_tokens: 920,
            output_tokens: 88,
            total_tokens: 1008,
            cost: Some(0.0042),
            avg_latency_ms: Some(500.0),
        },
        LlmRollup {
            dimension: LlmRollupDimension::Model,
            label: "gpt-4o-mini".to_string(),
            call_count: 1,
            error_count: 0,
            input_tokens: 320,
            output_tokens: 40,
            total_tokens: 360,
            cost: Some(0.0011),
            avg_latency_ms: Some(612.4),
        },
    ]
}

fn llm_sessions() -> Vec<LlmSessionSummary> {
    vec![LlmSessionSummary {
        correlation_kind: "conversation_id".to_string(),
        correlation_id: "customer-thread-42".to_string(),
        service_name: "checkout-api".to_string(),
        call_count: 1,
        error_count: 0,
        model_count: 1,
        provider_count: 1,
        total_tokens: 1008,
        cost: Some(0.0042),
        duration_ms: 500.0,
        first_seen_unix_nano: BASE_NANOS + 680_000_000,
        last_seen_unix_nano: BASE_NANOS + 1_180_000_000,
    }]
}

fn llm_model_comparisons() -> Vec<LlmModelComparison> {
    vec![LlmModelComparison {
        provider: "openai".to_string(),
        model: "gpt-4.1-mini".to_string(),
        call_count: 1,
        error_count: 0,
        total_tokens: 1008,
        cost: Some(0.0042),
        avg_latency_ms: Some(500.0),
    }]
}

fn llm_timeline() -> Vec<LlmTimelineItem> {
    vec![
        LlmTimelineItem {
            kind: LlmTimelineKind::Prompt,
            label: "prompt input".to_string(),
            detail: Some("Recommend two add-ons for a premium checkout customer.".to_string()),
            offset_ms: 0.0,
            duration_ms: Some(0.0),
            status: None,
        },
        LlmTimelineItem {
            kind: LlmTimelineKind::Output,
            label: "output response".to_string(),
            detail: Some("Offer expedited shipping and warranty coverage.".to_string()),
            offset_ms: 500.0,
            duration_ms: Some(0.0),
            status: None,
        },
    ]
}

fn span(
    span_id: &str,
    parent_span_id: &str,
    span_name: &str,
    span_kind: &str,
    start_offset: i64,
    end_offset: i64,
    attributes: AttributeMap,
) -> SpanDetail {
    SpanDetail {
        trace_id: "019e2d0a6fb7e5337b121ed7562519cb".to_string(),
        span_id: span_id.to_string(),
        parent_span_id: parent_span_id.to_string(),
        service_name: "checkout-api".to_string(),
        span_name: span_name.to_string(),
        span_kind: span_kind.to_string(),
        status_code: "STATUS_CODE_OK".to_string(),
        start_time_unix_nano: BASE_NANOS + start_offset,
        end_time_unix_nano: BASE_NANOS + end_offset,
        duration_ms: (end_offset - start_offset) as f64 / 1_000_000.0,
        resource_attributes: attrs([("deployment.environment", json!("test"))]),
        attributes,
        events: vec![SpanEventDetail {
            name: "checkpoint".to_string(),
            timestamp_unix_nano: BASE_NANOS + start_offset + 1,
            attributes: attrs([("phase", json!("snapshot"))]),
        }],
        links: vec![SpanLinkDetail {
            trace_id: "019e2d0a6fb7e5337b121ed7562519cc".to_string(),
            span_id: "linked-span".to_string(),
            trace_state: String::new(),
            attributes: BTreeMap::new(),
        }],
        llm: None,
    }
}

fn log(
    service_name: &str,
    severity: &str,
    body: &str,
    span_id: &str,
    timestamp_unix_nano: i64,
) -> LogSummary {
    LogSummary {
        service_name: service_name.to_string(),
        timestamp_unix_nano,
        severity: severity.to_string(),
        body: body.to_string(),
        trace_id: if span_id.is_empty() {
            String::new()
        } else {
            "019e2d0a6fb7e5337b121ed7562519cb".to_string()
        },
        span_id: span_id.to_string(),
        resource_attributes: attrs([("host.name", json!("test-host"))]),
        attributes: attrs([("component", json!("fixture"))]),
    }
}

fn attrs<const N: usize>(items: [(&str, serde_json::Value); N]) -> AttributeMap {
    items
        .into_iter()
        .map(|(key, value)| (key.to_string(), value))
        .collect()
}

trait SpanFixtureExt {
    fn with_error(self) -> Self;
    fn with_llm(self) -> Self;
}

impl SpanFixtureExt for SpanDetail {
    fn with_error(mut self) -> Self {
        self.status_code = "STATUS_CODE_ERROR".to_string();
        self.attributes
            .insert("error.type".to_string(), json!("payment_declined"));
        self
    }

    fn with_llm(mut self) -> Self {
        self.llm = Some(LlmAttributes {
            provider: Some("openai".to_string()),
            model: Some("gpt-4.1-mini".to_string()),
            operation: Some("chat".to_string()),
            span_kind: Some("llm".to_string()),
            prompt_preview: Some("Recommend checkout add-ons.".to_string()),
            output_preview: Some("Suggest expedited shipping.".to_string()),
            input_tokens: Some(920),
            output_tokens: Some(88),
            total_tokens: Some(1008),
            cost: Some(0.0042),
            latency_ms: Some(500.0),
            status: Some("STATUS_CODE_OK".to_string()),
            ..LlmAttributes::default()
        });
        self
    }
}
