use std::{
    fs,
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result};
use clap::Parser;
use opentelemetry_proto::tonic::{
    collector::{
        logs::v1::ExportLogsServiceRequest, metrics::v1::ExportMetricsServiceRequest,
        trace::v1::ExportTraceServiceRequest,
    },
    common::v1::{AnyValue, InstrumentationScope, KeyValue, any_value},
    logs::v1::{LogRecord, ResourceLogs, ScopeLogs, SeverityNumber},
    metrics::v1::{
        AggregationTemporality, Gauge, Histogram, HistogramDataPoint, Metric, NumberDataPoint,
        ResourceMetrics, ScopeMetrics, Sum, metric, number_data_point,
    },
    resource::v1::Resource,
    trace::v1::{ResourceSpans, ScopeSpans, Span, Status, span, status},
};
use ottyel::store::Store;

#[derive(Debug, Parser)]
#[command(name = "seed_fake_data")]
#[command(about = "Populate an ottyel SQLite database with realistic fake telemetry")]
struct Args {
    #[arg(long, default_value = ".ottyel/ottyel.db")]
    db_path: PathBuf,
    #[arg(long, default_value_t = 12)]
    traces: usize,
    #[arg(long, default_value_t = 24)]
    retention_hours: u64,
    #[arg(long, default_value_t = 50_000)]
    max_spans: usize,
    #[arg(long)]
    reset: bool,
}

fn main() -> Result<()> {
    let args = Args::parse();
    if args.reset {
        reset_db(&args.db_path)?;
    }

    let store = Store::open(&args.db_path, args.retention_hours, args.max_spans)?;
    let base_time = now_unix_nanos().saturating_sub(90 * 60 * 1_000_000_000);

    let traces = build_trace_request(args.traces, base_time);
    let logs = build_log_request(args.traces, base_time);
    let metrics = build_metric_request(base_time);

    let inserted_spans = store.ingest_traces(traces)?;
    let inserted_logs = store.ingest_logs(logs)?;
    let inserted_metrics = store.ingest_metrics(metrics)?;

    let (trace_count, error_span_count, log_count, metric_count, llm_count) = store.counts(None)?;
    println!("db={}", args.db_path.display());
    println!("inserted spans={inserted_spans} logs={inserted_logs} metrics={inserted_metrics}");
    println!(
        "current counts traces={trace_count} error_spans={error_span_count} logs={log_count} metrics={metric_count} llm={llm_count}"
    );
    Ok(())
}

fn reset_db(path: &Path) -> Result<()> {
    for suffix in ["", "-wal", "-shm"] {
        let candidate = PathBuf::from(format!("{}{}", path.display(), suffix));
        if candidate.exists() {
            fs::remove_file(&candidate)
                .with_context(|| format!("failed to remove {}", candidate.display()))?;
        }
    }
    Ok(())
}

fn build_trace_request(trace_count: usize, base_time: u64) -> ExportTraceServiceRequest {
    let mut gateway_spans = Vec::new();
    let mut dialog_spans = Vec::new();
    let mut retrieval_spans = Vec::new();

    for trace_index in 0..trace_count {
        let trace_id = trace_id(trace_index + 1);
        let started_at = base_time + (trace_index as u64 * 12 * 1_000_000_000);
        let has_error = trace_index % 4 == 2;
        let uses_tool = trace_index % 2 == 0;

        let gateway_root = span_id(trace_index, 1);
        let orchestrate = span_id(trace_index, 2);
        let retrieval = span_id(trace_index, 3);
        let llm = span_id(trace_index, 4);
        let tool = span_id(trace_index, 5);

        gateway_spans.push(Span {
            trace_id: trace_id.clone(),
            span_id: gateway_root.clone(),
            parent_span_id: Vec::new(),
            name: if trace_index % 3 == 0 {
                "POST /chat".to_string()
            } else {
                "GraphQL Operation".to_string()
            },
            kind: span::SpanKind::Server as i32,
            start_time_unix_nano: started_at,
            end_time_unix_nano: started_at + millis(900 + (trace_index % 5) as u64 * 160),
            attributes: vec![
                kv_str("http.method", "POST"),
                kv_str("http.route", "/chat"),
                kv_str("user.id", &format!("user-{:02}", trace_index % 5)),
                kv_str("environment", "debug"),
            ],
            events: vec![event(
                started_at + millis(15),
                "request.received",
                vec![kv_str("http.request_id", &format!("req-{trace_index:03}"))],
            )],
            status: Some(ok_status()),
            ..Span::default()
        });

        dialog_spans.push(Span {
            trace_id: trace_id.clone(),
            span_id: orchestrate.clone(),
            parent_span_id: gateway_root.clone(),
            name: "agent.orchestrate".to_string(),
            kind: span::SpanKind::Internal as i32,
            start_time_unix_nano: started_at + millis(20),
            end_time_unix_nano: started_at + millis(840 + (trace_index % 3) as u64 * 140),
            attributes: vec![
                kv_str("component", "planner"),
                kv_str("workflow.id", &format!("wf-{trace_index:03}")),
            ],
            events: vec![
                event(
                    started_at + millis(35),
                    "prompt.compiled",
                    vec![kv_int("step", 1)],
                ),
                event(
                    started_at + millis(150),
                    "retrieval.started",
                    vec![kv_int("k", 6)],
                ),
            ],
            status: Some(ok_status()),
            ..Span::default()
        });

        retrieval_spans.push(Span {
            trace_id: trace_id.clone(),
            span_id: retrieval.clone(),
            parent_span_id: orchestrate.clone(),
            name: if has_error {
                "retrieval.vector_search"
            } else {
                "retrieval.rank_documents"
            }
            .to_string(),
            kind: span::SpanKind::Client as i32,
            start_time_unix_nano: started_at + millis(140),
            end_time_unix_nano: started_at + millis(280 + (trace_index % 4) as u64 * 40),
            attributes: vec![
                kv_str("db.system", "vector-db"),
                kv_int("retrieval.candidates", 24),
            ],
            status: Some(if has_error {
                error_status("vector backend timeout")
            } else {
                ok_status()
            }),
            ..Span::default()
        });

        let mut llm_events = vec![
            event(
                started_at + millis(340),
                "gen_ai.request",
                vec![kv_str("provider", "openai")],
            ),
            event(
                started_at + millis(860),
                "gen_ai.response",
                vec![kv_int("output_tokens", 180 + (trace_index % 5) as i64 * 20)],
            ),
        ];
        if uses_tool {
            llm_events.push(event(
                started_at + millis(570),
                "tool.called",
                vec![kv_str("tool.name", "lookup_customer")],
            ));
        }

        dialog_spans.push(Span {
            trace_id: trace_id.clone(),
            span_id: llm.clone(),
            parent_span_id: orchestrate.clone(),
            name: "chat.completion".to_string(),
            kind: span::SpanKind::Internal as i32,
            start_time_unix_nano: started_at + millis(300),
            end_time_unix_nano: started_at + millis(920 + (trace_index % 4) as u64 * 120),
            attributes: llm_attributes(trace_index, uses_tool),
            events: llm_events,
            links: vec![span::Link {
                trace_id: trace_id.clone(),
                span_id: retrieval.clone(),
                trace_state: "seeded".to_string(),
                attributes: vec![kv_str("relationship", "uses_retrieval_context")],
                ..span::Link::default()
            }],
            status: Some(ok_status()),
            ..Span::default()
        });

        if uses_tool {
            dialog_spans.push(Span {
                trace_id,
                span_id: tool,
                parent_span_id: llm,
                name: "tool.lookup_customer".to_string(),
                kind: span::SpanKind::Internal as i32,
                start_time_unix_nano: started_at + millis(560),
                end_time_unix_nano: started_at + millis(710),
                attributes: vec![
                    kv_str("tool.name", "lookup_customer"),
                    kv_str(
                        "tool.arguments",
                        r#"{"customer_id":"cust-123","region":"us"}"#,
                    ),
                ],
                events: vec![event(
                    started_at + millis(565),
                    "tool.input",
                    vec![kv_str("customer_id", "cust-123")],
                )],
                status: Some(ok_status()),
                ..Span::default()
            });
        }
    }

    ExportTraceServiceRequest {
        resource_spans: vec![
            resource_spans("edge-gateway", "ottyel.seed", gateway_spans),
            resource_spans("dialog-agent-service", "ottyel.seed", dialog_spans),
            resource_spans("retrieval-worker", "ottyel.seed", retrieval_spans),
        ],
    }
}

fn build_log_request(trace_count: usize, base_time: u64) -> ExportLogsServiceRequest {
    let mut gateway_logs = Vec::new();
    let mut dialog_logs = Vec::new();
    let mut retrieval_logs = Vec::new();

    for trace_index in 0..trace_count {
        let trace_id = trace_id(trace_index + 1);
        let started_at = base_time + (trace_index as u64 * 12 * 1_000_000_000);
        let gateway_root = span_id(trace_index, 1);
        let orchestrate = span_id(trace_index, 2);
        let retrieval = span_id(trace_index, 3);
        let llm = span_id(trace_index, 4);
        let has_error = trace_index % 4 == 2;

        gateway_logs.push(log_record(
            started_at + millis(18),
            SeverityNumber::Info,
            "INFO",
            json_text(&format!(
                r#"{{"message":"incoming request","trace":"{:02}","route":"/chat"}}"#,
                trace_index
            )),
            &trace_id,
            &gateway_root,
            vec![kv_str("http.route", "/chat")],
        ));

        dialog_logs.push(log_record(
            started_at + millis(360),
            SeverityNumber::Info,
            "INFO",
            "constructed prompt with retrieval context",
            &trace_id,
            &orchestrate,
            vec![kv_int("context.docs", 6)],
        ));

        dialog_logs.push(log_record(
            started_at + millis(930),
            SeverityNumber::Info,
            "INFO",
            json_text(
                r#"{"message":"llm response received","provider":"openai","model":"gpt-4o-mini"}"#,
            ),
            &trace_id,
            &llm,
            vec![kv_int("token.total", 1200 + trace_index as i64 * 5)],
        ));

        retrieval_logs.push(log_record(
            started_at + millis(220),
            if has_error {
                SeverityNumber::Error
            } else {
                SeverityNumber::Debug
            },
            if has_error { "ERROR" } else { "DEBUG" },
            if has_error {
                "vector backend timeout while ranking candidates"
            } else {
                "retrieval returned 6 ranked documents"
            },
            &trace_id,
            &retrieval,
            vec![kv_bool("cache_hit", trace_index % 3 == 0)],
        ));
    }

    ExportLogsServiceRequest {
        resource_logs: vec![
            resource_logs("edge-gateway", gateway_logs),
            resource_logs("dialog-agent-service", dialog_logs),
            resource_logs("retrieval-worker", retrieval_logs),
        ],
    }
}

fn build_metric_request(base_time: u64) -> ExportMetricsServiceRequest {
    let mut dialog_metrics = Vec::new();
    let mut retrieval_metrics = Vec::new();

    let start = base_time.saturating_sub(20 * 60 * 1_000_000_000);
    for index in 0..30_u64 {
        let time = start + index * 60 * 1_000_000_000;
        dialog_metrics.push(number_point(
            time,
            (4 + (index % 6)) as f64,
            vec![kv_str("queue", "agent")],
        ));
        retrieval_metrics.push(number_point(
            time,
            (28 + index * 3) as f64,
            vec![kv_str("query", "vector_search")],
        ));
    }

    ExportMetricsServiceRequest {
        resource_metrics: vec![
            ResourceMetrics {
                resource: Some(resource("dialog-agent-service")),
                scope_metrics: vec![ScopeMetrics {
                    scope: Some(scope("ottyel.seed.metrics")),
                    metrics: vec![
                        Metric {
                            name: "queue.depth".to_string(),
                            description: "Synthetic queue depth".to_string(),
                            unit: "{item}".to_string(),
                            data: Some(metric::Data::Gauge(Gauge {
                                data_points: dialog_metrics,
                            })),
                            ..Metric::default()
                        },
                        Metric {
                            name: "requests.total".to_string(),
                            description: "Synthetic cumulative request count".to_string(),
                            unit: "{request}".to_string(),
                            data: Some(metric::Data::Sum(Sum {
                                data_points: vec![number_point(
                                    start + 31 * 60 * 1_000_000_000,
                                    1_240.0,
                                    vec![kv_str("route", "/chat")],
                                )],
                                aggregation_temporality: AggregationTemporality::Cumulative as i32,
                                is_monotonic: true,
                            })),
                            ..Metric::default()
                        },
                        Metric {
                            name: "llm.latency".to_string(),
                            description: "Synthetic LLM latency histogram".to_string(),
                            unit: "ms".to_string(),
                            data: Some(metric::Data::Histogram(Histogram {
                                data_points: vec![histogram_point(
                                    start + 31 * 60 * 1_000_000_000,
                                    18,
                                    24_600.0,
                                    vec![200.0, 500.0, 1_000.0, 2_000.0],
                                    vec![3, 8, 5, 2, 0],
                                    vec![kv_str("model", "gpt-4o-mini")],
                                )],
                                aggregation_temporality: AggregationTemporality::Cumulative as i32,
                            })),
                            ..Metric::default()
                        },
                    ],
                    ..ScopeMetrics::default()
                }],
                ..ResourceMetrics::default()
            },
            ResourceMetrics {
                resource: Some(resource("retrieval-worker")),
                scope_metrics: vec![ScopeMetrics {
                    scope: Some(scope("ottyel.seed.metrics")),
                    metrics: vec![Metric {
                        name: "retrieval.documents".to_string(),
                        description: "Synthetic retrieval candidate counts".to_string(),
                        unit: "{document}".to_string(),
                        data: Some(metric::Data::Gauge(Gauge {
                            data_points: retrieval_metrics,
                        })),
                        ..Metric::default()
                    }],
                    ..ScopeMetrics::default()
                }],
                ..ResourceMetrics::default()
            },
        ],
    }
}

fn resource_spans(service_name: &str, scope_name: &str, spans: Vec<Span>) -> ResourceSpans {
    ResourceSpans {
        resource: Some(resource(service_name)),
        scope_spans: vec![ScopeSpans {
            scope: Some(scope(scope_name)),
            spans,
            ..ScopeSpans::default()
        }],
        ..ResourceSpans::default()
    }
}

fn resource_logs(service_name: &str, log_records: Vec<LogRecord>) -> ResourceLogs {
    ResourceLogs {
        resource: Some(resource(service_name)),
        scope_logs: vec![ScopeLogs {
            scope: Some(scope("ottyel.seed.logs")),
            log_records,
            ..ScopeLogs::default()
        }],
        ..ResourceLogs::default()
    }
}

fn resource(service_name: &str) -> Resource {
    Resource {
        attributes: vec![
            kv_str("service.name", service_name),
            kv_str("deployment.environment", "seed"),
            kv_str("host.name", "local-debug"),
        ],
        ..Resource::default()
    }
}

fn scope(name: &str) -> InstrumentationScope {
    InstrumentationScope {
        name: name.to_string(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        ..InstrumentationScope::default()
    }
}

fn llm_attributes(trace_index: usize, uses_tool: bool) -> Vec<KeyValue> {
    let prompt = format!(
        "You are helping customer {}.\nSummarize the recent orders and highlight shipping risks.\nContext: premium tier, region us-east-1.",
        trace_index % 5
    );
    let output = format!(
        "Customer summary ready.\n- 3 recent orders\n- 1 delayed shipment\n- Suggested follow-up: confirm address and expedited replacement."
    );
    let mut attrs = vec![
        kv_str("llm.provider", "openai"),
        kv_str("llm.model_name", "gpt-4o-mini"),
        kv_str("llm.operation", "chat"),
        kv_str("input.value", &prompt),
        kv_str("output.value", &output),
        kv_int("llm.token_count.prompt", 1180 + trace_index as i64 * 2),
        kv_int(
            "llm.token_count.completion",
            220 + (trace_index % 5) as i64 * 8,
        ),
        kv_int("llm.token_count.total", 1400 + trace_index as i64 * 5),
        kv_f64("llm.cost.total", 0.0132 + trace_index as f64 * 0.0004),
        kv_str("gen_ai.provider.name", "openai"),
    ];
    if uses_tool {
        attrs.push(kv_str("tool.name", "lookup_customer"));
        attrs.push(kv_str(
            "tool.arguments",
            r#"{"customer_id":"cust-123","region":"us"}"#,
        ));
    }
    attrs
}

fn log_record(
    time_unix_nano: u64,
    severity_number: SeverityNumber,
    severity_text: &str,
    body: impl Into<String>,
    trace_id: &[u8],
    span_id: &[u8],
    attributes: Vec<KeyValue>,
) -> LogRecord {
    LogRecord {
        time_unix_nano,
        severity_number: severity_number as i32,
        severity_text: severity_text.to_string(),
        body: Some(string_value(&body.into())),
        attributes,
        trace_id: trace_id.to_vec(),
        span_id: span_id.to_vec(),
        ..LogRecord::default()
    }
}

fn number_point(time_unix_nano: u64, value: f64, attributes: Vec<KeyValue>) -> NumberDataPoint {
    NumberDataPoint {
        start_time_unix_nano: time_unix_nano.saturating_sub(60 * 1_000_000_000),
        time_unix_nano,
        attributes,
        value: Some(number_data_point::Value::AsDouble(value)),
        ..NumberDataPoint::default()
    }
}

fn histogram_point(
    time_unix_nano: u64,
    count: u64,
    sum: f64,
    explicit_bounds: Vec<f64>,
    bucket_counts: Vec<u64>,
    attributes: Vec<KeyValue>,
) -> HistogramDataPoint {
    HistogramDataPoint {
        start_time_unix_nano: time_unix_nano.saturating_sub(5 * 60 * 1_000_000_000),
        time_unix_nano,
        count,
        sum: Some(sum),
        bucket_counts,
        explicit_bounds,
        attributes,
        ..HistogramDataPoint::default()
    }
}

fn event(time_unix_nano: u64, name: &str, attributes: Vec<KeyValue>) -> span::Event {
    span::Event {
        time_unix_nano,
        name: name.to_string(),
        attributes,
        ..span::Event::default()
    }
}

fn ok_status() -> Status {
    Status {
        code: status::StatusCode::Ok as i32,
        ..Status::default()
    }
}

fn error_status(message: &str) -> Status {
    Status {
        message: message.to_string(),
        code: status::StatusCode::Error as i32,
    }
}

fn kv_str(key: &str, value: &str) -> KeyValue {
    KeyValue {
        key: key.to_string(),
        value: Some(string_value(value)),
    }
}

fn kv_int(key: &str, value: i64) -> KeyValue {
    KeyValue {
        key: key.to_string(),
        value: Some(AnyValue {
            value: Some(any_value::Value::IntValue(value)),
        }),
    }
}

fn kv_f64(key: &str, value: f64) -> KeyValue {
    KeyValue {
        key: key.to_string(),
        value: Some(AnyValue {
            value: Some(any_value::Value::DoubleValue(value)),
        }),
    }
}

fn kv_bool(key: &str, value: bool) -> KeyValue {
    KeyValue {
        key: key.to_string(),
        value: Some(AnyValue {
            value: Some(any_value::Value::BoolValue(value)),
        }),
    }
}

fn string_value(value: &str) -> AnyValue {
    AnyValue {
        value: Some(any_value::Value::StringValue(value.to_string())),
    }
}

fn json_text(value: &str) -> String {
    value.to_string()
}

fn trace_id(index: usize) -> Vec<u8> {
    let mut bytes = [0_u8; 16];
    bytes[8..].copy_from_slice(&(index as u64).to_be_bytes());
    bytes.to_vec()
}

fn span_id(trace_index: usize, span_index: u8) -> Vec<u8> {
    let raw = ((trace_index as u64 + 1) << 8) | u64::from(span_index);
    raw.to_be_bytes().to_vec()
}

fn millis(value: u64) -> u64 {
    value * 1_000_000
}

fn now_unix_nanos() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos() as u64)
        .unwrap_or_default()
}
