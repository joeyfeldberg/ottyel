//! Renders README screenshots from a realistic workload ingested through the real store.
//!
//! `cargo test ui::demo_screens -- --ignored` writes colored HTML screens to
//! `target/ui-preview/readme/`; `scripts/readme-screenshots.sh` turns them into PNGs.

use std::{
    fs,
    path::Path,
    time::{SystemTime, UNIX_EPOCH},
};

use opentelemetry_proto::tonic::{
    collector::{
        logs::v1::ExportLogsServiceRequest, metrics::v1::ExportMetricsServiceRequest,
        trace::v1::ExportTraceServiceRequest,
    },
    common::v1::{AnyValue, KeyValue, any_value},
    logs::v1::{LogRecord, ResourceLogs, ScopeLogs},
    metrics::v1::{
        Gauge, Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics, metric, number_data_point,
    },
    resource::v1::Resource,
    trace::v1::{ResourceSpans, ScopeSpans, Span, Status, span::Event, status::StatusCode},
};
use ratatui::{Terminal, backend::TestBackend, layout::Rect};
use tempfile::tempdir;

use super::{
    LlmFocus, RenderCache, Tab, TraceFocus, TraceViewMode, UiState, render,
    snapshot_tests::buffer_html, sync_detail_scroll, sync_render_cache, sync_trace_tree_scroll,
    trace_tree_rows,
};
use crate::{
    config::Theme,
    domain::DashboardSnapshot,
    query::{QueryFilters, QueryService},
    store::Store,
};

const MS: u64 = 1_000_000;
const WIDTH: u16 = 160;
const HEIGHT: u16 = 45;

fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64
}

fn text(key: &str, value: &str) -> KeyValue {
    KeyValue {
        key: key.to_string(),
        value: Some(AnyValue {
            value: Some(any_value::Value::StringValue(value.to_string())),
        }),
    }
}

fn int(key: &str, value: i64) -> KeyValue {
    KeyValue {
        key: key.to_string(),
        value: Some(AnyValue {
            value: Some(any_value::Value::IntValue(value)),
        }),
    }
}

fn double(key: &str, value: f64) -> KeyValue {
    KeyValue {
        key: key.to_string(),
        value: Some(AnyValue {
            value: Some(any_value::Value::DoubleValue(value)),
        }),
    }
}

fn resource(service: &str) -> Option<Resource> {
    Some(Resource {
        attributes: vec![
            text("service.name", service),
            text("deployment.environment", "local"),
            text("host.name", "dev-laptop"),
        ],
        ..Resource::default()
    })
}

/// A span in a trace under construction; offsets are milliseconds from the trace start.
struct Builder {
    trace: Vec<u8>,
    start: u64,
    next: u8,
    spans: Vec<Span>,
}

impl Builder {
    fn new(trace_seed: u64, start: u64) -> Self {
        let mut trace = vec![0; 16];
        trace[..8].copy_from_slice(&(0x5eed_0000_0000_0000 | trace_seed).to_be_bytes());
        trace[8..].copy_from_slice(&trace_seed.wrapping_mul(0x9e37_79b9_7f4a_7c15).to_be_bytes());
        Self {
            trace,
            start,
            next: 1,
            spans: Vec::new(),
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn span(
        &mut self,
        parent: Option<u8>,
        name: &str,
        kind: i32,
        offset_ms: u64,
        duration_ms: u64,
        attributes: Vec<KeyValue>,
        error: Option<&str>,
    ) -> u8 {
        let id = self.next;
        self.next += 1;
        let mut span_id = self.trace[8..].to_vec();
        span_id[7] = id;
        let parent_span_id = parent.map_or_else(Vec::new, |parent| {
            let mut parent_id = self.trace[8..].to_vec();
            parent_id[7] = parent;
            parent_id
        });
        let start = self.start + offset_ms * MS;
        let end = start + duration_ms * MS;
        self.spans.push(Span {
            trace_id: self.trace.clone(),
            span_id,
            parent_span_id,
            name: name.to_string(),
            kind,
            start_time_unix_nano: start,
            end_time_unix_nano: end,
            attributes,
            events: error
                .map(|message| {
                    vec![Event {
                        time_unix_nano: end.saturating_sub(MS),
                        name: "exception".to_string(),
                        attributes: vec![text("exception.message", message)],
                        ..Event::default()
                    }]
                })
                .unwrap_or_default(),
            status: Some(Status {
                code: if error.is_some() {
                    StatusCode::Error as i32
                } else {
                    StatusCode::Ok as i32
                },
                message: error.unwrap_or_default().to_string(),
            }),
            ..Span::default()
        });
        id
    }

    fn export(self, service: &str) -> ExportTraceServiceRequest {
        ExportTraceServiceRequest {
            resource_spans: vec![ResourceSpans {
                resource: resource(service),
                scope_spans: vec![ScopeSpans {
                    spans: self.spans,
                    ..ScopeSpans::default()
                }],
                ..ResourceSpans::default()
            }],
        }
    }
}

const SERVER: i32 = 2;
const CLIENT: i32 = 3;
const INTERNAL: i32 = 1;

fn checkout_trace(seed: u64, start: u64, declined: bool, slow: u64) -> ExportTraceServiceRequest {
    let mut trace = Builder::new(seed, start);
    let root = trace.span(
        None,
        "POST /checkout",
        SERVER,
        0,
        640 + slow,
        vec![
            text("http.route", "/checkout"),
            int("http.status_code", if declined { 402 } else { 200 }),
        ],
        None,
    );
    let validate = trace.span(
        Some(root),
        "validate cart",
        INTERNAL,
        4,
        38,
        vec![text("cart.id", "c-8812")],
        None,
    );
    trace.span(
        Some(validate),
        "SELECT carts",
        CLIENT,
        8,
        22,
        vec![
            text("db.system", "postgresql"),
            text("db.statement", "SELECT * FROM carts WHERE id = $1"),
        ],
        None,
    );
    let auth = trace.span(
        Some(root),
        "authenticate customer",
        INTERNAL,
        44,
        61,
        vec![text("customer.tier", "premium")],
        None,
    );
    trace.span(
        Some(auth),
        "GET session",
        CLIENT,
        48,
        9,
        vec![text("db.system", "redis")],
        None,
    );
    let inventory = trace.span(
        Some(root),
        "reserve inventory",
        INTERNAL,
        108,
        142 + slow / 2,
        vec![int("items", 3)],
        None,
    );
    trace.span(
        Some(inventory),
        "UPDATE stock",
        CLIENT,
        116,
        96 + slow / 2,
        vec![text("db.system", "postgresql")],
        None,
    );
    trace.span(
        Some(root),
        "Prompt: Recommend Add-ons",
        CLIENT,
        256 + slow / 2,
        268,
        llm_attributes(
            "openai",
            "gpt-4.1-mini",
            "Recommend two add-ons for a premium customer buying a standing desk.",
            "Suggest the cable tray and the anti-fatigue mat.",
            912,
            86,
            0.0041,
            "checkout-session-42",
        ),
        None,
    );
    let payment = trace.span(
        Some(root),
        "charge payment",
        CLIENT,
        530 + slow,
        104,
        vec![
            text("payment.provider", "stripe"),
            text("peer.service", "payments"),
        ],
        declined.then_some("card_declined: insufficient funds"),
    );
    trace.span(
        Some(payment),
        "POST api.stripe.com/v1/charges",
        CLIENT,
        536 + slow,
        92,
        vec![
            text("http.method", "POST"),
            int("http.status_code", if declined { 402 } else { 200 }),
        ],
        declined.then_some("402 Payment Required"),
    );
    trace.export("checkout-api")
}

#[allow(clippy::too_many_arguments)]
fn llm_attributes(
    provider: &str,
    model: &str,
    prompt: &str,
    output: &str,
    input_tokens: i64,
    output_tokens: i64,
    cost: f64,
    session: &str,
) -> Vec<KeyValue> {
    vec![
        text("openinference.span.kind", "LLM"),
        text("llm.provider", provider),
        text("llm.model_name", model),
        text("llm.operation", "chat"),
        text("input.value", prompt),
        text("output.value", output),
        int("llm.token_count.prompt", input_tokens),
        int("llm.token_count.completion", output_tokens),
        int("llm.token_count.total", input_tokens + output_tokens),
        double("llm.cost.total", cost),
        text("session.id", session),
        text("llm.conversation_id", "support-thread-7"),
    ]
}

fn agent_trace(seed: u64, start: u64, failed_tool: bool) -> ExportTraceServiceRequest {
    let mut trace = Builder::new(seed, start);
    let root = trace.span(
        None,
        "agent.run support-triage",
        SERVER,
        0,
        2_480,
        vec![
            text("openinference.span.kind", "AGENT"),
            text("session.id", "support-session-7"),
        ],
        None,
    );
    trace.span(
        Some(root),
        "Prompt: Plan Resolution",
        CLIENT,
        12,
        930,
        llm_attributes(
            "anthropic",
            "claude-sonnet-4",
            "Customer says order #4471 never arrived. Decide which tools to call.",
            "Call lookup_order for #4471, then check carrier status.",
            1_640,
            212,
            0.0081,
            "support-session-7",
        ),
        None,
    );
    trace.span(
        Some(root),
        "tool lookup_order",
        INTERNAL,
        960,
        340,
        vec![
            text("openinference.span.kind", "TOOL"),
            text("tool.name", "lookup_order"),
            text("tool.arguments", r#"{"order_id":"4471"}"#),
        ],
        failed_tool.then_some("carrier API timeout"),
    );
    trace.span(
        Some(root),
        "Prompt: Draft Reply",
        CLIENT,
        1_320,
        1_120,
        llm_attributes(
            "openai",
            "gpt-4o-mini",
            "Draft a short apology with the tracking status for order #4471.",
            "Sorry for the delay! Your order is out for delivery and should arrive today.",
            1_210,
            96,
            0.0009,
            "support-session-7",
        ),
        None,
    );
    trace.export("llm-gateway")
}

fn simple_trace(
    seed: u64,
    start: u64,
    service: &str,
    name: &str,
    duration: u64,
) -> ExportTraceServiceRequest {
    let mut trace = Builder::new(seed, start);
    let root = trace.span(
        None,
        name,
        SERVER,
        0,
        duration,
        vec![text("http.route", name)],
        None,
    );
    trace.span(
        Some(root),
        "query index",
        CLIENT,
        3,
        duration * 6 / 10,
        vec![text("db.system", "opensearch")],
        None,
    );
    trace.export(service)
}

fn logs(base: u64) -> ExportLogsServiceRequest {
    let entries = [
        ("checkout-api", "INFO", "checkout started for cart c-8812"),
        ("checkout-api", "INFO", "inventory reserved for 3 items"),
        (
            "payments",
            "ERROR",
            r#"{"message":"card declined","code":"insufficient_funds","cart":"c-8812"}"#,
        ),
        (
            "catalog-worker",
            "WARN",
            "catalog sync lag above target: 42s",
        ),
        (
            "llm-gateway",
            "INFO",
            "agent support-triage selected tool lookup_order",
        ),
        (
            "llm-gateway",
            "ERROR",
            "tool lookup_order failed: carrier API timeout after 3 retries",
        ),
        ("search-api", "DEBUG", "cache hit for query 'standing desk'"),
        ("checkout-api", "INFO", "order o-99123 confirmed"),
        ("catalog-worker", "INFO", "synced 1,284 products in 8.2s"),
        ("payments", "WARN", "stripe latency above 400ms for 5m"),
    ];
    let records = (0..40)
        .map(|index| {
            let (service, severity, body) = entries[index % entries.len()];
            (
                service,
                LogRecord {
                    time_unix_nano: base + index as u64 * 55_000 * MS,
                    severity_text: severity.to_string(),
                    body: Some(AnyValue {
                        value: Some(any_value::Value::StringValue(body.to_string())),
                    }),
                    attributes: vec![
                        text("component", "demo"),
                        int("attempt", (index % 3) as i64 + 1),
                    ],
                    ..LogRecord::default()
                },
            )
        })
        .collect::<Vec<_>>();
    ExportLogsServiceRequest {
        resource_logs: records
            .into_iter()
            .map(|(service, record)| ResourceLogs {
                resource: resource(service),
                scope_logs: vec![ScopeLogs {
                    log_records: vec![record],
                    ..ScopeLogs::default()
                }],
                ..ResourceLogs::default()
            })
            .collect(),
    }
}

/// A metric value as a function of minutes since the demo start.
type Shape = fn(f64) -> f64;

fn metrics(base: u64) -> ExportMetricsServiceRequest {
    let series: [(&str, &str, Shape); 4] = [
        ("checkout-api", "http.server.duration.p95", |t| {
            180.0 + 60.0 * (t / 5.0).sin() + if t > 30.0 { 140.0 } else { 0.0 }
        }),
        ("checkout-api", "checkout.queue.depth", |t| {
            12.0 + 8.0 * (t / 3.0).sin().abs() + t / 4.0
        }),
        ("catalog-worker", "process.cpu.utilization", |t| {
            0.35 + 0.2 * (t / 7.0).cos()
        }),
        ("llm-gateway", "llm.tokens.per_minute", |t| {
            4_200.0 + 1_800.0 * (t / 6.0).sin()
        }),
    ];
    ExportMetricsServiceRequest {
        resource_metrics: series
            .into_iter()
            .map(|(service, name, shape)| ResourceMetrics {
                resource: resource(service),
                scope_metrics: vec![ScopeMetrics {
                    metrics: vec![Metric {
                        name: name.to_string(),
                        data: Some(metric::Data::Gauge(Gauge {
                            data_points: (0..40)
                                .map(|point| NumberDataPoint {
                                    time_unix_nano: base + point * 60_000 * MS,
                                    value: Some(number_data_point::Value::AsDouble(
                                        (shape(point as f64) * 100.0).round() / 100.0,
                                    )),
                                    ..NumberDataPoint::default()
                                })
                                .collect(),
                        })),
                        ..Metric::default()
                    }],
                    ..ScopeMetrics::default()
                }],
                ..ResourceMetrics::default()
            })
            .collect(),
    }
}

fn seeded_query() -> (tempfile::TempDir, QueryService) {
    let directory = tempdir().unwrap();
    let store = Store::open(&directory.path().join("demo.db"), 24, 100_000).unwrap();
    let base = now() - 42 * 60_000 * MS;
    for index in 0..18_u64 {
        let declined = index % 6 == 4;
        let slow = if index % 7 == 3 {
            900
        } else {
            index * 13 % 120
        };
        store
            .ingest_traces(checkout_trace(
                index + 1,
                base + index * 130_000 * MS,
                declined,
                slow,
            ))
            .unwrap();
    }
    for index in 0..4_u64 {
        store
            .ingest_traces(agent_trace(
                100 + index,
                base + (index * 9 + 5) * 60_000 * MS,
                index == 2,
            ))
            .unwrap();
    }
    for index in 0..8_u64 {
        let (service, name) = if index % 2 == 0 {
            ("search-api", "GET /search")
        } else {
            ("catalog-worker", "sync catalog")
        };
        store
            .ingest_traces(simple_trace(
                200 + index,
                base + (index * 5 + 2) * 60_000 * MS,
                service,
                name,
                40 + index * 37,
            ))
            .unwrap();
    }
    store.ingest_logs(logs(base)).unwrap();
    store.ingest_metrics(metrics(base)).unwrap();
    (directory, QueryService::new(store, 500))
}

fn render_screen(snapshot: &DashboardSnapshot, mut state: UiState) -> String {
    let mut cache = RenderCache::default();
    let root = Rect::new(0, 0, WIDTH, HEIGHT);
    sync_trace_tree_scroll(root, snapshot, &mut state);
    sync_render_cache(snapshot, &state, &mut cache);
    sync_detail_scroll(root, snapshot, &mut state, &cache);
    let mut terminal = Terminal::new(TestBackend::new(WIDTH, HEIGHT)).unwrap();
    terminal
        .draw(|frame| render(frame, snapshot, &state, &cache))
        .unwrap();
    buffer_html(terminal.backend().buffer())
}

#[test]
#[ignore = "writes README screenshot sources; run explicitly"]
fn write_readme_screens() {
    let (_directory, query) = seeded_query();
    let base_state = UiState {
        theme: Theme::Ember,
        ..UiState::default()
    };
    let mut snapshot = query.snapshot(&QueryFilters::default()).unwrap();

    let trace_index = snapshot
        .traces
        .iter()
        .position(|trace| trace.root_name == "POST /checkout" && trace.error_count > 0)
        .expect("the demo has a declined checkout");
    snapshot.selected_trace = query
        .trace_detail(&snapshot.traces[trace_index].trace_id)
        .unwrap();
    let tree = trace_tree_rows(&snapshot.selected_trace, &Default::default());
    let error_row = tree
        .iter()
        .position(|row| row.span.span_name == "charge payment")
        .unwrap_or(0);

    let llm_index = snapshot
        .llm
        .iter()
        .position(|call| call.model == "claude-sonnet-4")
        .unwrap_or(0);
    let call = &snapshot.llm[llm_index];
    snapshot.selected_llm_timeline = query.llm_timeline(&call.trace_id, &call.span_id).unwrap();

    let declined_log = snapshot
        .logs
        .iter()
        .position(|log| log.body.contains("card declined"))
        .unwrap_or(0);

    let screens = [
        (
            "overview",
            UiState {
                active_tab: Tab::Overview.index(),
                ..base_state.clone()
            },
        ),
        (
            "traces",
            UiState {
                active_tab: Tab::Traces.index(),
                trace_view_mode: TraceViewMode::Detail,
                trace_focus: TraceFocus::TraceTree,
                selected_trace: trace_index,
                selected_trace_span: error_row,
                trace_split_pct: 48,
                ..base_state.clone()
            },
        ),
        (
            "llm",
            UiState {
                active_tab: Tab::Llm.index(),
                llm_focus: LlmFocus::Detail,
                selected_llm: llm_index,
                ..base_state.clone()
            },
        ),
        (
            "metrics",
            UiState {
                active_tab: Tab::Metrics.index(),
                ..base_state.clone()
            },
        ),
        (
            "logs",
            UiState {
                active_tab: Tab::Logs.index(),
                selected_log: declined_log,
                ..base_state
            },
        ),
    ];
    let directory = Path::new(env!("CARGO_MANIFEST_DIR")).join("target/ui-preview/readme");
    fs::create_dir_all(&directory).unwrap();
    for (name, state) in screens {
        fs::write(
            directory.join(format!("{name}.html")),
            format!(
                "<html><body style='margin:0;background:#100c0a'>{}</body></html>",
                render_screen(&snapshot, state)
            ),
        )
        .unwrap();
    }
}
