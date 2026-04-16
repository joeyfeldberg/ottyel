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

const GATEWAY_SERVICE: &str = "starlight-gateway";
const ORCHESTRATOR_SERVICE: &str = "moonbeam-concierge";
const RETRIEVAL_SERVICE: &str = "atlas-catalog";
const TOOL_SERVICE: &str = "parcel-oracle";

#[derive(Clone, Copy)]
struct SeedScenario {
    route: &'static str,
    request_name: &'static str,
    workflow_name: &'static str,
    retrieval_name: &'static str,
    llm_name: &'static str,
    tool_name: &'static str,
    collection: &'static str,
    prompt: &'static str,
    output: &'static str,
}

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
    let mut orchestrator_spans = Vec::new();
    let mut retrieval_spans = Vec::new();
    let mut tool_spans = Vec::new();

    for trace_index in 0..trace_count {
        let scenario = scenario(trace_index);
        let trace_id = trace_id(trace_index + 1);
        let started_at = base_time + (trace_index as u64 * 12 * 1_000_000_000);
        let has_error = trace_index % 4 == 2;
        let uses_tool = trace_index % 2 == 0;
        let is_showcase = trace_index == 0;

        let gateway_root = span_id(trace_index, 1);
        let orchestrate = span_id(trace_index, 2);
        let retrieval = span_id(trace_index, 3);
        let llm = span_id(trace_index, 4);
        let tool = span_id(trace_index, 5);
        let root_end = if is_showcase {
            started_at + millis(3_800)
        } else {
            started_at + millis(900 + (trace_index % 5) as u64 * 160)
        };
        let orchestrate_end = if is_showcase {
            started_at + millis(3_620)
        } else {
            started_at + millis(840 + (trace_index % 3) as u64 * 140)
        };
        let llm_end = if is_showcase {
            started_at + millis(3_300)
        } else {
            started_at + millis(920 + (trace_index % 4) as u64 * 120)
        };

        gateway_spans.push(Span {
            trace_id: trace_id.clone(),
            span_id: gateway_root.clone(),
            parent_span_id: Vec::new(),
            name: scenario.request_name.to_string(),
            kind: span::SpanKind::Server as i32,
            start_time_unix_nano: started_at,
            end_time_unix_nano: root_end,
            attributes: vec![
                kv_str("http.method", "POST"),
                kv_str("http.route", scenario.route),
                kv_str("account.id", &format!("acct-{:02}", trace_index % 7)),
                kv_str("region", "ember-coast-1"),
            ],
            events: vec![event(
                started_at + millis(15),
                "request.received",
                vec![kv_str("request.id", &format!("rq-{trace_index:03}"))],
            )],
            status: Some(ok_status()),
            ..Span::default()
        });

        orchestrator_spans.push(Span {
            trace_id: trace_id.clone(),
            span_id: orchestrate.clone(),
            parent_span_id: gateway_root.clone(),
            name: scenario.workflow_name.to_string(),
            kind: span::SpanKind::Internal as i32,
            start_time_unix_nano: started_at + millis(20),
            end_time_unix_nano: orchestrate_end,
            attributes: vec![
                kv_str("collection", scenario.collection),
                kv_str("session.id", &format!("session-{trace_index:03}")),
                kv_str(
                    "conversation.id",
                    &format!("conversation-{:02}", trace_index % 5),
                ),
            ],
            events: vec![
                event(
                    started_at + millis(35),
                    "brief.composed",
                    vec![kv_str("brief.stage", "initial")],
                ),
                event(
                    started_at + millis(150),
                    "retrieval.started",
                    vec![kv_int("top_k", 6)],
                ),
            ],
            status: Some(ok_status()),
            ..Span::default()
        });

        retrieval_spans.push(Span {
            trace_id: trace_id.clone(),
            span_id: retrieval.clone(),
            parent_span_id: orchestrate.clone(),
            name: scenario.retrieval_name.to_string(),
            kind: span::SpanKind::Client as i32,
            start_time_unix_nano: started_at + millis(140),
            end_time_unix_nano: started_at + millis(280 + (trace_index % 4) as u64 * 40),
            attributes: vec![
                kv_str("store.system", "atlas-index"),
                kv_str("collection", scenario.collection),
                kv_int("retrieval.candidates", 24),
            ],
            status: Some(if has_error {
                error_status("catalog shard timeout")
            } else {
                ok_status()
            }),
            ..Span::default()
        });

        let mut llm_events = vec![
            event(
                started_at + millis(340),
                "gen_ai.request",
                vec![kv_str("provider", "lumenai")],
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
                vec![kv_str("tool.name", scenario.tool_name)],
            ));
        }

        orchestrator_spans.push(Span {
            trace_id: trace_id.clone(),
            span_id: llm.clone(),
            parent_span_id: orchestrate.clone(),
            name: scenario.llm_name.to_string(),
            kind: span::SpanKind::Internal as i32,
            start_time_unix_nano: started_at + millis(300),
            end_time_unix_nano: llm_end,
            attributes: llm_attributes(trace_index, uses_tool, scenario),
            events: llm_events,
            links: vec![span::Link {
                trace_id: trace_id.clone(),
                span_id: retrieval.clone(),
                trace_state: "seeded".to_string(),
                attributes: vec![kv_str("relationship", "uses_catalog_context")],
                ..span::Link::default()
            }],
            status: Some(ok_status()),
            ..Span::default()
        });

        if uses_tool {
            tool_spans.push(Span {
                trace_id: trace_id.clone(),
                span_id: tool.clone(),
                parent_span_id: llm.clone(),
                name: format!("tool.{}", scenario.tool_name),
                kind: span::SpanKind::Internal as i32,
                start_time_unix_nano: started_at + millis(560),
                end_time_unix_nano: started_at + millis(710),
                attributes: vec![
                    kv_str("tool.name", scenario.tool_name),
                    kv_str(
                        "tool.arguments",
                        &format!(
                            r#"{{"account_id":"acct-{:02}","region":"ember-coast-1"}}"#,
                            trace_index % 7
                        ),
                    ),
                ],
                events: vec![event(
                    started_at + millis(565),
                    "tool.input",
                    vec![kv_str(
                        "account_id",
                        &format!("acct-{:02}", trace_index % 7),
                    )],
                )],
                status: Some(ok_status()),
                ..Span::default()
            });
        }

        if is_showcase {
            extend_showcase_trace(
                trace_index,
                started_at,
                &trace_id,
                &orchestrate,
                &llm,
                &tool,
                scenario,
                &mut orchestrator_spans,
                &mut retrieval_spans,
                &mut tool_spans,
            );
        }
    }

    ExportTraceServiceRequest {
        resource_spans: vec![
            resource_spans(GATEWAY_SERVICE, "seed.gateway", gateway_spans),
            resource_spans(ORCHESTRATOR_SERVICE, "seed.concierge", orchestrator_spans),
            resource_spans(RETRIEVAL_SERVICE, "seed.catalog", retrieval_spans),
            resource_spans(TOOL_SERVICE, "seed.tooling", tool_spans),
        ],
    }
}

#[allow(clippy::too_many_arguments)]
fn extend_showcase_trace(
    trace_index: usize,
    started_at: u64,
    trace_id: &[u8],
    orchestrate: &[u8],
    llm: &[u8],
    tool: &[u8],
    scenario: SeedScenario,
    orchestrator_spans: &mut Vec<Span>,
    retrieval_spans: &mut Vec<Span>,
    tool_spans: &mut Vec<Span>,
) {
    let palette_branch = span_id(trace_index, 6);
    let palette_story = span_id(trace_index, 7);
    let palette_notes = span_id(trace_index, 8);
    let shipping_branch = span_id(trace_index, 9);
    let courier_rules = span_id(trace_index, 10);
    let reserve_hold = span_id(trace_index, 11);
    let follow_up = span_id(trace_index, 12);
    let tradeoff = span_id(trace_index, 13);
    let editorial_pass = span_id(trace_index, 14);
    let concierge_tool = span_id(trace_index, 15);
    let courier_eta = span_id(trace_index, 16);
    let weather_hold = span_id(trace_index, 17);

    retrieval_spans.push(Span {
        trace_id: trace_id.to_vec(),
        span_id: palette_branch.clone(),
        parent_span_id: orchestrate.to_vec(),
        name: "catalog.expand_reference_set".to_string(),
        kind: span::SpanKind::Client as i32,
        start_time_unix_nano: started_at + millis(310),
        end_time_unix_nano: started_at + millis(890),
        attributes: vec![
            kv_str("store.system", "atlas-index"),
            kv_str("collection", scenario.collection),
            kv_int("seed.count", 12),
        ],
        status: Some(ok_status()),
        ..Span::default()
    });
    retrieval_spans.push(Span {
        trace_id: trace_id.to_vec(),
        span_id: palette_story.clone(),
        parent_span_id: palette_branch.clone(),
        name: "catalog.fetch_palette_story".to_string(),
        kind: span::SpanKind::Client as i32,
        start_time_unix_nano: started_at + millis(360),
        end_time_unix_nano: started_at + millis(760),
        attributes: vec![kv_str("story.kind", "material-moodboard")],
        status: Some(ok_status()),
        ..Span::default()
    });
    retrieval_spans.push(Span {
        trace_id: trace_id.to_vec(),
        span_id: palette_notes.clone(),
        parent_span_id: palette_story.clone(),
        name: "catalog.rank_material_notes".to_string(),
        kind: span::SpanKind::Internal as i32,
        start_time_unix_nano: started_at + millis(430),
        end_time_unix_nano: started_at + millis(700),
        attributes: vec![kv_int("notes.selected", 9)],
        status: Some(ok_status()),
        ..Span::default()
    });
    retrieval_spans.push(Span {
        trace_id: trace_id.to_vec(),
        span_id: shipping_branch.clone(),
        parent_span_id: orchestrate.to_vec(),
        name: "policy.load_shipping_promises".to_string(),
        kind: span::SpanKind::Client as i32,
        start_time_unix_nano: started_at + millis(910),
        end_time_unix_nano: started_at + millis(1_480),
        attributes: vec![kv_str("region", "ember-coast-1")],
        status: Some(ok_status()),
        ..Span::default()
    });
    retrieval_spans.push(Span {
        trace_id: trace_id.to_vec(),
        span_id: courier_rules.clone(),
        parent_span_id: shipping_branch.clone(),
        name: "policy.resolve_courier_rules".to_string(),
        kind: span::SpanKind::Internal as i32,
        start_time_unix_nano: started_at + millis(990),
        end_time_unix_nano: started_at + millis(1_360),
        attributes: vec![kv_str("policy.bundle", "priority-lane")],
        status: Some(ok_status()),
        ..Span::default()
    });

    tool_spans.push(Span {
        trace_id: trace_id.to_vec(),
        span_id: reserve_hold.clone(),
        parent_span_id: tool.to_vec(),
        name: "inventory.reserve_hold_option".to_string(),
        kind: span::SpanKind::Internal as i32,
        start_time_unix_nano: started_at + millis(720),
        end_time_unix_nano: started_at + millis(1_180),
        attributes: vec![kv_str("hold.window", "48h")],
        events: vec![event(
            started_at + millis(728),
            "hold.created",
            vec![kv_str("hold.id", "hold-ember-01")],
        )],
        status: Some(ok_status()),
        ..Span::default()
    });

    orchestrator_spans.push(Span {
        trace_id: trace_id.to_vec(),
        span_id: follow_up.clone(),
        parent_span_id: llm.to_vec(),
        name: "draft.follow_up_options".to_string(),
        kind: span::SpanKind::Internal as i32,
        start_time_unix_nano: started_at + millis(1_460),
        end_time_unix_nano: started_at + millis(3_020),
        attributes: vec![
            kv_str("llm.provider", "lumenai"),
            kv_str("llm.model_name", "storyweaver-mini"),
            kv_str("llm.operation", "chat"),
            kv_str(
                "input.value",
                "Write two concise follow-up options: one confident and one conservative. Mention inventory reserve and courier fallback.",
            ),
            kv_str(
                "output.value",
                "Follow-up options drafted.\n- Confident: present the reserved hold as the primary path.\n- Conservative: offer courier fallback if the atelier hold expires.",
            ),
            kv_int("llm.token_count.prompt", 610),
            kv_int("llm.token_count.completion", 130),
            kv_int("llm.token_count.total", 740),
            kv_f64("llm.cost.total", 0.0076),
        ],
        events: vec![
            event(
                started_at + millis(1_500),
                "gen_ai.request",
                vec![kv_str("provider", "lumenai")],
            ),
            event(
                started_at + millis(2_980),
                "gen_ai.response",
                vec![kv_int("output_tokens", 130)],
            ),
        ],
        status: Some(ok_status()),
        ..Span::default()
    });
    orchestrator_spans.push(Span {
        trace_id: trace_id.to_vec(),
        span_id: tradeoff.clone(),
        parent_span_id: follow_up.clone(),
        name: "draft.compare_tradeoffs".to_string(),
        kind: span::SpanKind::Internal as i32,
        start_time_unix_nano: started_at + millis(1_690),
        end_time_unix_nano: started_at + millis(2_740),
        attributes: vec![kv_str("draft.mode", "structured")],
        status: Some(ok_status()),
        ..Span::default()
    });
    orchestrator_spans.push(Span {
        trace_id: trace_id.to_vec(),
        span_id: editorial_pass.clone(),
        parent_span_id: tradeoff.clone(),
        name: "draft.editorial_pass".to_string(),
        kind: span::SpanKind::Internal as i32,
        start_time_unix_nano: started_at + millis(2_020),
        end_time_unix_nano: started_at + millis(2_960),
        attributes: vec![kv_str("tone", "calm-practical")],
        status: Some(ok_status()),
        ..Span::default()
    });

    tool_spans.push(Span {
        trace_id: trace_id.to_vec(),
        span_id: concierge_tool.clone(),
        parent_span_id: follow_up.clone(),
        name: "tool.quote_messenger_eta".to_string(),
        kind: span::SpanKind::Internal as i32,
        start_time_unix_nano: started_at + millis(2_120),
        end_time_unix_nano: started_at + millis(2_920),
        attributes: vec![
            kv_str("tool.name", "quote_messenger_eta"),
            kv_str(
                "tool.arguments",
                r#"{"region":"ember-coast-1","service_level":"priority"}"#,
            ),
        ],
        status: Some(ok_status()),
        ..Span::default()
    });
    tool_spans.push(Span {
        trace_id: trace_id.to_vec(),
        span_id: courier_eta.clone(),
        parent_span_id: concierge_tool.clone(),
        name: "messenger.plan_priority_route".to_string(),
        kind: span::SpanKind::Internal as i32,
        start_time_unix_nano: started_at + millis(2_200),
        end_time_unix_nano: started_at + millis(2_780),
        attributes: vec![kv_str("fleet", "north-ribbon")],
        status: Some(ok_status()),
        ..Span::default()
    });
    tool_spans.push(Span {
        trace_id: trace_id.to_vec(),
        span_id: weather_hold,
        parent_span_id: courier_eta,
        name: "messenger.check_weather_hold".to_string(),
        kind: span::SpanKind::Internal as i32,
        start_time_unix_nano: started_at + millis(2_320),
        end_time_unix_nano: started_at + millis(2_660),
        attributes: vec![kv_str("forecast.band", "amber")],
        status: Some(ok_status()),
        ..Span::default()
    });
}

fn build_log_request(trace_count: usize, base_time: u64) -> ExportLogsServiceRequest {
    let mut gateway_logs = Vec::new();
    let mut orchestrator_logs = Vec::new();
    let mut retrieval_logs = Vec::new();
    let mut tool_logs = Vec::new();

    for trace_index in 0..trace_count {
        let scenario = scenario(trace_index);
        let trace_id = trace_id(trace_index + 1);
        let started_at = base_time + (trace_index as u64 * 12 * 1_000_000_000);
        let gateway_root = span_id(trace_index, 1);
        let orchestrate = span_id(trace_index, 2);
        let retrieval = span_id(trace_index, 3);
        let llm = span_id(trace_index, 4);
        let tool = span_id(trace_index, 5);
        let has_error = trace_index % 4 == 2;
        let uses_tool = trace_index % 2 == 0;

        gateway_logs.push(log_record(
            started_at + millis(18),
            SeverityNumber::Info,
            "INFO",
            json_text(&format!(
                r#"{{"message":"accepted concierge request","route":"{}","account":"acct-{:02}"}}"#,
                scenario.route,
                trace_index % 7
            )),
            &trace_id,
            &gateway_root,
            vec![kv_str("http.route", scenario.route)],
        ));

        orchestrator_logs.push(log_record(
            started_at + millis(360),
            SeverityNumber::Info,
            "INFO",
            format!(
                "assembled {} brief from ranked catalog context",
                scenario.collection
            ),
            &trace_id,
            &orchestrate,
            vec![kv_int("context.docs", 6)],
        ));

        orchestrator_logs.push(log_record(
            started_at + millis(930),
            SeverityNumber::Info,
            "INFO",
            json_text(
                r#"{"message":"draft reply ready","provider":"lumenai","model":"storyweaver-mini"}"#,
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
                "catalog shard timeout while building ranked context"
            } else {
                "catalog retrieval returned six ranked briefs"
            },
            &trace_id,
            &retrieval,
            vec![kv_bool("cache_hit", trace_index % 3 == 0)],
        ));

        if uses_tool {
            tool_logs.push(log_record(
                started_at + millis(590),
                SeverityNumber::Info,
                "INFO",
                format!("tool {} enriched the concierge reply", scenario.tool_name),
                &trace_id,
                &tool,
                vec![kv_str("tool.name", scenario.tool_name)],
            ));
        }
    }

    ExportLogsServiceRequest {
        resource_logs: vec![
            resource_logs(GATEWAY_SERVICE, gateway_logs),
            resource_logs(ORCHESTRATOR_SERVICE, orchestrator_logs),
            resource_logs(RETRIEVAL_SERVICE, retrieval_logs),
            resource_logs(TOOL_SERVICE, tool_logs),
        ],
    }
}

fn build_metric_request(base_time: u64) -> ExportMetricsServiceRequest {
    let mut concierge_metrics = Vec::new();
    let mut retrieval_metrics = Vec::new();
    let mut tool_metrics = Vec::new();

    let start = base_time.saturating_sub(20 * 60 * 1_000_000_000);
    for index in 0..30_u64 {
        let time = start + index * 60 * 1_000_000_000;
        concierge_metrics.push(number_point(
            time,
            (4 + (index % 6)) as f64,
            vec![kv_str("queue", "concierge")],
        ));
        retrieval_metrics.push(number_point(
            time,
            (28 + index * 3) as f64,
            vec![kv_str("query", "catalog_briefs")],
        ));
        tool_metrics.push(number_point(
            time,
            (1 + (index % 4)) as f64,
            vec![kv_str("tool.family", "concierge_enrichment")],
        ));
    }

    ExportMetricsServiceRequest {
        resource_metrics: vec![
            ResourceMetrics {
                resource: Some(resource(ORCHESTRATOR_SERVICE)),
                scope_metrics: vec![ScopeMetrics {
                    scope: Some(scope("seed.concierge.metrics")),
                    metrics: vec![
                        Metric {
                            name: "concierge.queue.depth".to_string(),
                            description: "Synthetic concierge backlog".to_string(),
                            unit: "{item}".to_string(),
                            data: Some(metric::Data::Gauge(Gauge {
                                data_points: concierge_metrics,
                            })),
                            ..Metric::default()
                        },
                        Metric {
                            name: "concierge.requests.total".to_string(),
                            description: "Synthetic cumulative concierge requests".to_string(),
                            unit: "{request}".to_string(),
                            data: Some(metric::Data::Sum(Sum {
                                data_points: vec![number_point(
                                    start + 31 * 60 * 1_000_000_000,
                                    1_240.0,
                                    vec![kv_str("route", "/concierge/style-board")],
                                )],
                                aggregation_temporality: AggregationTemporality::Cumulative as i32,
                                is_monotonic: true,
                            })),
                            ..Metric::default()
                        },
                        Metric {
                            name: "concierge.reply.latency".to_string(),
                            description: "Synthetic LLM reply latency histogram".to_string(),
                            unit: "ms".to_string(),
                            data: Some(metric::Data::Histogram(Histogram {
                                data_points: vec![histogram_point(
                                    start + 31 * 60 * 1_000_000_000,
                                    18,
                                    24_600.0,
                                    vec![200.0, 500.0, 1_000.0, 2_000.0],
                                    vec![3, 8, 5, 2, 0],
                                    vec![kv_str("model", "storyweaver-mini")],
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
                resource: Some(resource(RETRIEVAL_SERVICE)),
                scope_metrics: vec![ScopeMetrics {
                    scope: Some(scope("seed.catalog.metrics")),
                    metrics: vec![Metric {
                        name: "catalog.candidates".to_string(),
                        description: "Synthetic catalog candidate counts".to_string(),
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
            ResourceMetrics {
                resource: Some(resource(TOOL_SERVICE)),
                scope_metrics: vec![ScopeMetrics {
                    scope: Some(scope("seed.tool.metrics")),
                    metrics: vec![Metric {
                        name: "tool.enrichment.calls".to_string(),
                        description: "Synthetic enrichment tool calls".to_string(),
                        unit: "{document}".to_string(),
                        data: Some(metric::Data::Gauge(Gauge {
                            data_points: tool_metrics,
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
            scope: Some(scope("seed.logs")),
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
            kv_str("host.name", "fictional-workstation"),
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

fn llm_attributes(trace_index: usize, uses_tool: bool, scenario: SeedScenario) -> Vec<KeyValue> {
    let prompt = format!(
        "{}\nAccount: acct-{:02}\nRegion: ember-coast-1.\nKeep the tone calm and practical.",
        scenario.prompt,
        trace_index % 7
    );
    let output = format!(
        "{}\nReference collection: {}.",
        scenario.output, scenario.collection
    );
    let mut attrs = vec![
        kv_str("llm.provider", "lumenai"),
        kv_str("llm.model_name", "storyweaver-mini"),
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
        kv_str("gen_ai.provider.name", "lumenai"),
        kv_str(
            "gen_ai.conversation.id",
            &format!("conversation-{:02}", trace_index % 5),
        ),
    ];
    if uses_tool {
        attrs.push(kv_str("tool.name", scenario.tool_name));
        attrs.push(kv_str(
            "tool.arguments",
            &format!(
                r#"{{"account_id":"acct-{:02}","region":"ember-coast-1"}}"#,
                trace_index % 7
            ),
        ));
    }
    attrs
}

fn scenario(trace_index: usize) -> SeedScenario {
    match trace_index % 4 {
        0 => SeedScenario {
            route: "/concierge/style-board",
            request_name: "POST /concierge/style-board",
            workflow_name: "workflow.build_style_board",
            retrieval_name: "catalog.match_signature_pieces",
            llm_name: "draft.style_board_reply",
            tool_name: "lookup_studio_inventory",
            collection: "seasonal-lookbook",
            prompt: "Build a spring capsule wardrobe for a customer who wants warm earth tones and compact travel layers.",
            output: "Style board ready.\n- 4 layered looks selected\n- 2 low-stock items flagged\n- Suggest swapping in the dune field jacket if the amber overshirt is unavailable.",
        },
        1 => SeedScenario {
            route: "/concierge/delivery-rescue",
            request_name: "POST /concierge/delivery-rescue",
            workflow_name: "workflow.recover_delivery_plan",
            retrieval_name: "orders.load_shipment_timeline",
            llm_name: "draft.delivery_reassurance",
            tool_name: "quote_courier_eta",
            collection: "shipment-ledger",
            prompt: "Explain a delayed parcel, summarize the latest scan events, and offer the safest next step without overpromising.",
            output: "Delivery rescue summary ready.\n- Last scan confirmed at harbor depot\n- New arrival estimate is tomorrow afternoon\n- Recommend proactive refund of express upgrade if the parcel misses the next handoff.",
        },
        2 => SeedScenario {
            route: "/concierge/gift-finder",
            request_name: "POST /concierge/gift-finder",
            workflow_name: "workflow.curate_gift_bundle",
            retrieval_name: "catalog.find_gift_clusters",
            llm_name: "draft.gift_bundle_reply",
            tool_name: "check_gift_wrap_options",
            collection: "gift-atlas",
            prompt: "Recommend a compact gift set for a customer shopping for a design-minded host with a soft budget cap.",
            output: "Gift bundle ready.\n- Picked a linen candle pair and brass tray set\n- Added one budget-safe fallback bundle\n- Mention premium wrap availability and dispatch cutoff.",
        },
        _ => SeedScenario {
            route: "/concierge/return-helper",
            request_name: "POST /concierge/return-helper",
            workflow_name: "workflow.prepare_return_plan",
            retrieval_name: "returns.load_policy_snapshot",
            llm_name: "draft.return_plan_reply",
            tool_name: "schedule_return_pickup",
            collection: "returns-playbook",
            prompt: "Summarize the return policy, highlight any restocking fee, and offer the least-friction resolution path.",
            output: "Return plan ready.\n- Eligible for courier pickup within seven days\n- No restocking fee on unopened accessories\n- Recommend pickup window before the weekend stock count.",
        },
    }
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
