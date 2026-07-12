use std::{
    path::PathBuf,
    time::{Duration, Instant},
};

use anyhow::{Context, Result, ensure};
use opentelemetry_proto::tonic::{
    collector::{
        logs::v1::ExportLogsServiceRequest, metrics::v1::ExportMetricsServiceRequest,
        trace::v1::ExportTraceServiceRequest,
    },
    common::v1::{AnyValue, InstrumentationScope, KeyValue, any_value},
    logs::v1::{LogRecord, ResourceLogs, ScopeLogs},
    metrics::v1::{
        Gauge, Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics, metric, number_data_point,
    },
    resource::v1::Resource,
    trace::v1::{ResourceSpans, ScopeSpans, Span, Status, span},
};
use ottyel::{query::QueryService, store::Store};
use tempfile::TempDir;

use super::config::Scale;

pub(crate) const SERVICE: &str = "perf-service";
pub(crate) const AI_SERVICE: &str = "perf-ai";
pub(crate) const SEARCH_MARKER: &str = "ottyel-perf-needle";
const SYNTHETIC_EPOCH_NANOS: u64 = 4_000_000_000_000_000_000;
const SPANS_PER_TRACE: usize = 10;

pub(crate) struct SeededStore {
    pub store: Store,
    pub query: QueryService,
    pub database_path: PathBuf,
    pub large_trace_id: String,
    pub setup_duration: Duration,
    _directory: TempDir,
}

impl SeededStore {
    pub(crate) fn create(scale: Scale, acknowledgement_capacity: usize) -> Result<Self> {
        let started = Instant::now();
        let directory = tempfile::tempdir().context("failed to create benchmark directory")?;
        let database_path = directory.path().join("store.sqlite3");
        let maximum_spans = scale
            .trace_spans
            .saturating_add(scale.large_trace_spans)
            .saturating_add(scale.ai_operations)
            .saturating_add(acknowledgement_capacity)
            .saturating_add(1_000);
        let store = Store::open(&database_path, 24 * 30, maximum_spans)?;

        seed_trace_spans(&store, scale.trace_spans, scale.trace_batch_size)?;
        eprintln!("seeded {} ordinary spans", scale.trace_spans);
        seed_large_trace(&store, scale.large_trace_spans, scale.trace_batch_size)?;
        eprintln!("seeded {} large-trace spans", scale.large_trace_spans);
        seed_logs(&store, scale.logs, scale.log_batch_size)?;
        eprintln!("seeded {} logs", scale.logs);
        seed_metrics(&store, scale.metric_points, scale.metric_batch_size)?;
        eprintln!("seeded {} metric points", scale.metric_points);
        seed_ai_operations(&store, scale.ai_operations, scale.ai_batch_size)?;
        eprintln!("seeded {} AI operations", scale.ai_operations);

        Ok(Self {
            query: QueryService::new(store.clone(), 50),
            store,
            database_path,
            large_trace_id: trace_id_hex(0x12, 1),
            setup_duration: started.elapsed(),
            _directory: directory,
        })
    }
}

pub(crate) fn acknowledgement_request(
    count: usize,
    batch_index: usize,
) -> ExportTraceServiceRequest {
    let first_span = batch_index.saturating_mul(count);
    trace_request(
        SERVICE,
        (0..count)
            .map(|index| {
                let unique_index = first_span.saturating_add(index);
                basic_span(
                    trace_id(0x14, (unique_index / SPANS_PER_TRACE) as u64),
                    span_id(0x24, unique_index as u64),
                    parent_id(unique_index, 0x24),
                    format!("ack-span-{}", index % SPANS_PER_TRACE),
                    SYNTHETIC_EPOCH_NANOS + 4_000_000_000 + unique_index as u64 * 1_000,
                )
            })
            .collect(),
    )
}

fn seed_trace_spans(store: &Store, total: usize, batch_size: usize) -> Result<()> {
    for_each_batch(total, batch_size, |start, count| {
        let spans = (start..start + count)
            .map(|index| {
                let name = if index == 42 {
                    format!("checkout {SEARCH_MARKER}")
                } else {
                    format!("request-span-{}", index % SPANS_PER_TRACE)
                };
                basic_span(
                    trace_id(0x11, (index / SPANS_PER_TRACE) as u64),
                    span_id(0x21, index as u64),
                    parent_id(index, 0x21),
                    name,
                    SYNTHETIC_EPOCH_NANOS + index as u64 * 1_000,
                )
            })
            .collect();
        ingest_trace_batch(store, trace_request(SERVICE, spans), count)
    })
}

fn seed_large_trace(store: &Store, total: usize, batch_size: usize) -> Result<()> {
    for_each_batch(total, batch_size, |start, count| {
        let spans = (start..start + count)
            .map(|index| {
                basic_span(
                    trace_id(0x12, 1),
                    span_id(0x22, index as u64),
                    if index != 0 {
                        span_id(0x22, 0)
                    } else {
                        Vec::new()
                    },
                    format!("large-trace-span-{index}"),
                    SYNTHETIC_EPOCH_NANOS + 1_000_000_000 + index as u64 * 1_000,
                )
            })
            .collect();
        ingest_trace_batch(store, trace_request(SERVICE, spans), count)
    })
}

fn seed_logs(store: &Store, total: usize, batch_size: usize) -> Result<()> {
    for_each_batch(total, batch_size, |start, count| {
        let records = (start..start + count)
            .map(|index| LogRecord {
                time_unix_nano: SYNTHETIC_EPOCH_NANOS + 2_000_000_000 + index as u64 * 1_000,
                observed_time_unix_nano: SYNTHETIC_EPOCH_NANOS
                    + 2_000_000_100
                    + index as u64 * 1_000,
                severity_number: 9,
                severity_text: "INFO".to_string(),
                body: Some(string_value(if index == 42 {
                    SEARCH_MARKER
                } else {
                    "deterministic benchmark log"
                })),
                attributes: vec![string_attr("benchmark.record.kind", "log")],
                dropped_attributes_count: 0,
                flags: 0,
                trace_id: trace_id(0x11, (index / SPANS_PER_TRACE) as u64),
                span_id: span_id(0x21, (index % 2_000) as u64),
                event_name: String::new(),
            })
            .collect();
        let request = ExportLogsServiceRequest {
            resource_logs: vec![ResourceLogs {
                resource: Some(resource(SERVICE)),
                scope_logs: vec![ScopeLogs {
                    scope: Some(InstrumentationScope::default()),
                    log_records: records,
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        };
        let accepted = store.ingest_logs(request)?;
        ensure!(
            accepted == count,
            "log batch inserted {accepted}, expected {count}"
        );
        Ok(())
    })
}

fn seed_metrics(store: &Store, total: usize, batch_size: usize) -> Result<()> {
    for_each_batch(total, batch_size, |start, count| {
        let points = (start..start + count)
            .map(|index| NumberDataPoint {
                attributes: vec![string_attr(
                    "benchmark.route",
                    if index % 2 == 0 { "/alpha" } else { "/beta" },
                )],
                start_time_unix_nano: SYNTHETIC_EPOCH_NANOS,
                time_unix_nano: SYNTHETIC_EPOCH_NANOS + 3_000_000_000 + index as u64 * 1_000,
                exemplars: Vec::new(),
                flags: 0,
                value: Some(number_data_point::Value::AsInt((index % 1_000) as i64)),
            })
            .collect();
        let request = ExportMetricsServiceRequest {
            resource_metrics: vec![ResourceMetrics {
                resource: Some(resource(SERVICE)),
                scope_metrics: vec![ScopeMetrics {
                    scope: Some(InstrumentationScope::default()),
                    metrics: vec![Metric {
                        name: "benchmark.queue.depth".to_string(),
                        description: "deterministic benchmark gauge".to_string(),
                        unit: "1".to_string(),
                        data: Some(metric::Data::Gauge(Gauge {
                            data_points: points,
                        })),
                        metadata: Vec::new(),
                    }],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        };
        let accepted = store.ingest_metrics(request)?;
        ensure!(
            accepted == count,
            "metric batch inserted {accepted}, expected {count}"
        );
        Ok(())
    })
}

fn seed_ai_operations(store: &Store, total: usize, batch_size: usize) -> Result<()> {
    for_each_batch(total, batch_size, |start, count| {
        let spans = (start..start + count)
            .map(|index| {
                let mut span = basic_span(
                    trace_id(0x13, (index / SPANS_PER_TRACE) as u64),
                    span_id(0x23, index as u64),
                    parent_id(index, 0x23),
                    format!("chat gpt-benchmark-{}", index % 4),
                    SYNTHETIC_EPOCH_NANOS + 5_000_000_000 + index as u64 * 1_000,
                );
                span.attributes = vec![
                    string_attr("gen_ai.provider.name", "benchmark-provider"),
                    string_attr(
                        "gen_ai.request.model",
                        &format!("gpt-benchmark-{}", index % 4),
                    ),
                    string_attr("gen_ai.operation.name", "chat"),
                    string_attr("gen_ai.conversation.id", &format!("run-{}", index / 10)),
                    int_attr("gen_ai.usage.input_tokens", 100 + (index % 100) as i64),
                    int_attr("gen_ai.usage.output_tokens", 20 + (index % 20) as i64),
                ];
                span
            })
            .collect();
        ingest_trace_batch(store, trace_request(AI_SERVICE, spans), count)
    })
}

fn for_each_batch(
    total: usize,
    batch_size: usize,
    mut ingest: impl FnMut(usize, usize) -> Result<()>,
) -> Result<()> {
    let mut start = 0;
    while start < total {
        let count = batch_size.min(total - start);
        ingest(start, count)?;
        start += count;
    }
    Ok(())
}

fn ingest_trace_batch(
    store: &Store,
    request: ExportTraceServiceRequest,
    expected: usize,
) -> Result<()> {
    let accepted = store.ingest_traces(request)?;
    ensure!(
        accepted == expected,
        "trace batch inserted {accepted}, expected {expected}"
    );
    Ok(())
}

fn trace_request(service: &str, spans: Vec<Span>) -> ExportTraceServiceRequest {
    ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            resource: Some(resource(service)),
            scope_spans: vec![ScopeSpans {
                scope: Some(InstrumentationScope::default()),
                spans,
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    }
}

fn basic_span(
    trace_id: Vec<u8>,
    span_id: Vec<u8>,
    parent_span_id: Vec<u8>,
    name: String,
    start_time_unix_nano: u64,
) -> Span {
    Span {
        trace_id,
        span_id,
        parent_span_id,
        trace_state: String::new(),
        name,
        kind: span::SpanKind::Internal as i32,
        start_time_unix_nano,
        end_time_unix_nano: start_time_unix_nano + 1_000_000,
        attributes: vec![string_attr("benchmark.dataset", "store-baseline")],
        dropped_attributes_count: 0,
        events: Vec::new(),
        dropped_events_count: 0,
        links: Vec::new(),
        dropped_links_count: 0,
        status: Some(Status {
            message: String::new(),
            code: 1,
        }),
        flags: 0,
    }
}

fn parent_id(index: usize, namespace: u8) -> Vec<u8> {
    if index.is_multiple_of(SPANS_PER_TRACE) {
        Vec::new()
    } else {
        span_id(namespace, (index - index % SPANS_PER_TRACE) as u64)
    }
}

fn resource(service: &str) -> Resource {
    Resource {
        attributes: vec![string_attr("service.name", service)],
        dropped_attributes_count: 0,
        entity_refs: Vec::new(),
    }
}

fn string_attr(key: &str, value: &str) -> KeyValue {
    KeyValue {
        key: key.to_string(),
        value: Some(string_value(value)),
    }
}

fn int_attr(key: &str, value: i64) -> KeyValue {
    KeyValue {
        key: key.to_string(),
        value: Some(AnyValue {
            value: Some(any_value::Value::IntValue(value)),
        }),
    }
}

fn string_value(value: &str) -> AnyValue {
    AnyValue {
        value: Some(any_value::Value::StringValue(value.to_string())),
    }
}

fn trace_id(namespace: u8, index: u64) -> Vec<u8> {
    let mut id = vec![0; 16];
    id[0] = namespace;
    id[8..].copy_from_slice(&index.to_be_bytes());
    id
}

fn span_id(namespace: u8, index: u64) -> Vec<u8> {
    let mut id = index.to_be_bytes();
    id[0] = namespace;
    id.to_vec()
}

fn trace_id_hex(namespace: u8, index: u64) -> String {
    trace_id(namespace, index)
        .into_iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}
