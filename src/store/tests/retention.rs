use opentelemetry_proto::tonic::{
    collector::trace::v1::ExportTraceServiceRequest,
    common::v1::InstrumentationScope,
    resource::v1::Resource,
    trace::v1::{ResourceSpans, ScopeSpans, Span, Status, span, span::Event, span::Link},
};
use tempfile::tempdir;

use super::{Store, now_nanos, string_attr};

const HOUR_NANOS: i64 = 60 * 60 * 1_000_000_000;

#[test]
fn time_retention_keeps_complete_mixed_age_traces_and_old_events() {
    let tempdir = tempdir().unwrap();
    let store = Store::open(&tempdir.path().join("ottyel.db"), 1, 100).unwrap();
    let now = now_nanos();
    let trace_byte = 0xa1;

    store
        .ingest_traces(trace_request(vec![
            test_span(
                trace_byte,
                0x11,
                None,
                now - 2 * HOUR_NANOS,
                now - 2 * HOUR_NANOS + 2_000_000,
                true,
            ),
            test_span(trace_byte, 0x22, Some(0x11), now - 30_000_000, now, false),
        ]))
        .unwrap();

    let trace_id = hex_id(trace_byte, 16);
    let detail = store.trace_detail(&trace_id).unwrap();
    assert_eq!(detail.len(), 2);

    let old_span = detail
        .iter()
        .find(|span| span.span_id == hex_id(0x11, 8))
        .unwrap();
    assert_eq!(old_span.events.len(), 1);
    assert_eq!(old_span.events[0].name, "retention.event");
    assert_eq!(old_span.links.len(), 1);

    let llm = store.recent_llm(None, 10, None, None).unwrap();
    assert_eq!(llm.len(), 1);
    assert_eq!(llm[0].trace_id, trace_id);
    assert_eq!(dependent_row_counts(&store), (1, 1, 1));
    assert_eq!(orphan_counts(&store), (0, 0, 0));
}

#[test]
fn time_retention_deletes_wholly_expired_traces_and_dependents() {
    let tempdir = tempdir().unwrap();
    let store = Store::open(&tempdir.path().join("ottyel.db"), 1, 100).unwrap();
    let now = now_nanos();
    let trace_byte = 0xa2;

    store
        .ingest_traces(trace_request(vec![
            test_span(
                trace_byte,
                0x31,
                None,
                now - 3 * HOUR_NANOS,
                now - 3 * HOUR_NANOS + 2_000_000,
                true,
            ),
            test_span(
                trace_byte,
                0x32,
                Some(0x31),
                now - 2 * HOUR_NANOS,
                now - 2 * HOUR_NANOS + 2_000_000,
                false,
            ),
        ]))
        .unwrap();

    assert!(
        store
            .trace_detail(&hex_id(trace_byte, 16))
            .unwrap()
            .is_empty()
    );
    assert_eq!(dependent_row_counts(&store), (0, 0, 0));
    assert_eq!(orphan_counts(&store), (0, 0, 0));
}

#[test]
fn max_span_retention_evicts_oldest_whole_trace_and_cleans_dependents() {
    let tempdir = tempdir().unwrap();
    let store = Store::open(&tempdir.path().join("ottyel.db"), 24, 3).unwrap();
    let now = now_nanos();
    let old_trace_byte = 0xb1;
    let new_trace_byte = 0xb2;

    store
        .ingest_traces(trace_request(vec![
            test_span(
                old_trace_byte,
                0x41,
                None,
                now - 60_000_000,
                now - 50_000_000,
                true,
            ),
            test_span(
                old_trace_byte,
                0x42,
                Some(0x41),
                now - 49_000_000,
                now - 40_000_000,
                false,
            ),
        ]))
        .unwrap();
    store
        .ingest_traces(trace_request(vec![
            test_span(
                new_trace_byte,
                0x51,
                None,
                now - 30_000_000,
                now - 20_000_000,
                false,
            ),
            test_span(
                new_trace_byte,
                0x52,
                Some(0x51),
                now - 19_000_000,
                now - 10_000_000,
                false,
            ),
        ]))
        .unwrap();

    let old_trace_id = hex_id(old_trace_byte, 16);
    let new_trace_id = hex_id(new_trace_byte, 16);
    assert!(store.trace_detail(&old_trace_id).unwrap().is_empty());
    assert_eq!(store.trace_detail(&new_trace_id).unwrap().len(), 2);

    let traces = store.recent_traces(None, false, 10, None, None).unwrap();
    assert_eq!(
        traces
            .iter()
            .map(|trace| trace.trace_id.as_str())
            .collect::<Vec<_>>(),
        vec![new_trace_id]
    );
    assert_eq!(dependent_row_counts(&store), (0, 0, 0));
    assert_eq!(orphan_counts(&store), (0, 0, 0));
}

#[test]
fn retention_cleanup_correlates_dependents_by_trace_and_span() {
    let tempdir = tempdir().unwrap();
    let store = Store::open(&tempdir.path().join("ottyel.db"), 24, 100).unwrap();
    let now = now_nanos();
    let old_trace_byte = 0xc1;
    let new_trace_byte = 0xc2;
    let shared_span_byte = 0x61;

    store
        .ingest_traces(trace_request(vec![test_span(
            old_trace_byte,
            shared_span_byte,
            None,
            now - 20_000_000,
            now - 10_000_000,
            true,
        )]))
        .unwrap();
    store
        .ingest_traces(trace_request(vec![test_span(
            new_trace_byte,
            shared_span_byte,
            None,
            now - 5_000_000,
            now,
            false,
        )]))
        .unwrap();

    // The v1 schema keys spans globally by span_id. Until that migration lands,
    // retention must still remove projections left under the displaced trace ID.
    assert!(
        store
            .trace_detail(&hex_id(old_trace_byte, 16))
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        store
            .trace_detail(&hex_id(new_trace_byte, 16))
            .unwrap()
            .len(),
        1
    );
    assert_eq!(dependent_row_counts(&store), (0, 0, 0));
    assert_eq!(orphan_counts(&store), (0, 0, 0));
}

fn trace_request(spans: Vec<Span>) -> ExportTraceServiceRequest {
    ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            resource: Some(Resource {
                attributes: vec![string_attr("service.name", "retention-test")],
                dropped_attributes_count: 0,
                entity_refs: Vec::new(),
            }),
            schema_url: String::new(),
            scope_spans: vec![ScopeSpans {
                scope: Some(InstrumentationScope::default()),
                spans,
                schema_url: String::new(),
            }],
        }],
    }
}

fn test_span(
    trace_byte: u8,
    span_byte: u8,
    parent_span_byte: Option<u8>,
    start_time_unix_nano: i64,
    end_time_unix_nano: i64,
    with_dependents: bool,
) -> Span {
    let attributes = if with_dependents {
        vec![
            string_attr("llm.provider", "openai"),
            string_attr("llm.model_name", "retention-model"),
        ]
    } else {
        Vec::new()
    };
    let events = if with_dependents {
        vec![Event {
            time_unix_nano: start_time_unix_nano as u64 + 1_000_000,
            name: "retention.event".to_string(),
            attributes: vec![string_attr("retention.kind", "test")],
            dropped_attributes_count: 0,
        }]
    } else {
        Vec::new()
    };
    let links = if with_dependents {
        vec![Link {
            trace_id: vec![0xcc; 16],
            span_id: vec![0xdd; 8],
            trace_state: String::new(),
            attributes: vec![string_attr("retention.kind", "test")],
            dropped_attributes_count: 0,
            flags: 0,
        }]
    } else {
        Vec::new()
    };

    Span {
        trace_id: vec![trace_byte; 16],
        span_id: vec![span_byte; 8],
        parent_span_id: parent_span_byte
            .map(|byte| vec![byte; 8])
            .unwrap_or_default(),
        trace_state: String::new(),
        name: format!("retention-span-{span_byte:02x}"),
        kind: span::SpanKind::Internal as i32,
        start_time_unix_nano: start_time_unix_nano as u64,
        end_time_unix_nano: end_time_unix_nano as u64,
        attributes,
        dropped_attributes_count: 0,
        events,
        dropped_events_count: 0,
        links,
        dropped_links_count: 0,
        status: Some(Status {
            message: String::new(),
            code: 1,
        }),
        flags: 0,
    }
}

fn dependent_row_counts(store: &Store) -> (i64, i64, i64) {
    let conn = store.reader_connection_for_test();
    conn.query_row(
        r#"
        SELECT
            (SELECT COUNT(*) FROM span_events),
            (SELECT COUNT(*) FROM span_links),
            (SELECT COUNT(*) FROM llm_spans)
        "#,
        [],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
    )
    .unwrap()
}

fn orphan_counts(store: &Store) -> (i64, i64, i64) {
    let conn = store.reader_connection_for_test();
    conn.query_row(
        r#"
        SELECT
            (
                SELECT COUNT(*) FROM span_events AS event
                WHERE NOT EXISTS (
                    SELECT 1 FROM spans AS span
                    WHERE span.trace_id = event.trace_id AND span.span_id = event.span_id
                )
            ),
            (
                SELECT COUNT(*) FROM span_links AS link
                WHERE NOT EXISTS (
                    SELECT 1 FROM spans AS span
                    WHERE span.trace_id = link.trace_id AND span.span_id = link.span_id
                )
            ),
            (
                SELECT COUNT(*) FROM llm_spans AS llm
                WHERE NOT EXISTS (
                    SELECT 1 FROM spans AS span
                    WHERE span.trace_id = llm.trace_id AND span.span_id = llm.span_id
                )
            )
        "#,
        [],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
    )
    .unwrap()
}

fn hex_id(byte: u8, width: usize) -> String {
    format!("{byte:02x}").repeat(width)
}
