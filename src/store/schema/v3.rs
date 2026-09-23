//! Schema v3 keys spans and their projections by `(trace_id, span_id)`.
//!
//! v1 keyed spans by `span_id` alone, so a span ID reused in another trace silently replaced
//! the first span. v3 rebuilds `spans`, `span_events`, `span_links`, and `llm_spans` with
//! composite identity and cascading foreign keys, so deleting a span deletes its projections.
//! Rows that no longer reference a span are removed first because the new keys forbid them.

use anyhow::Result;
use rusqlite::Connection;

use super::{
    v1::{self, IndexDefinition, TableDefinition, column},
    v2,
};

macro_rules! spans_sql {
    () => {
        "CREATE TABLE spans (
            trace_id TEXT NOT NULL,
            span_id TEXT NOT NULL,
            parent_span_id TEXT NOT NULL,
            service_name TEXT NOT NULL,
            span_name TEXT NOT NULL,
            span_kind TEXT NOT NULL,
            status_code TEXT NOT NULL,
            start_time_unix_nano INTEGER NOT NULL,
            end_time_unix_nano INTEGER NOT NULL,
            duration_ms REAL NOT NULL,
            resource_attributes_json TEXT NOT NULL,
            attributes_json TEXT NOT NULL,
            llm_json TEXT,
            PRIMARY KEY (trace_id, span_id)
        )"
    };
}

macro_rules! span_events_sql {
    () => {
        "CREATE TABLE span_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            trace_id TEXT NOT NULL,
            span_id TEXT NOT NULL,
            name TEXT NOT NULL,
            timestamp_unix_nano INTEGER NOT NULL,
            attributes_json TEXT NOT NULL,
            FOREIGN KEY (trace_id, span_id) REFERENCES spans (trace_id, span_id) ON DELETE CASCADE
        )"
    };
}

macro_rules! span_links_sql {
    () => {
        "CREATE TABLE span_links (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            trace_id TEXT NOT NULL,
            span_id TEXT NOT NULL,
            linked_trace_id TEXT NOT NULL,
            linked_span_id TEXT NOT NULL,
            trace_state TEXT NOT NULL,
            attributes_json TEXT NOT NULL,
            FOREIGN KEY (trace_id, span_id) REFERENCES spans (trace_id, span_id) ON DELETE CASCADE
        )"
    };
}

macro_rules! llm_spans_sql {
    () => {
        "CREATE TABLE llm_spans (
            trace_id TEXT NOT NULL,
            span_id TEXT NOT NULL,
            service_name TEXT NOT NULL,
            provider TEXT NOT NULL,
            model TEXT NOT NULL,
            operation TEXT NOT NULL,
            input_tokens INTEGER,
            output_tokens INTEGER,
            total_tokens INTEGER,
            cost REAL,
            latency_ms REAL,
            status TEXT NOT NULL,
            raw_json TEXT NOT NULL,
            PRIMARY KEY (trace_id, span_id),
            FOREIGN KEY (trace_id, span_id) REFERENCES spans (trace_id, span_id) ON DELETE CASCADE
        )"
    };
}

// Shipped schema definitions are immutable; future changes require a new versioned migration.
// Foreign keys are not enforced inside the migration transaction; the orphan deletes make the
// copied rows satisfy them before the writer enables enforcement.
pub(super) const DDL: &str = concat!(
    "
DELETE FROM span_events WHERE NOT EXISTS (
    SELECT 1 FROM spans
    WHERE spans.trace_id = span_events.trace_id AND spans.span_id = span_events.span_id
);
DELETE FROM span_links WHERE NOT EXISTS (
    SELECT 1 FROM spans
    WHERE spans.trace_id = span_links.trace_id AND spans.span_id = span_links.span_id
);
DELETE FROM llm_spans WHERE NOT EXISTS (
    SELECT 1 FROM spans
    WHERE spans.trace_id = llm_spans.trace_id AND spans.span_id = llm_spans.span_id
);

ALTER TABLE spans RENAME TO spans_v2;
ALTER TABLE span_events RENAME TO span_events_v2;
ALTER TABLE span_links RENAME TO span_links_v2;
ALTER TABLE llm_spans RENAME TO llm_spans_v2;
",
    spans_sql!(),
    ";
INSERT INTO spans (
    trace_id, span_id, parent_span_id, service_name, span_name, span_kind, status_code,
    start_time_unix_nano, end_time_unix_nano, duration_ms, resource_attributes_json,
    attributes_json, llm_json
)
SELECT
    trace_id, span_id, parent_span_id, service_name, span_name, span_kind, status_code,
    start_time_unix_nano, end_time_unix_nano, duration_ms, resource_attributes_json,
    attributes_json, llm_json
FROM spans_v2;
",
    span_events_sql!(),
    ";
INSERT INTO span_events (id, trace_id, span_id, name, timestamp_unix_nano, attributes_json)
SELECT id, trace_id, span_id, name, timestamp_unix_nano, attributes_json FROM span_events_v2;
",
    span_links_sql!(),
    ";
INSERT INTO span_links (
    id, trace_id, span_id, linked_trace_id, linked_span_id, trace_state, attributes_json
)
SELECT id, trace_id, span_id, linked_trace_id, linked_span_id, trace_state, attributes_json
FROM span_links_v2;
",
    llm_spans_sql!(),
    ";
INSERT INTO llm_spans (
    trace_id, span_id, service_name, provider, model, operation, input_tokens, output_tokens,
    total_tokens, cost, latency_ms, status, raw_json
)
SELECT
    trace_id, span_id, service_name, provider, model, operation, input_tokens, output_tokens,
    total_tokens, cost, latency_ms, status, raw_json
FROM llm_spans_v2;

DROP TABLE span_events_v2;
DROP TABLE span_links_v2;
DROP TABLE llm_spans_v2;
DROP TABLE spans_v2;

CREATE INDEX idx_spans_service_start ON spans(service_name, start_time_unix_nano DESC);
CREATE INDEX idx_spans_status ON spans(status_code);
CREATE INDEX idx_spans_end ON spans(end_time_unix_nano);
CREATE INDEX idx_span_events_trace ON span_events(trace_id, span_id);
CREATE INDEX idx_span_links_trace ON span_links(trace_id, span_id);
CREATE INDEX idx_llm_service ON llm_spans(service_name);
"
);

const SPANS: TableDefinition = TableDefinition {
    name: "spans",
    columns: &[
        column("trace_id", "TEXT", true, 1),
        column("span_id", "TEXT", true, 2),
        column("parent_span_id", "TEXT", true, 0),
        column("service_name", "TEXT", true, 0),
        column("span_name", "TEXT", true, 0),
        column("span_kind", "TEXT", true, 0),
        column("status_code", "TEXT", true, 0),
        column("start_time_unix_nano", "INTEGER", true, 0),
        column("end_time_unix_nano", "INTEGER", true, 0),
        column("duration_ms", "REAL", true, 0),
        column("resource_attributes_json", "TEXT", true, 0),
        column("attributes_json", "TEXT", true, 0),
        column("llm_json", "TEXT", false, 0),
    ],
    autoincrement: false,
    create_sql: spans_sql!(),
};

const SPAN_EVENTS: TableDefinition = TableDefinition {
    name: "span_events",
    columns: &[
        column("id", "INTEGER", false, 1),
        column("trace_id", "TEXT", true, 0),
        column("span_id", "TEXT", true, 0),
        column("name", "TEXT", true, 0),
        column("timestamp_unix_nano", "INTEGER", true, 0),
        column("attributes_json", "TEXT", true, 0),
    ],
    autoincrement: true,
    create_sql: span_events_sql!(),
};

const SPAN_LINKS: TableDefinition = TableDefinition {
    name: "span_links",
    columns: &[
        column("id", "INTEGER", false, 1),
        column("trace_id", "TEXT", true, 0),
        column("span_id", "TEXT", true, 0),
        column("linked_trace_id", "TEXT", true, 0),
        column("linked_span_id", "TEXT", true, 0),
        column("trace_state", "TEXT", true, 0),
        column("attributes_json", "TEXT", true, 0),
    ],
    autoincrement: true,
    create_sql: span_links_sql!(),
};

const LLM_SPANS: TableDefinition = TableDefinition {
    name: "llm_spans",
    columns: &[
        column("trace_id", "TEXT", true, 1),
        column("span_id", "TEXT", true, 2),
        column("service_name", "TEXT", true, 0),
        column("provider", "TEXT", true, 0),
        column("model", "TEXT", true, 0),
        column("operation", "TEXT", true, 0),
        column("input_tokens", "INTEGER", false, 0),
        column("output_tokens", "INTEGER", false, 0),
        column("total_tokens", "INTEGER", false, 0),
        column("cost", "REAL", false, 0),
        column("latency_ms", "REAL", false, 0),
        column("status", "TEXT", true, 0),
        column("raw_json", "TEXT", true, 0),
    ],
    autoincrement: false,
    create_sql: llm_spans_sql!(),
};

pub(super) fn validate_strict(conn: &Connection) -> Result<()> {
    let tables = [
        &SPANS,
        &SPAN_EVENTS,
        &SPAN_LINKS,
        v1::table("logs"),
        v1::table("metrics"),
        &LLM_SPANS,
    ];
    let mut indexes: Vec<&IndexDefinition> = [
        "idx_spans_service_start",
        "idx_spans_status",
        "idx_span_events_trace",
        "idx_span_links_trace",
        "idx_logs_service_time",
        "idx_logs_trace",
        "idx_metrics_service_time",
        "idx_metrics_name",
        "idx_llm_service",
    ]
    .into_iter()
    .map(v1::index_named)
    .collect();
    indexes.extend(v2::INDEXES);
    v1::validate_exact(conn, &tables, &indexes)
}
