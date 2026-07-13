mod validation;

use anyhow::Result;
use rusqlite::Connection;

pub(super) fn validate_strict(conn: &Connection) -> Result<()> {
    validation::validate_strict(conn)
}

// Shipped schema definitions are immutable; future changes require a new versioned migration.
pub(super) const DDL: &str = r#"
CREATE TABLE IF NOT EXISTS spans (
    trace_id TEXT NOT NULL,
    span_id TEXT PRIMARY KEY,
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
    llm_json TEXT
);
CREATE INDEX IF NOT EXISTS idx_spans_trace ON spans(trace_id);
CREATE INDEX IF NOT EXISTS idx_spans_service_start ON spans(service_name, start_time_unix_nano DESC);
CREATE INDEX IF NOT EXISTS idx_spans_status ON spans(status_code);

CREATE TABLE IF NOT EXISTS span_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    trace_id TEXT NOT NULL,
    span_id TEXT NOT NULL,
    name TEXT NOT NULL,
    timestamp_unix_nano INTEGER NOT NULL,
    attributes_json TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_span_events_trace ON span_events(trace_id, span_id);

CREATE TABLE IF NOT EXISTS span_links (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    trace_id TEXT NOT NULL,
    span_id TEXT NOT NULL,
    linked_trace_id TEXT NOT NULL,
    linked_span_id TEXT NOT NULL,
    trace_state TEXT NOT NULL,
    attributes_json TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_span_links_trace ON span_links(trace_id, span_id);

CREATE TABLE IF NOT EXISTS logs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    service_name TEXT NOT NULL,
    timestamp_unix_nano INTEGER NOT NULL,
    severity TEXT NOT NULL,
    body TEXT NOT NULL,
    trace_id TEXT NOT NULL,
    span_id TEXT NOT NULL,
    resource_attributes_json TEXT NOT NULL,
    attributes_json TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_logs_service_time ON logs(service_name, timestamp_unix_nano DESC);
CREATE INDEX IF NOT EXISTS idx_logs_trace ON logs(trace_id, span_id);

CREATE TABLE IF NOT EXISTS metrics (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    service_name TEXT NOT NULL,
    metric_name TEXT NOT NULL,
    instrument_kind TEXT NOT NULL,
    aggregation_temporality TEXT NOT NULL,
    timestamp_unix_nano INTEGER NOT NULL,
    value REAL,
    summary TEXT NOT NULL,
    resource_attributes_json TEXT NOT NULL,
    attributes_json TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_metrics_service_time ON metrics(service_name, timestamp_unix_nano DESC);
CREATE INDEX IF NOT EXISTS idx_metrics_name ON metrics(metric_name, timestamp_unix_nano DESC);

CREATE TABLE IF NOT EXISTS llm_spans (
    span_id TEXT PRIMARY KEY,
    trace_id TEXT NOT NULL,
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
    raw_json TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_llm_trace ON llm_spans(trace_id);
CREATE INDEX IF NOT EXISTS idx_llm_service ON llm_spans(service_name);
"#;

#[derive(Debug, Clone, Copy)]
struct ColumnDefinition {
    name: &'static str,
    data_type: &'static str,
    not_null: bool,
    primary_key: i64,
}

#[derive(Debug)]
struct TableDefinition {
    name: &'static str,
    columns: &'static [ColumnDefinition],
    autoincrement: bool,
    create_sql: &'static str,
}

#[derive(Debug)]
struct IndexDefinition {
    name: &'static str,
    table: &'static str,
    columns: &'static [IndexColumn],
}

#[derive(Debug)]
struct IndexColumn {
    name: &'static str,
    descending: bool,
    collation: &'static str,
}

const TABLES: &[TableDefinition] = &[
    TableDefinition {
        name: "spans",
        columns: &[
            column("trace_id", "TEXT", true, 0),
            column("span_id", "TEXT", false, 1),
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
        create_sql: r#"CREATE TABLE spans (
            trace_id TEXT NOT NULL,
            span_id TEXT PRIMARY KEY,
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
            llm_json TEXT
        )"#,
    },
    TableDefinition {
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
        create_sql: r#"CREATE TABLE span_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            trace_id TEXT NOT NULL,
            span_id TEXT NOT NULL,
            name TEXT NOT NULL,
            timestamp_unix_nano INTEGER NOT NULL,
            attributes_json TEXT NOT NULL
        )"#,
    },
    TableDefinition {
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
        create_sql: r#"CREATE TABLE span_links (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            trace_id TEXT NOT NULL,
            span_id TEXT NOT NULL,
            linked_trace_id TEXT NOT NULL,
            linked_span_id TEXT NOT NULL,
            trace_state TEXT NOT NULL,
            attributes_json TEXT NOT NULL
        )"#,
    },
    TableDefinition {
        name: "logs",
        columns: &[
            column("id", "INTEGER", false, 1),
            column("service_name", "TEXT", true, 0),
            column("timestamp_unix_nano", "INTEGER", true, 0),
            column("severity", "TEXT", true, 0),
            column("body", "TEXT", true, 0),
            column("trace_id", "TEXT", true, 0),
            column("span_id", "TEXT", true, 0),
            column("resource_attributes_json", "TEXT", true, 0),
            column("attributes_json", "TEXT", true, 0),
        ],
        autoincrement: true,
        create_sql: r#"CREATE TABLE logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            service_name TEXT NOT NULL,
            timestamp_unix_nano INTEGER NOT NULL,
            severity TEXT NOT NULL,
            body TEXT NOT NULL,
            trace_id TEXT NOT NULL,
            span_id TEXT NOT NULL,
            resource_attributes_json TEXT NOT NULL,
            attributes_json TEXT NOT NULL
        )"#,
    },
    TableDefinition {
        name: "metrics",
        columns: &[
            column("id", "INTEGER", false, 1),
            column("service_name", "TEXT", true, 0),
            column("metric_name", "TEXT", true, 0),
            column("instrument_kind", "TEXT", true, 0),
            column("aggregation_temporality", "TEXT", true, 0),
            column("timestamp_unix_nano", "INTEGER", true, 0),
            column("value", "REAL", false, 0),
            column("summary", "TEXT", true, 0),
            column("resource_attributes_json", "TEXT", true, 0),
            column("attributes_json", "TEXT", true, 0),
        ],
        autoincrement: true,
        create_sql: r#"CREATE TABLE metrics (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            service_name TEXT NOT NULL,
            metric_name TEXT NOT NULL,
            instrument_kind TEXT NOT NULL,
            aggregation_temporality TEXT NOT NULL,
            timestamp_unix_nano INTEGER NOT NULL,
            value REAL,
            summary TEXT NOT NULL,
            resource_attributes_json TEXT NOT NULL,
            attributes_json TEXT NOT NULL
        )"#,
    },
    TableDefinition {
        name: "llm_spans",
        columns: &[
            column("span_id", "TEXT", false, 1),
            column("trace_id", "TEXT", true, 0),
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
        create_sql: r#"CREATE TABLE llm_spans (
            span_id TEXT PRIMARY KEY,
            trace_id TEXT NOT NULL,
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
            raw_json TEXT NOT NULL
        )"#,
    },
];

const INDEXES: &[IndexDefinition] = &[
    index("idx_spans_trace", "spans", &[ascending("trace_id")]),
    index(
        "idx_spans_service_start",
        "spans",
        &[
            ascending("service_name"),
            descending("start_time_unix_nano"),
        ],
    ),
    index("idx_spans_status", "spans", &[ascending("status_code")]),
    index(
        "idx_span_events_trace",
        "span_events",
        &[ascending("trace_id"), ascending("span_id")],
    ),
    index(
        "idx_span_links_trace",
        "span_links",
        &[ascending("trace_id"), ascending("span_id")],
    ),
    index(
        "idx_logs_service_time",
        "logs",
        &[ascending("service_name"), descending("timestamp_unix_nano")],
    ),
    index(
        "idx_logs_trace",
        "logs",
        &[ascending("trace_id"), ascending("span_id")],
    ),
    index(
        "idx_metrics_service_time",
        "metrics",
        &[ascending("service_name"), descending("timestamp_unix_nano")],
    ),
    index(
        "idx_metrics_name",
        "metrics",
        &[ascending("metric_name"), descending("timestamp_unix_nano")],
    ),
    index("idx_llm_trace", "llm_spans", &[ascending("trace_id")]),
    index("idx_llm_service", "llm_spans", &[ascending("service_name")]),
];

const fn column(
    name: &'static str,
    data_type: &'static str,
    not_null: bool,
    primary_key: i64,
) -> ColumnDefinition {
    ColumnDefinition {
        name,
        data_type,
        not_null,
        primary_key,
    }
}

const fn index(
    name: &'static str,
    table: &'static str,
    columns: &'static [IndexColumn],
) -> IndexDefinition {
    IndexDefinition {
        name,
        table,
        columns,
    }
}

const fn ascending(name: &'static str) -> IndexColumn {
    IndexColumn {
        name,
        descending: false,
        collation: "BINARY",
    }
}

const fn descending(name: &'static str) -> IndexColumn {
    IndexColumn {
        name,
        descending: true,
        collation: "BINARY",
    }
}
