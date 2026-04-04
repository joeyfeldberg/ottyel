use anyhow::Result;
use rusqlite::Connection;

pub(super) fn initialize(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        r#"
        PRAGMA journal_mode = WAL;
        PRAGMA synchronous = NORMAL;
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
        "#,
    )?;

    Ok(())
}
