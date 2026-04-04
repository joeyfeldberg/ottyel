use std::{
    fs,
    path::Path,
    sync::{Arc, Mutex},
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result};
use opentelemetry_proto::tonic::{
    collector::{
        logs::v1::ExportLogsServiceRequest, metrics::v1::ExportMetricsServiceRequest,
        trace::v1::ExportTraceServiceRequest,
    },
    common::v1::{AnyValue, any_value},
    logs::v1::LogRecord,
    metrics::v1::{Metric, metric, number_data_point::Value as NumberValue},
    resource::v1::Resource,
    trace::v1::status::StatusCode,
};
use rusqlite::{Connection, params};
use serde_json::Value;

use crate::domain::{
    AttributeMap, LlmAttributes, SpanEventDetail, SpanLinkDetail, attributes_to_map,
    extract_llm_attributes, extract_service_name,
};
use crate::query::{LogCorrelationFilter, LogFilters, LogSeverityFilter};

#[derive(Debug, Clone)]
pub struct Store {
    conn: Arc<Mutex<Connection>>,
    retention_hours: u64,
    max_spans: usize,
}

#[derive(Debug, Clone)]
pub struct SpanRow {
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
    pub attributes_json: String,
    pub llm_json: Option<String>,
}

impl Store {
    pub fn open(path: &Path, retention_hours: u64, max_spans: usize) -> Result<Self> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("failed to create {}", parent.display()))?;
        }

        let conn = Connection::open(path)
            .with_context(|| format!("failed to open sqlite db {}", path.display()))?;
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

        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
            retention_hours,
            max_spans,
        })
    }

    pub fn ingest_traces(&self, request: ExportTraceServiceRequest) -> Result<usize> {
        let mut conn = self.conn.lock().expect("sqlite mutex poisoned");
        let tx = conn.transaction()?;
        let mut inserted = 0usize;

        for resource_spans in request.resource_spans {
            let resource_attrs = resource_to_map(resource_spans.resource.as_ref());
            let resource_json = serde_json::to_string(&resource_attrs)?;
            let service_name = extract_service_name(&resource_attrs);

            for scope_spans in resource_spans.scope_spans {
                for span in scope_spans.spans {
                    inserted += 1;
                    let attrs = attributes_to_map(&span.attributes);
                    let attributes_json = serde_json::to_string(&attrs)?;
                    let trace_id = hex_bytes(&span.trace_id);
                    let span_id = hex_bytes(&span.span_id);
                    let parent_span_id = hex_bytes(&span.parent_span_id);
                    let span_kind = span_kind_name(span.kind);
                    let status_code = status_code_name(span.status.as_ref());
                    let duration_ms = (span.end_time_unix_nano as f64
                        - span.start_time_unix_nano as f64)
                        / 1_000_000.0;
                    let llm = extract_llm_attributes(
                        &attrs,
                        Some(status_code.as_str()),
                        Some(duration_ms.max(0.0)),
                    );
                    let llm_json = llm
                        .as_ref()
                        .map(serde_json::to_string)
                        .transpose()
                        .map_err(|err| anyhow::anyhow!(err))?;

                    tx.execute(
                        r#"
                        INSERT INTO spans (
                            trace_id, span_id, parent_span_id, service_name, span_name,
                            span_kind, status_code, start_time_unix_nano, end_time_unix_nano,
                            duration_ms, resource_attributes_json, attributes_json, llm_json
                        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)
                        ON CONFLICT(span_id) DO UPDATE SET
                            trace_id = excluded.trace_id,
                            parent_span_id = excluded.parent_span_id,
                            service_name = excluded.service_name,
                            span_name = excluded.span_name,
                            span_kind = excluded.span_kind,
                            status_code = excluded.status_code,
                            start_time_unix_nano = excluded.start_time_unix_nano,
                            end_time_unix_nano = excluded.end_time_unix_nano,
                            duration_ms = excluded.duration_ms,
                            resource_attributes_json = excluded.resource_attributes_json,
                            attributes_json = excluded.attributes_json,
                            llm_json = excluded.llm_json
                        "#,
                        params![
                            trace_id,
                            span_id,
                            parent_span_id,
                            service_name,
                            span.name,
                            span_kind,
                            status_code,
                            span.start_time_unix_nano as i64,
                            span.end_time_unix_nano as i64,
                            duration_ms.max(0.0),
                            resource_json,
                            attributes_json,
                            llm_json,
                        ],
                    )?;

                    tx.execute(
                        "DELETE FROM span_events WHERE trace_id = ?1 AND span_id = ?2",
                        params![trace_id, span_id],
                    )?;
                    tx.execute(
                        "DELETE FROM span_links WHERE trace_id = ?1 AND span_id = ?2",
                        params![trace_id, span_id],
                    )?;

                    for event in span.events {
                        let event_attrs = attributes_to_map(&event.attributes);
                        tx.execute(
                            r#"
                            INSERT INTO span_events (
                                trace_id, span_id, name, timestamp_unix_nano, attributes_json
                            ) VALUES (?1, ?2, ?3, ?4, ?5)
                            "#,
                            params![
                                trace_id,
                                span_id,
                                event.name,
                                event.time_unix_nano as i64,
                                serde_json::to_string(&event_attrs)?,
                            ],
                        )?;
                    }

                    for link in span.links {
                        let link_attrs = attributes_to_map(&link.attributes);
                        tx.execute(
                            r#"
                            INSERT INTO span_links (
                                trace_id, span_id, linked_trace_id, linked_span_id, trace_state, attributes_json
                            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)
                            "#,
                            params![
                                trace_id,
                                span_id,
                                hex_bytes(&link.trace_id),
                                hex_bytes(&link.span_id),
                                link.trace_state,
                                serde_json::to_string(&link_attrs)?,
                            ],
                        )?;
                    }

                    if let Some(llm) = llm {
                        self.insert_llm_row(&tx, &trace_id, &span_id, &service_name, &llm)?;
                    }
                }
            }
        }

        tx.commit()?;
        drop(conn);
        self.enforce_retention()?;
        Ok(inserted)
    }

    pub fn ingest_logs(&self, request: ExportLogsServiceRequest) -> Result<usize> {
        let mut conn = self.conn.lock().expect("sqlite mutex poisoned");
        let tx = conn.transaction()?;
        let mut inserted = 0usize;

        for resource_logs in request.resource_logs {
            let resource_attrs = resource_to_map(resource_logs.resource.as_ref());
            let resource_json = serde_json::to_string(&resource_attrs)?;
            let service_name = extract_service_name(&resource_attrs);

            for scope_logs in resource_logs.scope_logs {
                for log in scope_logs.log_records {
                    inserted += 1;
                    tx.execute(
                        r#"
                        INSERT INTO logs (
                            service_name, timestamp_unix_nano, severity, body, trace_id, span_id,
                            resource_attributes_json, attributes_json
                        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
                        "#,
                        params![
                            service_name,
                            log_time_unix_nano(&log),
                            log.severity_text,
                            any_value_text(log.body.as_ref()),
                            hex_bytes(&log.trace_id),
                            hex_bytes(&log.span_id),
                            resource_json,
                            serde_json::to_string(&attributes_to_map(&log.attributes))?,
                        ],
                    )?;
                }
            }
        }

        tx.commit()?;
        drop(conn);
        self.enforce_retention()?;
        Ok(inserted)
    }

    pub fn ingest_metrics(&self, request: ExportMetricsServiceRequest) -> Result<usize> {
        let mut conn = self.conn.lock().expect("sqlite mutex poisoned");
        let tx = conn.transaction()?;
        let mut inserted = 0usize;

        for resource_metrics in request.resource_metrics {
            let resource_attrs = resource_to_map(resource_metrics.resource.as_ref());
            let resource_json = serde_json::to_string(&resource_attrs)?;
            let service_name = extract_service_name(&resource_attrs);

            for scope_metrics in resource_metrics.scope_metrics {
                for metric in scope_metrics.metrics {
                    inserted +=
                        self.insert_metric_rows(&tx, &service_name, &resource_json, metric)?;
                }
            }
        }

        tx.commit()?;
        drop(conn);
        self.enforce_retention()?;
        Ok(inserted)
    }

    pub fn services(&self, threshold_unix_nano: Option<i64>) -> Result<Vec<String>> {
        let conn = self.conn.lock().expect("sqlite mutex poisoned");
        let mut sql = String::from("SELECT service_name FROM (");
        sql.push_str("SELECT DISTINCT service_name FROM spans");
        if let Some(threshold) = threshold_unix_nano {
            sql.push_str(&format!(" WHERE end_time_unix_nano >= {threshold}"));
        }
        sql.push_str(" UNION SELECT DISTINCT service_name FROM logs");
        if let Some(threshold) = threshold_unix_nano {
            sql.push_str(&format!(" WHERE timestamp_unix_nano >= {threshold}"));
        }
        sql.push_str(" UNION SELECT DISTINCT service_name FROM metrics");
        if let Some(threshold) = threshold_unix_nano {
            sql.push_str(&format!(" WHERE timestamp_unix_nano >= {threshold}"));
        }
        sql.push_str(") WHERE service_name != '' ORDER BY service_name");
        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt
            .query_map([], |row| row.get(0))?
            .collect::<rusqlite::Result<Vec<String>>>()?;
        Ok(rows)
    }

    pub fn counts(
        &self,
        threshold_unix_nano: Option<i64>,
    ) -> Result<(usize, usize, usize, usize, usize)> {
        let conn = self.conn.lock().expect("sqlite mutex poisoned");
        let trace_count: usize = conn.query_row(
            &format!(
                "SELECT COUNT(DISTINCT trace_id) FROM spans{}",
                threshold_clause("end_time_unix_nano", threshold_unix_nano)
            ),
            [],
            |row| row.get(0),
        )?;
        let error_span_count: usize = conn.query_row(
            &format!(
                "SELECT COUNT(*) FROM spans WHERE status_code NOT IN ('STATUS_CODE_UNSET', 'STATUS_CODE_OK'){}",
                and_threshold_clause("end_time_unix_nano", threshold_unix_nano)
            ),
            [],
            |row| row.get(0),
        )?;
        let log_count: usize = conn.query_row(
            &format!(
                "SELECT COUNT(*) FROM logs{}",
                threshold_clause("timestamp_unix_nano", threshold_unix_nano)
            ),
            [],
            |row| row.get(0),
        )?;
        let metric_count: usize = conn.query_row(
            &format!(
                "SELECT COUNT(*) FROM metrics{}",
                threshold_clause("timestamp_unix_nano", threshold_unix_nano)
            ),
            [],
            |row| row.get(0),
        )?;
        let llm_count: usize = conn.query_row(
            &format!(
                "SELECT COUNT(*) FROM llm_spans INNER JOIN spans ON spans.span_id = llm_spans.span_id{}",
                threshold_clause("spans.end_time_unix_nano", threshold_unix_nano)
            ),
            [],
            |row| row.get(0),
        )?;
        Ok((
            trace_count,
            error_span_count,
            log_count,
            metric_count,
            llm_count,
        ))
    }

    pub fn recent_traces(
        &self,
        service_filter: Option<&str>,
        errors_only: bool,
        limit: usize,
        threshold_unix_nano: Option<i64>,
        search_query: Option<&str>,
    ) -> Result<Vec<crate::domain::TraceSummary>> {
        let conn = self.conn.lock().expect("sqlite mutex poisoned");
        let mut sql = r#"
            SELECT
                trace_id,
                MIN(service_name) AS service_name,
                COALESCE(MAX(CASE WHEN parent_span_id = '' THEN span_name END), MIN(span_name)) AS root_name,
                COUNT(*) AS span_count,
                SUM(CASE WHEN status_code NOT IN ('STATUS_CODE_UNSET', 'STATUS_CODE_OK') THEN 1 ELSE 0 END) AS error_count,
                MAX(end_time_unix_nano) - MIN(start_time_unix_nano) AS duration_nano,
                MIN(start_time_unix_nano) AS started_at
            FROM spans
        "#
        .to_string();

        let mut where_clauses = Vec::new();
        if let Some(service) = service_filter {
            where_clauses.push(format!("service_name = '{}'", escape_sql(service)));
        }
        if errors_only {
            where_clauses
                .push("status_code NOT IN ('STATUS_CODE_UNSET', 'STATUS_CODE_OK')".to_string());
        }
        if let Some(threshold) = threshold_unix_nano {
            where_clauses.push(format!("end_time_unix_nano >= {threshold}"));
        }
        if let Some(query) = search_query.filter(|query| !query.is_empty()) {
            let pattern = like_pattern(query);
            where_clauses.push(format!(
                "(trace_id LIKE '{pattern}' ESCAPE '\\' OR service_name LIKE '{pattern}' ESCAPE '\\' OR span_name LIKE '{pattern}' ESCAPE '\\' OR attributes_json LIKE '{pattern}' ESCAPE '\\' OR resource_attributes_json LIKE '{pattern}' ESCAPE '\\')"
            ));
        }
        if !where_clauses.is_empty() {
            sql.push_str(" WHERE ");
            sql.push_str(&where_clauses.join(" AND "));
        }
        sql.push_str(" GROUP BY trace_id ORDER BY started_at DESC LIMIT ");
        sql.push_str(&limit.to_string());

        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map([], |row| {
            Ok(crate::domain::TraceSummary {
                trace_id: row.get(0)?,
                service_name: row.get(1)?,
                root_name: row.get(2)?,
                span_count: row.get(3)?,
                error_count: row.get(4)?,
                duration_ms: row.get::<_, i64>(5)? as f64 / 1_000_000.0,
                started_at_unix_nano: row.get(6)?,
            })
        })?;
        Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
    }

    pub fn trace_detail(&self, trace_id: &str) -> Result<Vec<crate::domain::SpanDetail>> {
        let events_by_span = self.span_events_by_trace(trace_id)?;
        let links_by_span = self.span_links_by_trace(trace_id)?;
        let conn = self.conn.lock().expect("sqlite mutex poisoned");
        let mut stmt = conn.prepare(
            r#"
            SELECT
                trace_id, span_id, parent_span_id, service_name, span_name, span_kind,
                status_code, start_time_unix_nano, end_time_unix_nano, duration_ms,
                resource_attributes_json, attributes_json, llm_json
            FROM spans
            WHERE trace_id = ?1
            ORDER BY start_time_unix_nano ASC
            "#,
        )?;
        let rows = stmt.query_map([trace_id], |row| {
            let span_id: String = row.get(1)?;
            let resource_attributes_json: String = row.get(10)?;
            let attributes_json: String = row.get(11)?;
            let llm_json: Option<String> = row.get(12)?;
            Ok(crate::domain::SpanDetail {
                trace_id: row.get(0)?,
                span_id: span_id.clone(),
                parent_span_id: row.get(2)?,
                service_name: row.get(3)?,
                span_name: row.get(4)?,
                span_kind: row.get(5)?,
                status_code: row.get(6)?,
                start_time_unix_nano: row.get(7)?,
                end_time_unix_nano: row.get(8)?,
                duration_ms: row.get(9)?,
                resource_attributes: serde_json::from_str(&resource_attributes_json)
                    .unwrap_or_default(),
                attributes: serde_json::from_str(&attributes_json).unwrap_or_default(),
                events: events_by_span.get(&span_id).cloned().unwrap_or_default(),
                links: links_by_span.get(&span_id).cloned().unwrap_or_default(),
                llm: llm_json
                    .as_deref()
                    .map(serde_json::from_str)
                    .transpose()
                    .unwrap_or_default(),
            })
        })?;
        Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
    }

    pub fn recent_logs(
        &self,
        service_filter: Option<&str>,
        limit: usize,
        threshold_unix_nano: Option<i64>,
        search_query: Option<&str>,
        log_filters: &LogFilters,
    ) -> Result<Vec<crate::domain::LogSummary>> {
        let conn = self.conn.lock().expect("sqlite mutex poisoned");
        let mut sql = String::from(
            "SELECT service_name, timestamp_unix_nano, severity, body, trace_id, span_id, resource_attributes_json, attributes_json FROM logs",
        );
        let mut where_clauses = Vec::new();
        if let Some(service) = service_filter {
            where_clauses.push(format!("service_name = '{}'", escape_sql(service)));
        }
        if let Some(threshold) = threshold_unix_nano {
            where_clauses.push(format!("timestamp_unix_nano >= {threshold}"));
        }
        if let Some(query) = search_query.filter(|query| !query.is_empty()) {
            let pattern = like_pattern(query);
            where_clauses.push(format!(
                "(service_name LIKE '{pattern}' ESCAPE '\\' OR severity LIKE '{pattern}' ESCAPE '\\' OR body LIKE '{pattern}' ESCAPE '\\' OR trace_id LIKE '{pattern}' ESCAPE '\\' OR span_id LIKE '{pattern}' ESCAPE '\\' OR attributes_json LIKE '{pattern}' ESCAPE '\\' OR resource_attributes_json LIKE '{pattern}' ESCAPE '\\')"
            ));
        }
        if let Some(clause) = log_severity_clause(log_filters.severity) {
            where_clauses.push(clause);
        }
        if let Some(clause) = log_correlation_clause(log_filters.correlation) {
            where_clauses.push(clause);
        }
        if let Some(query) = log_filters
            .search_query
            .as_deref()
            .filter(|query| !query.is_empty())
        {
            let pattern = like_pattern(query);
            where_clauses.push(format!(
                "(body LIKE '{pattern}' ESCAPE '\\' OR severity LIKE '{pattern}' ESCAPE '\\' OR trace_id LIKE '{pattern}' ESCAPE '\\' OR span_id LIKE '{pattern}' ESCAPE '\\' OR attributes_json LIKE '{pattern}' ESCAPE '\\' OR resource_attributes_json LIKE '{pattern}' ESCAPE '\\')"
            ));
        }
        if !where_clauses.is_empty() {
            sql.push_str(" WHERE ");
            sql.push_str(&where_clauses.join(" AND "));
        }
        sql.push_str(" ORDER BY timestamp_unix_nano DESC LIMIT ");
        sql.push_str(&limit.to_string());

        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map([], |row| {
            let resource_attributes_json: String = row.get(6)?;
            let attributes_json: String = row.get(7)?;
            Ok(crate::domain::LogSummary {
                service_name: row.get(0)?,
                timestamp_unix_nano: row.get(1)?,
                severity: row.get(2)?,
                body: row.get(3)?,
                trace_id: row.get(4)?,
                span_id: row.get(5)?,
                resource_attributes: serde_json::from_str(&resource_attributes_json)
                    .unwrap_or_default(),
                attributes: serde_json::from_str(&attributes_json).unwrap_or_default(),
            })
        })?;
        Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
    }

    pub fn recent_metrics(
        &self,
        service_filter: Option<&str>,
        limit: usize,
        threshold_unix_nano: Option<i64>,
        search_query: Option<&str>,
    ) -> Result<Vec<crate::domain::MetricSummary>> {
        let conn = self.conn.lock().expect("sqlite mutex poisoned");
        let mut sql = String::from(
            r#"
            SELECT service_name, metric_name, instrument_kind, timestamp_unix_nano, value, summary
            FROM metrics
            "#,
        );
        let mut where_clauses = Vec::new();
        if let Some(service) = service_filter {
            where_clauses.push(format!("service_name = '{}'", escape_sql(service)));
        }
        if let Some(threshold) = threshold_unix_nano {
            where_clauses.push(format!("timestamp_unix_nano >= {threshold}"));
        }
        if let Some(query) = search_query.filter(|query| !query.is_empty()) {
            let pattern = like_pattern(query);
            where_clauses.push(format!(
                "(service_name LIKE '{pattern}' ESCAPE '\\' OR metric_name LIKE '{pattern}' ESCAPE '\\' OR instrument_kind LIKE '{pattern}' ESCAPE '\\' OR summary LIKE '{pattern}' ESCAPE '\\' OR attributes_json LIKE '{pattern}' ESCAPE '\\' OR resource_attributes_json LIKE '{pattern}' ESCAPE '\\')"
            ));
        }
        if !where_clauses.is_empty() {
            sql.push_str(" WHERE ");
            sql.push_str(&where_clauses.join(" AND "));
        }
        sql.push_str(" ORDER BY timestamp_unix_nano DESC LIMIT ");
        sql.push_str(&limit.to_string());

        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map([], |row| {
            Ok(crate::domain::MetricSummary {
                service_name: row.get(0)?,
                metric_name: row.get(1)?,
                instrument_kind: row.get(2)?,
                timestamp_unix_nano: row.get(3)?,
                value: row.get(4)?,
                summary: row.get(5)?,
            })
        })?;
        Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
    }

    pub fn recent_llm(
        &self,
        service_filter: Option<&str>,
        limit: usize,
        threshold_unix_nano: Option<i64>,
        search_query: Option<&str>,
    ) -> Result<Vec<crate::domain::LlmSummary>> {
        let conn = self.conn.lock().expect("sqlite mutex poisoned");
        let mut sql = String::from(
            r#"
            SELECT llm_spans.trace_id, llm_spans.span_id, llm_spans.service_name, provider, model, operation,
                   input_tokens, output_tokens, total_tokens, cost, latency_ms, status
            FROM llm_spans
            "#,
        );
        if threshold_unix_nano.is_some() {
            sql.push_str(" INNER JOIN spans ON spans.span_id = llm_spans.span_id");
        }
        let mut where_clauses = Vec::new();
        if let Some(service) = service_filter {
            where_clauses.push(format!(
                "llm_spans.service_name = '{}'",
                escape_sql(service)
            ));
        }
        if let Some(threshold) = threshold_unix_nano {
            where_clauses.push(format!("spans.end_time_unix_nano >= {threshold}"));
        }
        if let Some(query) = search_query.filter(|query| !query.is_empty()) {
            let pattern = like_pattern(query);
            where_clauses.push(format!(
                "(llm_spans.trace_id LIKE '{pattern}' ESCAPE '\\' OR llm_spans.span_id LIKE '{pattern}' ESCAPE '\\' OR llm_spans.service_name LIKE '{pattern}' ESCAPE '\\' OR provider LIKE '{pattern}' ESCAPE '\\' OR model LIKE '{pattern}' ESCAPE '\\' OR operation LIKE '{pattern}' ESCAPE '\\' OR raw_json LIKE '{pattern}' ESCAPE '\\')"
            ));
        }
        if !where_clauses.is_empty() {
            sql.push_str(" WHERE ");
            sql.push_str(&where_clauses.join(" AND "));
        }
        sql.push_str(" ORDER BY llm_spans.latency_ms DESC, llm_spans.trace_id DESC LIMIT ");
        sql.push_str(&limit.to_string());

        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map([], |row| {
            Ok(crate::domain::LlmSummary {
                trace_id: row.get(0)?,
                span_id: row.get(1)?,
                service_name: row.get(2)?,
                provider: row.get(3)?,
                model: row.get(4)?,
                operation: row.get(5)?,
                input_tokens: row.get(6)?,
                output_tokens: row.get(7)?,
                total_tokens: row.get(8)?,
                cost: row.get(9)?,
                latency_ms: row.get(10)?,
                status: row.get(11)?,
            })
        })?;
        Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
    }

    fn insert_llm_row(
        &self,
        tx: &rusqlite::Transaction<'_>,
        trace_id: &str,
        span_id: &str,
        service_name: &str,
        llm: &LlmAttributes,
    ) -> Result<()> {
        tx.execute(
            r#"
            INSERT INTO llm_spans (
                span_id, trace_id, service_name, provider, model, operation,
                input_tokens, output_tokens, total_tokens, cost, latency_ms, status, raw_json
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)
            ON CONFLICT(span_id) DO UPDATE SET
                trace_id = excluded.trace_id,
                service_name = excluded.service_name,
                provider = excluded.provider,
                model = excluded.model,
                operation = excluded.operation,
                input_tokens = excluded.input_tokens,
                output_tokens = excluded.output_tokens,
                total_tokens = excluded.total_tokens,
                cost = excluded.cost,
                latency_ms = excluded.latency_ms,
                status = excluded.status,
                raw_json = excluded.raw_json
            "#,
            params![
                span_id,
                trace_id,
                service_name,
                llm.provider
                    .clone()
                    .unwrap_or_else(|| "unknown".to_string()),
                llm.model.clone().unwrap_or_else(|| "unknown".to_string()),
                llm.operation.clone().unwrap_or_else(|| "llm".to_string()),
                llm.input_tokens.map(|value| value as i64),
                llm.output_tokens.map(|value| value as i64),
                llm.total_tokens.map(|value| value as i64),
                llm.cost,
                llm.latency_ms,
                llm.status.clone().unwrap_or_else(|| "unknown".to_string()),
                serde_json::to_string(llm)?,
            ],
        )?;
        Ok(())
    }

    fn insert_metric_rows(
        &self,
        tx: &rusqlite::Transaction<'_>,
        service_name: &str,
        resource_json: &str,
        metric: Metric,
    ) -> Result<usize> {
        let mut inserted = 0usize;
        let metric_name = metric.name;
        match metric.data {
            Some(metric::Data::Gauge(gauge)) => {
                for point in gauge.data_points {
                    inserted += 1;
                    tx.execute(
                        r#"
                        INSERT INTO metrics (
                            service_name, metric_name, instrument_kind, aggregation_temporality,
                            timestamp_unix_nano, value, summary, resource_attributes_json, attributes_json
                        ) VALUES (?1, ?2, 'gauge', 'unspecified', ?3, ?4, ?5, ?6, ?7)
                        "#,
                        params![
                            service_name,
                            metric_name,
                            point.time_unix_nano as i64,
                            number_value(point.value.as_ref()),
                            format_metric_summary("gauge", number_value(point.value.as_ref())),
                            resource_json,
                            serde_json::to_string(&attributes_to_map(&point.attributes))?,
                        ],
                    )?;
                }
            }
            Some(metric::Data::Sum(sum)) => {
                for point in sum.data_points {
                    inserted += 1;
                    tx.execute(
                        r#"
                        INSERT INTO metrics (
                            service_name, metric_name, instrument_kind, aggregation_temporality,
                            timestamp_unix_nano, value, summary, resource_attributes_json, attributes_json
                        ) VALUES (?1, ?2, 'sum', ?3, ?4, ?5, ?6, ?7, ?8)
                        "#,
                        params![
                            service_name,
                            metric_name,
                            format!("{:?}", sum.aggregation_temporality),
                            point.time_unix_nano as i64,
                            number_value(point.value.as_ref()),
                            format_metric_summary("sum", number_value(point.value.as_ref())),
                            resource_json,
                            serde_json::to_string(&attributes_to_map(&point.attributes))?,
                        ],
                    )?;
                }
            }
            Some(metric::Data::Histogram(histogram)) => {
                for point in histogram.data_points {
                    inserted += 1;
                    tx.execute(
                        r#"
                        INSERT INTO metrics (
                            service_name, metric_name, instrument_kind, aggregation_temporality,
                            timestamp_unix_nano, value, summary, resource_attributes_json, attributes_json
                        ) VALUES (?1, ?2, 'histogram', ?3, ?4, ?5, ?6, ?7, ?8)
                        "#,
                        params![
                            service_name,
                            metric_name,
                            format!("{:?}", histogram.aggregation_temporality),
                            point.time_unix_nano as i64,
                            point.sum,
                            format!("count={} sum={:?}", point.count, point.sum),
                            resource_json,
                            serde_json::to_string(&attributes_to_map(&point.attributes))?,
                        ],
                    )?;
                }
            }
            Some(metric::Data::Summary(summary)) => {
                for point in summary.data_points {
                    inserted += 1;
                    tx.execute(
                        r#"
                        INSERT INTO metrics (
                            service_name, metric_name, instrument_kind, aggregation_temporality,
                            timestamp_unix_nano, value, summary, resource_attributes_json, attributes_json
                        ) VALUES (?1, ?2, 'summary', 'summary', ?3, ?4, ?5, ?6, ?7)
                        "#,
                        params![
                            service_name,
                            metric_name,
                            point.time_unix_nano as i64,
                            point.sum,
                            format!("count={} sum={}", point.count, point.sum),
                            resource_json,
                            serde_json::to_string(&attributes_to_map(&point.attributes))?,
                        ],
                    )?;
                }
            }
            Some(metric::Data::ExponentialHistogram(histogram)) => {
                for point in histogram.data_points {
                    inserted += 1;
                    tx.execute(
                        r#"
                        INSERT INTO metrics (
                            service_name, metric_name, instrument_kind, aggregation_temporality,
                            timestamp_unix_nano, value, summary, resource_attributes_json, attributes_json
                        ) VALUES (?1, ?2, 'exp_histogram', ?3, ?4, ?5, ?6, ?7, ?8)
                        "#,
                        params![
                            service_name,
                            metric_name,
                            format!("{:?}", histogram.aggregation_temporality),
                            point.time_unix_nano as i64,
                            point.sum,
                            format!("count={} sum={:?}", point.count, point.sum),
                            resource_json,
                            serde_json::to_string(&attributes_to_map(&point.attributes))?,
                        ],
                    )?;
                }
            }
            None => {}
        }
        Ok(inserted)
    }

    fn span_events_by_trace(
        &self,
        trace_id: &str,
    ) -> Result<std::collections::HashMap<String, Vec<SpanEventDetail>>> {
        let conn = self.conn.lock().expect("sqlite mutex poisoned");
        let mut stmt = conn.prepare(
            r#"
            SELECT span_id, name, timestamp_unix_nano, attributes_json
            FROM span_events
            WHERE trace_id = ?1
            ORDER BY timestamp_unix_nano ASC
            "#,
        )?;
        let rows = stmt.query_map([trace_id], |row| {
            let attributes_json: String = row.get(3)?;
            Ok((
                row.get::<_, String>(0)?,
                SpanEventDetail {
                    name: row.get(1)?,
                    timestamp_unix_nano: row.get(2)?,
                    attributes: serde_json::from_str(&attributes_json).unwrap_or_default(),
                },
            ))
        })?;

        let mut by_span = std::collections::HashMap::new();
        for row in rows {
            let (span_id, event) = row?;
            by_span.entry(span_id).or_insert_with(Vec::new).push(event);
        }

        Ok(by_span)
    }

    fn span_links_by_trace(
        &self,
        trace_id: &str,
    ) -> Result<std::collections::HashMap<String, Vec<SpanLinkDetail>>> {
        let conn = self.conn.lock().expect("sqlite mutex poisoned");
        let mut stmt = conn.prepare(
            r#"
            SELECT span_id, linked_trace_id, linked_span_id, trace_state, attributes_json
            FROM span_links
            WHERE trace_id = ?1
            ORDER BY id ASC
            "#,
        )?;
        let rows = stmt.query_map([trace_id], |row| {
            let attributes_json: String = row.get(4)?;
            Ok((
                row.get::<_, String>(0)?,
                SpanLinkDetail {
                    trace_id: row.get(1)?,
                    span_id: row.get(2)?,
                    trace_state: row.get(3)?,
                    attributes: serde_json::from_str(&attributes_json).unwrap_or_default(),
                },
            ))
        })?;

        let mut by_span = std::collections::HashMap::new();
        for row in rows {
            let (span_id, link) = row?;
            by_span.entry(span_id).or_insert_with(Vec::new).push(link);
        }

        Ok(by_span)
    }

    fn enforce_retention(&self) -> Result<()> {
        let threshold_nanos =
            now_unix_nanos().saturating_sub(self.retention_hours as i64 * 60 * 60 * 1_000_000_000);

        let conn = self.conn.lock().expect("sqlite mutex poisoned");
        conn.execute(
            "DELETE FROM logs WHERE timestamp_unix_nano < ?1",
            [threshold_nanos],
        )?;
        conn.execute(
            "DELETE FROM metrics WHERE timestamp_unix_nano < ?1",
            [threshold_nanos],
        )?;
        conn.execute(
            "DELETE FROM spans WHERE end_time_unix_nano < ?1",
            [threshold_nanos],
        )?;
        conn.execute(
            "DELETE FROM span_events WHERE timestamp_unix_nano < ?1",
            [threshold_nanos],
        )?;
        conn.execute(
            "DELETE FROM span_links WHERE span_id NOT IN (SELECT span_id FROM spans)",
            [],
        )?;
        conn.execute(
            "DELETE FROM llm_spans WHERE span_id NOT IN (SELECT span_id FROM spans)",
            [],
        )?;

        let span_count: i64 = conn.query_row("SELECT COUNT(*) FROM spans", [], |row| row.get(0))?;
        if span_count > self.max_spans as i64 {
            let to_trim = span_count - self.max_spans as i64;
            conn.execute(
                r#"
                DELETE FROM spans
                WHERE span_id IN (
                    SELECT span_id FROM spans ORDER BY start_time_unix_nano ASC LIMIT ?1
                )
                "#,
                [to_trim],
            )?;
            conn.execute(
                "DELETE FROM span_links WHERE span_id NOT IN (SELECT span_id FROM spans)",
                [],
            )?;
            conn.execute(
                "DELETE FROM llm_spans WHERE span_id NOT IN (SELECT span_id FROM spans)",
                [],
            )?;
        }
        Ok(())
    }
}

fn resource_to_map(resource: Option<&Resource>) -> AttributeMap {
    resource
        .map(|resource| attributes_to_map(&resource.attributes))
        .unwrap_or_default()
}

fn span_kind_name(kind: i32) -> String {
    match kind {
        1 => "INTERNAL",
        2 => "SERVER",
        3 => "CLIENT",
        4 => "PRODUCER",
        5 => "CONSUMER",
        _ => "UNSPECIFIED",
    }
    .to_string()
}

fn status_code_name(status: Option<&opentelemetry_proto::tonic::trace::v1::Status>) -> String {
    match status.map(|status| status.code) {
        Some(code) if code == StatusCode::Ok as i32 => "STATUS_CODE_OK",
        Some(code) if code == StatusCode::Error as i32 => "STATUS_CODE_ERROR",
        _ => "STATUS_CODE_UNSET",
    }
    .to_string()
}

fn log_time_unix_nano(log: &LogRecord) -> i64 {
    let observed = log.observed_time_unix_nano as i64;
    let timestamp = log.time_unix_nano as i64;
    timestamp.max(observed)
}

fn number_value(value: Option<&NumberValue>) -> Option<f64> {
    match value {
        Some(NumberValue::AsDouble(number)) => Some(*number),
        Some(NumberValue::AsInt(number)) => Some(*number as f64),
        None => None,
    }
}

fn format_metric_summary(kind: &str, value: Option<f64>) -> String {
    match value {
        Some(number) => format!("{kind}={number:.3}"),
        None => kind.to_string(),
    }
}

fn hex_bytes(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn any_value_text(value: Option<&AnyValue>) -> String {
    match value.and_then(|inner| inner.value.as_ref()) {
        Some(any_value::Value::StringValue(text)) => text.clone(),
        Some(any_value::Value::BoolValue(value)) => value.to_string(),
        Some(any_value::Value::IntValue(value)) => value.to_string(),
        Some(any_value::Value::DoubleValue(value)) => value.to_string(),
        Some(any_value::Value::BytesValue(value)) => hex_bytes(value),
        Some(any_value::Value::KvlistValue(value)) => serde_json::to_string(
            &value
                .values
                .iter()
                .map(|item| {
                    (
                        item.key.clone(),
                        crate::domain::any_value_to_json(item.value.as_ref()),
                    )
                })
                .collect::<serde_json::Map<String, Value>>(),
        )
        .unwrap_or_default(),
        Some(any_value::Value::ArrayValue(value)) => serde_json::to_string(
            &value
                .values
                .iter()
                .map(|item| crate::domain::any_value_to_json(Some(item)))
                .collect::<Vec<Value>>(),
        )
        .unwrap_or_default(),
        None => String::new(),
    }
}

fn now_unix_nanos() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as i64
}

fn escape_sql(value: &str) -> String {
    value.replace('\'', "''")
}

fn threshold_clause(column: &str, threshold_unix_nano: Option<i64>) -> String {
    threshold_unix_nano
        .map(|threshold| format!(" WHERE {column} >= {threshold}"))
        .unwrap_or_default()
}

fn and_threshold_clause(column: &str, threshold_unix_nano: Option<i64>) -> String {
    threshold_unix_nano
        .map(|threshold| format!(" AND {column} >= {threshold}"))
        .unwrap_or_default()
}

fn like_pattern(value: &str) -> String {
    format!("%{}%", escape_like(value))
}

fn escape_like(value: &str) -> String {
    escape_sql(value)
        .replace('\\', "\\\\")
        .replace('%', "\\%")
        .replace('_', "\\_")
}

fn log_severity_clause(filter: LogSeverityFilter) -> Option<String> {
    match filter {
        LogSeverityFilter::All => None,
        LogSeverityFilter::Error => {
            Some("(UPPER(severity) LIKE 'ERROR%' OR UPPER(severity) LIKE 'FATAL%')".to_string())
        }
        LogSeverityFilter::Warn => Some("UPPER(severity) LIKE 'WARN%'".to_string()),
        LogSeverityFilter::Info => Some("UPPER(severity) LIKE 'INFO%'".to_string()),
        LogSeverityFilter::Debug => {
            Some("(UPPER(severity) LIKE 'DEBUG%' OR UPPER(severity) LIKE 'TRACE%')".to_string())
        }
    }
}

fn log_correlation_clause(filter: LogCorrelationFilter) -> Option<String> {
    match filter {
        LogCorrelationFilter::All => None,
        LogCorrelationFilter::TraceLinked => Some("trace_id != ''".to_string()),
        LogCorrelationFilter::SpanLinked => Some("span_id != ''".to_string()),
        LogCorrelationFilter::Uncorrelated => Some("trace_id = '' AND span_id = ''".to_string()),
    }
}

#[cfg(test)]
mod tests {
    use std::time::{SystemTime, UNIX_EPOCH};

    use opentelemetry_proto::tonic::{
        collector::{
            logs::v1::ExportLogsServiceRequest, metrics::v1::ExportMetricsServiceRequest,
            trace::v1::ExportTraceServiceRequest,
        },
        common::v1::{AnyValue, InstrumentationScope, KeyValue, any_value},
        logs::v1::{LogRecord, ResourceLogs, ScopeLogs},
        metrics::v1::{
            Gauge, Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics, metric,
            number_data_point,
        },
        resource::v1::Resource,
        trace::v1::{ResourceSpans, ScopeSpans, Span, Status, span, span::Event, span::Link},
    };
    use tempfile::tempdir;

    use super::Store;
    use crate::query::{LogCorrelationFilter, LogFilters, LogSeverityFilter};

    #[test]
    fn store_ingests_all_three_signals() {
        let tempdir = tempdir().unwrap();
        let store = Store::open(&tempdir.path().join("ottyel.db"), 24, 1000).unwrap();
        let now = now_nanos();

        store.ingest_traces(trace_request(now)).unwrap();
        store.ingest_logs(log_request(now)).unwrap();
        store.ingest_metrics(metric_request(now)).unwrap();

        let (trace_count, _error_spans, log_count, metric_count, llm_count) =
            store.counts(None).unwrap();
        assert_eq!(trace_count, 1);
        assert_eq!(log_count, 3);
        assert_eq!(metric_count, 1);
        assert_eq!(llm_count, 1);

        let traces = store.recent_traces(None, false, 10, None, None).unwrap();
        assert_eq!(traces[0].trace_id, "0102030405060708090a0b0c0d0e0f10");
        let detail = store
            .trace_detail("0102030405060708090a0b0c0d0e0f10")
            .unwrap();
        assert_eq!(detail[0].events.len(), 1);
        assert_eq!(detail[0].events[0].name, "model.invoke");
        assert_eq!(detail[0].links.len(), 1);
        assert_eq!(detail[0].links[0].span_id, "0909090909090909");
        let llm = store.recent_llm(None, 10, None, None).unwrap();
        assert_eq!(llm[0].model, "gpt-5.4");
        assert_eq!(
            store
                .recent_traces(None, false, 10, None, Some("input.value"))
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            store
                .recent_logs(
                    None,
                    10,
                    None,
                    Some("completion finished"),
                    &LogFilters::default(),
                )
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            store
                .recent_metrics(None, 10, None, Some("tokens.total"))
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            store
                .recent_llm(None, 10, None, Some("gpt-5.4"))
                .unwrap()
                .len(),
            1
        );

        let threshold_after_trace = now + 2_050_000;
        assert!(
            store
                .recent_traces(None, false, 10, Some(threshold_after_trace), None)
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            store
                .recent_logs(
                    None,
                    10,
                    Some(threshold_after_trace),
                    None,
                    &LogFilters::default(),
                )
                .unwrap()
                .len(),
            0
        );
        assert_eq!(
            store
                .recent_metrics(None, 10, Some(threshold_after_trace), None)
                .unwrap()
                .len(),
            1
        );
        assert!(
            store
                .recent_llm(None, 10, Some(threshold_after_trace), None)
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn recent_logs_apply_severity_correlation_and_text_filters() {
        let tempdir = tempdir().unwrap();
        let store = Store::open(&tempdir.path().join("ottyel.db"), 24, 1000).unwrap();
        let now = now_nanos();

        store.ingest_logs(log_request(now)).unwrap();

        let error_logs = store
            .recent_logs(
                None,
                10,
                None,
                None,
                &LogFilters {
                    severity: LogSeverityFilter::Error,
                    ..LogFilters::default()
                },
            )
            .unwrap();
        assert_eq!(error_logs.len(), 1);
        assert_eq!(error_logs[0].severity, "ERROR");

        let span_linked = store
            .recent_logs(
                None,
                10,
                None,
                None,
                &LogFilters {
                    correlation: LogCorrelationFilter::SpanLinked,
                    ..LogFilters::default()
                },
            )
            .unwrap();
        assert_eq!(span_linked.len(), 1);
        assert!(!span_linked[0].span_id.is_empty());

        let uncorrelated = store
            .recent_logs(
                None,
                10,
                None,
                None,
                &LogFilters {
                    correlation: LogCorrelationFilter::Uncorrelated,
                    ..LogFilters::default()
                },
            )
            .unwrap();
        assert_eq!(uncorrelated.len(), 1);
        assert!(uncorrelated[0].trace_id.is_empty());

        let pane_text = store
            .recent_logs(
                None,
                10,
                None,
                None,
                &LogFilters {
                    search_query: Some("validation".to_string()),
                    ..LogFilters::default()
                },
            )
            .unwrap();
        assert_eq!(pane_text.len(), 1);
        assert!(pane_text[0].body.contains("validation"));
    }

    fn trace_request(now: i64) -> ExportTraceServiceRequest {
        let now = now as u64;
        ExportTraceServiceRequest {
            resource_spans: vec![ResourceSpans {
                resource: Some(Resource {
                    attributes: vec![KeyValue {
                        key: "service.name".to_string(),
                        value: Some(AnyValue {
                            value: Some(any_value::Value::StringValue("api".to_string())),
                        }),
                    }],
                    dropped_attributes_count: 0,
                    entity_refs: Vec::new(),
                }),
                schema_url: String::new(),
                scope_spans: vec![ScopeSpans {
                    scope: Some(InstrumentationScope::default()),
                    schema_url: String::new(),
                    spans: vec![Span {
                        trace_id: vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16],
                        span_id: vec![1, 2, 3, 4, 5, 6, 7, 8],
                        parent_span_id: vec![],
                        trace_state: String::new(),
                        name: "chat.completion".to_string(),
                        kind: span::SpanKind::Server as i32,
                        start_time_unix_nano: now,
                        end_time_unix_nano: now + 2_000_000,
                        attributes: vec![
                            string_attr("llm.provider", "openai"),
                            string_attr("llm.model_name", "gpt-5.4"),
                            string_attr("input.value", "hello"),
                            string_attr("output.value", "world"),
                            int_attr("llm.token_count.prompt", 5),
                            int_attr("llm.token_count.completion", 7),
                        ],
                        dropped_attributes_count: 0,
                        events: vec![Event {
                            time_unix_nano: now + 1_000_000,
                            name: "model.invoke".to_string(),
                            attributes: vec![string_attr("event.phase", "request")],
                            dropped_attributes_count: 0,
                        }],
                        dropped_events_count: 0,
                        links: vec![Link {
                            trace_id: vec![7; 16],
                            span_id: vec![9; 8],
                            trace_state: "linked=true".to_string(),
                            attributes: vec![string_attr("link.kind", "retry")],
                            dropped_attributes_count: 0,
                            flags: 0,
                        }],
                        dropped_links_count: 0,
                        status: Some(Status {
                            message: String::new(),
                            code: 1,
                        }),
                        flags: 0,
                    }],
                }],
            }],
        }
    }

    fn log_request(now: i64) -> ExportLogsServiceRequest {
        let now = now as u64;
        ExportLogsServiceRequest {
            resource_logs: vec![ResourceLogs {
                resource: Some(Resource {
                    attributes: vec![string_attr("service.name", "api")],
                    dropped_attributes_count: 0,
                    entity_refs: Vec::new(),
                }),
                schema_url: String::new(),
                scope_logs: vec![ScopeLogs {
                    scope: Some(InstrumentationScope::default()),
                    schema_url: String::new(),
                    log_records: vec![
                        LogRecord {
                            time_unix_nano: now + 2_000_000,
                            observed_time_unix_nano: now + 2_000_100,
                            severity_number: 0,
                            severity_text: "INFO".to_string(),
                            body: Some(AnyValue {
                                value: Some(any_value::Value::StringValue(
                                    "completion finished".to_string(),
                                )),
                            }),
                            attributes: vec![string_attr("phase", "completion")],
                            dropped_attributes_count: 0,
                            flags: 0,
                            trace_id: vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16],
                            span_id: vec![1, 2, 3, 4, 5, 6, 7, 8],
                            event_name: String::new(),
                        },
                        LogRecord {
                            time_unix_nano: now + 2_000_200,
                            observed_time_unix_nano: now + 2_000_250,
                            severity_number: 0,
                            severity_text: "ERROR".to_string(),
                            body: Some(AnyValue {
                                value: Some(any_value::Value::StringValue(
                                    "validation failed".to_string(),
                                )),
                            }),
                            attributes: vec![string_attr("error.type", "validation")],
                            dropped_attributes_count: 0,
                            flags: 0,
                            trace_id: vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16],
                            span_id: vec![],
                            event_name: String::new(),
                        },
                        LogRecord {
                            time_unix_nano: now + 2_000_300,
                            observed_time_unix_nano: now + 2_000_350,
                            severity_number: 0,
                            severity_text: "DEBUG".to_string(),
                            body: Some(AnyValue {
                                value: Some(any_value::Value::StringValue(
                                    "{\"message\":\"cache warm\",\"hit\":true}".to_string(),
                                )),
                            }),
                            attributes: vec![string_attr("cache.layer", "memory")],
                            dropped_attributes_count: 0,
                            flags: 0,
                            trace_id: vec![],
                            span_id: vec![],
                            event_name: String::new(),
                        },
                    ],
                }],
            }],
        }
    }

    fn metric_request(now: i64) -> ExportMetricsServiceRequest {
        let now = now as u64;
        ExportMetricsServiceRequest {
            resource_metrics: vec![ResourceMetrics {
                resource: Some(Resource {
                    attributes: vec![string_attr("service.name", "api")],
                    dropped_attributes_count: 0,
                    entity_refs: Vec::new(),
                }),
                schema_url: String::new(),
                scope_metrics: vec![ScopeMetrics {
                    scope: Some(InstrumentationScope::default()),
                    schema_url: String::new(),
                    metrics: vec![Metric {
                        name: "tokens.total".to_string(),
                        description: String::new(),
                        unit: "1".to_string(),
                        metadata: vec![],
                        data: Some(metric::Data::Gauge(Gauge {
                            data_points: vec![NumberDataPoint {
                                attributes: vec![],
                                start_time_unix_nano: 0,
                                time_unix_nano: now + 2_500_000,
                                exemplars: vec![],
                                flags: 0,
                                value: Some(number_data_point::Value::AsInt(12)),
                            }],
                        })),
                    }],
                }],
            }],
        }
    }

    fn string_attr(key: &str, value: &str) -> KeyValue {
        KeyValue {
            key: key.to_string(),
            value: Some(AnyValue {
                value: Some(any_value::Value::StringValue(value.to_string())),
            }),
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

    fn now_nanos() -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos() as i64
    }
}
