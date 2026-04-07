use std::collections::{HashMap, HashSet};

use anyhow::Result;

use crate::{
    domain::{
        LlmAttributes, LlmRollup, LlmRollupDimension, LlmSessionSummary, LlmSummary,
        LlmTimelineItem, LogSummary, MetricSummary, SpanDetail, SpanEventDetail, SpanLinkDetail,
        TraceSummary, project_llm_timeline,
    },
    query::{LlmCursor, LogCursor, LogFilters, MetricCursor, Page, PageRequest, TraceCursor},
};

use super::{
    Store,
    helpers::{
        and_threshold_clause, escape_sql, like_pattern, log_correlation_clause,
        log_severity_clause, threshold_clause,
    },
};

impl Store {
    pub fn recent_traces_page(
        &self,
        service_filter: Option<&str>,
        errors_only: bool,
        page: &PageRequest<TraceCursor>,
        threshold_unix_nano: Option<i64>,
        search_query: Option<&str>,
    ) -> Result<Page<TraceSummary, TraceCursor>> {
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
        sql.push_str(" GROUP BY trace_id");
        if let Some(cursor) = &page.cursor {
            sql.push_str(&format!(
                " HAVING (MIN(start_time_unix_nano) < {started}) OR (MIN(start_time_unix_nano) = {started} AND trace_id < '{trace_id}')",
                started = cursor.started_at_unix_nano,
                trace_id = escape_sql(&cursor.trace_id),
            ));
        }
        sql.push_str(" ORDER BY started_at DESC, trace_id DESC LIMIT ");
        sql.push_str(&page.limit.saturating_add(1).to_string());

        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map([], |row| {
            Ok(TraceSummary {
                trace_id: row.get(0)?,
                service_name: row.get(1)?,
                root_name: row.get(2)?,
                span_count: row.get(3)?,
                error_count: row.get(4)?,
                duration_ms: row.get::<_, i64>(5)? as f64 / 1_000_000.0,
                started_at_unix_nano: row.get(6)?,
            })
        })?;
        let items = rows.collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(finish_page(items, page.limit, |item| TraceCursor {
            started_at_unix_nano: item.started_at_unix_nano,
            trace_id: item.trace_id.clone(),
        }))
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
    ) -> Result<Vec<TraceSummary>> {
        Ok(self
            .recent_traces_page(
                service_filter,
                errors_only,
                &PageRequest::first(limit),
                threshold_unix_nano,
                search_query,
            )?
            .items)
    }

    pub fn trace_detail(&self, trace_id: &str) -> Result<Vec<SpanDetail>> {
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
            Ok(SpanDetail {
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
    ) -> Result<Vec<LogSummary>> {
        Ok(self
            .recent_logs_page(
                service_filter,
                &PageRequest::first(limit),
                threshold_unix_nano,
                search_query,
                log_filters,
            )?
            .items)
    }

    pub fn recent_logs_page(
        &self,
        service_filter: Option<&str>,
        page: &PageRequest<LogCursor>,
        threshold_unix_nano: Option<i64>,
        search_query: Option<&str>,
        log_filters: &LogFilters,
    ) -> Result<Page<LogSummary, LogCursor>> {
        let conn = self.conn.lock().expect("sqlite mutex poisoned");
        let mut sql = String::from(
            "SELECT id, service_name, timestamp_unix_nano, severity, body, trace_id, span_id, resource_attributes_json, attributes_json FROM logs",
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
        if let Some(cursor) = &page.cursor {
            where_clauses.push(format!(
                "(timestamp_unix_nano < {timestamp} OR (timestamp_unix_nano = {timestamp} AND id < {row_id}))",
                timestamp = cursor.timestamp_unix_nano,
                row_id = cursor.row_id,
            ));
        }
        if !where_clauses.is_empty() {
            sql.push_str(" WHERE ");
            sql.push_str(&where_clauses.join(" AND "));
        }
        sql.push_str(" ORDER BY timestamp_unix_nano DESC, id DESC LIMIT ");
        sql.push_str(&page.limit.saturating_add(1).to_string());

        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map([], |row| {
            let resource_attributes_json: String = row.get(7)?;
            let attributes_json: String = row.get(8)?;
            Ok((
                row.get::<_, i64>(0)?,
                LogSummary {
                    service_name: row.get(1)?,
                    timestamp_unix_nano: row.get(2)?,
                    severity: row.get(3)?,
                    body: row.get(4)?,
                    trace_id: row.get(5)?,
                    span_id: row.get(6)?,
                    resource_attributes: serde_json::from_str(&resource_attributes_json)
                        .unwrap_or_default(),
                    attributes: serde_json::from_str(&attributes_json).unwrap_or_default(),
                },
            ))
        })?;
        let items = rows.collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(finish_page(items, page.limit, |(row_id, item)| LogCursor {
            timestamp_unix_nano: item.timestamp_unix_nano,
            row_id: *row_id,
        })
        .map_items(|(_, item)| item))
    }

    pub fn recent_metrics(
        &self,
        service_filter: Option<&str>,
        limit: usize,
        threshold_unix_nano: Option<i64>,
        search_query: Option<&str>,
    ) -> Result<Vec<MetricSummary>> {
        Ok(self
            .recent_metrics_page(
                service_filter,
                &PageRequest::first(limit),
                threshold_unix_nano,
                search_query,
            )?
            .items)
    }

    pub fn recent_metrics_page(
        &self,
        service_filter: Option<&str>,
        page: &PageRequest<MetricCursor>,
        threshold_unix_nano: Option<i64>,
        search_query: Option<&str>,
    ) -> Result<Page<MetricSummary, MetricCursor>> {
        let conn = self.conn.lock().expect("sqlite mutex poisoned");
        let mut sql = String::from(
            r#"
            SELECT id, service_name, metric_name, instrument_kind, timestamp_unix_nano, value, summary
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
        if let Some(cursor) = &page.cursor {
            where_clauses.push(format!(
                "(timestamp_unix_nano < {timestamp} OR (timestamp_unix_nano = {timestamp} AND id < {row_id}))",
                timestamp = cursor.timestamp_unix_nano,
                row_id = cursor.row_id,
            ));
        }
        if !where_clauses.is_empty() {
            sql.push_str(" WHERE ");
            sql.push_str(&where_clauses.join(" AND "));
        }
        sql.push_str(" ORDER BY timestamp_unix_nano DESC, id DESC LIMIT ");
        sql.push_str(&page.limit.saturating_add(1).to_string());

        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map([], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                MetricSummary {
                    service_name: row.get(1)?,
                    metric_name: row.get(2)?,
                    instrument_kind: row.get(3)?,
                    timestamp_unix_nano: row.get(4)?,
                    value: row.get(5)?,
                    summary: row.get(6)?,
                },
            ))
        })?;
        let items = rows.collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(
            finish_page(items, page.limit, |(row_id, item)| MetricCursor {
                timestamp_unix_nano: item.timestamp_unix_nano,
                row_id: *row_id,
            })
            .map_items(|(_, item)| item),
        )
    }

    pub fn recent_llm(
        &self,
        service_filter: Option<&str>,
        limit: usize,
        threshold_unix_nano: Option<i64>,
        search_query: Option<&str>,
    ) -> Result<Vec<LlmSummary>> {
        Ok(self
            .recent_llm_page(
                service_filter,
                &PageRequest::first(limit),
                threshold_unix_nano,
                search_query,
            )?
            .items)
    }

    pub fn recent_llm_page(
        &self,
        service_filter: Option<&str>,
        page: &PageRequest<LlmCursor>,
        threshold_unix_nano: Option<i64>,
        search_query: Option<&str>,
    ) -> Result<Page<LlmSummary, LlmCursor>> {
        let conn = self.conn.lock().expect("sqlite mutex poisoned");
        let mut sql = String::from(
            r#"
            SELECT llm_spans.trace_id, llm_spans.span_id, llm_spans.service_name, provider, model, operation,
                   input_tokens, output_tokens, total_tokens, cost, latency_ms, status, raw_json
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
        if let Some(cursor) = &page.cursor {
            where_clauses.push(format!(
                "(COALESCE(llm_spans.latency_ms, -1) < {latency} OR (COALESCE(llm_spans.latency_ms, -1) = {latency} AND llm_spans.span_id < '{span_id}'))",
                latency = cursor.latency_ms,
                span_id = escape_sql(&cursor.span_id),
            ));
        }
        if !where_clauses.is_empty() {
            sql.push_str(" WHERE ");
            sql.push_str(&where_clauses.join(" AND "));
        }
        sql.push_str(
            " ORDER BY COALESCE(llm_spans.latency_ms, -1) DESC, llm_spans.span_id DESC LIMIT ",
        );
        sql.push_str(&page.limit.saturating_add(1).to_string());

        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map([], |row| {
            Ok(LlmSummary {
                trace_id: row.get(0)?,
                span_id: row.get(1)?,
                service_name: row.get(2)?,
                provider: row.get(3)?,
                model: row.get(4)?,
                operation: row.get(5)?,
                span_kind: None,
                session_id: None,
                conversation_id: None,
                prompt_preview: None,
                output_preview: None,
                tool_name: None,
                tool_args: None,
                input_tokens: row.get(6)?,
                output_tokens: row.get(7)?,
                total_tokens: row.get(8)?,
                cost: row.get(9)?,
                latency_ms: row.get(10)?,
                status: row.get(11)?,
                raw_json: serde_json::from_str::<serde_json::Value>(&row.get::<_, String>(12)?)
                    .unwrap_or_default(),
            })
        })?;
        let mut rows = rows.collect::<rusqlite::Result<Vec<_>>>()?;
        for row in &mut rows {
            hydrate_llm_summary(row);
        }
        Ok(finish_page(rows, page.limit, |item| LlmCursor {
            latency_ms: item.latency_ms.unwrap_or(-1.0),
            span_id: item.span_id.clone(),
        }))
    }

    pub fn llm_timeline(&self, trace_id: &str, span_id: &str) -> Result<Vec<LlmTimelineItem>> {
        let spans = self.trace_detail(trace_id)?;
        Ok(project_llm_timeline(&spans, span_id))
    }

    pub fn llm_rollups(
        &self,
        service_filter: Option<&str>,
        threshold_unix_nano: Option<i64>,
        search_query: Option<&str>,
    ) -> Result<Vec<LlmRollup>> {
        let mut rollups = Vec::new();
        for dimension in [
            LlmRollupDimension::Model,
            LlmRollupDimension::Provider,
            LlmRollupDimension::Service,
        ] {
            rollups.extend(self.llm_rollups_for(
                dimension,
                service_filter,
                threshold_unix_nano,
                search_query,
            )?);
        }
        Ok(rollups)
    }

    fn llm_rollups_for(
        &self,
        dimension: LlmRollupDimension,
        service_filter: Option<&str>,
        threshold_unix_nano: Option<i64>,
        search_query: Option<&str>,
    ) -> Result<Vec<LlmRollup>> {
        let conn = self.conn.lock().expect("sqlite mutex poisoned");
        let column = match dimension {
            LlmRollupDimension::Model => "model",
            LlmRollupDimension::Provider => "provider",
            LlmRollupDimension::Service => "llm_spans.service_name",
        };
        let mut sql = format!(
            r#"
            SELECT {column} AS label,
                   COUNT(*) AS call_count,
                   SUM(CASE WHEN status NOT IN ('STATUS_CODE_UNSET', 'STATUS_CODE_OK', 'unknown') THEN 1 ELSE 0 END) AS error_count,
                   COALESCE(SUM(input_tokens), 0) AS input_tokens,
                   COALESCE(SUM(output_tokens), 0) AS output_tokens,
                   COALESCE(SUM(total_tokens), 0) AS total_tokens,
                   SUM(cost) AS cost,
                   AVG(latency_ms) AS avg_latency_ms
            FROM llm_spans
            "#
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
        sql.push_str(&format!(
            " GROUP BY {column} ORDER BY total_tokens DESC, call_count DESC, label ASC LIMIT 5"
        ));

        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map([], |row| {
            Ok(LlmRollup {
                dimension,
                label: row.get(0)?,
                call_count: row.get::<_, i64>(1)? as usize,
                error_count: row.get::<_, i64>(2)? as usize,
                input_tokens: row.get::<_, i64>(3)? as u64,
                output_tokens: row.get::<_, i64>(4)? as u64,
                total_tokens: row.get::<_, i64>(5)? as u64,
                cost: row.get(6)?,
                avg_latency_ms: row.get(7)?,
            })
        })?;
        Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
    }

    pub fn llm_sessions(
        &self,
        service_filter: Option<&str>,
        threshold_unix_nano: Option<i64>,
        search_query: Option<&str>,
    ) -> Result<Vec<LlmSessionSummary>> {
        let conn = self.conn.lock().expect("sqlite mutex poisoned");
        let mut sql = String::from(
            r#"
            SELECT llm_spans.service_name, provider, model, total_tokens, cost, latency_ms, status,
                   raw_json, spans.start_time_unix_nano, spans.end_time_unix_nano
            FROM llm_spans
            INNER JOIN spans ON spans.span_id = llm_spans.span_id
            "#,
        );
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
        sql.push_str(" ORDER BY spans.start_time_unix_nano ASC");

        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map([], |row| {
            let raw_json: String = row.get(7)?;
            Ok(LlmSessionRow {
                service_name: row.get(0)?,
                provider: row.get(1)?,
                model: row.get(2)?,
                total_tokens: row.get::<_, Option<i64>>(3)?.unwrap_or_default() as u64,
                cost: row.get(4)?,
                status: row.get(6)?,
                llm: serde_json::from_str(&raw_json).unwrap_or_default(),
                start_time_unix_nano: row.get(8)?,
                end_time_unix_nano: row.get(9)?,
            })
        })?;

        let mut by_session = HashMap::new();
        for row in rows {
            let row = row?;
            let Some((kind, id)) = llm_session_key(&row.llm) else {
                continue;
            };
            by_session
                .entry((kind, id))
                .or_insert_with(|| LlmSessionAccumulator::empty(&row))
                .push(&row);
        }

        let mut sessions = by_session
            .into_iter()
            .map(|((correlation_kind, correlation_id), accumulator)| {
                accumulator.finish(correlation_kind, correlation_id)
            })
            .collect::<Vec<_>>();
        sessions.sort_by(|left, right| {
            right
                .last_seen_unix_nano
                .cmp(&left.last_seen_unix_nano)
                .then_with(|| right.total_tokens.cmp(&left.total_tokens))
                .then_with(|| left.correlation_id.cmp(&right.correlation_id))
        });
        sessions.truncate(5);
        Ok(sessions)
    }

    fn span_events_by_trace(
        &self,
        trace_id: &str,
    ) -> Result<HashMap<String, Vec<SpanEventDetail>>> {
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

        let mut by_span = HashMap::new();
        for row in rows {
            let (span_id, event) = row?;
            by_span.entry(span_id).or_insert_with(Vec::new).push(event);
        }

        Ok(by_span)
    }

    fn span_links_by_trace(&self, trace_id: &str) -> Result<HashMap<String, Vec<SpanLinkDetail>>> {
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

        let mut by_span = HashMap::new();
        for row in rows {
            let (span_id, link) = row?;
            by_span.entry(span_id).or_insert_with(Vec::new).push(link);
        }

        Ok(by_span)
    }
}

fn finish_page<T, C, F>(mut items: Vec<T>, limit: usize, cursor_for: F) -> Page<T, C>
where
    F: Fn(&T) -> C,
{
    let has_more = items.len() > limit;
    if has_more {
        items.truncate(limit);
    }
    let next_cursor = has_more.then(|| items.last().map(&cursor_for)).flatten();

    Page { items, next_cursor }
}

trait PageMapItems<T, C> {
    fn map_items<U, F>(self, map: F) -> Page<U, C>
    where
        F: Fn(T) -> U;
}

impl<T, C> PageMapItems<T, C> for Page<T, C> {
    fn map_items<U, F>(self, map: F) -> Page<U, C>
    where
        F: Fn(T) -> U,
    {
        Page {
            items: self.items.into_iter().map(map).collect(),
            next_cursor: self.next_cursor,
        }
    }
}

fn hydrate_llm_summary(summary: &mut LlmSummary) {
    let parsed: Result<LlmAttributes, _> = serde_json::from_value(summary.raw_json.clone());
    let Ok(llm) = parsed else {
        return;
    };

    summary.span_kind = llm.span_kind;
    summary.session_id = llm.session_id;
    summary.conversation_id = llm.conversation_id;
    summary.prompt_preview = llm.prompt_preview;
    summary.output_preview = llm.output_preview;
    summary.tool_name = llm.tool_name;
    summary.tool_args = llm.tool_args;
}

fn llm_session_key(llm: &LlmAttributes) -> Option<(String, String)> {
    llm.conversation_id
        .as_deref()
        .filter(|value| !value.is_empty())
        .map(|value| ("conversation".to_string(), value.to_string()))
        .or_else(|| {
            llm.session_id
                .as_deref()
                .filter(|value| !value.is_empty())
                .map(|value| ("session".to_string(), value.to_string()))
        })
}

#[derive(Debug)]
struct LlmSessionRow {
    service_name: String,
    provider: String,
    model: String,
    total_tokens: u64,
    cost: Option<f64>,
    status: String,
    llm: LlmAttributes,
    start_time_unix_nano: i64,
    end_time_unix_nano: i64,
}

#[derive(Debug)]
struct LlmSessionAccumulator {
    service_name: String,
    call_count: usize,
    error_count: usize,
    models: HashSet<String>,
    providers: HashSet<String>,
    total_tokens: u64,
    cost: Option<f64>,
    first_seen_unix_nano: i64,
    last_seen_unix_nano: i64,
}

impl LlmSessionAccumulator {
    fn empty(row: &LlmSessionRow) -> Self {
        Self {
            service_name: row.service_name.clone(),
            call_count: 0,
            error_count: 0,
            models: HashSet::new(),
            providers: HashSet::new(),
            total_tokens: 0,
            cost: None,
            first_seen_unix_nano: row.start_time_unix_nano,
            last_seen_unix_nano: row.end_time_unix_nano,
        }
    }

    fn push(&mut self, row: &LlmSessionRow) {
        self.call_count += 1;
        if row.status != "STATUS_CODE_UNSET" && row.status != "STATUS_CODE_OK" {
            self.error_count += 1;
        }
        self.models.insert(row.model.clone());
        self.providers.insert(row.provider.clone());
        self.total_tokens = self.total_tokens.saturating_add(row.total_tokens);
        self.cost = match (self.cost, row.cost) {
            (Some(total), Some(cost)) => Some(total + cost),
            (None, Some(cost)) => Some(cost),
            (total, None) => total,
        };
        self.first_seen_unix_nano = self.first_seen_unix_nano.min(row.start_time_unix_nano);
        self.last_seen_unix_nano = self.last_seen_unix_nano.max(row.end_time_unix_nano);
    }

    fn finish(self, correlation_kind: String, correlation_id: String) -> LlmSessionSummary {
        LlmSessionSummary {
            correlation_kind,
            correlation_id,
            service_name: self.service_name,
            call_count: self.call_count,
            error_count: self.error_count,
            model_count: self.models.len(),
            provider_count: self.providers.len(),
            total_tokens: self.total_tokens,
            cost: self.cost,
            duration_ms: (self.last_seen_unix_nano - self.first_seen_unix_nano).max(0) as f64
                / 1_000_000.0,
            first_seen_unix_nano: self.first_seen_unix_nano,
            last_seen_unix_nano: self.last_seen_unix_nano,
        }
    }
}
