use anyhow::Result;
use opentelemetry_proto::tonic::{
    collector::{
        logs::v1::ExportLogsServiceRequest, metrics::v1::ExportMetricsServiceRequest,
        trace::v1::ExportTraceServiceRequest,
    },
    metrics::v1::{Metric, metric},
};
use rusqlite::{Connection, params};

use crate::domain::{
    LlmAttributes, attributes_to_map, extract_llm_attributes, extract_service_name,
};

use super::{
    AsyncWriteReceipt, PreparedIngest, Store,
    helpers::{
        any_value_text, format_metric_summary, hex_bytes, log_severity, log_time_unix_nano,
        number_value, resource_to_map, span_kind_name, status_code_name,
    },
};

impl Store {
    /// Attempts immediate admission to bounded writer capacity. A request larger than the total
    /// capacity returns [`super::StoreWriteError::TooLarge`]; aggregate capacity or queue
    /// exhaustion returns [`super::StoreWriteError::Overloaded`]. Once admitted, this waits
    /// without a completion deadline for the definitive writer acknowledgement or an
    /// [`super::StoreWriteError::OutcomeUnknown`] failure.
    ///
    /// Queue and acknowledgement failures can be classified by downcasting the returned error to
    /// [`super::StoreWriteError`].
    pub fn ingest_traces(&self, request: ExportTraceServiceRequest) -> Result<usize> {
        self.ingest_prepared_traces(PreparedIngest::prepare(request))
    }

    fn ingest_prepared_traces(
        &self,
        prepared: PreparedIngest<ExportTraceServiceRequest>,
    ) -> Result<usize> {
        let writer = self.write_access()?;
        let (request, weight) = prepared.into_parts();
        writer.execute_ingest(weight, move |conn| Self::write_traces(conn, request))
    }

    pub(crate) fn try_ingest_traces(
        &self,
        prepared: PreparedIngest<ExportTraceServiceRequest>,
    ) -> Result<AsyncWriteReceipt<usize>> {
        let writer = self.write_access()?;
        let (request, weight) = prepared.into_parts();
        writer.try_execute_ingest_async(weight, move |conn| Self::write_traces(conn, request))
    }

    fn write_traces(conn: &Connection, request: ExportTraceServiceRequest) -> Result<usize> {
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

                    conn.execute(
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

                    conn.execute(
                        "DELETE FROM span_events WHERE trace_id = ?1 AND span_id = ?2",
                        params![trace_id, span_id],
                    )?;
                    conn.execute(
                        "DELETE FROM span_links WHERE trace_id = ?1 AND span_id = ?2",
                        params![trace_id, span_id],
                    )?;

                    for event in span.events {
                        let event_attrs = attributes_to_map(&event.attributes);
                        conn.execute(
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
                        conn.execute(
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
                        Self::insert_llm_row(conn, &trace_id, &span_id, &service_name, &llm)?;
                    }
                }
            }
        }

        Ok(inserted)
    }

    /// Attempts immediate admission to bounded writer capacity. A request larger than the total
    /// capacity returns [`super::StoreWriteError::TooLarge`]; aggregate capacity or queue
    /// exhaustion returns [`super::StoreWriteError::Overloaded`]. Once admitted, this waits
    /// without a completion deadline for the definitive writer acknowledgement or an
    /// [`super::StoreWriteError::OutcomeUnknown`] failure.
    ///
    /// Queue and acknowledgement failures can be classified by downcasting the returned error to
    /// [`super::StoreWriteError`].
    pub fn ingest_logs(&self, request: ExportLogsServiceRequest) -> Result<usize> {
        self.ingest_prepared_logs(PreparedIngest::prepare(request))
    }

    fn ingest_prepared_logs(
        &self,
        prepared: PreparedIngest<ExportLogsServiceRequest>,
    ) -> Result<usize> {
        let writer = self.write_access()?;
        let (request, weight) = prepared.into_parts();
        writer.execute_ingest(weight, move |conn| Self::write_logs(conn, request))
    }

    pub(crate) fn try_ingest_logs(
        &self,
        prepared: PreparedIngest<ExportLogsServiceRequest>,
    ) -> Result<AsyncWriteReceipt<usize>> {
        let writer = self.write_access()?;
        let (request, weight) = prepared.into_parts();
        writer.try_execute_ingest_async(weight, move |conn| Self::write_logs(conn, request))
    }

    fn write_logs(conn: &Connection, request: ExportLogsServiceRequest) -> Result<usize> {
        let mut inserted = 0usize;

        for resource_logs in request.resource_logs {
            let resource_attrs = resource_to_map(resource_logs.resource.as_ref());
            let resource_json = serde_json::to_string(&resource_attrs)?;
            let service_name = extract_service_name(&resource_attrs);

            for scope_logs in resource_logs.scope_logs {
                for log in scope_logs.log_records {
                    inserted += 1;
                    conn.execute(
                        r#"
                        INSERT INTO logs (
                            service_name, timestamp_unix_nano, severity, body, trace_id, span_id,
                            resource_attributes_json, attributes_json
                        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
                        "#,
                        params![
                            service_name,
                            log_time_unix_nano(&log),
                            log_severity(&log),
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

        Ok(inserted)
    }

    /// Attempts immediate admission to bounded writer capacity. A request larger than the total
    /// capacity returns [`super::StoreWriteError::TooLarge`]; aggregate capacity or queue
    /// exhaustion returns [`super::StoreWriteError::Overloaded`]. Once admitted, this waits
    /// without a completion deadline for the definitive writer acknowledgement or an
    /// [`super::StoreWriteError::OutcomeUnknown`] failure.
    ///
    /// Queue and acknowledgement failures can be classified by downcasting the returned error to
    /// [`super::StoreWriteError`].
    pub fn ingest_metrics(&self, request: ExportMetricsServiceRequest) -> Result<usize> {
        self.ingest_prepared_metrics(PreparedIngest::prepare(request))
    }

    fn ingest_prepared_metrics(
        &self,
        prepared: PreparedIngest<ExportMetricsServiceRequest>,
    ) -> Result<usize> {
        let writer = self.write_access()?;
        let (request, weight) = prepared.into_parts();
        writer.execute_ingest(weight, move |conn| Self::write_metrics(conn, request))
    }

    pub(crate) fn try_ingest_metrics(
        &self,
        prepared: PreparedIngest<ExportMetricsServiceRequest>,
    ) -> Result<AsyncWriteReceipt<usize>> {
        let writer = self.write_access()?;
        let (request, weight) = prepared.into_parts();
        writer.try_execute_ingest_async(weight, move |conn| Self::write_metrics(conn, request))
    }

    fn write_metrics(conn: &Connection, request: ExportMetricsServiceRequest) -> Result<usize> {
        let mut inserted = 0usize;

        for resource_metrics in request.resource_metrics {
            let resource_attrs = resource_to_map(resource_metrics.resource.as_ref());
            let resource_json = serde_json::to_string(&resource_attrs)?;
            let service_name = extract_service_name(&resource_attrs);

            for scope_metrics in resource_metrics.scope_metrics {
                for metric in scope_metrics.metrics {
                    inserted +=
                        Self::insert_metric_rows(conn, &service_name, &resource_json, metric)?;
                }
            }
        }

        Ok(inserted)
    }

    fn insert_llm_row(
        conn: &Connection,
        trace_id: &str,
        span_id: &str,
        service_name: &str,
        llm: &LlmAttributes,
    ) -> Result<()> {
        conn.execute(
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
        conn: &Connection,
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
                    conn.execute(
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
                    conn.execute(
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
                    conn.execute(
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
                    conn.execute(
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
                    conn.execute(
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
}
