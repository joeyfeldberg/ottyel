//! Record-level OTLP validation and partial-success reporting.
//!
//! Screening runs after the request-wide policy and before writer admission, so the writer
//! weight always describes exactly the records that will be stored. Only a record whose primary
//! identity is invalid is rejected. Malformed secondary fields are normalized and reported as
//! warnings so the receiver keeps as much valid telemetry as the OTLP data model allows.

use std::{collections::BTreeMap, fmt::Write as _};

use opentelemetry_proto::tonic::{
    collector::{
        logs::v1::{ExportLogsPartialSuccess, ExportLogsServiceRequest, ExportLogsServiceResponse},
        metrics::v1::{
            ExportMetricsPartialSuccess, ExportMetricsServiceRequest, ExportMetricsServiceResponse,
        },
        trace::v1::{
            ExportTracePartialSuccess, ExportTraceServiceRequest, ExportTraceServiceResponse,
        },
    },
    metrics::v1::{Metric, metric},
    trace::v1::Span,
};
use prost::Message;

const TRACE_ID_BYTES: usize = 16;
const SPAN_ID_BYTES: usize = 8;

/// One reason a record was rejected or normalized.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(super) enum Issue {
    SpanTraceId,
    SpanId,
    SpanParentId,
    MetricName,
    HistogramBuckets,
    SpanZeroParentId,
    SpanLinkId,
    SpanEndBeforeStart,
    LogTraceId,
    LogSpanId,
}

impl Issue {
    fn rejects(self) -> bool {
        match self {
            Self::SpanTraceId
            | Self::SpanId
            | Self::SpanParentId
            | Self::MetricName
            | Self::HistogramBuckets => true,
            Self::SpanZeroParentId
            | Self::SpanLinkId
            | Self::SpanEndBeforeStart
            | Self::LogTraceId
            | Self::LogSpanId => false,
        }
    }

    fn describe(self) -> &'static str {
        match self {
            Self::SpanTraceId => "span(s) rejected: trace_id is not 16 non-zero bytes",
            Self::SpanId => "span(s) rejected: span_id is not 8 non-zero bytes",
            Self::SpanParentId => "span(s) rejected: parent_span_id is neither empty nor 8 bytes",
            Self::MetricName => "data point(s) rejected: metric name is empty",
            Self::HistogramBuckets => {
                "data point(s) rejected: histogram bucket_counts length is not explicit_bounds + 1"
            }
            Self::SpanZeroParentId => "span(s) with an all-zero parent_span_id stored as roots",
            Self::SpanLinkId => "span link(s) dropped: trace_id or span_id is invalid",
            Self::SpanEndBeforeStart => "span(s) end before they start",
            Self::LogTraceId => {
                "log record(s) with an invalid trace_id stored without trace association"
            }
            Self::LogSpanId => {
                "log record(s) with an invalid span_id stored without span association"
            }
        }
    }
}

/// Rejected-record count plus counted reasons for one screened export.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(super) struct RecordReport {
    rejected: u64,
    issues: BTreeMap<Issue, u64>,
}

impl RecordReport {
    fn note(&mut self, issue: Issue, count: u64) {
        if count == 0 {
            return;
        }
        if issue.rejects() {
            self.rejected = self.rejected.saturating_add(count);
        }
        let total = self.issues.entry(issue).or_default();
        *total = total.saturating_add(count);
    }

    pub(super) fn rejected(&self) -> u64 {
        self.rejected
    }

    /// Whether any record was normalized rather than rejected.
    pub(super) fn warned(&self) -> bool {
        self.issues.keys().any(|issue| !issue.rejects())
    }

    /// Returns `None` for a clean export, which OTLP requires to leave `partial_success` unset.
    fn partial_success(&self) -> Option<(i64, String)> {
        if self.issues.is_empty() {
            return None;
        }
        let mut message = String::new();
        for (index, (issue, count)) in self.issues.iter().enumerate() {
            if index > 0 {
                message.push_str("; ");
            }
            let _ = write!(message, "{count} {}", issue.describe());
        }
        Some((i64::try_from(self.rejected).unwrap_or(i64::MAX), message))
    }
}

/// Removes records that violate OTLP invariants and builds the matching export response.
pub trait ScreenRecords: Send + 'static {
    type Response: Message + Default + Send + 'static;

    fn screen(&mut self) -> RecordReport;

    fn response(report: &RecordReport) -> Self::Response;
}

impl ScreenRecords for ExportTraceServiceRequest {
    type Response = ExportTraceServiceResponse;

    fn screen(&mut self) -> RecordReport {
        let mut report = RecordReport::default();
        for resource_spans in &mut self.resource_spans {
            for scope_spans in &mut resource_spans.scope_spans {
                scope_spans
                    .spans
                    .retain_mut(|span| screen_span(span, &mut report));
            }
        }
        report
    }

    fn response(report: &RecordReport) -> Self::Response {
        ExportTraceServiceResponse {
            partial_success: report
                .partial_success()
                .map(
                    |(rejected_spans, error_message)| ExportTracePartialSuccess {
                        rejected_spans,
                        error_message,
                    },
                ),
        }
    }
}

fn screen_span(span: &mut Span, report: &mut RecordReport) -> bool {
    let rejection = if !valid_id(&span.trace_id, TRACE_ID_BYTES) {
        Some(Issue::SpanTraceId)
    } else if !valid_id(&span.span_id, SPAN_ID_BYTES) {
        Some(Issue::SpanId)
    } else if !span.parent_span_id.is_empty() && span.parent_span_id.len() != SPAN_ID_BYTES {
        Some(Issue::SpanParentId)
    } else {
        None
    };
    if let Some(issue) = rejection {
        report.note(issue, 1);
        return false;
    }

    if !span.parent_span_id.is_empty() && all_zero(&span.parent_span_id) {
        span.parent_span_id.clear();
        report.note(Issue::SpanZeroParentId, 1);
    }
    let links = span.links.len();
    span.links.retain(|link| {
        valid_id(&link.trace_id, TRACE_ID_BYTES) && valid_id(&link.span_id, SPAN_ID_BYTES)
    });
    report.note(Issue::SpanLinkId, (links - span.links.len()) as u64);
    if span.end_time_unix_nano < span.start_time_unix_nano {
        report.note(Issue::SpanEndBeforeStart, 1);
    }
    true
}

impl ScreenRecords for ExportLogsServiceRequest {
    type Response = ExportLogsServiceResponse;

    fn screen(&mut self) -> RecordReport {
        let mut report = RecordReport::default();
        let records = self
            .resource_logs
            .iter_mut()
            .flat_map(|resource| &mut resource.scope_logs)
            .flat_map(|scope| &mut scope.log_records);
        for record in records {
            // OTLP receivers treat a log with an invalid ID as unassociated, not as invalid.
            if !record.trace_id.is_empty() && !valid_id(&record.trace_id, TRACE_ID_BYTES) {
                record.trace_id.clear();
                report.note(Issue::LogTraceId, 1);
            }
            if !record.span_id.is_empty() && !valid_id(&record.span_id, SPAN_ID_BYTES) {
                record.span_id.clear();
                report.note(Issue::LogSpanId, 1);
            }
        }
        report
    }

    fn response(report: &RecordReport) -> Self::Response {
        ExportLogsServiceResponse {
            partial_success: report.partial_success().map(
                |(rejected_log_records, error_message)| ExportLogsPartialSuccess {
                    rejected_log_records,
                    error_message,
                },
            ),
        }
    }
}

impl ScreenRecords for ExportMetricsServiceRequest {
    type Response = ExportMetricsServiceResponse;

    fn screen(&mut self) -> RecordReport {
        let mut report = RecordReport::default();
        for resource_metrics in &mut self.resource_metrics {
            for scope_metrics in &mut resource_metrics.scope_metrics {
                scope_metrics
                    .metrics
                    .retain_mut(|metric| screen_metric(metric, &mut report));
            }
        }
        report
    }

    fn response(report: &RecordReport) -> Self::Response {
        ExportMetricsServiceResponse {
            partial_success: report.partial_success().map(
                |(rejected_data_points, error_message)| ExportMetricsPartialSuccess {
                    rejected_data_points,
                    error_message,
                },
            ),
        }
    }
}

fn screen_metric(metric: &mut Metric, report: &mut RecordReport) -> bool {
    if metric.name.is_empty() {
        report.note(Issue::MetricName, data_points(metric) as u64);
        return false;
    }
    if let Some(metric::Data::Histogram(histogram)) = &mut metric.data {
        let points = histogram.data_points.len();
        histogram.data_points.retain(|point| {
            point.bucket_counts.is_empty()
                || point.bucket_counts.len() == point.explicit_bounds.len() + 1
        });
        report.note(
            Issue::HistogramBuckets,
            (points - histogram.data_points.len()) as u64,
        );
    }
    true
}

fn data_points(metric: &Metric) -> usize {
    match &metric.data {
        Some(metric::Data::Gauge(gauge)) => gauge.data_points.len(),
        Some(metric::Data::Sum(sum)) => sum.data_points.len(),
        Some(metric::Data::Histogram(histogram)) => histogram.data_points.len(),
        Some(metric::Data::ExponentialHistogram(histogram)) => histogram.data_points.len(),
        Some(metric::Data::Summary(summary)) => summary.data_points.len(),
        None => 0,
    }
}

fn valid_id(id: &[u8], length: usize) -> bool {
    id.len() == length && !all_zero(id)
}

fn all_zero(bytes: &[u8]) -> bool {
    bytes.iter().all(|byte| *byte == 0)
}

#[cfg(test)]
#[path = "records_tests.rs"]
mod tests;
