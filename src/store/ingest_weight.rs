use opentelemetry_proto::tonic::{
    collector::{
        logs::v1::ExportLogsServiceRequest, metrics::v1::ExportMetricsServiceRequest,
        trace::v1::ExportTraceServiceRequest,
    },
    metrics::v1::metric,
};
use prost::Message;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct IngestWeight {
    pub(super) primary_records: usize,
    pub(super) canonical_bytes: usize,
}

impl IngestWeight {
    pub(super) const ZERO: Self = Self {
        primary_records: 0,
        canonical_bytes: 0,
    };

    pub(super) fn is_zero(self) -> bool {
        self == Self::ZERO
    }
}

pub(crate) trait MeasureIngest: Message + Send + 'static {
    fn primary_records(&self) -> usize;
}

/// A decoded OTLP request paired with its canonical writer-admission weight.
///
/// Construction always derives the weight from the request, so callers cannot submit a lower
/// weight than the payload actually contains.
pub(crate) struct PreparedIngest<T> {
    request: T,
    weight: IngestWeight,
}

impl<T: MeasureIngest> PreparedIngest<T> {
    pub(crate) fn prepare(request: T) -> Self {
        let weight = IngestWeight {
            primary_records: request.primary_records(),
            canonical_bytes: request.encoded_len(),
        };
        Self { request, weight }
    }

    pub(super) fn into_parts(self) -> (T, IngestWeight) {
        (self.request, self.weight)
    }
}

impl MeasureIngest for ExportTraceServiceRequest {
    fn primary_records(&self) -> usize {
        exact_primary_count(
            self.resource_spans
                .iter()
                .flat_map(|resource| &resource.scope_spans)
                .map(|scope| scope.spans.len()),
        )
    }
}

impl MeasureIngest for ExportLogsServiceRequest {
    fn primary_records(&self) -> usize {
        exact_primary_count(
            self.resource_logs
                .iter()
                .flat_map(|resource| &resource.scope_logs)
                .map(|scope| scope.log_records.len()),
        )
    }
}

impl MeasureIngest for ExportMetricsServiceRequest {
    fn primary_records(&self) -> usize {
        exact_primary_count(
            self.resource_metrics
                .iter()
                .flat_map(|resource| &resource.scope_metrics)
                .flat_map(|scope| &scope.metrics)
                .map(|metric| match metric.data.as_ref() {
                    Some(metric::Data::Gauge(gauge)) => gauge.data_points.len(),
                    Some(metric::Data::Sum(sum)) => sum.data_points.len(),
                    Some(metric::Data::Histogram(histogram)) => histogram.data_points.len(),
                    Some(metric::Data::ExponentialHistogram(histogram)) => {
                        histogram.data_points.len()
                    }
                    Some(metric::Data::Summary(summary)) => summary.data_points.len(),
                    None => 0,
                }),
        )
    }
}

fn exact_primary_count(mut counts: impl Iterator<Item = usize>) -> usize {
    counts
        .try_fold(0, usize::checked_add)
        .expect("an owned OTLP request cannot contain more records than addressable memory")
}

#[cfg(test)]
mod tests {
    use opentelemetry_proto::tonic::{
        collector::{
            logs::v1::ExportLogsServiceRequest, metrics::v1::ExportMetricsServiceRequest,
            trace::v1::ExportTraceServiceRequest,
        },
        logs::v1::{LogRecord, ResourceLogs, ScopeLogs},
        metrics::v1::{
            ExponentialHistogram, ExponentialHistogramDataPoint, Gauge, Histogram,
            HistogramDataPoint, Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics, Sum,
            Summary, SummaryDataPoint, metric,
        },
        trace::v1::{ResourceSpans, ScopeSpans, Span},
    };
    use prost::Message;

    use super::PreparedIngest;

    #[test]
    fn empty_request_has_zero_weight() {
        let (_, weight) =
            PreparedIngest::prepare(ExportTraceServiceRequest::default()).into_parts();

        assert_eq!(weight.primary_records, 0);
        assert_eq!(weight.canonical_bytes, 0);
    }

    #[test]
    fn canonical_otlp_encoding_can_expand_compact_negative_enum_wire_values() {
        // Sum.aggregation_temporality is enum field 2. Prost accepts the compact uint32
        // representation of -1, then canonically re-encodes the int32 value as ten bytes.
        let compact_wire = [0x10, 0xff, 0xff, 0xff, 0xff, 0x0f];
        let decoded = Sum::decode(compact_wire.as_slice()).unwrap();
        let canonical = decoded.encode_to_vec();

        assert_eq!(decoded.aggregation_temporality, -1);
        assert_eq!(compact_wire.len(), 6);
        assert_eq!(canonical.len(), 11);
        assert!(canonical.len() <= compact_wire.len() * 2);
    }

    #[test]
    fn traces_and_logs_count_primary_records_across_all_scopes() {
        let traces = ExportTraceServiceRequest {
            resource_spans: vec![
                ResourceSpans {
                    scope_spans: vec![ScopeSpans {
                        spans: vec![Span::default(), Span::default()],
                        ..ScopeSpans::default()
                    }],
                    ..ResourceSpans::default()
                },
                ResourceSpans {
                    scope_spans: vec![ScopeSpans {
                        spans: vec![Span::default()],
                        ..ScopeSpans::default()
                    }],
                    ..ResourceSpans::default()
                },
            ],
        };
        let trace_encoded_len = traces.encode_to_vec().len();
        let (_, trace_weight) = PreparedIngest::prepare(traces).into_parts();
        assert_eq!(trace_weight.primary_records, 3);
        assert_eq!(trace_weight.canonical_bytes, trace_encoded_len);

        let logs = ExportLogsServiceRequest {
            resource_logs: vec![ResourceLogs {
                scope_logs: vec![
                    ScopeLogs {
                        log_records: vec![LogRecord::default()],
                        ..ScopeLogs::default()
                    },
                    ScopeLogs {
                        log_records: vec![LogRecord::default(), LogRecord::default()],
                        ..ScopeLogs::default()
                    },
                ],
                ..ResourceLogs::default()
            }],
        };
        let log_encoded_len = logs.encode_to_vec().len();
        let (_, log_weight) = PreparedIngest::prepare(logs).into_parts();
        assert_eq!(log_weight.primary_records, 3);
        assert_eq!(log_weight.canonical_bytes, log_encoded_len);
    }

    #[test]
    fn metrics_count_data_points_from_every_supported_variant() {
        let metrics = ExportMetricsServiceRequest {
            resource_metrics: vec![ResourceMetrics {
                scope_metrics: vec![ScopeMetrics {
                    metrics: vec![
                        Metric {
                            data: Some(metric::Data::Gauge(Gauge {
                                data_points: vec![NumberDataPoint::default()],
                            })),
                            ..Metric::default()
                        },
                        Metric {
                            data: Some(metric::Data::Sum(Sum {
                                data_points: vec![
                                    NumberDataPoint::default(),
                                    NumberDataPoint::default(),
                                ],
                                ..Sum::default()
                            })),
                            ..Metric::default()
                        },
                        Metric {
                            data: Some(metric::Data::Histogram(Histogram {
                                data_points: vec![HistogramDataPoint::default()],
                                ..Histogram::default()
                            })),
                            ..Metric::default()
                        },
                        Metric {
                            data: Some(metric::Data::ExponentialHistogram(ExponentialHistogram {
                                data_points: vec![
                                    ExponentialHistogramDataPoint::default(),
                                    ExponentialHistogramDataPoint::default(),
                                ],
                                ..ExponentialHistogram::default()
                            })),
                            ..Metric::default()
                        },
                        Metric {
                            data: Some(metric::Data::Summary(Summary {
                                data_points: vec![SummaryDataPoint::default()],
                            })),
                            ..Metric::default()
                        },
                        Metric::default(),
                    ],
                    ..ScopeMetrics::default()
                }],
                ..ResourceMetrics::default()
            }],
        };
        let encoded_len = metrics.encode_to_vec().len();
        let (_, weight) = PreparedIngest::prepare(metrics).into_parts();

        assert_eq!(weight.primary_records, 7);
        assert_eq!(weight.canonical_bytes, encoded_len);
    }
}
