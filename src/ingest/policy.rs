use std::time::Duration;

use opentelemetry_proto::tonic::{
    collector::{
        logs::v1::ExportLogsServiceRequest, metrics::v1::ExportMetricsServiceRequest,
        trace::v1::ExportTraceServiceRequest,
    },
    common::v1::{AnyValue, InstrumentationScope, KeyValue, any_value},
    metrics::v1::{
        Exemplar, ExponentialHistogramDataPoint, HistogramDataPoint, NumberDataPoint,
        SummaryDataPoint, metric,
    },
    resource::v1::Resource,
};

use crate::config::ServeArgs;

#[derive(Clone, Debug)]
/// Validated, immutable per-process budgets shared by every OTLP transport and signal.
///
/// Production values are built from validated, nonzero [`ServeArgs`] fields. HTTP applies separate
/// transport and decompressed byte budgets. The gRPC decoder uses the smaller budget because Tonic
/// applies one limit to both the compressed message and its decompressed form. Record, attribute,
/// structure, depth, and dynamic-value budgets are checked against encoded fields before Prost
/// allocates the request graph, then checked again against the decoded graph.
pub struct IngestLimits {
    pub(super) max_in_flight: usize,
    pub(super) max_wire_bytes: usize,
    pub(super) max_decompressed_bytes: usize,
    pub(super) request_timeout: Duration,
    pub(super) max_records: usize,
    pub(super) max_attributes: usize,
    pub(super) max_structures: usize,
    pub(super) max_any_value_depth: usize,
    pub(super) max_value_bytes: usize,
}

impl IngestLimits {
    pub(crate) fn try_from_args(args: &ServeArgs) -> Result<Self, IngestConfigError> {
        let max_in_flight = args.max_otlp_in_flight.get();
        if max_in_flight > tokio::sync::Semaphore::MAX_PERMITS {
            return Err(IngestConfigError::TooManyInFlight {
                configured: max_in_flight,
                maximum: tokio::sync::Semaphore::MAX_PERMITS,
            });
        }
        Ok(Self {
            max_in_flight,
            max_wire_bytes: args.max_otlp_wire_bytes.get(),
            max_decompressed_bytes: args.max_otlp_decompressed_bytes.get(),
            request_timeout: Duration::from_millis(args.otlp_request_timeout_ms.get()),
            max_records: args.max_otlp_records.get(),
            max_attributes: args.max_otlp_attributes.get(),
            max_structures: args.max_otlp_structures.get(),
            max_any_value_depth: args.max_otlp_any_value_depth.get(),
            max_value_bytes: args.max_otlp_value_bytes.get(),
        })
    }

    pub(super) fn grpc_message_bytes(&self) -> usize {
        self.max_wire_bytes.min(self.max_decompressed_bytes)
    }
}

impl Default for IngestLimits {
    fn default() -> Self {
        Self::try_from_args(&ServeArgs::default()).expect("default ingest limits must be valid")
    }
}

#[derive(Debug, Eq, PartialEq, thiserror::Error)]
pub(crate) enum IngestConfigError {
    #[error("OTLP in-flight limit {configured} exceeds semaphore maximum {maximum}")]
    TooManyInFlight { configured: usize, maximum: usize },
}

#[derive(Debug, thiserror::Error)]
pub enum PolicyError {
    #[error("OTLP request exceeds the {budget} budget of {limit}")]
    Budget { budget: &'static str, limit: usize },
}

pub trait ValidateOtlp: Send + 'static {
    fn validate(&self, limits: &IngestLimits) -> Result<(), PolicyError>;
}

impl ValidateOtlp for ExportTraceServiceRequest {
    fn validate(&self, limits: &IngestLimits) -> Result<(), PolicyError> {
        let mut visitor = Visitor::new(limits);
        visitor.structures(self.resource_spans.len())?;
        for resource_spans in &self.resource_spans {
            visitor.value(&resource_spans.schema_url)?;
            if let Some(resource) = &resource_spans.resource {
                visitor.structure()?;
                visitor.resource(resource)?;
            }
            visitor.structures(resource_spans.scope_spans.len())?;
            for scope_spans in &resource_spans.scope_spans {
                visitor.value(&scope_spans.schema_url)?;
                if let Some(scope) = &scope_spans.scope {
                    visitor.structure()?;
                    visitor.scope(scope)?;
                }
                visitor.primary(scope_spans.spans.len())?;
                visitor.structures(scope_spans.spans.len())?;
                for span in &scope_spans.spans {
                    visitor.bytes(&span.trace_id)?;
                    visitor.bytes(&span.span_id)?;
                    visitor.value(&span.trace_state)?;
                    visitor.bytes(&span.parent_span_id)?;
                    visitor.value(&span.name)?;
                    visitor.attributes(&span.attributes)?;
                    visitor.structures(span.events.len())?;
                    for event in &span.events {
                        visitor.value(&event.name)?;
                        visitor.attributes(&event.attributes)?;
                    }
                    visitor.structures(span.links.len())?;
                    for link in &span.links {
                        visitor.bytes(&link.trace_id)?;
                        visitor.bytes(&link.span_id)?;
                        visitor.value(&link.trace_state)?;
                        visitor.attributes(&link.attributes)?;
                    }
                    if let Some(status) = &span.status {
                        visitor.structure()?;
                        visitor.value(&status.message)?;
                    }
                }
            }
        }
        Ok(())
    }
}

impl ValidateOtlp for ExportLogsServiceRequest {
    fn validate(&self, limits: &IngestLimits) -> Result<(), PolicyError> {
        let mut visitor = Visitor::new(limits);
        visitor.structures(self.resource_logs.len())?;
        for resource_logs in &self.resource_logs {
            visitor.value(&resource_logs.schema_url)?;
            if let Some(resource) = &resource_logs.resource {
                visitor.structure()?;
                visitor.resource(resource)?;
            }
            visitor.structures(resource_logs.scope_logs.len())?;
            for scope_logs in &resource_logs.scope_logs {
                visitor.value(&scope_logs.schema_url)?;
                if let Some(scope) = &scope_logs.scope {
                    visitor.structure()?;
                    visitor.scope(scope)?;
                }
                visitor.primary(scope_logs.log_records.len())?;
                visitor.structures(scope_logs.log_records.len())?;
                for log in &scope_logs.log_records {
                    visitor.value(&log.severity_text)?;
                    if let Some(body) = &log.body {
                        visitor.any_value(body, 1)?;
                    }
                    visitor.attributes(&log.attributes)?;
                    visitor.bytes(&log.trace_id)?;
                    visitor.bytes(&log.span_id)?;
                    visitor.value(&log.event_name)?;
                }
            }
        }
        Ok(())
    }
}

impl ValidateOtlp for ExportMetricsServiceRequest {
    fn validate(&self, limits: &IngestLimits) -> Result<(), PolicyError> {
        let mut visitor = Visitor::new(limits);
        visitor.structures(self.resource_metrics.len())?;
        for resource_metrics in &self.resource_metrics {
            visitor.value(&resource_metrics.schema_url)?;
            if let Some(resource) = &resource_metrics.resource {
                visitor.structure()?;
                visitor.resource(resource)?;
            }
            visitor.structures(resource_metrics.scope_metrics.len())?;
            for scope_metrics in &resource_metrics.scope_metrics {
                visitor.value(&scope_metrics.schema_url)?;
                if let Some(scope) = &scope_metrics.scope {
                    visitor.structure()?;
                    visitor.scope(scope)?;
                }
                visitor.structures(scope_metrics.metrics.len())?;
                for metric in &scope_metrics.metrics {
                    visitor.value(&metric.name)?;
                    visitor.value(&metric.description)?;
                    visitor.value(&metric.unit)?;
                    visitor.attributes(&metric.metadata)?;
                    if let Some(data) = &metric.data {
                        visitor.structure()?;
                        match data {
                            metric::Data::Gauge(gauge) => {
                                visitor.number_points(&gauge.data_points)?;
                            }
                            metric::Data::Sum(sum) => {
                                visitor.number_points(&sum.data_points)?;
                            }
                            metric::Data::Histogram(histogram) => {
                                visitor.histogram_points(&histogram.data_points)?;
                            }
                            metric::Data::ExponentialHistogram(histogram) => {
                                visitor.exponential_histogram_points(&histogram.data_points)?;
                            }
                            metric::Data::Summary(summary) => {
                                visitor.summary_points(&summary.data_points)?;
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    }
}

struct Visitor<'a> {
    limits: &'a IngestLimits,
    records: usize,
    attributes: usize,
    structures: usize,
}

impl<'a> Visitor<'a> {
    fn new(limits: &'a IngestLimits) -> Self {
        Self {
            limits,
            records: 0,
            attributes: 0,
            structures: 0,
        }
    }

    fn primary(&mut self, count: usize) -> Result<(), PolicyError> {
        add_budget(
            &mut self.records,
            count,
            self.limits.max_records,
            "primary record",
        )
    }

    fn structure(&mut self) -> Result<(), PolicyError> {
        self.structures(1)
    }

    fn structures(&mut self, count: usize) -> Result<(), PolicyError> {
        add_budget(
            &mut self.structures,
            count,
            self.limits.max_structures,
            "structural element",
        )
    }

    fn value(&self, value: &str) -> Result<(), PolicyError> {
        self.dynamic_len(value.len())
    }

    fn bytes(&self, value: &[u8]) -> Result<(), PolicyError> {
        self.dynamic_len(value.len())
    }

    fn dynamic_len(&self, len: usize) -> Result<(), PolicyError> {
        if len > self.limits.max_value_bytes {
            return Err(PolicyError::Budget {
                budget: "individual value byte",
                limit: self.limits.max_value_bytes,
            });
        }
        Ok(())
    }

    fn resource(&mut self, resource: &Resource) -> Result<(), PolicyError> {
        self.attributes(&resource.attributes)?;
        self.structures(resource.entity_refs.len())?;
        for entity in &resource.entity_refs {
            self.value(&entity.schema_url)?;
            self.value(&entity.r#type)?;
            self.structures(entity.id_keys.len())?;
            for key in &entity.id_keys {
                self.value(key)?;
            }
            self.structures(entity.description_keys.len())?;
            for key in &entity.description_keys {
                self.value(key)?;
            }
        }
        Ok(())
    }

    fn scope(&mut self, scope: &InstrumentationScope) -> Result<(), PolicyError> {
        self.value(&scope.name)?;
        self.value(&scope.version)?;
        self.attributes(&scope.attributes)
    }

    fn attributes(&mut self, attributes: &[KeyValue]) -> Result<(), PolicyError> {
        add_budget(
            &mut self.attributes,
            attributes.len(),
            self.limits.max_attributes,
            "attribute",
        )?;
        self.structures(attributes.len())?;
        for attribute in attributes {
            self.value(&attribute.key)?;
            if let Some(value) = &attribute.value {
                self.any_value(value, 1)?;
            }
        }
        Ok(())
    }

    fn any_value(&mut self, root: &AnyValue, root_depth: usize) -> Result<(), PolicyError> {
        if root_depth > self.limits.max_any_value_depth {
            return Err(PolicyError::Budget {
                budget: "AnyValue depth",
                limit: self.limits.max_any_value_depth,
            });
        }
        match &root.value {
            Some(any_value::Value::StringValue(value)) => {
                self.structure()?;
                return self.value(value);
            }
            Some(any_value::Value::BytesValue(value)) => {
                self.structure()?;
                return self.bytes(value);
            }
            Some(
                any_value::Value::BoolValue(_)
                | any_value::Value::IntValue(_)
                | any_value::Value::DoubleValue(_),
            )
            | None => {
                self.structure()?;
                return Ok(());
            }
            Some(any_value::Value::ArrayValue(_) | any_value::Value::KvlistValue(_)) => {}
        }

        enum Node<'a> {
            Value(&'a AnyValue, usize),
            Attribute(&'a KeyValue, usize),
        }

        let mut stack = vec![Node::Value(root, root_depth)];
        while let Some(node) = stack.pop() {
            match node {
                Node::Attribute(attribute, depth) => {
                    self.value(&attribute.key)?;
                    if let Some(value) = &attribute.value {
                        stack.push(Node::Value(value, depth));
                    }
                }
                Node::Value(value, depth) => {
                    if depth > self.limits.max_any_value_depth {
                        return Err(PolicyError::Budget {
                            budget: "AnyValue depth",
                            limit: self.limits.max_any_value_depth,
                        });
                    }
                    self.structure()?;
                    match &value.value {
                        Some(any_value::Value::StringValue(value)) => self.value(value)?,
                        Some(any_value::Value::BytesValue(value)) => self.bytes(value)?,
                        Some(any_value::Value::ArrayValue(array)) => {
                            self.structures(array.values.len())?;
                            stack.extend(
                                array
                                    .values
                                    .iter()
                                    .map(|value| Node::Value(value, depth + 1)),
                            );
                        }
                        Some(any_value::Value::KvlistValue(list)) => {
                            add_budget(
                                &mut self.attributes,
                                list.values.len(),
                                self.limits.max_attributes,
                                "attribute",
                            )?;
                            self.structures(list.values.len())?;
                            stack.extend(
                                list.values
                                    .iter()
                                    .map(|value| Node::Attribute(value, depth + 1)),
                            );
                        }
                        Some(
                            any_value::Value::BoolValue(_)
                            | any_value::Value::IntValue(_)
                            | any_value::Value::DoubleValue(_),
                        )
                        | None => {}
                    }
                }
            }
        }
        Ok(())
    }

    fn exemplars(&mut self, exemplars: &[Exemplar]) -> Result<(), PolicyError> {
        self.structures(exemplars.len())?;
        for exemplar in exemplars {
            self.attributes(&exemplar.filtered_attributes)?;
            self.bytes(&exemplar.span_id)?;
            self.bytes(&exemplar.trace_id)?;
        }
        Ok(())
    }

    fn number_points(&mut self, points: &[NumberDataPoint]) -> Result<(), PolicyError> {
        self.primary(points.len())?;
        self.structures(points.len())?;
        for point in points {
            self.attributes(&point.attributes)?;
            self.exemplars(&point.exemplars)?;
        }
        Ok(())
    }

    fn histogram_points(&mut self, points: &[HistogramDataPoint]) -> Result<(), PolicyError> {
        self.primary(points.len())?;
        self.structures(points.len())?;
        for point in points {
            self.attributes(&point.attributes)?;
            self.structures(point.bucket_counts.len())?;
            self.structures(point.explicit_bounds.len())?;
            self.exemplars(&point.exemplars)?;
        }
        Ok(())
    }

    fn exponential_histogram_points(
        &mut self,
        points: &[ExponentialHistogramDataPoint],
    ) -> Result<(), PolicyError> {
        self.primary(points.len())?;
        self.structures(points.len())?;
        for point in points {
            self.attributes(&point.attributes)?;
            for buckets in [&point.positive, &point.negative].into_iter().flatten() {
                self.structure()?;
                self.structures(buckets.bucket_counts.len())?;
            }
            self.exemplars(&point.exemplars)?;
        }
        Ok(())
    }

    fn summary_points(&mut self, points: &[SummaryDataPoint]) -> Result<(), PolicyError> {
        self.primary(points.len())?;
        self.structures(points.len())?;
        for point in points {
            self.attributes(&point.attributes)?;
            self.structures(point.quantile_values.len())?;
        }
        Ok(())
    }
}

fn add_budget(
    current: &mut usize,
    add: usize,
    limit: usize,
    budget: &'static str,
) -> Result<(), PolicyError> {
    *current = current
        .checked_add(add)
        .ok_or(PolicyError::Budget { budget, limit })?;
    if *current > limit {
        return Err(PolicyError::Budget { budget, limit });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{num::NonZeroUsize, time::Duration};

    use opentelemetry_proto::tonic::{
        collector::{
            logs::v1::ExportLogsServiceRequest, metrics::v1::ExportMetricsServiceRequest,
            trace::v1::ExportTraceServiceRequest,
        },
        common::v1::{AnyValue, ArrayValue, KeyValue, KeyValueList, any_value},
        logs::v1::{LogRecord, ResourceLogs, ScopeLogs},
        metrics::v1::{
            ExponentialHistogram, ExponentialHistogramDataPoint, Gauge, Histogram,
            HistogramDataPoint, Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics, Sum,
            Summary, SummaryDataPoint, exemplar, exponential_histogram_data_point, metric,
            summary_data_point::ValueAtQuantile,
        },
        resource::v1::Resource,
        trace::v1::{ResourceSpans, ScopeSpans, Span, span},
    };

    use super::{IngestConfigError, IngestLimits, ValidateOtlp};
    use crate::config::ServeArgs;

    #[test]
    fn programmatic_in_flight_limit_is_rejected_at_the_consumption_boundary() {
        let args = ServeArgs {
            max_otlp_in_flight: NonZeroUsize::new(tokio::sync::Semaphore::MAX_PERMITS + 1).unwrap(),
            ..ServeArgs::default()
        };

        assert_eq!(
            IngestLimits::try_from_args(&args).unwrap_err(),
            IngestConfigError::TooManyInFlight {
                configured: tokio::sync::Semaphore::MAX_PERMITS + 1,
                maximum: tokio::sync::Semaphore::MAX_PERMITS,
            }
        );
    }

    #[test]
    fn primary_record_budget_is_request_wide_and_exact_boundary_is_allowed() {
        let mut limits = permissive_limits();
        limits.max_records = 1;
        let mut request = trace_request();
        request.validate(&limits).unwrap();

        request.resource_spans[0].scope_spans[0]
            .spans
            .push(Span::default());
        assert_budget(request.validate(&limits), "primary record");
    }

    #[test]
    fn nested_attributes_and_any_value_depth_are_iteratively_bounded() {
        let mut limits = permissive_limits();
        limits.max_attributes = 1;
        limits.max_any_value_depth = 2;
        let nested = AnyValue {
            value: Some(any_value::Value::KvlistValue(KeyValueList {
                values: vec![KeyValue {
                    key: "nested".into(),
                    value: Some(AnyValue {
                        value: Some(any_value::Value::ArrayValue(ArrayValue {
                            values: vec![AnyValue::default()],
                        })),
                    }),
                }],
            })),
        };
        let mut request = log_request(nested);
        assert_budget(request.validate(&limits), "AnyValue depth");

        limits.max_any_value_depth = 3;
        request.validate(&limits).unwrap();
        if let Some(any_value::Value::KvlistValue(list)) = request.resource_logs[0].scope_logs[0]
            .log_records[0]
            .body
            .as_mut()
            .unwrap()
            .value
            .as_mut()
        {
            list.values.push(KeyValue::default());
        }
        assert_budget(request.validate(&limits), "attribute");
    }

    #[test]
    fn all_metric_data_variants_contribute_primary_records() {
        let mut limits = permissive_limits();
        limits.max_records = 5;
        let mut request = metric_request_with_all_variants();
        request.validate(&limits).unwrap();

        let metric::Data::Gauge(gauge) = request.resource_metrics[0].scope_metrics[0].metrics[0]
            .data
            .as_mut()
            .unwrap()
        else {
            panic!("expected gauge");
        };
        gauge.data_points.push(NumberDataPoint::default());
        assert_budget(request.validate(&limits), "primary record");
    }

    #[test]
    fn metric_bucket_and_quantile_structures_have_exact_boundaries() {
        let mut limits = permissive_limits();
        let request = ExportMetricsServiceRequest {
            resource_metrics: vec![ResourceMetrics {
                scope_metrics: vec![ScopeMetrics {
                    metrics: vec![Metric {
                        data: Some(metric::Data::Summary(Summary {
                            data_points: vec![SummaryDataPoint {
                                quantile_values: vec![ValueAtQuantile::default()],
                                ..Default::default()
                            }],
                        })),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        };
        limits.max_structures = 6;
        request.validate(&limits).unwrap();
        limits.max_structures = 5;
        assert_budget(request.validate(&limits), "structural element");
    }

    #[test]
    fn ignored_resource_trace_log_and_metric_fields_obey_value_budget() {
        let mut limits = permissive_limits();
        limits.max_value_bytes = 3;

        let mut trace = trace_request();
        trace.resource_spans[0].resource = Some(Resource {
            entity_refs: vec![opentelemetry_proto::tonic::common::v1::EntityRef {
                description_keys: vec!["long".into()],
                ..Default::default()
            }],
            ..Default::default()
        });
        assert_budget(trace.validate(&limits), "value byte");

        let mut log = log_request(AnyValue::default());
        log.resource_logs[0].scope_logs[0].log_records[0].event_name = "long".into();
        assert_budget(log.validate(&limits), "value byte");

        let mut metrics = metric_request_with_all_variants();
        metrics.resource_metrics[0].scope_metrics[0].metrics[0].description = "long".into();
        assert_budget(metrics.validate(&limits), "value byte");
    }

    #[test]
    fn legal_empty_bytes_and_heterogeneous_any_values_are_accepted() {
        let value = AnyValue {
            value: Some(any_value::Value::ArrayValue(ArrayValue {
                values: vec![
                    AnyValue::default(),
                    AnyValue {
                        value: Some(any_value::Value::BytesValue(vec![1, 2])),
                    },
                    AnyValue {
                        value: Some(any_value::Value::BoolValue(true)),
                    },
                    AnyValue {
                        value: Some(any_value::Value::KvlistValue(KeyValueList::default())),
                    },
                ],
            })),
        };
        log_request(value).validate(&permissive_limits()).unwrap();
    }

    #[test]
    fn events_links_and_exemplars_are_traversed() {
        let mut limits = permissive_limits();
        limits.max_value_bytes = 3;
        let mut trace = trace_request();
        trace.resource_spans[0].scope_spans[0].spans[0]
            .events
            .push(span::Event {
                name: "long".into(),
                ..Default::default()
            });
        assert_budget(trace.validate(&limits), "value byte");

        let mut trace = trace_request();
        trace.resource_spans[0].scope_spans[0].spans[0]
            .links
            .push(span::Link {
                trace_state: "long".into(),
                ..Default::default()
            });
        assert_budget(trace.validate(&limits), "value byte");

        let mut metrics = metric_request_with_all_variants();
        let metric::Data::Gauge(gauge) = metrics.resource_metrics[0].scope_metrics[0].metrics[0]
            .data
            .as_mut()
            .unwrap()
        else {
            panic!("expected gauge");
        };
        gauge.data_points[0]
            .exemplars
            .push(opentelemetry_proto::tonic::metrics::v1::Exemplar {
                trace_id: vec![0; 4],
                value: Some(exemplar::Value::AsInt(1)),
                ..Default::default()
            });
        assert_budget(metrics.validate(&limits), "value byte");
    }

    #[test]
    fn explicit_and_exponential_bucket_elements_consume_structure_budget() {
        let histogram = ExportMetricsServiceRequest {
            resource_metrics: vec![ResourceMetrics {
                scope_metrics: vec![ScopeMetrics {
                    metrics: vec![Metric {
                        data: Some(metric::Data::Histogram(Histogram {
                            data_points: vec![HistogramDataPoint {
                                bucket_counts: vec![1, 2],
                                explicit_bounds: vec![1.0],
                                ..Default::default()
                            }],
                            ..Default::default()
                        })),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        };
        let mut limits = permissive_limits();
        limits.max_structures = 8;
        histogram.validate(&limits).unwrap();
        limits.max_structures = 7;
        assert_budget(histogram.validate(&limits), "structural element");

        let exponential = ExportMetricsServiceRequest {
            resource_metrics: vec![ResourceMetrics {
                scope_metrics: vec![ScopeMetrics {
                    metrics: vec![Metric {
                        data: Some(metric::Data::ExponentialHistogram(ExponentialHistogram {
                            data_points: vec![ExponentialHistogramDataPoint {
                                positive: Some(exponential_histogram_data_point::Buckets {
                                    bucket_counts: vec![1, 2],
                                    ..Default::default()
                                }),
                                ..Default::default()
                            }],
                            ..Default::default()
                        })),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        };
        limits.max_structures = 8;
        exponential.validate(&limits).unwrap();
        limits.max_structures = 7;
        assert_budget(exponential.validate(&limits), "structural element");
    }

    fn permissive_limits() -> IngestLimits {
        IngestLimits {
            max_in_flight: 4,
            max_wire_bytes: 1_000_000,
            max_decompressed_bytes: 1_000_000,
            request_timeout: Duration::from_secs(30),
            max_records: 1_000,
            max_attributes: 1_000,
            max_structures: 10_000,
            max_any_value_depth: 32,
            max_value_bytes: 1_000,
        }
    }

    fn trace_request() -> ExportTraceServiceRequest {
        ExportTraceServiceRequest {
            resource_spans: vec![ResourceSpans {
                scope_spans: vec![ScopeSpans {
                    spans: vec![Span::default()],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        }
    }

    fn log_request(body: AnyValue) -> ExportLogsServiceRequest {
        ExportLogsServiceRequest {
            resource_logs: vec![ResourceLogs {
                scope_logs: vec![ScopeLogs {
                    log_records: vec![LogRecord {
                        body: Some(body),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        }
    }

    fn metric_request_with_all_variants() -> ExportMetricsServiceRequest {
        let number = NumberDataPoint::default();
        ExportMetricsServiceRequest {
            resource_metrics: vec![ResourceMetrics {
                scope_metrics: vec![ScopeMetrics {
                    metrics: vec![
                        Metric {
                            data: Some(metric::Data::Gauge(Gauge {
                                data_points: vec![number.clone()],
                            })),
                            ..Default::default()
                        },
                        Metric {
                            data: Some(metric::Data::Sum(Sum {
                                data_points: vec![number],
                                ..Default::default()
                            })),
                            ..Default::default()
                        },
                        Metric {
                            data: Some(metric::Data::Histogram(Histogram {
                                data_points: vec![HistogramDataPoint {
                                    bucket_counts: vec![1],
                                    explicit_bounds: vec![1.0],
                                    ..Default::default()
                                }],
                                ..Default::default()
                            })),
                            ..Default::default()
                        },
                        Metric {
                            data: Some(metric::Data::ExponentialHistogram(ExponentialHistogram {
                                data_points: vec![ExponentialHistogramDataPoint::default()],
                                ..Default::default()
                            })),
                            ..Default::default()
                        },
                        Metric {
                            data: Some(metric::Data::Summary(Summary {
                                data_points: vec![SummaryDataPoint::default()],
                            })),
                            ..Default::default()
                        },
                    ],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        }
    }

    fn assert_budget(result: Result<(), super::PolicyError>, expected: &str) {
        let message = result.unwrap_err().to_string();
        assert!(message.contains(expected), "unexpected error: {message}");
    }
}
