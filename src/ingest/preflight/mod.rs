mod schema;
mod wire;

use std::str;

use opentelemetry_proto::tonic::collector::{
    logs::v1::ExportLogsServiceRequest, metrics::v1::ExportMetricsServiceRequest,
    trace::v1::ExportTraceServiceRequest,
};

use super::{
    IngestLimits,
    policy::{PolicyError, ValidateOtlp},
};
use schema::{Field, MessageCount, Schema, field};
use wire::{Cursor, MAX_PROTOBUF_NESTING, WireType};

#[derive(Debug, thiserror::Error)]
pub(super) enum PreflightError {
    #[error("{0}")]
    Malformed(#[from] MalformedProtobuf),
    #[error("{0}")]
    Budget(#[from] PolicyError),
}

#[derive(Debug, thiserror::Error)]
#[error("{message}")]
pub(super) struct MalformedProtobuf {
    message: &'static str,
}

impl MalformedProtobuf {
    fn new(message: &'static str) -> Self {
        Self { message }
    }
}

pub(super) trait PreflightOtlp: ValidateOtlp {
    fn preflight(bytes: &[u8], limits: &IngestLimits) -> Result<(), PreflightError>;
}

macro_rules! impl_preflight {
    ($request:ty, $schema:ident) => {
        impl PreflightOtlp for $request {
            fn preflight(bytes: &[u8], limits: &IngestLimits) -> Result<(), PreflightError> {
                Walker::new(limits).message(bytes, Schema::$schema, 1, 0)
            }
        }
    };
}

impl_preflight!(ExportTraceServiceRequest, TraceRequest);
impl_preflight!(ExportLogsServiceRequest, LogsRequest);
impl_preflight!(ExportMetricsServiceRequest, MetricsRequest);

struct Walker<'a> {
    limits: &'a IngestLimits,
    records: usize,
    attributes: usize,
    structures: usize,
    work: WorkBudget,
}

struct WorkBudget {
    used: usize,
    limit: usize,
}

impl WorkBudget {
    fn new(limit: usize) -> Self {
        Self { used: 0, limit }
    }

    fn charge(&mut self, count: usize) -> Result<(), PolicyError> {
        add_budget(&mut self.used, count, self.limit, "protobuf work unit")
    }
}

impl<'a> Walker<'a> {
    fn new(limits: &'a IngestLimits) -> Self {
        Self {
            limits,
            records: 0,
            attributes: 0,
            structures: 0,
            work: WorkBudget::new(limits.max_work_units),
        }
    }

    fn message(
        &mut self,
        bytes: &[u8],
        schema: Schema,
        any_depth: usize,
        wire_depth: usize,
    ) -> Result<(), PreflightError> {
        if wire_depth > MAX_PROTOBUF_NESTING {
            return Err(
                MalformedProtobuf::new("protobuf nesting exceeds the decoder limit").into(),
            );
        }
        let mut cursor = Cursor::new(bytes);
        // Canonical fields retain the decoded-graph accounting. Replaced string, bytes, and
        // allocating oneof values consume additional structure budget before Prost sees them.
        let mut seen_allocating_fields = 0_u32;
        let mut any_value_allocation_seen = false;
        while !cursor.is_empty() {
            let (tag, wire) = cursor.key()?;
            self.work.charge(1)?;
            let Some(spec) = field(schema, tag) else {
                cursor.skip_unknown(tag, wire, wire_depth, &mut self.work)?;
                continue;
            };
            self.known(&mut cursor, wire, spec, any_depth, wire_depth)?;
            if schema == Schema::AnyValue && matches!(tag, 1 | 5 | 6 | 7) {
                if any_value_allocation_seen {
                    self.structure()?;
                }
                any_value_allocation_seen = true;
            } else if spec.duplicate_needs_structure() {
                let mask = 1_u32 << tag;
                if seen_allocating_fields & mask != 0 {
                    self.structure()?;
                }
                seen_allocating_fields |= mask;
            }
        }
        Ok(())
    }

    fn known(
        &mut self,
        cursor: &mut Cursor<'_>,
        wire: WireType,
        spec: Field,
        any_depth: usize,
        wire_depth: usize,
    ) -> Result<(), PreflightError> {
        match spec {
            Field::Varint => {
                expect(wire, WireType::Varint)?;
                cursor.varint()?;
            }
            Field::Fixed32 => {
                expect(wire, WireType::Fixed32)?;
                cursor.fixed32()?;
            }
            Field::Fixed64 => {
                expect(wire, WireType::Fixed64)?;
                cursor.fixed64()?;
            }
            Field::String { structure } => {
                expect(wire, WireType::LengthDelimited)?;
                let value = cursor.length_delimited()?;
                if structure {
                    self.structure()?;
                }
                self.value(value)?;
                str::from_utf8(value)
                    .map_err(|_| MalformedProtobuf::new("protobuf string is not valid UTF-8"))?;
            }
            Field::Bytes => {
                expect(wire, WireType::LengthDelimited)?;
                self.value(cursor.length_delimited()?)?;
            }
            Field::Message(child, count) => {
                expect(wire, WireType::LengthDelimited)?;
                let bytes = cursor.length_delimited()?;
                if wire_depth >= MAX_PROTOBUF_NESTING {
                    return Err(MalformedProtobuf::new(
                        "protobuf nesting exceeds the decoder limit",
                    )
                    .into());
                }
                self.work.charge(1)?;
                let child_depth = self.count_message(count, any_depth)?;
                self.message(bytes, child, child_depth, wire_depth + 1)?;
            }
            Field::RepeatedVarintStructure => match wire {
                WireType::Varint => {
                    cursor.varint()?;
                    self.structure()?;
                }
                WireType::LengthDelimited => {
                    self.consume_packed_varints(cursor.length_delimited()?)?;
                }
                _ => return Err(wrong_wire()),
            },
            Field::RepeatedFixed64Structure => {
                let count = match wire {
                    WireType::Fixed64 => {
                        cursor.fixed64()?;
                        1
                    }
                    WireType::LengthDelimited => {
                        let bytes = cursor.length_delimited()?;
                        if bytes.len() % 8 != 0 {
                            return Err(MalformedProtobuf::new(
                                "packed fixed64 field has an invalid length",
                            )
                            .into());
                        }
                        let count = bytes.len() / 8;
                        self.work.charge(count)?;
                        count
                    }
                    _ => return Err(wrong_wire()),
                };
                self.structures(count)?;
            }
        }
        Ok(())
    }

    fn consume_packed_varints(&mut self, bytes: &[u8]) -> Result<(), PreflightError> {
        let mut cursor = Cursor::new(bytes);
        while !cursor.is_empty() {
            cursor.varint()?;
            self.work.charge(1)?;
            self.structure()?;
        }
        Ok(())
    }

    fn count_message(
        &mut self,
        count: MessageCount,
        any_depth: usize,
    ) -> Result<usize, PreflightError> {
        match count {
            MessageCount::None => Ok(any_depth),
            MessageCount::Structure => {
                self.structure()?;
                Ok(any_depth)
            }
            MessageCount::Primary => {
                self.primary()?;
                self.structure()?;
                Ok(any_depth)
            }
            MessageCount::Attribute => {
                self.attribute()?;
                self.structure()?;
                Ok(1)
            }
            MessageCount::AnyValue => {
                self.structure()?;
                self.check_any_depth(any_depth)?;
                Ok(any_depth)
            }
            MessageCount::ArrayItem => {
                self.structures(2)?;
                let depth = any_depth.checked_add(1).ok_or_else(|| self.depth_error())?;
                self.check_any_depth(depth)?;
                Ok(depth)
            }
            MessageCount::NestedAttribute => {
                self.attribute()?;
                self.structure()?;
                let depth = any_depth.checked_add(1).ok_or_else(|| self.depth_error())?;
                Ok(depth)
            }
        }
    }

    fn primary(&mut self) -> Result<(), PolicyError> {
        add_budget(
            &mut self.records,
            1,
            self.limits.max_records,
            "primary record",
        )
    }

    fn attribute(&mut self) -> Result<(), PolicyError> {
        add_budget(
            &mut self.attributes,
            1,
            self.limits.max_attributes,
            "attribute",
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

    fn value(&self, value: &[u8]) -> Result<(), PolicyError> {
        if value.len() > self.limits.max_value_bytes {
            return Err(PolicyError::Budget {
                budget: "individual value byte",
                limit: self.limits.max_value_bytes,
            });
        }
        Ok(())
    }

    fn check_any_depth(&self, depth: usize) -> Result<(), PolicyError> {
        if depth > self.limits.max_any_value_depth {
            return Err(self.depth_error());
        }
        Ok(())
    }

    fn depth_error(&self) -> PolicyError {
        PolicyError::Budget {
            budget: "AnyValue depth",
            limit: self.limits.max_any_value_depth,
        }
    }
}

fn expect(actual: WireType, expected: WireType) -> Result<(), PreflightError> {
    if actual != expected {
        return Err(wrong_wire());
    }
    Ok(())
}

fn wrong_wire() -> PreflightError {
    MalformedProtobuf::new("known protobuf field has the wrong wire type").into()
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
    use std::time::Duration;

    use opentelemetry_proto::tonic::{
        collector::{
            logs::v1::ExportLogsServiceRequest, metrics::v1::ExportMetricsServiceRequest,
            trace::v1::ExportTraceServiceRequest,
        },
        common::v1::{
            AnyValue, ArrayValue, EntityRef, InstrumentationScope, KeyValue, KeyValueList,
            any_value,
        },
        logs::v1::{LogRecord, ResourceLogs, ScopeLogs},
        metrics::v1::{
            Exemplar, ExponentialHistogram, ExponentialHistogramDataPoint, Gauge, Histogram,
            HistogramDataPoint, Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics, Sum,
            Summary, SummaryDataPoint, exponential_histogram_data_point, metric,
            summary_data_point::ValueAtQuantile,
        },
        resource::v1::Resource,
        trace::v1::{ResourceSpans, ScopeSpans, Span, Status, span},
    };
    use prost::Message;

    use super::{
        PolicyError, PreflightError, PreflightOtlp, Schema, ValidateOtlp, Walker, WorkBudget,
        schema, wire::WireType,
    };
    use crate::ingest::IngestLimits;

    #[test]
    fn work_budget_counts_nested_message_and_unknown_group_entries_and_ends_globally() {
        // TraceRequest.resource_spans entry (2), then an unknown group in ResourceSpans
        // whose start field, group entry, and end field cost another 3.
        let nested_message_group = [0x0a, 0x02, 0x23, 0x24];
        let mut limits = permissive_limits();
        limits.max_work_units = 5;
        ExportTraceServiceRequest::preflight(&nested_message_group, &limits).unwrap();
        limits.max_work_units = 4;
        assert_work_budget(
            ExportTraceServiceRequest::preflight(&nested_message_group, &limits),
            4,
        );

        // Each nested group costs its start key, an entry, and its matching end key.
        let nested_groups = [0x13, 0x1b, 0x1c, 0x14];
        limits.max_work_units = 6;
        ExportTraceServiceRequest::preflight(&nested_groups, &limits).unwrap();
        limits.max_work_units = 5;
        assert_work_budget(
            ExportTraceServiceRequest::preflight(&nested_groups, &limits),
            5,
        );
    }

    #[test]
    fn work_budget_counts_packed_elements_during_iteration() {
        let unpacked_varints = [0x10, 0x01, 0x10, 0x02];
        let packed_varints = [0x12, 0x02, 0x01, 0x02];
        let mut limits = permissive_limits();
        limits.max_work_units = 2;
        Walker::new(&limits)
            .message(&unpacked_varints, Schema::Buckets, 1, 0)
            .unwrap();
        assert_work_budget(
            Walker::new(&limits).message(&packed_varints, Schema::Buckets, 1, 0),
            2,
        );
        limits.max_work_units = 3;
        Walker::new(&limits)
            .message(&packed_varints, Schema::Buckets, 1, 0)
            .unwrap();

        let mut packed_fixed = vec![(6 << 3) | WireType::LengthDelimited as u8, 16];
        packed_fixed.extend_from_slice(&1_u64.to_le_bytes());
        packed_fixed.extend_from_slice(&2_u64.to_le_bytes());
        limits.max_work_units = 2;
        assert_work_budget(
            Walker::new(&limits).message(&packed_fixed, Schema::HistogramPoint, 1, 0),
            2,
        );
        limits.max_work_units = 3;
        Walker::new(&limits)
            .message(&packed_fixed, Schema::HistogramPoint, 1, 0)
            .unwrap();
    }

    #[test]
    fn work_budget_counts_duplicate_known_scalars_and_unknown_fields() {
        let known_scalars = [0x30, 0x00, 0x30, 0x00];
        let unknown_fields = [0x10, 0x00, 0x10, 0x00];
        let mut limits = permissive_limits();
        limits.max_work_units = 2;
        Walker::new(&limits)
            .message(&known_scalars, Schema::Span, 1, 0)
            .unwrap();
        ExportTraceServiceRequest::preflight(&unknown_fields, &limits).unwrap();

        limits.max_work_units = 1;
        assert_work_budget(
            Walker::new(&limits).message(&known_scalars, Schema::Span, 1, 0),
            1,
        );
        assert_work_budget(
            ExportTraceServiceRequest::preflight(&unknown_fields, &limits),
            1,
        );
    }

    #[test]
    fn work_budget_is_shared_by_all_signal_preflights_and_defaults_accept_canonical_requests() {
        let duplicate_unknown_fields = [0x10, 0x00, 0x10, 0x00];
        let mut limits = permissive_limits();
        limits.max_work_units = 1;
        assert_work_budget(
            ExportTraceServiceRequest::preflight(&duplicate_unknown_fields, &limits),
            1,
        );
        assert_work_budget(
            ExportLogsServiceRequest::preflight(&duplicate_unknown_fields, &limits),
            1,
        );
        assert_work_budget(
            ExportMetricsServiceRequest::preflight(&duplicate_unknown_fields, &limits),
            1,
        );

        let trace = ExportTraceServiceRequest {
            resource_spans: vec![ResourceSpans {
                scope_spans: vec![ScopeSpans {
                    spans: vec![Span::default()],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        };
        let logs = ExportLogsServiceRequest {
            resource_logs: vec![ResourceLogs {
                scope_logs: vec![ScopeLogs {
                    log_records: vec![LogRecord::default()],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        };
        let metrics = ExportMetricsServiceRequest {
            resource_metrics: vec![ResourceMetrics {
                scope_metrics: vec![ScopeMetrics {
                    metrics: vec![Metric {
                        data: Some(metric::Data::Gauge(Gauge {
                            data_points: vec![NumberDataPoint::default()],
                        })),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        };
        let defaults = IngestLimits::default();
        ExportTraceServiceRequest::preflight(&trace.encode_to_vec(), &defaults).unwrap();
        ExportLogsServiceRequest::preflight(&logs.encode_to_vec(), &defaults).unwrap();
        ExportMetricsServiceRequest::preflight(&metrics.encode_to_vec(), &defaults).unwrap();
    }

    #[test]
    fn work_budget_overflow_fails_closed() {
        let mut budget = WorkBudget {
            used: usize::MAX,
            limit: usize::MAX,
        };
        assert!(matches!(
            budget.charge(1),
            Err(PolicyError::Budget {
                budget: "protobuf work unit",
                limit: usize::MAX,
            })
        ));
    }

    #[test]
    fn default_work_budget_accepts_a_near_limit_canonical_link_encoding() {
        const LINK_COUNT: usize = 249_997;
        const FLAGGED_LINK_COUNT: usize = 188_865;
        const EXPECTED_WORK_UNITS: usize = 1_688_853;
        const MAX_REQUEST_BYTES: usize = 4 * 1024 * 1024;

        let link_message = span::Link {
            trace_id: vec![0],
            span_id: vec![0],
            trace_state: "x".into(),
            dropped_attributes_count: 1,
            ..Default::default()
        };
        let link = link_message.encode_to_vec();
        assert_eq!(
            link,
            [
                0x0a, 0x01, 0x00, 0x12, 0x01, 0x00, 0x1a, 0x01, b'x', 0x28, 0x01
            ]
        );
        let flagged_link = span::Link {
            flags: 1,
            ..link_message
        }
        .encode_to_vec();
        assert_eq!(
            flagged_link,
            [
                0x0a, 0x01, 0x00, 0x12, 0x01, 0x00, 0x1a, 0x01, b'x', 0x28, 0x01, 0x35, 0x01, 0x00,
                0x00, 0x00
            ]
        );

        let mut span = Vec::with_capacity(MAX_REQUEST_BYTES);
        for index in 0..LINK_COUNT {
            let encoded = if index < FLAGGED_LINK_COUNT {
                &flagged_link
            } else {
                &link
            };
            span.extend_from_slice(&[0x6a, encoded.len() as u8]);
            span.extend_from_slice(encoded);
        }
        let scope_spans = wrap_message(2, span);
        let resource_spans = wrap_message(2, scope_spans);
        let request = wrap_message(1, resource_spans);
        assert_eq!(request.len(), MAX_REQUEST_BYTES - 3);

        let limits = IngestLimits::default();
        let mut walker = Walker::new(&limits);
        walker
            .message(&request, Schema::TraceRequest, 1, 0)
            .unwrap();
        assert_eq!(walker.structures, limits.max_structures);
        assert_eq!(walker.work.used, EXPECTED_WORK_UNITS);
        assert!(walker.work.used < limits.max_work_units);
    }

    #[test]
    fn canonical_trace_and_nested_any_value_boundaries_match_post_decode_validation() {
        let trace = ExportTraceServiceRequest {
            resource_spans: vec![ResourceSpans {
                resource: Some(Resource {
                    attributes: vec![attribute()],
                    entity_refs: vec![EntityRef {
                        schema_url: "entity-schema".into(),
                        r#type: "service".into(),
                        id_keys: vec!["id".into()],
                        description_keys: vec!["description".into()],
                    }],
                    ..Default::default()
                }),
                scope_spans: vec![ScopeSpans {
                    scope: Some(InstrumentationScope {
                        name: "scope".into(),
                        version: "version".into(),
                        attributes: vec![attribute()],
                        ..Default::default()
                    }),
                    spans: vec![Span {
                        trace_id: vec![1],
                        span_id: vec![2],
                        trace_state: "state".into(),
                        parent_span_id: vec![3],
                        name: "span".into(),
                        attributes: vec![attribute()],
                        events: vec![span::Event {
                            name: "event".into(),
                            attributes: vec![attribute()],
                            ..Default::default()
                        }],
                        links: vec![span::Link {
                            trace_id: vec![4],
                            span_id: vec![5],
                            trace_state: "link".into(),
                            attributes: vec![attribute()],
                            ..Default::default()
                        }],
                        status: Some(Status {
                            message: "status".into(),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }],
                    schema_url: "scope-schema".into(),
                }],
                schema_url: "resource-schema".into(),
            }],
        };
        let mut limits = permissive_limits();
        let structure_boundary = counter_boundary(&trace, |limits, value| {
            limits.max_structures = value;
        });
        assert!(structure_boundary > 3);
        let attribute_boundary = counter_boundary(&trace, |limits, value| {
            limits.max_attributes = value;
        });
        assert!(attribute_boundary > 1);
        limits.max_structures = structure_boundary;
        assert_parity(&trace, &limits, true);
        limits.max_structures = structure_boundary - 1;
        assert_parity(&trace, &limits, false);

        let logs = ExportLogsServiceRequest {
            resource_logs: vec![ResourceLogs {
                scope_logs: vec![ScopeLogs {
                    log_records: vec![LogRecord {
                        severity_text: "severity".into(),
                        body: Some(AnyValue {
                            value: Some(any_value::Value::ArrayValue(ArrayValue {
                                values: vec![
                                    AnyValue::default(),
                                    AnyValue {
                                        value: Some(any_value::Value::BytesValue(vec![1])),
                                    },
                                ],
                            })),
                        }),
                        attributes: vec![attribute()],
                        trace_id: vec![1],
                        span_id: vec![2],
                        event_name: "event".into(),
                        ..Default::default()
                    }],
                    schema_url: "scope-schema".into(),
                    ..Default::default()
                }],
                schema_url: "resource-schema".into(),
                ..Default::default()
            }],
        };
        let log_structure_boundary = counter_boundary(&logs, |limits, value| {
            limits.max_structures = value;
        });
        assert!(
            counter_boundary(&logs, |limits, value| {
                limits.max_attributes = value;
            }) > 1
        );
        assert!(
            counter_boundary(&logs, |limits, value| {
                limits.max_value_bytes = value;
            }) >= "resource-schema".len()
        );
        limits.max_structures = log_structure_boundary;
        limits.max_any_value_depth = 2;
        assert_parity(&logs, &limits, true);
        limits.max_structures = log_structure_boundary - 1;
        assert_parity(&logs, &limits, false);
        limits.max_structures = log_structure_boundary;
        limits.max_any_value_depth = 1;
        assert_parity(&logs, &limits, false);
    }

    #[test]
    fn every_metric_variant_has_record_budget_parity() {
        let exemplar = Exemplar {
            filtered_attributes: vec![attribute()],
            span_id: vec![1],
            trace_id: vec![2],
            ..Default::default()
        };
        let number = NumberDataPoint {
            attributes: vec![attribute()],
            exemplars: vec![exemplar.clone()],
            ..Default::default()
        };
        let request = ExportMetricsServiceRequest {
            resource_metrics: vec![ResourceMetrics {
                resource: Some(Resource {
                    attributes: vec![attribute()],
                    entity_refs: vec![EntityRef {
                        id_keys: vec!["id".into()],
                        ..Default::default()
                    }],
                    ..Default::default()
                }),
                scope_metrics: vec![ScopeMetrics {
                    scope: Some(InstrumentationScope {
                        attributes: vec![attribute()],
                        ..Default::default()
                    }),
                    metrics: vec![
                        Metric {
                            name: "gauge".into(),
                            description: "description".into(),
                            unit: "unit".into(),
                            metadata: vec![attribute()],
                            data: Some(metric::Data::Gauge(Gauge {
                                data_points: vec![number.clone()],
                            })),
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
                                    attributes: vec![attribute()],
                                    bucket_counts: vec![1],
                                    explicit_bounds: vec![1.0],
                                    exemplars: vec![exemplar.clone()],
                                    ..Default::default()
                                }],
                                ..Default::default()
                            })),
                            ..Default::default()
                        },
                        Metric {
                            data: Some(metric::Data::ExponentialHistogram(ExponentialHistogram {
                                data_points: vec![ExponentialHistogramDataPoint {
                                    attributes: vec![attribute()],
                                    positive: Some(exponential_histogram_data_point::Buckets {
                                        bucket_counts: vec![1, 2],
                                        ..Default::default()
                                    }),
                                    negative: Some(exponential_histogram_data_point::Buckets {
                                        bucket_counts: vec![3],
                                        ..Default::default()
                                    }),
                                    exemplars: vec![exemplar],
                                    ..Default::default()
                                }],
                                ..Default::default()
                            })),
                            ..Default::default()
                        },
                        Metric {
                            data: Some(metric::Data::Summary(Summary {
                                data_points: vec![SummaryDataPoint {
                                    attributes: vec![attribute()],
                                    quantile_values: vec![ValueAtQuantile::default()],
                                    ..Default::default()
                                }],
                            })),
                            ..Default::default()
                        },
                    ],
                    ..Default::default()
                }],
                schema_url: "resource-schema".into(),
            }],
        };
        let mut limits = permissive_limits();
        limits.max_records = 5;
        assert_parity(&request, &limits, true);
        limits.max_records = 4;
        assert_parity(&request, &limits, false);
        assert!(
            counter_boundary(&request, |limits, value| {
                limits.max_structures = value;
            }) > 10
        );
        assert!(
            counter_boundary(&request, |limits, value| {
                limits.max_attributes = value;
            }) > 3
        );
    }

    #[test]
    fn duplicate_optional_messages_are_bounded_before_prost_collapses_them() {
        let bytes = [0x0a, 0x04, 0x0a, 0x00, 0x0a, 0x00];
        let request = ExportTraceServiceRequest::decode(bytes.as_slice()).unwrap();
        let mut limits = permissive_limits();
        limits.max_structures = 2;
        request.validate(&limits).unwrap();
        assert!(matches!(
            ExportTraceServiceRequest::preflight(&bytes, &limits),
            Err(PreflightError::Budget(_))
        ));
    }

    #[test]
    fn duplicate_singular_and_oneof_allocations_are_monotonically_bounded() {
        let singular = [
            0x0a, 0x06, // resource_spans
            0x1a, 0x01, b'a', // first schema_url
            0x1a, 0x01, b'b', // replacement schema_url
        ];
        let request = ExportTraceServiceRequest::decode(singular.as_slice()).unwrap();
        let mut limits = permissive_limits();
        limits.max_structures = 1;
        request.validate(&limits).unwrap();
        assert!(matches!(
            ExportTraceServiceRequest::preflight(&singular, &limits),
            Err(PreflightError::Budget(_))
        ));

        let duplicate_oneof = [0x0a, 0x01, b'a', 0x3a, 0x01, b'b'];
        limits.max_structures = 0;
        assert!(matches!(
            Walker::new(&limits).message(&duplicate_oneof, Schema::AnyValue, 1, 0),
            Err(PreflightError::Budget(_))
        ));
    }

    #[test]
    fn budget_prefix_wins_before_a_malformed_suffix_reaches_prost() {
        let bytes = [0x0a, 0x00, 0x0a, 0x00, 0x0f];
        let mut limits = permissive_limits();
        limits.max_structures = 1;
        assert!(matches!(
            ExportTraceServiceRequest::preflight(&bytes, &limits),
            Err(PreflightError::Budget(_))
        ));
        limits.max_structures = 10;
        assert!(matches!(
            ExportTraceServiceRequest::preflight(&bytes, &limits),
            Err(PreflightError::Malformed(_))
        ));
    }

    #[test]
    fn packed_and_unpacked_varints_stop_at_the_same_structural_prefix() {
        let unpacked = [0x10, 0x01, 0x10, 0x02, 0x10, 0x03, 0x80];
        let packed = [0x12, 0x04, 0x01, 0x02, 0x03, 0x80];
        let mut limits = permissive_limits();
        limits.max_structures = 2;
        limits.max_work_units = 100;

        assert_structure_budget(
            Walker::new(&limits).message(&unpacked, Schema::Buckets, 1, 0),
            2,
        );
        assert_structure_budget(
            Walker::new(&limits).message(&packed, Schema::Buckets, 1, 0),
            2,
        );

        limits.max_structures = 3;
        assert!(matches!(
            Walker::new(&limits).message(&unpacked, Schema::Buckets, 1, 0),
            Err(PreflightError::Malformed(_))
        ));
        assert!(matches!(
            Walker::new(&limits).message(&packed, Schema::Buckets, 1, 0),
            Err(PreflightError::Malformed(_))
        ));
    }

    #[test]
    fn packed_and_unpacked_bucket_elements_have_identical_structure_cost() {
        let mut unpacked = Vec::new();
        for value in [1_u64, 2] {
            unpacked.push((6 << 3) | WireType::Fixed64 as u8);
            unpacked.extend_from_slice(&value.to_le_bytes());
        }
        let mut packed = vec![(6 << 3) | WireType::LengthDelimited as u8, 16];
        packed.extend_from_slice(&1_u64.to_le_bytes());
        packed.extend_from_slice(&2_u64.to_le_bytes());

        let mut limits = permissive_limits();
        limits.max_structures = 2;
        Walker::new(&limits)
            .message(&unpacked, Schema::HistogramPoint, 1, 0)
            .unwrap();
        Walker::new(&limits)
            .message(&packed, Schema::HistogramPoint, 1, 0)
            .unwrap();
        limits.max_structures = 1;
        assert!(matches!(
            Walker::new(&limits).message(&packed, Schema::HistogramPoint, 1, 0),
            Err(PreflightError::Budget(_))
        ));

        let malformed = [(6 << 3) | WireType::LengthDelimited as u8, 1, 0];
        assert!(matches!(
            Walker::new(&permissive_limits()).message(&malformed, Schema::HistogramPoint, 1, 0),
            Err(PreflightError::Malformed(_))
        ));

        let unpacked_varints = [0x10, 0x01, 0x10, 0x02];
        let packed_varints = [0x12, 0x02, 0x01, 0x02];
        limits.max_structures = 2;
        Walker::new(&limits)
            .message(&unpacked_varints, Schema::Buckets, 1, 0)
            .unwrap();
        Walker::new(&limits)
            .message(&packed_varints, Schema::Buckets, 1, 0)
            .unwrap();
        let malformed_varints = [0x12, 0x01, 0x80];
        assert!(matches!(
            Walker::new(&limits).message(&malformed_varints, Schema::Buckets, 1, 0),
            Err(PreflightError::Malformed(_))
        ));
    }

    #[test]
    fn known_wrong_wire_types_invalid_utf8_and_unknown_groups_are_distinguished() {
        let limits = permissive_limits();
        for malformed in [&[0x08, 0x00][..], &[0x0a, 0x03, 0x1a, 0x01, 0xff][..]] {
            assert!(matches!(
                ExportTraceServiceRequest::preflight(malformed, &limits),
                Err(PreflightError::Malformed(_))
            ));
        }

        let unknown_group = [0x13, 0x18, 0x01, 0x14];
        ExportTraceServiceRequest::preflight(&unknown_group, &limits).unwrap();
        assert!(schema::field(Schema::TraceRequest, 2).is_none());
    }

    #[test]
    fn malformed_wire_corpus_is_rejected_without_reaching_prost() {
        let limits = permissive_limits();
        let overflowing_varint = [
            0x10, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x02,
        ];
        for malformed in [
            &[0x00][..],
            &[0x16][..],
            &[0x12, 0x02, 0x01][..],
            &[0x11, 0x01][..],
            &[0x15, 0x01][..],
            &[0x14][..],
            &[0x13][..],
            &[0x13, 0x1c][..],
            &overflowing_varint,
        ] {
            assert!(matches!(
                ExportTraceServiceRequest::preflight(malformed, &limits),
                Err(PreflightError::Malformed(_))
            ));
        }

        let mut too_deep = vec![0x13; 101];
        too_deep.extend(std::iter::repeat_n(0x14, 101));
        assert!(matches!(
            ExportTraceServiceRequest::preflight(&too_deep, &limits),
            Err(PreflightError::Malformed(_))
        ));
    }

    #[test]
    fn deepest_known_message_rejects_unknown_scalar_at_prost_recursion_boundary() {
        let mut encoded = vec![0x40, 0x01]; // Unknown AnyValue field 8.
        for _ in 0..50 {
            encoded = wrap_message(1, encoded); // ArrayValue.values -> AnyValue.
            encoded = wrap_message(5, encoded); // AnyValue.array_value -> ArrayValue.
        }
        let mut limits = permissive_limits();
        limits.max_any_value_depth = 1_000;
        limits.max_structures = 1_000;

        assert!(matches!(
            Walker::new(&limits).message(&encoded, Schema::AnyValue, 1, 0),
            Err(PreflightError::Malformed(_))
        ));
        let prost_error = AnyValue::decode(encoded.as_slice()).unwrap_err();
        assert!(prost_error.to_string().contains("recursion limit"));

        let mut allowed = wrap_message(5, vec![0x40, 0x01]);
        for _ in 0..49 {
            allowed = wrap_message(1, allowed);
            allowed = wrap_message(5, allowed);
        }
        Walker::new(&limits)
            .message(&allowed, Schema::AnyValue, 1, 0)
            .unwrap();
        AnyValue::decode(allowed.as_slice()).unwrap();
    }

    fn assert_parity<T>(request: &T, limits: &IngestLimits, accepted: bool)
    where
        T: Message + PreflightOtlp + ValidateOtlp,
    {
        let bytes = request.encode_to_vec();
        assert_eq!(T::preflight(&bytes, limits).is_ok(), accepted);
        assert_eq!(request.validate(limits).is_ok(), accepted);
    }

    fn counter_boundary<T>(request: &T, set_limit: impl Fn(&mut IngestLimits, usize)) -> usize
    where
        T: Message + PreflightOtlp + ValidateOtlp,
    {
        let bytes = request.encode_to_vec();
        for value in 0..=256 {
            let mut limits = permissive_limits();
            set_limit(&mut limits, value);
            let preflight = T::preflight(&bytes, &limits).is_ok();
            let decoded = request.validate(&limits).is_ok();
            assert_eq!(preflight, decoded, "budget diverged at {value}");
            if preflight {
                return value;
            }
        }
        panic!("request did not fit the differential test range");
    }

    fn attribute() -> KeyValue {
        KeyValue {
            key: "key".into(),
            value: Some(AnyValue {
                value: Some(any_value::Value::KvlistValue(KeyValueList {
                    values: vec![KeyValue {
                        key: "nested".into(),
                        value: Some(AnyValue {
                            value: Some(any_value::Value::StringValue("value".into())),
                        }),
                    }],
                })),
            }),
        }
    }

    fn wrap_message(tag: u64, payload: Vec<u8>) -> Vec<u8> {
        let mut encoded = Vec::with_capacity(payload.len() + 12);
        push_varint(&mut encoded, (tag << 3) | 2);
        push_varint(&mut encoded, payload.len() as u64);
        encoded.extend(payload);
        encoded
    }

    fn push_varint(encoded: &mut Vec<u8>, mut value: u64) {
        while value >= 0x80 {
            encoded.push((value as u8) | 0x80);
            value >>= 7;
        }
        encoded.push(value as u8);
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
            max_work_units: 100_000,
            max_any_value_depth: 32,
            max_value_bytes: 1_000,
        }
    }

    fn assert_work_budget(result: Result<(), PreflightError>, limit: usize) {
        assert!(matches!(
            result,
            Err(PreflightError::Budget(PolicyError::Budget {
                budget: "protobuf work unit",
                limit: actual,
            })) if actual == limit
        ));
    }

    fn assert_structure_budget(result: Result<(), PreflightError>, limit: usize) {
        assert!(matches!(
            result,
            Err(PreflightError::Budget(PolicyError::Budget {
                budget: "structural element",
                limit: actual,
            })) if actual == limit
        ));
    }
}
