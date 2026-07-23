use anyhow::{Context, Result, ensure};
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use prost::Message;
use serde::Serialize;

pub(crate) const FIXTURE_BYTES: usize = 4 * 1024 * 1024;
pub(crate) const FIXTURE_GENERATOR_VERSION: u32 = 1;
const MAX_GROUP_DEPTH: usize = 100;
const UNKNOWN_FIELD_KEY: u8 = 0x10;
const UNKNOWN_LENGTH_DELIMITED_KEY: u8 = 0x12;
const UNKNOWN_START_GROUP_KEY: u8 = 0x13;
const UNKNOWN_END_GROUP_KEY: u8 = 0x14;
const SPAN_DROPPED_ATTRIBUTES_KEY: u8 = 0x50;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Classification {
    Control,
    Adversarial,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub(crate) struct DecodedCardinality {
    pub resource_spans: usize,
    pub scope_spans: usize,
    pub spans: usize,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct FixtureMetadata {
    pub name: &'static str,
    pub classification: Classification,
    pub description: &'static str,
    pub encoded_bytes: usize,
    pub wire_field_keys: usize,
    pub semantic_group_pairs: usize,
    pub target_field_occurrences: usize,
    pub maximum_group_depth: usize,
    pub expected_primary_records: usize,
    pub expected_structural_elements: usize,
    pub expected_decoded: DecodedCardinality,
}

#[derive(Debug)]
pub(crate) struct Fixture {
    pub metadata: FixtureMetadata,
    pub bytes: Vec<u8>,
}

struct OccurrenceCounts {
    wire_field_keys: usize,
    semantic_group_pairs: usize,
    target_field_occurrences: usize,
}

struct ExpectedPolicyShape {
    maximum_group_depth: usize,
    primary_records: usize,
    structural_elements: usize,
}

pub(crate) fn build_all() -> Result<Vec<Fixture>> {
    let fixtures = vec![
        unknown_blob_control(),
        unknown_varint_zero_fields(),
        unknown_long_varint_fields(),
        unknown_depth_100_groups(),
        known_duplicate_span_scalars(),
    ];
    for fixture in &fixtures {
        fixture.verify()?;
    }
    Ok(fixtures)
}

impl Fixture {
    fn verify(&self) -> Result<()> {
        ensure!(
            self.bytes.len() == FIXTURE_BYTES,
            "{} is {} bytes instead of {FIXTURE_BYTES}",
            self.metadata.name,
            self.bytes.len()
        );
        ensure!(
            self.metadata.encoded_bytes == self.bytes.len(),
            "{} metadata has the wrong encoded size",
            self.metadata.name
        );
        let decoded = ExportTraceServiceRequest::decode(self.bytes.as_slice())
            .with_context(|| format!("{} does not Prost-decode", self.metadata.name))?;
        ensure!(
            decoded_cardinality(&decoded) == self.metadata.expected_decoded,
            "{} decoded cardinality changed",
            self.metadata.name
        );
        if self.metadata.name == "known_duplicate_span_scalars" {
            let span = &decoded.resource_spans[0].scope_spans[0].spans[0];
            ensure!(
                span.dropped_attributes_count == 128,
                "the last duplicate Span scalar was not retained"
            );
        }
        Ok(())
    }
}

fn unknown_blob_control() -> Fixture {
    let blob_len = FIXTURE_BYTES - 5;
    let mut bytes = Vec::with_capacity(FIXTURE_BYTES);
    bytes.push(UNKNOWN_LENGTH_DELIMITED_KEY);
    encode_varint(blob_len as u64, &mut bytes);
    bytes.resize(FIXTURE_BYTES, 0);
    fixture(
        "unknown_length_delimited_blob",
        Classification::Control,
        "one unknown top-level length-delimited field; a same-size low-dispatch control",
        OccurrenceCounts {
            wire_field_keys: 1,
            semantic_group_pairs: 0,
            target_field_occurrences: 1,
        },
        ExpectedPolicyShape::empty(),
        empty_cardinality(),
        bytes,
    )
}

fn unknown_varint_zero_fields() -> Fixture {
    let occurrences = FIXTURE_BYTES / 2;
    let mut bytes = Vec::with_capacity(FIXTURE_BYTES);
    for _ in 0..occurrences {
        bytes.extend_from_slice(&[UNKNOWN_FIELD_KEY, 0]);
    }
    fixture(
        "unknown_varint_zero_fields",
        Classification::Adversarial,
        "repeated two-byte unknown top-level fields with a one-byte zero varint value",
        OccurrenceCounts {
            wire_field_keys: occurrences,
            semantic_group_pairs: 0,
            target_field_occurrences: occurrences,
        },
        ExpectedPolicyShape::empty(),
        empty_cardinality(),
        bytes,
    )
}

fn unknown_long_varint_fields() -> Fixture {
    const FULL_FIELD_BYTES: usize = 11;
    let full_occurrences = FIXTURE_BYTES / FULL_FIELD_BYTES;
    let mut bytes = Vec::with_capacity(FIXTURE_BYTES);
    for _ in 0..full_occurrences {
        bytes.push(UNKNOWN_FIELD_KEY);
        bytes.extend_from_slice(&[0xff; 9]);
        bytes.push(0x01);
    }
    // Four remaining bytes encode one final valid three-byte varint.
    bytes.extend_from_slice(&[UNKNOWN_FIELD_KEY, 0x80, 0x80, 0x01]);
    fixture(
        "unknown_long_varint_fields",
        Classification::Adversarial,
        "unknown top-level fields with maximum-length valid u64 varints plus one exact-size tail",
        OccurrenceCounts {
            wire_field_keys: full_occurrences + 1,
            semantic_group_pairs: 0,
            target_field_occurrences: full_occurrences + 1,
        },
        ExpectedPolicyShape::empty(),
        empty_cardinality(),
        bytes,
    )
}

fn unknown_depth_100_groups() -> Fixture {
    let group_pairs = FIXTURE_BYTES / 2;
    let full_chains = group_pairs / MAX_GROUP_DEPTH;
    let tail_depth = group_pairs % MAX_GROUP_DEPTH;
    let mut bytes = Vec::with_capacity(FIXTURE_BYTES);
    for _ in 0..full_chains {
        append_group_chain(&mut bytes, MAX_GROUP_DEPTH);
    }
    append_group_chain(&mut bytes, tail_depth);
    fixture(
        "unknown_groups_depth_100",
        Classification::Adversarial,
        "repeated unknown groups nested to the decoder limit, followed by one valid tail chain",
        OccurrenceCounts {
            wire_field_keys: group_pairs * 2,
            semantic_group_pairs: group_pairs,
            target_field_occurrences: group_pairs,
        },
        ExpectedPolicyShape {
            maximum_group_depth: MAX_GROUP_DEPTH,
            primary_records: 0,
            structural_elements: 0,
        },
        empty_cardinality(),
        bytes,
    )
}

fn known_duplicate_span_scalars() -> Fixture {
    // Three one-byte length-delimited keys and three four-byte lengths leave this exact Span body.
    const SPAN_BODY_BYTES: usize = FIXTURE_BYTES - 15;
    let occurrences = (SPAN_BODY_BYTES - 1) / 2;
    let mut span = Vec::with_capacity(SPAN_BODY_BYTES);
    for _ in 0..occurrences - 1 {
        span.extend_from_slice(&[SPAN_DROPPED_ATTRIBUTES_KEY, 0]);
    }
    // The extra value byte makes the complete nested request exactly 4 MiB. Prost retains 128.
    span.extend_from_slice(&[SPAN_DROPPED_ATTRIBUTES_KEY, 0x80, 0x01]);
    debug_assert_eq!(span.len(), SPAN_BODY_BYTES);

    let scope_spans = wrap_message(2, &span);
    let resource_spans = wrap_message(2, &scope_spans);
    let bytes = wrap_message(1, &resource_spans);
    fixture(
        "known_duplicate_span_scalars",
        Classification::Adversarial,
        "duplicate Span tag 10 dropped_attributes_count scalars inside one minimal trace envelope",
        OccurrenceCounts {
            wire_field_keys: occurrences + 3,
            semantic_group_pairs: 0,
            target_field_occurrences: occurrences,
        },
        ExpectedPolicyShape {
            maximum_group_depth: 0,
            primary_records: 1,
            structural_elements: 3,
        },
        DecodedCardinality {
            resource_spans: 1,
            scope_spans: 1,
            spans: 1,
        },
        bytes,
    )
}

fn fixture(
    name: &'static str,
    classification: Classification,
    description: &'static str,
    occurrences: OccurrenceCounts,
    expected_policy: ExpectedPolicyShape,
    expected_decoded: DecodedCardinality,
    bytes: Vec<u8>,
) -> Fixture {
    Fixture {
        metadata: FixtureMetadata {
            name,
            classification,
            description,
            encoded_bytes: bytes.len(),
            wire_field_keys: occurrences.wire_field_keys,
            semantic_group_pairs: occurrences.semantic_group_pairs,
            target_field_occurrences: occurrences.target_field_occurrences,
            maximum_group_depth: expected_policy.maximum_group_depth,
            expected_primary_records: expected_policy.primary_records,
            expected_structural_elements: expected_policy.structural_elements,
            expected_decoded,
        },
        bytes,
    }
}

impl ExpectedPolicyShape {
    fn empty() -> Self {
        Self {
            maximum_group_depth: 0,
            primary_records: 0,
            structural_elements: 0,
        }
    }
}

fn append_group_chain(bytes: &mut Vec<u8>, depth: usize) {
    bytes.extend(std::iter::repeat_n(UNKNOWN_START_GROUP_KEY, depth));
    bytes.extend(std::iter::repeat_n(UNKNOWN_END_GROUP_KEY, depth));
}

fn wrap_message(field: u32, body: &[u8]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(body.len() + 5);
    encode_varint(u64::from((field << 3) | 2), &mut bytes);
    encode_varint(body.len() as u64, &mut bytes);
    bytes.extend_from_slice(body);
    bytes
}

fn encode_varint(mut value: u64, output: &mut Vec<u8>) {
    while value >= 0x80 {
        output.push((value as u8 & 0x7f) | 0x80);
        value >>= 7;
    }
    output.push(value as u8);
}

fn decoded_cardinality(request: &ExportTraceServiceRequest) -> DecodedCardinality {
    DecodedCardinality {
        resource_spans: request.resource_spans.len(),
        scope_spans: request
            .resource_spans
            .iter()
            .map(|resource| resource.scope_spans.len())
            .sum(),
        spans: request
            .resource_spans
            .iter()
            .flat_map(|resource| &resource.scope_spans)
            .map(|scope| scope.spans.len())
            .sum(),
    }
}

fn empty_cardinality() -> DecodedCardinality {
    DecodedCardinality {
        resource_spans: 0,
        scope_spans: 0,
        spans: 0,
    }
}

#[cfg(test)]
mod tests {
    use super::{FIXTURE_BYTES, build_all};

    #[test]
    fn fixtures_are_exact_and_decode_as_declared() {
        let fixtures = build_all().unwrap();

        assert_eq!(fixtures.len(), 5);
        for fixture in fixtures {
            assert_eq!(
                fixture.bytes.len(),
                FIXTURE_BYTES,
                "{}",
                fixture.metadata.name
            );
            fixture.verify().unwrap();
        }
    }

    #[test]
    fn group_keys_and_pairs_are_not_conflated() {
        let fixtures = build_all().unwrap();
        let groups = fixtures
            .iter()
            .find(|fixture| fixture.metadata.name == "unknown_groups_depth_100")
            .unwrap();

        assert_eq!(
            groups.metadata.wire_field_keys,
            groups.metadata.semantic_group_pairs * 2
        );
        assert_eq!(groups.metadata.semantic_group_pairs, FIXTURE_BYTES / 2);
        assert_eq!(groups.metadata.maximum_group_depth, 100);
        assert_eq!(groups.metadata.expected_primary_records, 0);
        assert_eq!(groups.metadata.expected_structural_elements, 0);
    }

    #[test]
    fn scalar_occurrence_counts_match_wire_shapes() {
        let fixtures = build_all().unwrap();
        let zeroes = fixtures
            .iter()
            .find(|fixture| fixture.metadata.name == "unknown_varint_zero_fields")
            .unwrap();
        assert_eq!(zeroes.metadata.wire_field_keys, FIXTURE_BYTES / 2);

        let long = fixtures
            .iter()
            .find(|fixture| fixture.metadata.name == "unknown_long_varint_fields")
            .unwrap();
        assert_eq!(long.metadata.wire_field_keys, FIXTURE_BYTES / 11 + 1);

        let known = fixtures
            .iter()
            .find(|fixture| fixture.metadata.name == "known_duplicate_span_scalars")
            .unwrap();
        assert_eq!(
            known.metadata.wire_field_keys,
            known.metadata.target_field_occurrences + 3
        );
        assert_eq!(known.metadata.maximum_group_depth, 0);
        assert_eq!(known.metadata.expected_primary_records, 1);
        assert_eq!(known.metadata.expected_structural_elements, 3);
    }
}
