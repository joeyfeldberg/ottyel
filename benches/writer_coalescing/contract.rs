use serde::Serialize;

pub(crate) const REPORT_SCHEMA_NAME: &str = "ottyel.writer_coalescing_benchmark";
pub(crate) const REPORT_SCHEMA_VERSION: u32 = 2;
pub(crate) const FIXTURE_GENERATOR_NAME: &str = "ottyel.writer_coalescing.trace_fixture";
pub(crate) const FIXTURE_GENERATOR_VERSION: u32 = 1;

pub(crate) const COALESCING_MAX_EXPORTS: usize = 4;
pub(crate) const COALESCING_MAX_PRIMARY_RECORDS: usize = 10_000;
pub(crate) const COALESCING_MAX_CANONICAL_BYTES: usize = 4 * 1024 * 1024;
pub(crate) const COALESCING_MAX_TRANSACTION_AGE_NS: u64 = 25_000_000;

pub(crate) const RETENTION_PASS_INTERVAL_NS: u64 = 30_000_000_000;
pub(crate) const RETENTION_ROWS_PER_UNIT: usize = 2_000;
pub(crate) const RETENTION_TRACES_PER_UNIT: usize = 20;

#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize)]
pub(crate) struct VersionedName {
    pub name: &'static str,
    pub version: u32,
}

pub(crate) fn report_schema() -> VersionedName {
    VersionedName {
        name: REPORT_SCHEMA_NAME,
        version: REPORT_SCHEMA_VERSION,
    }
}

pub(crate) fn fixture_generator() -> VersionedName {
    VersionedName {
        name: FIXTURE_GENERATOR_NAME,
        version: FIXTURE_GENERATOR_VERSION,
    }
}

/// The writer policies the measured binary runs.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize)]
pub(crate) struct WriterPolicies {
    pub coalescing: CoalescingPolicy,
    pub retention: RetentionPolicy,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize)]
pub(crate) struct CoalescingPolicy {
    pub identity: VersionedName,
    pub maximum_exports_per_transaction: usize,
    pub maximum_primary_records_per_transaction: usize,
    pub maximum_canonical_bytes_per_transaction: usize,
    pub maximum_transaction_age_ns: u64,
    pub collection: &'static str,
    pub intentional_collection_wait_ns: u64,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize)]
pub(crate) struct RetentionPolicy {
    pub identity: VersionedName,
    pub pass_interval_ns: u64,
    pub rows_per_unit: usize,
    pub traces_per_unit: usize,
    pub early_pass_trigger: &'static str,
    pub unit_boundary: &'static str,
}

impl WriterPolicies {
    pub(crate) fn pinned() -> Self {
        Self {
            coalescing: CoalescingPolicy {
                identity: VersionedName {
                    name: "opportunistic_adjacent_otlp",
                    version: 1,
                },
                maximum_exports_per_transaction: COALESCING_MAX_EXPORTS,
                maximum_primary_records_per_transaction: COALESCING_MAX_PRIMARY_RECORDS,
                maximum_canonical_bytes_per_transaction: COALESCING_MAX_CANONICAL_BYTES,
                maximum_transaction_age_ns: COALESCING_MAX_TRANSACTION_AGE_NS,
                collection: "already_ready_try_recv_only",
                intentional_collection_wait_ns: 0,
            },
            retention: RetentionPolicy {
                identity: VersionedName {
                    name: "scheduled_bounded_retention",
                    version: 1,
                },
                pass_interval_ns: RETENTION_PASS_INTERVAL_NS,
                rows_per_unit: RETENTION_ROWS_PER_UNIT,
                traces_per_unit: RETENTION_TRACES_PER_UNIT,
                early_pass_trigger: "max(1000, max_spans / 10) committed primary records",
                unit_boundary: "one_sqlite_transaction_per_unit_at_most_one_unit_per_writer_job",
            },
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize)]
pub(crate) struct GateRequirement {
    pub metric_path: &'static str,
    pub requirement: &'static str,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize)]
pub(crate) struct GateContract {
    pub identity: VersionedName,
    pub applicability: &'static str,
    pub requirements: [GateRequirement; 4],
    pub structural_requirement: &'static str,
}

impl GateContract {
    /// The retention gate predeclared in `docs/performance.md` before implementation.
    pub(crate) fn pinned() -> Self {
        Self {
            identity: VersionedName {
                name: "scheduled_retention_acceptance",
                version: 1,
            },
            applicability: "clean_reference_before_after_same_fixture_generator_report_schema_v1_or_v2",
            requirements: [
                GateRequirement {
                    metric_path: "measurements.low_rate.submission_attempt_to_completion_ack.p50_ns",
                    requirement: "candidate <= baseline * 0.10",
                },
                GateRequirement {
                    metric_path: "measurements.low_rate.submission_attempt_to_completion_ack.p95_ns",
                    requirement: "candidate <= baseline * 0.25",
                },
                GateRequirement {
                    metric_path: "measurements.burst.release_to_completion_ack.p95_ns",
                    requirement: "candidate <= baseline",
                },
                GateRequirement {
                    metric_path: "measurements.burst.records_per_second.p50",
                    requirement: "candidate >= baseline * 2",
                },
            ],
            structural_requirement: "no ingest group transaction contains retention work; maintenance units commit in their own transactions",
        }
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn schema_generator_policies_and_gate_are_fully_pinned() {
        assert_eq!(
            serde_json::to_value((
                super::report_schema(),
                super::fixture_generator(),
                super::WriterPolicies::pinned(),
                super::GateContract::pinned(),
            ))
            .unwrap(),
            serde_json::json!([
                {"name": "ottyel.writer_coalescing_benchmark", "version": 2},
                {"name": "ottyel.writer_coalescing.trace_fixture", "version": 1},
                {
                    "coalescing": {
                        "identity": {"name": "opportunistic_adjacent_otlp", "version": 1},
                        "maximum_exports_per_transaction": 4,
                        "maximum_primary_records_per_transaction": 10000,
                        "maximum_canonical_bytes_per_transaction": 4194304,
                        "maximum_transaction_age_ns": 25000000,
                        "collection": "already_ready_try_recv_only",
                        "intentional_collection_wait_ns": 0
                    },
                    "retention": {
                        "identity": {"name": "scheduled_bounded_retention", "version": 1},
                        "pass_interval_ns": 30000000000u64,
                        "rows_per_unit": 2000,
                        "traces_per_unit": 20,
                        "early_pass_trigger": "max(1000, max_spans / 10) committed primary records",
                        "unit_boundary": "one_sqlite_transaction_per_unit_at_most_one_unit_per_writer_job"
                    }
                },
                {
                    "identity": {"name": "scheduled_retention_acceptance", "version": 1},
                    "applicability": "clean_reference_before_after_same_fixture_generator_report_schema_v1_or_v2",
                    "requirements": [
                        {
                            "metric_path": "measurements.low_rate.submission_attempt_to_completion_ack.p50_ns",
                            "requirement": "candidate <= baseline * 0.10"
                        },
                        {
                            "metric_path": "measurements.low_rate.submission_attempt_to_completion_ack.p95_ns",
                            "requirement": "candidate <= baseline * 0.25"
                        },
                        {
                            "metric_path": "measurements.burst.release_to_completion_ack.p95_ns",
                            "requirement": "candidate <= baseline"
                        },
                        {
                            "metric_path": "measurements.burst.records_per_second.p50",
                            "requirement": "candidate >= baseline * 2"
                        }
                    ],
                    "structural_requirement": "no ingest group transaction contains retention work; maintenance units commit in their own transactions"
                }
            ])
        );
    }

    #[test]
    fn pinned_retention_policy_matches_the_library() {
        let library = ottyel::store::benchmark_support::retention_policy();
        let pinned = super::WriterPolicies::pinned().retention;
        assert_eq!(
            (
                library.pass_interval.as_nanos() as u64,
                library.rows_per_unit,
                library.traces_per_unit
            ),
            (
                pinned.pass_interval_ns,
                pinned.rows_per_unit,
                pinned.traces_per_unit
            )
        );
    }
}
