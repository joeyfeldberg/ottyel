use serde::Serialize;

pub(crate) const REPORT_SCHEMA_NAME: &str = "ottyel.writer_coalescing_benchmark";
pub(crate) const REPORT_SCHEMA_VERSION: u32 = 1;
pub(crate) const FIXTURE_GENERATOR_NAME: &str = "ottyel.writer_coalescing.trace_fixture";
pub(crate) const FIXTURE_GENERATOR_VERSION: u32 = 1;
pub(crate) const CANDIDATE_POLICY_NAME: &str = "opportunistic_adjacent_otlp";
pub(crate) const CANDIDATE_POLICY_VERSION: u32 = 1;
pub(crate) const GATE_POLICY_NAME: &str = "writer_coalescing_acceptance";
pub(crate) const GATE_POLICY_VERSION: u32 = 1;

pub(crate) const CANDIDATE_MAX_EXPORTS: usize = 4;
pub(crate) const CANDIDATE_MAX_PRIMARY_RECORDS: usize = 10_000;
pub(crate) const CANDIDATE_MAX_CANONICAL_BYTES: usize = 4 * 1024 * 1024;
pub(crate) const CANDIDATE_MAX_TRANSACTION_AGE_NS: u64 = 25_000_000;
pub(crate) const CANDIDATE_INTENTIONAL_COLLECTION_WAIT_NS: u64 = 0;

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

#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize)]
pub(crate) struct CandidatePolicy {
    pub identity: VersionedName,
    pub maximum_exports_per_transaction: usize,
    pub maximum_primary_records_per_transaction: usize,
    pub maximum_canonical_bytes_per_transaction: usize,
    pub maximum_transaction_age_ns: u64,
    pub transaction_age_check: &'static str,
    pub collection: &'static str,
    pub intentional_collection_wait_ns: u64,
    pub retention_boundary: &'static str,
    pub singleton_over_cap_behavior: &'static str,
    pub boundary_behavior: &'static str,
}

impl CandidatePolicy {
    pub(crate) fn pinned() -> Self {
        Self {
            identity: VersionedName {
                name: CANDIDATE_POLICY_NAME,
                version: CANDIDATE_POLICY_VERSION,
            },
            maximum_exports_per_transaction: CANDIDATE_MAX_EXPORTS,
            maximum_primary_records_per_transaction: CANDIDATE_MAX_PRIMARY_RECORDS,
            maximum_canonical_bytes_per_transaction: CANDIDATE_MAX_CANONICAL_BYTES,
            maximum_transaction_age_ns: CANDIDATE_MAX_TRANSACTION_AGE_NS,
            transaction_age_check: "cooperative_between_jobs_only",
            collection: "already_ready_try_recv_only",
            intentional_collection_wait_ns: CANDIDATE_INTENTIONAL_COLLECTION_WAIT_NS,
            retention_boundary: "inside_shared_ingest_transaction_before_commit",
            singleton_over_cap_behavior: "execute_admitted_singleton_alone",
            boundary_behavior: "inclusive_exact_export_record_byte_and_observed_age_boundaries_join",
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize)]
pub(crate) struct GateMetricPaths {
    pub burst_sqlite_transactions_committed: &'static str,
    pub burst_retention_invocations: &'static str,
    pub burst_ack_p95_ns: &'static str,
    pub low_rate_ack_p95_ns: &'static str,
    pub burst_records_per_second_p50: &'static str,
    pub burst_retention_elapsed_p50_ns: &'static str,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize)]
pub(crate) struct GateFormulas {
    pub total_sqlite_commit_reduction_percent: &'static str,
    pub retention_invocation_reduction_percent: &'static str,
    pub median_records_per_second_ratio: &'static str,
    pub median_retention_elapsed_reduction_percent: &'static str,
    pub burst_ack_regression_percent: &'static str,
    pub low_rate_allowed_increase_ns: &'static str,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize)]
pub(crate) struct GateContract {
    pub identity: VersionedName,
    pub applicability: &'static str,
    pub metric_paths: GateMetricPaths,
    pub formulas: GateFormulas,
}

impl GateContract {
    pub(crate) fn pinned() -> Self {
        Self {
            identity: VersionedName {
                name: GATE_POLICY_NAME,
                version: GATE_POLICY_VERSION,
            },
            applicability: "schema_v1_same_fixture_generator_clean_reference_before_after_only",
            metric_paths: GateMetricPaths {
                burst_sqlite_transactions_committed: "measurements.burst.counters.sqlite_transactions_committed",
                burst_retention_invocations: "measurements.burst.counters.retention_invocations",
                burst_ack_p95_ns: "measurements.burst.release_to_completion_ack.p95_ns",
                low_rate_ack_p95_ns: "measurements.low_rate.submission_attempt_to_completion_ack.p95_ns",
                burst_records_per_second_p50: "measurements.burst.records_per_second.p50",
                burst_retention_elapsed_p50_ns: "measurements.burst.retention_elapsed_per_burst.p50_ns",
            },
            formulas: GateFormulas {
                total_sqlite_commit_reduction_percent: "(baseline_total_sqlite_committed - candidate_total_sqlite_committed) / baseline_total_sqlite_committed * 100",
                retention_invocation_reduction_percent: "(baseline_retention_invocations - candidate_retention_invocations) / baseline_retention_invocations * 100",
                median_records_per_second_ratio: "candidate_burst_records_per_second_p50 / baseline_burst_records_per_second_p50",
                median_retention_elapsed_reduction_percent: "(baseline_retention_elapsed_p50_ns - candidate_retention_elapsed_p50_ns) / baseline_retention_elapsed_p50_ns * 100",
                burst_ack_regression_percent: "(candidate_burst_ack_p95_ns - baseline_burst_ack_p95_ns) / baseline_burst_ack_p95_ns * 100",
                low_rate_allowed_increase_ns: "max(1000000, baseline_low_rate_ack_p95_ns * 0.10)",
            },
        }
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn schema_generator_candidate_and_gate_contract_are_fully_pinned() {
        assert_eq!(super::REPORT_SCHEMA_VERSION, 1);
        assert_eq!(super::FIXTURE_GENERATOR_VERSION, 1);
        assert_eq!(
            serde_json::to_value((
                super::report_schema(),
                super::fixture_generator(),
                super::CandidatePolicy::pinned(),
                super::GateContract::pinned(),
            ))
            .unwrap(),
            serde_json::json!([
                {
                    "name": "ottyel.writer_coalescing_benchmark",
                    "version": 1
                },
                {
                    "name": "ottyel.writer_coalescing.trace_fixture",
                    "version": 1
                },
                {
                    "identity": {
                        "name": "opportunistic_adjacent_otlp",
                        "version": 1
                    },
                    "maximum_exports_per_transaction": 4,
                    "maximum_primary_records_per_transaction": 10000,
                    "maximum_canonical_bytes_per_transaction": 4194304,
                    "maximum_transaction_age_ns": 25000000,
                    "transaction_age_check": "cooperative_between_jobs_only",
                    "collection": "already_ready_try_recv_only",
                    "intentional_collection_wait_ns": 0,
                    "retention_boundary": "inside_shared_ingest_transaction_before_commit",
                    "singleton_over_cap_behavior": "execute_admitted_singleton_alone",
                    "boundary_behavior": "inclusive_exact_export_record_byte_and_observed_age_boundaries_join"
                },
                {
                    "identity": {
                        "name": "writer_coalescing_acceptance",
                        "version": 1
                    },
                    "applicability": "schema_v1_same_fixture_generator_clean_reference_before_after_only",
                    "metric_paths": {
                        "burst_sqlite_transactions_committed": "measurements.burst.counters.sqlite_transactions_committed",
                        "burst_retention_invocations": "measurements.burst.counters.retention_invocations",
                        "burst_ack_p95_ns": "measurements.burst.release_to_completion_ack.p95_ns",
                        "low_rate_ack_p95_ns": "measurements.low_rate.submission_attempt_to_completion_ack.p95_ns",
                        "burst_records_per_second_p50": "measurements.burst.records_per_second.p50",
                        "burst_retention_elapsed_p50_ns": "measurements.burst.retention_elapsed_per_burst.p50_ns"
                    },
                    "formulas": {
                        "total_sqlite_commit_reduction_percent": "(baseline_total_sqlite_committed - candidate_total_sqlite_committed) / baseline_total_sqlite_committed * 100",
                        "retention_invocation_reduction_percent": "(baseline_retention_invocations - candidate_retention_invocations) / baseline_retention_invocations * 100",
                        "median_records_per_second_ratio": "candidate_burst_records_per_second_p50 / baseline_burst_records_per_second_p50",
                        "median_retention_elapsed_reduction_percent": "(baseline_retention_elapsed_p50_ns - candidate_retention_elapsed_p50_ns) / baseline_retention_elapsed_p50_ns * 100",
                        "burst_ack_regression_percent": "(candidate_burst_ack_p95_ns - baseline_burst_ack_p95_ns) / baseline_burst_ack_p95_ns * 100",
                        "low_rate_allowed_increase_ns": "max(1000000, baseline_low_rate_ack_p95_ns * 0.10)"
                    }
                }
            ])
        );
    }
}
