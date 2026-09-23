use std::{
    fs,
    path::{Path, PathBuf},
    process::Command,
};

use anyhow::{Context, Result};
use serde::Serialize;

use crate::{
    bench_config::{RunConfig, Sampling, Scale},
    contract::{GateContract, VersionedName, WriterPolicies, fixture_generator, report_schema},
    data::SeededStore,
    measurement::Measurements,
    runner::{EXPORTS_PER_BURST, RECORDS_PER_BURST, RECORDS_PER_EXPORT},
};

#[derive(Debug, Serialize)]
pub(crate) struct BenchmarkReport {
    schema: VersionedName,
    generated_at_utc: String,
    profile: super::bench_config::Profile,
    environment: Environment,
    fixture: Fixture,
    sampling: Sampling,
    writer_policies: WriterPolicies,
    acknowledgement_semantics: AcknowledgementSemantics,
    counter_semantics: CounterSemantics,
    predeclared_gate: PredeclaredGate,
    setup_duration_ns: u64,
    database_bytes: u64,
    wal_bytes: u64,
    measurements: Measurements,
}

#[derive(Debug, Serialize)]
struct Environment {
    os: &'static str,
    architecture: &'static str,
    logical_cpus: usize,
    git_revision: Option<String>,
    git_dirty: Option<bool>,
    machine_label: Option<String>,
    cpu: Option<String>,
    memory_gib: Option<u64>,
    storage_label: Option<String>,
    rustc_version: Option<String>,
    rust_debug_assertions: bool,
}

#[derive(Debug, Serialize)]
struct Fixture {
    generator: VersionedName,
    profile_scale: Scale,
    concurrent_exports: usize,
    records_per_export: usize,
    records_per_burst: usize,
    low_rate_records_per_export: usize,
    timestamps_are_future_and_unexpired: bool,
    retention_may_trim: bool,
    request_preparation_timed: bool,
}

#[derive(Debug, Serialize)]
struct AcknowledgementSemantics {
    completion_acknowledges: &'static str,
    sqlite_journal_mode: &'static str,
    sqlite_synchronous: &'static str,
    power_loss_fsync_claim: bool,
    throughput_definition: &'static str,
}

#[derive(Debug, Serialize)]
struct CounterSemantics {
    transaction_kinds: &'static str,
    sqlite_transaction_totals: &'static str,
    transaction_not_committed: &'static str,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize)]
struct ReferenceEligibility {
    profile_is_reference: bool,
    optimized_release_build: bool,
    git_revision_present: bool,
    git_state_known: bool,
    git_is_clean: bool,
    rustc_version_present: bool,
    machine_label_present: bool,
    cpu_present: bool,
    memory_gib_present: bool,
    storage_label_present: bool,
    complete_machine_identity: bool,
    eligible: bool,
}

#[derive(Debug, Serialize)]
struct ComparisonEvaluation {
    applicable_to_current_report: bool,
    inapplicability_reason: &'static str,
    passed: Option<bool>,
}

#[derive(Debug, Serialize)]
struct PredeclaredGate {
    contract: GateContract,
    evaluation: &'static str,
    required_clean_reference_runs: usize,
    comparison_requirements: &'static str,
    structural_requirements: &'static str,
    current_measurement_structural_assertions_passed: bool,
    reference_eligibility: ReferenceEligibility,
    comparison: ComparisonEvaluation,
}

impl BenchmarkReport {
    pub(crate) fn build(
        config: &RunConfig,
        seeded: &SeededStore,
        measurements: Measurements,
    ) -> Result<Self> {
        let environment = Environment::detect(config);
        let reference_eligibility = ReferenceEligibility::derive(config, &environment);
        let structural_assertions_passed = measurements.structural_assertions_passed;
        Ok(Self {
            schema: report_schema(),
            generated_at_utc: chrono::Utc::now().to_rfc3339(),
            profile: config.profile,
            environment,
            fixture: Fixture {
                generator: fixture_generator(),
                profile_scale: config.profile.scale(),
                concurrent_exports: EXPORTS_PER_BURST,
                records_per_export: RECORDS_PER_EXPORT,
                records_per_burst: RECORDS_PER_BURST,
                low_rate_records_per_export: 1,
                timestamps_are_future_and_unexpired: true,
                retention_may_trim: false,
                request_preparation_timed: false,
            },
            sampling: config.profile.sampling(),
            writer_policies: WriterPolicies::pinned(),
            acknowledgement_semantics: AcknowledgementSemantics {
                completion_acknowledges: "the ingest group COMMIT and writer receipt delivery; retention runs in separate maintenance transactions",
                sqlite_journal_mode: "WAL",
                sqlite_synchronous: "NORMAL",
                power_loss_fsync_claim: false,
                throughput_definition: "records or exports divided by writer-release-to-last-receipt burst makespan",
            },
            counter_semantics: CounterSemantics {
                transaction_kinds: "every SQLite transaction is exactly one ingest group or one maintenance unit",
                sqlite_transaction_totals: "total SQLite transaction counters equal the ingest group plus maintenance counters",
                transaction_not_committed: "the transaction guard ended without observing a successful COMMIT; this counter does not prove that rollback succeeded",
            },
            predeclared_gate: PredeclaredGate::candidate(
                structural_assertions_passed,
                reference_eligibility,
            ),
            setup_duration_ns: duration_nanos(seeded.setup_duration),
            database_bytes: file_size(&seeded.database_path)?,
            wal_bytes: file_size(&wal_path(&seeded.database_path))?,
            measurements,
        })
    }

    pub(crate) fn write(&self, output: &Path) -> Result<()> {
        if let Some(parent) = output
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
        {
            fs::create_dir_all(parent)
                .with_context(|| format!("failed to create {}", parent.display()))?;
        }
        let json = serde_json::to_vec_pretty(self)?;
        fs::write(output, json).with_context(|| format!("failed to write {}", output.display()))
    }

    pub(crate) fn print_summary(&self, output: &Path) {
        println!(
            "writer coalescing: profile={:?} setup={:.3}s db={}B wal={}B",
            self.profile,
            self.setup_duration_ns as f64 / 1_000_000_000.0,
            self.database_bytes,
            self.wal_bytes
        );
        println!(
            "  burst n={} p50={:.3}ms p95={:.3}ms p50_records/s={:.1}",
            self.measurements.burst.makespan.count,
            self.measurements.burst.makespan.p50_ns as f64 / 1_000_000.0,
            self.measurements.burst.makespan.p95_ns as f64 / 1_000_000.0,
            self.measurements.burst.records_per_second.p50,
        );
        println!(
            "  low-rate n={} p50={:.3}ms p95={:.3}ms",
            self.measurements
                .low_rate
                .submission_attempt_to_completion_ack
                .count,
            self.measurements
                .low_rate
                .submission_attempt_to_completion_ack
                .p50_ns as f64
                / 1_000_000.0,
            self.measurements
                .low_rate
                .submission_attempt_to_completion_ack
                .p95_ns as f64
                / 1_000_000.0,
        );
        println!("report: {}", output.display());
    }
}

impl Environment {
    fn detect(config: &RunConfig) -> Self {
        Self {
            os: std::env::consts::OS,
            architecture: std::env::consts::ARCH,
            logical_cpus: std::thread::available_parallelism().map_or(1, usize::from),
            git_revision: command_output("git", &["rev-parse", "HEAD"]),
            git_dirty: command_output("git", &["status", "--porcelain"])
                .map(|output| !output.is_empty()),
            machine_label: config.machine_label.clone(),
            cpu: config.cpu.clone(),
            memory_gib: config.memory_gib,
            storage_label: config.storage_label.clone(),
            rustc_version: command_output("rustc", &["--version"]),
            rust_debug_assertions: cfg!(debug_assertions),
        }
    }
}

impl ReferenceEligibility {
    fn derive(config: &RunConfig, environment: &Environment) -> Self {
        let profile_is_reference = config.profile == super::bench_config::Profile::Reference;
        let optimized_release_build = !environment.rust_debug_assertions;
        let git_revision_present = present(environment.git_revision.as_deref());
        let git_state_known = environment.git_dirty.is_some();
        let git_is_clean = environment.git_dirty == Some(false);
        let rustc_version_present = present(environment.rustc_version.as_deref());
        let machine_label_present = present(environment.machine_label.as_deref());
        let cpu_present = present(environment.cpu.as_deref());
        let memory_gib_present = environment.memory_gib.is_some_and(|memory| memory > 0);
        let storage_label_present = present(environment.storage_label.as_deref());
        let complete_machine_identity =
            machine_label_present && cpu_present && memory_gib_present && storage_label_present;
        Self {
            profile_is_reference,
            optimized_release_build,
            git_revision_present,
            git_state_known,
            git_is_clean,
            rustc_version_present,
            machine_label_present,
            cpu_present,
            memory_gib_present,
            storage_label_present,
            complete_machine_identity,
            eligible: profile_is_reference
                && optimized_release_build
                && git_revision_present
                && git_state_known
                && git_is_clean
                && rustc_version_present
                && complete_machine_identity,
        }
    }
}

impl PredeclaredGate {
    fn candidate(
        structural_assertions_passed: bool,
        reference_eligibility: ReferenceEligibility,
    ) -> Self {
        Self {
            contract: GateContract::pinned(),
            evaluation: "candidate_capture",
            required_clean_reference_runs: 2,
            comparison_requirements: "two consecutive clean release before/after comparisons on the same named machine, storage class, and Rust toolchain",
            structural_requirements: "per sample: one ingest group of all submitted exports commits exactly once; interleaved maintenance units commit in their own transactions with no failures; every transaction not_committed counter is 0; all receipts acknowledge every record; and persisted span rows match exactly",
            current_measurement_structural_assertions_passed: structural_assertions_passed,
            reference_eligibility,
            comparison: ComparisonEvaluation {
                applicable_to_current_report: false,
                inapplicability_reason: "one capture cannot compare itself; evaluate the gate across recorded baseline and candidate reports",
                passed: None,
            },
        }
    }
}

fn present(value: Option<&str>) -> bool {
    value.is_some_and(|value| !value.trim().is_empty())
}

fn command_output(program: &str, args: &[&str]) -> Option<String> {
    let output = Command::new(program).args(args).output().ok()?;
    output
        .status
        .success()
        .then(|| String::from_utf8_lossy(&output.stdout).trim().to_string())
}

fn file_size(path: &Path) -> Result<u64> {
    match fs::metadata(path) {
        Ok(metadata) => Ok(metadata.len()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(0),
        Err(error) => Err(error).with_context(|| format!("failed to inspect {}", path.display())),
    }
}

fn wal_path(database_path: &Path) -> PathBuf {
    let mut path = database_path.as_os_str().to_os_string();
    path.push("-wal");
    PathBuf::from(path)
}

fn duration_nanos(duration: std::time::Duration) -> u64 {
    duration.as_nanos().min(u128::from(u64::MAX)) as u64
}

#[cfg(test)]
mod tests {
    #[test]
    fn serialized_candidate_gate_shape_is_complete_and_comparison_is_inapplicable() {
        let eligibility = super::ReferenceEligibility {
            profile_is_reference: false,
            optimized_release_build: true,
            git_revision_present: true,
            git_state_known: true,
            git_is_clean: false,
            rustc_version_present: true,
            machine_label_present: false,
            cpu_present: false,
            memory_gib_present: false,
            storage_label_present: false,
            complete_machine_identity: false,
            eligible: false,
        };
        let gate = super::PredeclaredGate::candidate(true, eligibility);
        assert_eq!(
            serde_json::to_value(gate).unwrap(),
            serde_json::json!({
                "contract": serde_json::to_value(super::GateContract::pinned()).unwrap(),
                "evaluation": "candidate_capture",
                "required_clean_reference_runs": 2,
                "comparison_requirements": "two consecutive clean release before/after comparisons on the same named machine, storage class, and Rust toolchain",
                "structural_requirements": "per sample: one ingest group of all submitted exports commits exactly once; interleaved maintenance units commit in their own transactions with no failures; every transaction not_committed counter is 0; all receipts acknowledge every record; and persisted span rows match exactly",
                "current_measurement_structural_assertions_passed": true,
                "reference_eligibility": {
                    "profile_is_reference": false,
                    "optimized_release_build": true,
                    "git_revision_present": true,
                    "git_state_known": true,
                    "git_is_clean": false,
                    "rustc_version_present": true,
                    "machine_label_present": false,
                    "cpu_present": false,
                    "memory_gib_present": false,
                    "storage_label_present": false,
                    "complete_machine_identity": false,
                    "eligible": false
                },
                "comparison": {
                    "applicable_to_current_report": false,
                    "inapplicability_reason": "one capture cannot compare itself; evaluate the gate across recorded baseline and candidate reports",
                    "passed": null
                }
            })
        );
    }

    #[test]
    fn reference_eligibility_requires_clean_release_and_complete_storage_identity() {
        let config = super::RunConfig {
            profile: super::super::bench_config::Profile::Reference,
            output: "/tmp/report.json".into(),
            machine_label: Some("stable-host".to_string()),
            cpu: Some("Example CPU".to_string()),
            memory_gib: Some(32),
            storage_label: Some("internal-nvme".to_string()),
        };
        let environment = super::Environment {
            os: "test",
            architecture: "test",
            logical_cpus: 1,
            git_revision: Some("revision".to_string()),
            git_dirty: Some(false),
            machine_label: config.machine_label.clone(),
            cpu: config.cpu.clone(),
            memory_gib: config.memory_gib,
            storage_label: config.storage_label.clone(),
            rustc_version: Some("rustc test".to_string()),
            rust_debug_assertions: false,
        };
        let eligible = super::ReferenceEligibility::derive(&config, &environment);
        assert!(eligible.eligible);

        let dirty = super::Environment {
            git_dirty: Some(true),
            ..environment
        };
        assert!(!super::ReferenceEligibility::derive(&config, &dirty).eligible);

        let missing_revision = super::Environment {
            git_revision: None,
            git_dirty: Some(false),
            ..dirty
        };
        assert!(!super::ReferenceEligibility::derive(&config, &missing_revision).eligible);

        let missing_rustc = super::Environment {
            git_revision: Some("revision".to_string()),
            rustc_version: None,
            ..missing_revision
        };
        assert!(!super::ReferenceEligibility::derive(&config, &missing_rustc).eligible);

        let whitespace_machine = super::Environment {
            machine_label: Some(" \t\n ".to_string()),
            rustc_version: Some("rustc test".to_string()),
            ..missing_rustc
        };
        let eligibility = super::ReferenceEligibility::derive(&config, &whitespace_machine);
        assert!(!eligibility.machine_label_present);
        assert!(!eligibility.complete_machine_identity);
        assert!(!eligibility.eligible);
    }
}
