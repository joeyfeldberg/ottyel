use std::{fs, path::Path, process::Command};

use anyhow::{Context, Result};
use serde::Serialize;

use super::{
    bench_config::{Profile, RunConfig},
    fixtures::{
        Classification, DefaultPolicyOutcome, FIXTURE_GENERATOR_VERSION, Fixture, FixtureMetadata,
    },
    stats::Distribution,
};

pub(crate) const PROTO_CRATE_VERSION: &str = "opentelemetry-proto =0.31.0";

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Phase {
    Preflight,
    ProstDecode,
    PostdecodeValidate,
    FullPipeline,
}

impl Phase {
    pub(crate) const ALL: [Self; 4] = [
        Self::Preflight,
        Self::ProstDecode,
        Self::PostdecodeValidate,
        Self::FullPipeline,
    ];

    fn label(self) -> &'static str {
        match self {
            Self::Preflight => "preflight",
            Self::ProstDecode => "prost_decode",
            Self::PostdecodeValidate => "postdecode_validate",
            Self::FullPipeline => "full_pipeline",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum MeasurementOutcome {
    Completed,
    BudgetRejected,
}

impl MeasurementOutcome {
    pub(crate) fn for_policy_outcome(outcome: DefaultPolicyOutcome) -> Self {
        match outcome {
            DefaultPolicyOutcome::Accepted => Self::Completed,
            DefaultPolicyOutcome::BudgetRejected => Self::BudgetRejected,
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::Completed => "completed",
            Self::BudgetRejected => "budget_rejected",
        }
    }
}

#[derive(Debug, Serialize)]
pub(crate) struct Throughput {
    pub equivalent_input_bytes_per_second: f64,
    pub equivalent_input_mib_per_second: f64,
    pub equivalent_wire_field_keys_per_second: f64,
    pub equivalent_target_field_occurrences_per_second: f64,
    pub equivalent_semantic_group_pairs_per_second: f64,
}

#[derive(Debug, Serialize)]
pub(crate) struct Measurement {
    pub fixture: &'static str,
    pub phase: Phase,
    pub outcome: MeasurementOutcome,
    pub latency: Distribution,
    pub p50_throughput: Option<Throughput>,
}

impl Measurement {
    pub(crate) fn new(
        fixture: &FixtureMetadata,
        phase: Phase,
        outcome: MeasurementOutcome,
        latency: Distribution,
    ) -> Self {
        let p50_throughput = match outcome {
            MeasurementOutcome::Completed => {
                let seconds = latency.p50_ns as f64 / 1_000_000_000.0;
                let rate = |count: usize| {
                    if seconds == 0.0 {
                        0.0
                    } else {
                        count as f64 / seconds
                    }
                };
                let bytes_per_second = rate(fixture.encoded_bytes);
                Some(Throughput {
                    equivalent_input_bytes_per_second: bytes_per_second,
                    equivalent_input_mib_per_second: bytes_per_second / (1024.0 * 1024.0),
                    equivalent_wire_field_keys_per_second: rate(fixture.wire_field_keys),
                    equivalent_target_field_occurrences_per_second: rate(
                        fixture.target_field_occurrences,
                    ),
                    equivalent_semantic_group_pairs_per_second: rate(fixture.semantic_group_pairs),
                })
            }
            MeasurementOutcome::BudgetRejected => None,
        };
        Self {
            fixture: fixture.name,
            phase,
            outcome,
            p50_throughput,
            latency,
        }
    }
}

#[derive(Debug, Serialize)]
pub(crate) struct BenchmarkReport {
    schema_name: &'static str,
    schema_version: u32,
    generated_at_utc: String,
    benchmark: &'static str,
    proto_crate_version: &'static str,
    fixture_generator_version: u32,
    configured_default_work_unit_limit: usize,
    profile: Profile,
    warmup_count: usize,
    sample_count: usize,
    environment: Environment,
    historical_decision_gate: HistoricalDecisionGate,
    mitigation_observations: MitigationObservations,
    fixtures: Vec<FixtureMetadata>,
    measurements: Vec<Measurement>,
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
    rustc_version: Option<String>,
    rust_debug_assertions: bool,
}

#[derive(Debug, Serialize)]
struct HistoricalDecisionGate {
    policy_version: u32,
    evaluation: &'static str,
    applicable_to_current_measurements: bool,
    applicable_report_schema_version: u32,
    applicable_fixture_generator_version: u32,
    reference_profile_only: bool,
    release_mode_required: bool,
    same_designated_machine_required: bool,
    designated_machine_identity_fields: [&'static str; 3],
    required_consecutive_runs: usize,
    control_fixture: &'static str,
    adversarial_classification: Classification,
    phase: Phase,
    absolute_p95_ns: u64,
    conditional_p95_ns: u64,
    control_p95_multiplier: u32,
    rule: &'static str,
}

#[derive(Debug, Serialize)]
struct MitigationObservations {
    control_fixture: &'static str,
    control_full_pipeline_p95_ns: u64,
    control_full_pipeline_outcome: MeasurementOutcome,
    adversarial_full_pipeline: Vec<AdversarialMitigationObservation>,
}

#[derive(Debug, Serialize)]
struct AdversarialMitigationObservation {
    fixture: &'static str,
    outcome: MeasurementOutcome,
    full_pipeline_p95_ns: u64,
    control_p95_ratio: Option<f64>,
}

impl BenchmarkReport {
    pub(crate) fn build(
        config: &RunConfig,
        fixtures: &[Fixture],
        configured_default_work_unit_limit: usize,
        measurements: Vec<Measurement>,
    ) -> Self {
        let mitigation_observations =
            MitigationObservations::from_measurements(fixtures, &measurements);
        Self {
            schema_name: "ottyel.ingest_field_work",
            schema_version: 2,
            generated_at_utc: chrono::Utc::now().to_rfc3339(),
            benchmark: "ingest_field_work",
            proto_crate_version: PROTO_CRATE_VERSION,
            fixture_generator_version: FIXTURE_GENERATOR_VERSION,
            configured_default_work_unit_limit,
            profile: config.profile,
            warmup_count: config.warmup,
            sample_count: config.samples,
            environment: Environment::detect(config),
            historical_decision_gate: HistoricalDecisionGate::baseline_v1(),
            mitigation_observations,
            fixtures: fixtures
                .iter()
                .map(|fixture| fixture.metadata.clone())
                .collect(),
            measurements,
        }
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
            "ingest field work: profile={:?} fixtures={} samples={}",
            self.profile,
            self.fixtures.len(),
            self.sample_count
        );
        for measurement in &self.measurements {
            println!(
                "  {:32} {:19} {:15} p50={:>9.3}ms p95={:>9.3}ms p99={:>9.3}ms",
                measurement.fixture,
                measurement.phase.label(),
                measurement.outcome.label(),
                measurement.latency.p50_ns as f64 / 1_000_000.0,
                measurement.latency.p95_ns as f64 / 1_000_000.0,
                measurement.latency.p99_ns as f64 / 1_000_000.0,
            );
        }
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
            rustc_version: command_output("rustc", &["--version"]),
            rust_debug_assertions: cfg!(debug_assertions),
        }
    }
}

impl HistoricalDecisionGate {
    fn baseline_v1() -> Self {
        Self {
            policy_version: 1,
            evaluation: "historical_baseline_rule",
            applicable_to_current_measurements: false,
            applicable_report_schema_version: 1,
            applicable_fixture_generator_version: 1,
            reference_profile_only: true,
            release_mode_required: true,
            same_designated_machine_required: true,
            designated_machine_identity_fields: ["machine_label", "cpu", "memory_gib"],
            required_consecutive_runs: 2,
            control_fixture: "unknown_length_delimited_blob",
            adversarial_classification: Classification::Adversarial,
            phase: Phase::FullPipeline,
            absolute_p95_ns: 100_000_000,
            conditional_p95_ns: 25_000_000,
            control_p95_multiplier: 8,
            rule: "For report schema v1 and fixture generator v1, a field/work-unit budget was \
                   warranted only when an adversarial full-pipeline p95 exceeded 100 ms, or \
                   exceeded both 25 ms and 8x the blob-control p95, on the same designated \
                   reference machine in two consecutive release runs. This historical rule does \
                   not evaluate schema v2 post-mitigation measurements.",
        }
    }
}

impl MitigationObservations {
    fn from_measurements(fixtures: &[Fixture], measurements: &[Measurement]) -> Self {
        let control_fixture = "unknown_length_delimited_blob";
        let control_measurement = full_pipeline_measurement(measurements, control_fixture)
            .expect("the benchmark runner must emit a full-pipeline blob-control measurement");
        let control_full_pipeline_p95_ns = control_measurement.latency.p95_ns;
        let adversarial_full_pipeline = fixtures
            .iter()
            .filter(|fixture| fixture.metadata.classification == Classification::Adversarial)
            .map(|fixture| {
                let measurement = full_pipeline_measurement(measurements, fixture.metadata.name)
                    .unwrap_or_else(|| {
                        panic!(
                            "the benchmark runner must emit a full-pipeline measurement for {}",
                            fixture.metadata.name
                        )
                    });
                let full_pipeline_p95_ns = measurement.latency.p95_ns;
                AdversarialMitigationObservation {
                    fixture: fixture.metadata.name,
                    outcome: measurement.outcome,
                    full_pipeline_p95_ns,
                    control_p95_ratio: (control_full_pipeline_p95_ns != 0)
                        .then(|| full_pipeline_p95_ns as f64 / control_full_pipeline_p95_ns as f64),
                }
            })
            .collect();
        Self {
            control_fixture,
            control_full_pipeline_p95_ns,
            control_full_pipeline_outcome: control_measurement.outcome,
            adversarial_full_pipeline,
        }
    }
}

fn full_pipeline_measurement<'a>(
    measurements: &'a [Measurement],
    fixture: &str,
) -> Option<&'a Measurement> {
    measurements.iter().find(|measurement| {
        measurement.fixture == fixture && measurement.phase == Phase::FullPipeline
    })
}

fn command_output(program: &str, args: &[&str]) -> Option<String> {
    let output = Command::new(program).args(args).output().ok()?;
    output
        .status
        .success()
        .then(|| String::from_utf8_lossy(&output.stdout).trim().to_string())
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::{BenchmarkReport, HistoricalDecisionGate, Measurement, MeasurementOutcome, Phase};
    use crate::{
        bench_config::{Profile, RunConfig},
        fixtures::{DefaultPolicyOutcome, build_all},
        stats::Distribution,
    };

    #[test]
    fn historical_decision_gate_is_pinned_and_inapplicable_to_v2_measurements() {
        let gate = serde_json::to_value(HistoricalDecisionGate::baseline_v1()).unwrap();

        assert_eq!(gate["evaluation"], "historical_baseline_rule");
        assert_eq!(gate["applicable_to_current_measurements"], false);
        assert_eq!(gate["applicable_report_schema_version"], 1);
        assert_eq!(gate["applicable_fixture_generator_version"], 1);
        assert_eq!(gate["required_consecutive_runs"], 2);
        assert_eq!(gate["absolute_p95_ns"], 100_000_000_u64);
        assert_eq!(gate["conditional_p95_ns"], 25_000_000_u64);
        assert_eq!(gate["control_p95_multiplier"], 8);
        assert_eq!(gate["phase"], "full_pipeline");
    }

    #[test]
    fn report_schema_keeps_fixture_and_distribution_metadata() {
        let config = RunConfig {
            profile: Profile::Smoke,
            output: PathBuf::from("/tmp/unused.json"),
            warmup: 1,
            samples: 3,
            machine_label: None,
            cpu: None,
            memory_gib: None,
        };
        let fixtures = build_all().unwrap();
        let distribution = Distribution::from_samples(vec![10, 20, 30]).unwrap();
        let measurements = fixtures
            .iter()
            .map(|fixture| {
                Measurement::new(
                    &fixture.metadata,
                    Phase::FullPipeline,
                    MeasurementOutcome::for_policy_outcome(
                        fixture.metadata.expected_default_outcome,
                    ),
                    distribution.clone(),
                )
            })
            .collect();
        let value = serde_json::to_value(BenchmarkReport::build(
            &config,
            &fixtures,
            2_000_000,
            measurements,
        ))
        .unwrap();

        assert_eq!(value["schema_name"], "ottyel.ingest_field_work");
        assert_eq!(value["schema_version"], 2);
        assert_eq!(value["proto_crate_version"], "opentelemetry-proto =0.31.0");
        assert_eq!(value["fixture_generator_version"], 2);
        assert_eq!(value["configured_default_work_unit_limit"], 2_000_000);
        assert_eq!(value["fixtures"][0]["encoded_bytes"], 4 * 1024 * 1024);
        assert_eq!(value["fixtures"][0]["maximum_group_depth"], 0);
        assert_eq!(value["fixtures"][0]["expected_primary_records"], 0);
        assert_eq!(value["fixtures"][0]["expected_structural_elements"], 0);
        assert_eq!(value["fixtures"][0]["expected_default_outcome"], "accepted");
        assert_eq!(value["fixtures"][0]["expected_work_units"], 1);
        assert_eq!(value["fixtures"][5]["classification"], "accepted_canonical");
        assert_eq!(value["measurements"][0]["latency"]["p95_ns"], 30);
        assert_eq!(value["measurements"][0]["outcome"], "completed");
        assert_eq!(value["measurements"][1]["outcome"], "budget_rejected");
        assert!(value["measurements"][1]["p50_throughput"].is_null());
        assert_eq!(
            value["historical_decision_gate"]["evaluation"],
            "historical_baseline_rule"
        );
        assert_eq!(
            value["historical_decision_gate"]["applicable_to_current_measurements"],
            false
        );
        assert!(value.get("decision_gate").is_none());
        assert!(value.get("decision_inputs").is_none());
        assert_eq!(
            value["mitigation_observations"]["control_full_pipeline_p95_ns"],
            30
        );
        assert_eq!(
            value["mitigation_observations"]["control_full_pipeline_outcome"],
            "completed"
        );
        assert_eq!(
            value["mitigation_observations"]["adversarial_full_pipeline"][0]["control_p95_ratio"],
            1.0
        );
        assert_eq!(
            value["mitigation_observations"]["adversarial_full_pipeline"][0]["outcome"],
            "budget_rejected"
        );
        assert_eq!(
            value["mitigation_observations"]["adversarial_full_pipeline"]
                .as_array()
                .unwrap()
                .len(),
            4
        );
        assert_eq!(
            fixtures[1].metadata.expected_default_outcome,
            DefaultPolicyOutcome::BudgetRejected
        );
        assert!(
            value["measurements"][0]["p50_throughput"]["equivalent_input_mib_per_second"]
                .as_f64()
                .unwrap()
                > 0.0
        );
        assert!(
            value["measurements"][0]["p50_throughput"]["equivalent_wire_field_keys_per_second"]
                .as_f64()
                .unwrap()
                > 0.0
        );
    }
}
