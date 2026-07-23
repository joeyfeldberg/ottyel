use std::{fs, path::Path, process::Command};

use anyhow::{Context, Result};
use serde::Serialize;

use super::{
    bench_config::{Profile, RunConfig},
    fixtures::{Classification, FIXTURE_GENERATOR_VERSION, Fixture, FixtureMetadata},
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
    pub latency: Distribution,
    pub p50_throughput: Throughput,
}

impl Measurement {
    pub(crate) fn new(fixture: &FixtureMetadata, phase: Phase, latency: Distribution) -> Self {
        let seconds = latency.p50_ns as f64 / 1_000_000_000.0;
        let rate = |count: usize| {
            if seconds == 0.0 {
                0.0
            } else {
                count as f64 / seconds
            }
        };
        let bytes_per_second = rate(fixture.encoded_bytes);
        Self {
            fixture: fixture.name,
            phase,
            p50_throughput: Throughput {
                equivalent_input_bytes_per_second: bytes_per_second,
                equivalent_input_mib_per_second: bytes_per_second / (1024.0 * 1024.0),
                equivalent_wire_field_keys_per_second: rate(fixture.wire_field_keys),
                equivalent_target_field_occurrences_per_second: rate(
                    fixture.target_field_occurrences,
                ),
                equivalent_semantic_group_pairs_per_second: rate(fixture.semantic_group_pairs),
            },
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
    profile: Profile,
    warmup_count: usize,
    sample_count: usize,
    environment: Environment,
    decision_gate: DecisionGate,
    decision_inputs: DecisionInputs,
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
struct DecisionGate {
    policy_version: u32,
    evaluation: &'static str,
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
struct DecisionInputs {
    control_fixture: &'static str,
    control_full_pipeline_p95_ns: u64,
    adversarial_full_pipeline: Vec<AdversarialDecisionInput>,
}

#[derive(Debug, Serialize)]
struct AdversarialDecisionInput {
    fixture: &'static str,
    full_pipeline_p95_ns: u64,
    control_p95_ratio: Option<f64>,
}

impl BenchmarkReport {
    pub(crate) fn build(
        config: &RunConfig,
        fixtures: &[Fixture],
        measurements: Vec<Measurement>,
    ) -> Self {
        let decision_inputs = DecisionInputs::from_measurements(fixtures, &measurements);
        Self {
            schema_name: "ottyel.ingest_field_work",
            schema_version: 1,
            generated_at_utc: chrono::Utc::now().to_rfc3339(),
            benchmark: "ingest_field_work",
            proto_crate_version: PROTO_CRATE_VERSION,
            fixture_generator_version: FIXTURE_GENERATOR_VERSION,
            profile: config.profile,
            warmup_count: config.warmup,
            sample_count: config.samples,
            environment: Environment::detect(config),
            decision_gate: DecisionGate::v1(),
            decision_inputs,
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
                "  {:32} {:19} p50={:>9.3}ms p95={:>9.3}ms p99={:>9.3}ms",
                measurement.fixture,
                measurement.phase.label(),
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

impl DecisionGate {
    fn v1() -> Self {
        Self {
            policy_version: 1,
            evaluation: "manual",
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
            rule: "A field/work-unit budget is warranted only when an adversarial full-pipeline \
                   p95 exceeds 100 ms, or exceeds both 25 ms and 8x the blob-control p95, on the \
                   same designated reference machine in two consecutive release runs.",
        }
    }
}

impl DecisionInputs {
    fn from_measurements(fixtures: &[Fixture], measurements: &[Measurement]) -> Self {
        let control_fixture = "unknown_length_delimited_blob";
        let control_full_pipeline_p95_ns = full_pipeline_p95(measurements, control_fixture)
            .expect("the benchmark runner must emit a full-pipeline blob-control measurement");
        let adversarial_full_pipeline = fixtures
            .iter()
            .filter(|fixture| fixture.metadata.classification == Classification::Adversarial)
            .map(|fixture| {
                let full_pipeline_p95_ns = full_pipeline_p95(measurements, fixture.metadata.name)
                    .unwrap_or_else(|| {
                        panic!(
                            "the benchmark runner must emit a full-pipeline measurement for {}",
                            fixture.metadata.name
                        )
                    });
                AdversarialDecisionInput {
                    fixture: fixture.metadata.name,
                    full_pipeline_p95_ns,
                    control_p95_ratio: (control_full_pipeline_p95_ns != 0)
                        .then(|| full_pipeline_p95_ns as f64 / control_full_pipeline_p95_ns as f64),
                }
            })
            .collect();
        Self {
            control_fixture,
            control_full_pipeline_p95_ns,
            adversarial_full_pipeline,
        }
    }
}

fn full_pipeline_p95(measurements: &[Measurement], fixture: &str) -> Option<u64> {
    measurements
        .iter()
        .find(|measurement| {
            measurement.fixture == fixture && measurement.phase == Phase::FullPipeline
        })
        .map(|measurement| measurement.latency.p95_ns)
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

    use super::{BenchmarkReport, DecisionGate, Measurement, Phase};
    use crate::{
        bench_config::{Profile, RunConfig},
        fixtures::build_all,
        stats::Distribution,
    };

    #[test]
    fn decision_gate_is_pinned_and_manual() {
        let gate = serde_json::to_value(DecisionGate::v1()).unwrap();

        assert_eq!(gate["evaluation"], "manual");
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
                Measurement::new(&fixture.metadata, Phase::FullPipeline, distribution.clone())
            })
            .collect();
        let value =
            serde_json::to_value(BenchmarkReport::build(&config, &fixtures, measurements)).unwrap();

        assert_eq!(value["schema_name"], "ottyel.ingest_field_work");
        assert_eq!(value["schema_version"], 1);
        assert_eq!(value["proto_crate_version"], "opentelemetry-proto =0.31.0");
        assert_eq!(value["fixture_generator_version"], 1);
        assert_eq!(value["fixtures"][0]["encoded_bytes"], 4 * 1024 * 1024);
        assert_eq!(value["fixtures"][0]["maximum_group_depth"], 0);
        assert_eq!(value["fixtures"][0]["expected_primary_records"], 0);
        assert_eq!(value["fixtures"][0]["expected_structural_elements"], 0);
        assert_eq!(value["measurements"][0]["latency"]["p95_ns"], 30);
        assert_eq!(value["decision_inputs"]["control_full_pipeline_p95_ns"], 30);
        assert_eq!(
            value["decision_inputs"]["adversarial_full_pipeline"][0]["control_p95_ratio"],
            1.0
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
