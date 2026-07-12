use std::{
    fs,
    path::{Path, PathBuf},
    process::Command,
};

use anyhow::{Context, Result};
use serde::Serialize;

use super::{config::RunConfig, data::SeededStore, stats::Distribution};

#[derive(Debug, Serialize)]
pub(crate) struct Measurement {
    pub name: &'static str,
    pub description: &'static str,
    pub operations_per_sample: usize,
    pub result_count: usize,
    pub latency: Distribution,
    pub p50_operations_per_second: f64,
}

#[derive(Debug, Serialize)]
struct UnsupportedScenario {
    name: &'static str,
    reason: &'static str,
}

#[derive(Debug, Serialize)]
pub(crate) struct BenchmarkReport {
    schema_version: u32,
    generated_at_utc: String,
    profile: super::config::Profile,
    scale: super::config::Scale,
    warmup_count: usize,
    sample_count: usize,
    environment: Environment,
    setup_duration_ns: u64,
    database_bytes: u64,
    wal_bytes: u64,
    measurements: Vec<Measurement>,
    unsupported_scenarios: Vec<UnsupportedScenario>,
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

impl BenchmarkReport {
    pub(crate) fn build(
        config: &RunConfig,
        seeded: &SeededStore,
        measurements: Vec<Measurement>,
    ) -> Result<Self> {
        Ok(Self {
            schema_version: 1,
            generated_at_utc: chrono::Utc::now().to_rfc3339(),
            profile: config.profile,
            scale: config.profile.scale(),
            warmup_count: config.warmup,
            sample_count: config.samples,
            environment: Environment::detect(config),
            setup_duration_ns: duration_nanos(seeded.setup_duration),
            database_bytes: file_size(&seeded.database_path)?,
            wal_bytes: file_size(&wal_path(&seeded.database_path))?,
            measurements,
            unsupported_scenarios: vec![
                UnsupportedScenario {
                    name: "targeted_metric_series",
                    reason: "the current public query API exposes only the global metric feed; it has no series-identity or downsampled series query",
                },
                UnsupportedScenario {
                    name: "concurrent_ingest_read",
                    reason: "a controlled contention benchmark for the current single-connection Store is not implemented in this first harness slice; it requires a separately defined workload and latency contract",
                },
            ],
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
            "store baseline: profile={:?} setup={:.3}s db={}B wal={}B",
            self.profile,
            self.setup_duration_ns as f64 / 1_000_000_000.0,
            self.database_bytes,
            self.wal_bytes
        );
        for measurement in &self.measurements {
            println!(
                "  {:30} n={:<3} p50={:>9.3}ms p95={:>9.3}ms p99={:>9.3}ms p50_ops/s={:>10.1}",
                measurement.name,
                measurement.latency.count,
                measurement.latency.p50_ns as f64 / 1_000_000.0,
                measurement.latency.p95_ns as f64 / 1_000_000.0,
                measurement.latency.p99_ns as f64 / 1_000_000.0,
                measurement.p50_operations_per_second,
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

pub(crate) fn duration_nanos(duration: std::time::Duration) -> u64 {
    duration.as_nanos().min(u128::from(u64::MAX)) as u64
}
