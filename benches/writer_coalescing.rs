#[path = "writer_coalescing/config.rs"]
mod bench_config;
#[path = "writer_coalescing/contract.rs"]
mod contract;

mod config {
    pub(crate) use super::bench_config::Scale;
}

#[path = "writer_coalescing/counters.rs"]
mod counters;
#[path = "support/data.rs"]
mod data;
#[path = "writer_coalescing/measurement.rs"]
mod measurement;
#[path = "writer_coalescing/report.rs"]
mod report;
#[path = "writer_coalescing/runner.rs"]
mod runner;
#[path = "support/stats.rs"]
mod stats;

use anyhow::Result;
use bench_config::RunConfig;
use data::SeededStore;
use report::BenchmarkReport;

fn main() -> Result<()> {
    if cfg!(debug_assertions) {
        anyhow::bail!(
            "writer_coalescing must run in release mode; use \
             `cargo bench --features benchmark-support --bench writer_coalescing`"
        );
    }

    let config = RunConfig::parse()?;
    let sampling = config.profile.sampling();
    let acknowledgement_capacity = sampling
        .burst_warmup
        .saturating_add(sampling.burst_samples)
        .saturating_mul(runner::RECORDS_PER_BURST)
        .saturating_add(sampling.low_rate_warmup)
        .saturating_add(sampling.low_rate_samples);
    let seeded = SeededStore::create(config.profile.scale(), acknowledgement_capacity)?;
    std::hint::black_box((&seeded.query, &seeded.large_trace_id));
    let measurements = runner::run(&seeded, &config)?;
    let report = BenchmarkReport::build(&config, &seeded, measurements)?;
    report.write(&config.output)?;
    report.print_summary(&config.output);
    Ok(())
}
