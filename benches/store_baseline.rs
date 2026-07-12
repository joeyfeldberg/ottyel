#[path = "support/mod.rs"]
mod support;

use anyhow::Result;
use support::{config::RunConfig, data::SeededStore, report::BenchmarkReport};

fn main() -> Result<()> {
    if cfg!(debug_assertions) {
        anyhow::bail!(
            "store_baseline must run in release mode; use `cargo bench --bench store_baseline`"
        );
    }

    let config = RunConfig::parse()?;
    let acknowledgement_capacity = config
        .warmup
        .saturating_add(config.samples)
        .saturating_mul(config.profile.scale().acknowledgement_spans);
    let seeded = SeededStore::create(config.profile.scale(), acknowledgement_capacity)?;
    let measurements = support::scenarios::run(&seeded, &config)?;
    let report = BenchmarkReport::build(&config, &seeded, measurements)?;
    report.write(&config.output)?;
    report.print_summary(&config.output);
    Ok(())
}
