#[allow(dead_code, unused_imports)]
#[path = "ingest_field_work/config.rs"]
mod bench_config;
#[allow(dead_code, unused_imports)]
#[path = "ingest_field_work/fixtures.rs"]
mod fixtures;
#[allow(dead_code, unused_imports)]
#[path = "ingest_field_work/report.rs"]
mod report;
#[path = "ingest_field_work/runner.rs"]
mod runner;
#[allow(dead_code, unused_imports)]
#[path = "support/stats.rs"]
mod stats;

// Source inclusion keeps the benchmark on the exact private production policy and preflight
// implementation without widening Ottyel's public API. The aliases reproduce src/ingest.rs.
mod config {
    pub use ottyel::config::ServeArgs;
}

#[allow(dead_code, unused_imports)]
#[path = "../src/ingest/policy.rs"]
mod policy;
pub(crate) use policy::IngestLimits;
#[allow(dead_code, unused_imports)]
#[path = "../src/ingest/preflight/mod.rs"]
mod preflight;

// The included preflight unit-test module imports the production parent path when Cargo compiles
// this harness with cfg(test).
#[allow(unused_imports)]
mod ingest {
    pub(crate) use crate::IngestLimits;
}

use anyhow::Result;
use bench_config::RunConfig;
use report::BenchmarkReport;

fn main() -> Result<()> {
    if cfg!(debug_assertions) {
        anyhow::bail!(
            "ingest_field_work must run in release mode; use \
             `cargo bench --bench ingest_field_work`"
        );
    }

    let config = RunConfig::parse()?;
    let fixtures = fixtures::build_all()?;
    let run = runner::run(&fixtures, &config)?;
    let report = BenchmarkReport::build(
        &config,
        &fixtures,
        run.configured_default_work_unit_limit,
        run.measurements,
    );
    report.write(&config.output)?;
    report.print_summary(&config.output);
    Ok(())
}
