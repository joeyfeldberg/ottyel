#![allow(dead_code)]

#[path = "../benches/ingest_field_work/config.rs"]
mod bench_config;
#[path = "../benches/ingest_field_work/fixtures.rs"]
mod fixtures;
#[path = "../benches/ingest_field_work/report.rs"]
mod report;
#[path = "../benches/ingest_field_work/runner.rs"]
mod runner;
#[path = "../benches/support/stats.rs"]
mod stats;

mod config {
    pub use ottyel::config::ServeArgs;
}

#[path = "../src/ingest/policy.rs"]
mod policy;
pub(crate) use policy::IngestLimits;
#[path = "../src/ingest/preflight/mod.rs"]
mod preflight;

mod ingest {
    pub(crate) use crate::IngestLimits;
}
