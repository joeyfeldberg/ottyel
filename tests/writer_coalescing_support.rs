#![cfg(feature = "benchmark-support")]
#![allow(dead_code)]

#[path = "../benches/writer_coalescing/config.rs"]
mod bench_config;
#[path = "../benches/writer_coalescing/contract.rs"]
mod contract;

mod config {
    pub(crate) use super::bench_config::Scale;
}

#[path = "../benches/writer_coalescing/counters.rs"]
mod counters;
#[path = "../benches/support/data.rs"]
mod data;
#[path = "../benches/writer_coalescing/measurement.rs"]
mod measurement;
#[path = "../benches/writer_coalescing/report.rs"]
mod report;
#[path = "../benches/writer_coalescing/runner.rs"]
mod runner;
#[path = "../benches/support/stats.rs"]
mod stats;
