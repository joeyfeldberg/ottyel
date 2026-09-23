use anyhow::{Result, ensure};
use serde::Serialize;

use crate::stats::Distribution;

#[derive(Debug, Serialize)]
pub(crate) struct Measurements {
    #[serde(skip)]
    pub structural_assertions_passed: bool,
    pub initial_span_rows: u64,
    pub final_span_rows: u64,
    pub burst: BurstMeasurement,
    pub low_rate: LowRateMeasurement,
}

#[derive(Debug, Serialize)]
pub(crate) struct BurstMeasurement {
    pub makespan: Distribution,
    pub submission_attempt_to_completion_ack: Distribution,
    pub release_to_completion_ack: Distribution,
    pub records_per_second: RateDistribution,
    pub exports_per_second: RateDistribution,
    pub maintenance_elapsed_per_burst: Distribution,
    pub counters: CounterTotals,
}

#[derive(Debug, Serialize)]
pub(crate) struct LowRateMeasurement {
    pub submission_attempt_to_completion_ack: Distribution,
    pub maintenance_elapsed_per_export: Distribution,
    pub counters: CounterTotals,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub(crate) struct RateDistribution {
    pub count: usize,
    pub min: f64,
    pub p50: f64,
    pub p95: f64,
    pub p99: f64,
    pub max: f64,
}

impl RateDistribution {
    pub(crate) fn from_samples(mut samples: Vec<f64>) -> Result<Self> {
        ensure!(
            !samples.is_empty(),
            "cannot calculate a rate distribution without samples"
        );
        ensure!(
            samples.iter().all(|sample| sample.is_finite()),
            "rate samples must be finite"
        );
        samples.sort_by(f64::total_cmp);
        Ok(Self {
            count: samples.len(),
            min: samples[0],
            p50: percentile(&samples, 50),
            p95: percentile(&samples, 95),
            p99: percentile(&samples, 99),
            max: samples[samples.len() - 1],
        })
    }
}

#[derive(Debug, Clone, Default, Eq, PartialEq, Serialize)]
pub(crate) struct CounterTotals {
    pub groups_started: u64,
    pub exports_grouped: u64,
    pub group_size_1: u64,
    pub group_size_2: u64,
    pub group_size_3: u64,
    pub group_size_4: u64,
    pub group_size_5_or_more: u64,
    pub sqlite_transactions_started: u64,
    pub sqlite_transactions_committed: u64,
    pub sqlite_transactions_not_committed: u64,
    pub ingest_group_transactions_started: u64,
    pub ingest_group_transactions_committed: u64,
    pub ingest_group_transactions_not_committed: u64,
    pub maintenance_transactions_started: u64,
    pub maintenance_transactions_committed: u64,
    pub maintenance_transactions_not_committed: u64,
    pub maintenance_units: u64,
    pub maintenance_failures: u64,
    pub maintenance_elapsed_ns: u64,
}

impl CounterTotals {
    pub(crate) fn add(&mut self, delta: &CounterTotals) {
        self.groups_started += delta.groups_started;
        self.exports_grouped += delta.exports_grouped;
        self.group_size_1 += delta.group_size_1;
        self.group_size_2 += delta.group_size_2;
        self.group_size_3 += delta.group_size_3;
        self.group_size_4 += delta.group_size_4;
        self.group_size_5_or_more += delta.group_size_5_or_more;
        self.sqlite_transactions_started += delta.sqlite_transactions_started;
        self.sqlite_transactions_committed += delta.sqlite_transactions_committed;
        self.sqlite_transactions_not_committed += delta.sqlite_transactions_not_committed;
        self.ingest_group_transactions_started += delta.ingest_group_transactions_started;
        self.ingest_group_transactions_committed += delta.ingest_group_transactions_committed;
        self.ingest_group_transactions_not_committed +=
            delta.ingest_group_transactions_not_committed;
        self.maintenance_transactions_started += delta.maintenance_transactions_started;
        self.maintenance_transactions_committed += delta.maintenance_transactions_committed;
        self.maintenance_transactions_not_committed += delta.maintenance_transactions_not_committed;
        self.maintenance_units += delta.maintenance_units;
        self.maintenance_failures += delta.maintenance_failures;
        self.maintenance_elapsed_ns += delta.maintenance_elapsed_ns;
    }
}

fn percentile(sorted: &[f64], percent: usize) -> f64 {
    let rank = percent.saturating_mul(sorted.len()).div_ceil(100);
    sorted[rank.saturating_sub(1).min(sorted.len() - 1)]
}

#[cfg(test)]
mod tests {
    #[test]
    fn rate_distribution_uses_nearest_rank_percentiles() {
        let distribution =
            super::RateDistribution::from_samples((1..=100).rev().map(f64::from).collect())
                .unwrap();
        assert_eq!(distribution.p50, 50.0);
        assert_eq!(distribution.p95, 95.0);
        assert_eq!(distribution.p99, 99.0);
    }
}
