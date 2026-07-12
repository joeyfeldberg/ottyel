use anyhow::{Result, bail};
use serde::Serialize;

#[derive(Debug, Clone, Eq, PartialEq, Serialize)]
pub(crate) struct Distribution {
    pub count: usize,
    pub min_ns: u64,
    pub p50_ns: u64,
    pub p95_ns: u64,
    pub p99_ns: u64,
    pub max_ns: u64,
}

impl Distribution {
    pub(crate) fn from_samples(mut samples: Vec<u64>) -> Result<Self> {
        if samples.is_empty() {
            bail!("cannot calculate a distribution without samples");
        }
        samples.sort_unstable();
        Ok(Self {
            count: samples.len(),
            min_ns: samples[0],
            p50_ns: percentile(&samples, 50),
            p95_ns: percentile(&samples, 95),
            p99_ns: percentile(&samples, 99),
            max_ns: samples[samples.len() - 1],
        })
    }
}

fn percentile(sorted: &[u64], percent: usize) -> u64 {
    let rank = percent.saturating_mul(sorted.len()).div_ceil(100);
    sorted[rank.saturating_sub(1).min(sorted.len() - 1)]
}

#[cfg(test)]
mod tests {
    #[test]
    fn distribution_uses_nearest_rank_percentiles() {
        let distribution = super::Distribution::from_samples((1..=100).rev().collect()).unwrap();

        assert_eq!(
            distribution,
            super::Distribution {
                count: 100,
                min_ns: 1,
                p50_ns: 50,
                p95_ns: 95,
                p99_ns: 99,
                max_ns: 100,
            }
        );
    }

    #[test]
    fn distribution_rejects_empty_samples() {
        assert!(super::Distribution::from_samples(Vec::new()).is_err());
    }
}
