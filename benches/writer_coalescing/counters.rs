use anyhow::{Result, ensure};
use ottyel::store::benchmark_support::BenchmarkSnapshot;

use crate::measurement::CounterTotals;

/// Validates the pinned candidate contract: every sample's exports form exactly one group that
/// commits once, in one shared ingest-retention transaction, with one retention invocation.
pub(crate) fn validate_candidate(counters: &CounterTotals, exports: u64) -> Result<()> {
    let expected_sizes = match exports {
        1 => [1, 0, 0, 0, 0],
        2 => [0, 1, 0, 0, 0],
        3 => [0, 0, 1, 0, 0],
        4 => [0, 0, 0, 1, 0],
        _ => anyhow::bail!("the candidate policy groups at most four exports, not {exports}"),
    };
    ensure!(
        counters.groups_started == 1
            && counters.exports_grouped == exports
            && [
                counters.group_size_1,
                counters.group_size_2,
                counters.group_size_3,
                counters.group_size_4,
                counters.group_size_5_or_more,
            ] == expected_sizes,
        "candidate group counters do not describe one group of {exports}: {counters:?}"
    );
    ensure!(
        counters.sqlite_transactions_started == 1
            && counters.sqlite_transactions_committed == 1
            && counters.sqlite_transactions_not_committed == 0,
        "candidate total SQLite transaction counters are invalid: {counters:?}"
    );
    ensure!(
        counters.shared_ingest_retention_transactions_started == 1
            && counters.shared_ingest_retention_transactions_committed == 1
            && counters.shared_ingest_retention_transactions_not_committed == 0,
        "candidate shared ingest-retention transaction counters are invalid: {counters:?}"
    );
    ensure!(
        counters.retention_invocations == 1 && counters.retention_failures == 0,
        "candidate retention counters are invalid: {counters:?}"
    );
    ensure!(
        counters.ingest_only_transactions_started == 0
            && counters.ingest_only_transactions_committed == 0
            && counters.ingest_only_transactions_not_committed == 0
            && counters.retention_only_transactions_started == 0
            && counters.retention_only_transactions_committed == 0
            && counters.retention_only_transactions_not_committed == 0,
        "candidate unexpectedly used a separate ingest or retention transaction: {counters:?}"
    );
    Ok(())
}

pub(crate) fn validate_persisted_row_count(
    actual: u64,
    expected: u64,
    context: &str,
) -> Result<()> {
    ensure!(
        actual == expected,
        "{context} persisted {actual} span rows, expected {expected}"
    );
    Ok(())
}

pub(crate) fn snapshot_delta(
    before: BenchmarkSnapshot,
    after: BenchmarkSnapshot,
) -> Result<CounterTotals> {
    Ok(CounterTotals {
        groups_started: subtract(after.groups_started, before.groups_started, "groups")?,
        exports_grouped: subtract(after.exports_grouped, before.exports_grouped, "exports")?,
        group_size_1: subtract(
            after.group_size_counts[0],
            before.group_size_counts[0],
            "groups of one",
        )?,
        group_size_2: subtract(
            after.group_size_counts[1],
            before.group_size_counts[1],
            "groups of two",
        )?,
        group_size_3: subtract(
            after.group_size_counts[2],
            before.group_size_counts[2],
            "groups of three",
        )?,
        group_size_4: subtract(
            after.group_size_counts[3],
            before.group_size_counts[3],
            "groups of four",
        )?,
        group_size_5_or_more: subtract(
            after.group_size_counts[4],
            before.group_size_counts[4],
            "groups of five or more",
        )?,
        sqlite_transactions_started: subtract(
            after.sqlite_transactions_started,
            before.sqlite_transactions_started,
            "SQLite transactions started",
        )?,
        sqlite_transactions_committed: subtract(
            after.sqlite_transactions_committed,
            before.sqlite_transactions_committed,
            "SQLite transactions committed",
        )?,
        sqlite_transactions_not_committed: subtract(
            after.sqlite_transactions_not_committed,
            before.sqlite_transactions_not_committed,
            "SQLite transactions not committed",
        )?,
        ingest_only_transactions_started: subtract(
            after.ingest_only_transactions_started,
            before.ingest_only_transactions_started,
            "ingest-only transactions started",
        )?,
        ingest_only_transactions_committed: subtract(
            after.ingest_only_transactions_committed,
            before.ingest_only_transactions_committed,
            "ingest-only transactions committed",
        )?,
        ingest_only_transactions_not_committed: subtract(
            after.ingest_only_transactions_not_committed,
            before.ingest_only_transactions_not_committed,
            "ingest-only transactions not committed",
        )?,
        retention_invocations: subtract(
            after.retention_invocations,
            before.retention_invocations,
            "retention invocations",
        )?,
        retention_failures: subtract(
            after.retention_failures,
            before.retention_failures,
            "retention failures",
        )?,
        retention_elapsed_ns: subtract(
            after.retention_elapsed_ns,
            before.retention_elapsed_ns,
            "retention elapsed",
        )?,
        retention_only_transactions_started: subtract(
            after.retention_only_transactions_started,
            before.retention_only_transactions_started,
            "retention-only transactions started",
        )?,
        retention_only_transactions_committed: subtract(
            after.retention_only_transactions_committed,
            before.retention_only_transactions_committed,
            "retention-only transactions committed",
        )?,
        retention_only_transactions_not_committed: subtract(
            after.retention_only_transactions_not_committed,
            before.retention_only_transactions_not_committed,
            "retention-only transactions not committed",
        )?,
        shared_ingest_retention_transactions_started: subtract(
            after.shared_ingest_retention_transactions_started,
            before.shared_ingest_retention_transactions_started,
            "shared ingest-retention transactions started",
        )?,
        shared_ingest_retention_transactions_committed: subtract(
            after.shared_ingest_retention_transactions_committed,
            before.shared_ingest_retention_transactions_committed,
            "shared ingest-retention transactions committed",
        )?,
        shared_ingest_retention_transactions_not_committed: subtract(
            after.shared_ingest_retention_transactions_not_committed,
            before.shared_ingest_retention_transactions_not_committed,
            "shared ingest-retention transactions not committed",
        )?,
    })
}

fn subtract(after: u64, before: u64, label: &str) -> Result<u64> {
    after
        .checked_sub(before)
        .ok_or_else(|| anyhow::anyhow!("{label} counter moved backwards"))
}

#[cfg(test)]
mod tests {
    // Cargo checks this harness-free benchmark with `cfg(test)` while omitting test bodies.
    #[allow(dead_code)]
    fn valid() -> super::CounterTotals {
        super::CounterTotals {
            groups_started: 1,
            exports_grouped: 4,
            group_size_4: 1,
            sqlite_transactions_started: 1,
            sqlite_transactions_committed: 1,
            retention_invocations: 1,
            shared_ingest_retention_transactions_started: 1,
            shared_ingest_retention_transactions_committed: 1,
            ..super::CounterTotals::default()
        }
    }

    #[test]
    fn candidate_contract_requires_one_shared_transaction_per_burst() {
        super::validate_candidate(&valid(), 4).unwrap();
    }

    #[test]
    fn candidate_contract_accepts_a_low_rate_singleton() {
        let singleton = super::CounterTotals {
            exports_grouped: 1,
            group_size_1: 1,
            group_size_4: 0,
            ..valid()
        };
        super::validate_candidate(&singleton, 1).unwrap();
    }

    #[test]
    fn candidate_contract_rejects_split_groups() {
        let split = super::CounterTotals {
            groups_started: 2,
            group_size_4: 0,
            group_size_3: 1,
            group_size_1: 1,
            sqlite_transactions_started: 2,
            sqlite_transactions_committed: 2,
            shared_ingest_retention_transactions_started: 2,
            shared_ingest_retention_transactions_committed: 2,
            retention_invocations: 2,
            ..valid()
        };
        assert!(super::validate_candidate(&split, 4).is_err());
    }

    #[test]
    fn candidate_contract_rejects_missing_or_failed_retention() {
        let missing = super::CounterTotals {
            retention_invocations: 0,
            ..valid()
        };
        assert!(super::validate_candidate(&missing, 4).is_err());

        let failed = super::CounterTotals {
            retention_failures: 1,
            ..valid()
        };
        assert!(super::validate_candidate(&failed, 4).is_err());
    }

    #[test]
    fn candidate_contract_rejects_transactions_not_observed_committed() {
        let shared = super::CounterTotals {
            shared_ingest_retention_transactions_not_committed: 1,
            ..valid()
        };
        assert!(super::validate_candidate(&shared, 4).is_err());

        let total = super::CounterTotals {
            sqlite_transactions_not_committed: 1,
            ..valid()
        };
        assert!(super::validate_candidate(&total, 4).is_err());
    }

    #[test]
    fn candidate_contract_rejects_separate_ingest_or_retention_transactions() {
        let ingest = super::CounterTotals {
            ingest_only_transactions_started: 1,
            ingest_only_transactions_committed: 1,
            ..valid()
        };
        assert!(super::validate_candidate(&ingest, 4).is_err());

        let retention = super::CounterTotals {
            retention_only_transactions_started: 1,
            retention_only_transactions_committed: 1,
            ..valid()
        };
        assert!(super::validate_candidate(&retention, 4).is_err());
    }

    #[test]
    fn persistence_contract_rejects_an_inexact_row_count() {
        super::validate_persisted_row_count(1_000, 1_000, "burst").unwrap();
        assert!(super::validate_persisted_row_count(999, 1_000, "burst").is_err());
    }
}
