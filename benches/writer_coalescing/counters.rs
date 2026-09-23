use anyhow::{Result, ensure};
use ottyel::store::benchmark_support::BenchmarkSnapshot;

use crate::measurement::CounterTotals;

pub(crate) fn validate_baseline(counters: &CounterTotals, exports: u64) -> Result<()> {
    ensure!(
        counters.groups_started == exports
            && counters.exports_grouped == exports
            && counters.group_size_1 == exports
            && counters.group_size_2 == 0
            && counters.group_size_3 == 0
            && counters.group_size_4 == 0
            && counters.group_size_5_or_more == 0,
        "baseline group counters do not describe {exports} singleton exports: {counters:?}"
    );
    ensure!(
        counters.sqlite_transactions_started == exports * 2
            && counters.sqlite_transactions_committed == exports * 2
            && counters.sqlite_transactions_not_committed == 0,
        "baseline total SQLite transaction counters are invalid: {counters:?}"
    );
    ensure!(
        counters.ingest_only_transactions_started == exports
            && counters.ingest_only_transactions_committed == exports
            && counters.ingest_only_transactions_not_committed == 0,
        "baseline ingest-only transaction counters are invalid: {counters:?}"
    );
    ensure!(
        counters.retention_invocations == exports
            && counters.retention_failures == 0
            && counters.retention_only_transactions_started == exports
            && counters.retention_only_transactions_committed == exports
            && counters.retention_only_transactions_not_committed == 0,
        "baseline retention-only transaction counters are invalid: {counters:?}"
    );
    ensure!(
        counters.shared_ingest_retention_transactions_started == 0
            && counters.shared_ingest_retention_transactions_committed == 0
            && counters.shared_ingest_retention_transactions_not_committed == 0,
        "baseline unexpectedly used a shared ingest-retention transaction: {counters:?}"
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
            groups_started: 4,
            exports_grouped: 4,
            group_size_1: 4,
            sqlite_transactions_started: 8,
            sqlite_transactions_committed: 8,
            ingest_only_transactions_started: 4,
            ingest_only_transactions_committed: 4,
            retention_invocations: 4,
            retention_only_transactions_started: 4,
            retention_only_transactions_committed: 4,
            ..super::CounterTotals::default()
        }
    }

    #[test]
    fn baseline_contract_requires_singleton_transactions() {
        let counters = valid();
        super::validate_baseline(&counters, 4).unwrap();
    }

    #[test]
    fn baseline_contract_rejects_non_singleton_groups() {
        let mut counters = valid();
        counters.group_size_1 = 3;
        counters.group_size_4 = 1;
        assert!(super::validate_baseline(&counters, 4).is_err());
    }

    #[test]
    fn baseline_contract_rejects_missing_or_failed_retention() {
        let mut missing = valid();
        missing.retention_only_transactions_committed = 3;
        assert!(super::validate_baseline(&missing, 4).is_err());

        let mut failed = valid();
        failed.retention_failures = 1;
        assert!(super::validate_baseline(&failed, 4).is_err());
    }

    #[test]
    fn baseline_contract_rejects_transactions_not_observed_committed() {
        let mut ingest = valid();
        ingest.ingest_only_transactions_not_committed = 1;
        assert!(super::validate_baseline(&ingest, 4).is_err());

        let mut retention = valid();
        retention.retention_only_transactions_not_committed = 1;
        assert!(super::validate_baseline(&retention, 4).is_err());

        let mut total = valid();
        total.sqlite_transactions_not_committed = 1;
        assert!(super::validate_baseline(&total, 4).is_err());
    }

    #[test]
    fn baseline_contract_rejects_shared_transactions() {
        let mut counters = valid();
        counters.shared_ingest_retention_transactions_started = 1;
        counters.shared_ingest_retention_transactions_committed = 1;
        assert!(super::validate_baseline(&counters, 4).is_err());
    }

    #[test]
    fn persistence_contract_rejects_an_inexact_row_count() {
        super::validate_persisted_row_count(1_000, 1_000, "burst").unwrap();
        assert!(super::validate_persisted_row_count(999, 1_000, "burst").is_err());
    }
}
