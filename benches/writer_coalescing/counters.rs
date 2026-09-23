use anyhow::{Result, ensure};
use ottyel::store::benchmark_support::BenchmarkSnapshot;

use crate::measurement::CounterTotals;

/// Validates one sample against the current writer: all `exports` form exactly one ingest group
/// that commits once, and any maintenance units that interleave commit separately and succeed.
pub(crate) fn validate_sample(counters: &CounterTotals, exports: u64) -> Result<()> {
    let expected_sizes = match exports {
        1 => [1, 0, 0, 0, 0],
        2 => [0, 1, 0, 0, 0],
        3 => [0, 0, 1, 0, 0],
        4 => [0, 0, 0, 1, 0],
        _ => anyhow::bail!("the coalescing policy groups at most four exports, not {exports}"),
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
        "group counters do not describe one group of {exports}: {counters:?}"
    );
    ensure!(
        counters.ingest_group_transactions_started == 1
            && counters.ingest_group_transactions_committed == 1
            && counters.ingest_group_transactions_not_committed == 0,
        "ingest group transaction counters are invalid: {counters:?}"
    );
    ensure!(
        counters.maintenance_failures == 0
            && counters.maintenance_transactions_not_committed == 0
            && counters.maintenance_transactions_started == counters.maintenance_units
            && counters.maintenance_transactions_committed == counters.maintenance_units,
        "maintenance counters are invalid: {counters:?}"
    );
    ensure!(
        counters.sqlite_transactions_started == 1 + counters.maintenance_transactions_started
            && counters.sqlite_transactions_committed
                == 1 + counters.maintenance_transactions_committed
            && counters.sqlite_transactions_not_committed == 0,
        "total SQLite transaction counters are invalid: {counters:?}"
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
    let sub = |after: u64, before: u64, label: &str| {
        after
            .checked_sub(before)
            .ok_or_else(|| anyhow::anyhow!("{label} counter moved backwards"))
    };
    Ok(CounterTotals {
        groups_started: sub(after.groups_started, before.groups_started, "groups")?,
        exports_grouped: sub(after.exports_grouped, before.exports_grouped, "exports")?,
        group_size_1: sub(
            after.group_size_counts[0],
            before.group_size_counts[0],
            "size 1",
        )?,
        group_size_2: sub(
            after.group_size_counts[1],
            before.group_size_counts[1],
            "size 2",
        )?,
        group_size_3: sub(
            after.group_size_counts[2],
            before.group_size_counts[2],
            "size 3",
        )?,
        group_size_4: sub(
            after.group_size_counts[3],
            before.group_size_counts[3],
            "size 4",
        )?,
        group_size_5_or_more: sub(
            after.group_size_counts[4],
            before.group_size_counts[4],
            "size 5+",
        )?,
        sqlite_transactions_started: sub(
            after.sqlite_transactions_started,
            before.sqlite_transactions_started,
            "SQLite started",
        )?,
        sqlite_transactions_committed: sub(
            after.sqlite_transactions_committed,
            before.sqlite_transactions_committed,
            "SQLite committed",
        )?,
        sqlite_transactions_not_committed: sub(
            after.sqlite_transactions_not_committed,
            before.sqlite_transactions_not_committed,
            "SQLite not committed",
        )?,
        ingest_group_transactions_started: sub(
            after.ingest_group_transactions_started,
            before.ingest_group_transactions_started,
            "ingest group started",
        )?,
        ingest_group_transactions_committed: sub(
            after.ingest_group_transactions_committed,
            before.ingest_group_transactions_committed,
            "ingest group committed",
        )?,
        ingest_group_transactions_not_committed: sub(
            after.ingest_group_transactions_not_committed,
            before.ingest_group_transactions_not_committed,
            "ingest group not committed",
        )?,
        maintenance_transactions_started: sub(
            after.maintenance_transactions_started,
            before.maintenance_transactions_started,
            "maintenance started",
        )?,
        maintenance_transactions_committed: sub(
            after.maintenance_transactions_committed,
            before.maintenance_transactions_committed,
            "maintenance committed",
        )?,
        maintenance_transactions_not_committed: sub(
            after.maintenance_transactions_not_committed,
            before.maintenance_transactions_not_committed,
            "maintenance not committed",
        )?,
        maintenance_units: sub(
            after.maintenance_units,
            before.maintenance_units,
            "maintenance units",
        )?,
        maintenance_failures: sub(
            after.maintenance_failures,
            before.maintenance_failures,
            "maintenance failures",
        )?,
        maintenance_elapsed_ns: sub(
            after.maintenance_elapsed_ns,
            before.maintenance_elapsed_ns,
            "maintenance elapsed",
        )?,
    })
}

#[cfg(test)]
mod tests {
    // Cargo checks this harness-free benchmark with `cfg(test)` while omitting test bodies.
    #[allow(dead_code)]
    fn burst() -> super::CounterTotals {
        super::CounterTotals {
            groups_started: 1,
            exports_grouped: 4,
            group_size_4: 1,
            sqlite_transactions_started: 1,
            sqlite_transactions_committed: 1,
            ingest_group_transactions_started: 1,
            ingest_group_transactions_committed: 1,
            ..super::CounterTotals::default()
        }
    }

    #[test]
    fn one_group_with_no_maintenance_is_valid() {
        super::validate_sample(&burst(), 4).unwrap();
    }

    #[test]
    fn interleaved_maintenance_units_commit_separately() {
        let counters = super::CounterTotals {
            sqlite_transactions_started: 3,
            sqlite_transactions_committed: 3,
            maintenance_transactions_started: 2,
            maintenance_transactions_committed: 2,
            maintenance_units: 2,
            maintenance_elapsed_ns: 1_000,
            ..burst()
        };
        super::validate_sample(&counters, 4).unwrap();
    }

    #[test]
    fn a_low_rate_singleton_is_one_group_of_one() {
        let singleton = super::CounterTotals {
            exports_grouped: 1,
            group_size_1: 1,
            group_size_4: 0,
            ..burst()
        };
        super::validate_sample(&singleton, 1).unwrap();
    }

    #[test]
    fn split_groups_are_rejected() {
        let split = super::CounterTotals {
            groups_started: 2,
            group_size_4: 0,
            group_size_3: 1,
            group_size_1: 1,
            sqlite_transactions_started: 2,
            sqlite_transactions_committed: 2,
            ingest_group_transactions_started: 2,
            ingest_group_transactions_committed: 2,
            ..burst()
        };
        assert!(super::validate_sample(&split, 4).is_err());
    }

    #[test]
    fn failed_or_uncommitted_maintenance_is_rejected() {
        let failed = super::CounterTotals {
            maintenance_failures: 1,
            ..burst()
        };
        assert!(super::validate_sample(&failed, 4).is_err());

        let uncommitted = super::CounterTotals {
            sqlite_transactions_started: 2,
            sqlite_transactions_not_committed: 1,
            maintenance_transactions_started: 1,
            maintenance_transactions_not_committed: 1,
            maintenance_units: 1,
            ..burst()
        };
        assert!(super::validate_sample(&uncommitted, 4).is_err());
    }

    #[test]
    fn an_uncommitted_ingest_group_is_rejected() {
        let counters = super::CounterTotals {
            ingest_group_transactions_committed: 0,
            ingest_group_transactions_not_committed: 1,
            sqlite_transactions_committed: 0,
            sqlite_transactions_not_committed: 1,
            ..burst()
        };
        assert!(super::validate_sample(&counters, 4).is_err());
    }

    #[test]
    fn persistence_contract_rejects_an_inexact_row_count() {
        super::validate_persisted_row_count(1_000, 1_000, "burst").unwrap();
        assert!(super::validate_persisted_row_count(999, 1_000, "burst").is_err());
    }
}
