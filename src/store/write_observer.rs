#[cfg(not(any(test, feature = "benchmark-support")))]
use std::marker::PhantomData;
#[cfg(any(test, feature = "benchmark-support"))]
use std::{
    array,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Instant,
};

#[cfg(any(test, feature = "benchmark-support"))]
const OBSERVED_GROUP_SIZES: usize = 4;

/// Writer counters that compile to no-ops outside tests and the benchmark feature.
#[derive(Clone, Default)]
pub(super) struct WriteObserver {
    #[cfg(any(test, feature = "benchmark-support"))]
    counters: Arc<BenchmarkCounters>,
}

impl WriteObserver {
    #[inline]
    pub(super) fn group_formed(&self, _exports: usize) {
        #[cfg(any(test, feature = "benchmark-support"))]
        {
            let exports = _exports;
            saturating_increment(&self.counters.groups_started);
            saturating_add(&self.counters.exports_grouped, exports as u64);
            self.counters
                .maximum_group_size
                .fetch_max(exports as u64, Ordering::Relaxed);
            let bucket = exports.saturating_sub(1).min(OBSERVED_GROUP_SIZES);
            saturating_increment(&self.counters.group_size_counts[bucket]);
        }
    }

    #[inline]
    pub(super) fn retention(&self) -> RetentionObservation<'_> {
        #[cfg(any(test, feature = "benchmark-support"))]
        saturating_increment(&self.counters.retention_invocations);
        RetentionObservation {
            #[cfg(not(any(test, feature = "benchmark-support")))]
            _observer: PhantomData,
            #[cfg(any(test, feature = "benchmark-support"))]
            observer: self,
            #[cfg(any(test, feature = "benchmark-support"))]
            started: Instant::now(),
            #[cfg(any(test, feature = "benchmark-support"))]
            succeeded: false,
        }
    }

    #[inline]
    pub(super) fn shared_ingest_retention_transaction(&self) -> TransactionObservation<'_> {
        #[cfg(any(test, feature = "benchmark-support"))]
        {
            saturating_increment(&self.counters.sqlite_transactions_started);
            saturating_increment(&self.counters.shared_ingest_retention_transactions_started);
        }
        TransactionObservation {
            #[cfg(not(any(test, feature = "benchmark-support")))]
            _observer: PhantomData,
            #[cfg(any(test, feature = "benchmark-support"))]
            observer: self,
            #[cfg(any(test, feature = "benchmark-support"))]
            committed: false,
        }
    }

    #[cfg(any(test, feature = "benchmark-support"))]
    pub(super) fn snapshot(&self) -> BenchmarkSnapshot {
        BenchmarkSnapshot {
            groups_started: load(&self.counters.groups_started),
            exports_grouped: load(&self.counters.exports_grouped),
            maximum_group_size: load(&self.counters.maximum_group_size),
            group_size_counts: array::from_fn(|index| {
                load(&self.counters.group_size_counts[index])
            }),
            sqlite_transactions_started: load(&self.counters.sqlite_transactions_started),
            sqlite_transactions_committed: load(&self.counters.sqlite_transactions_committed),
            sqlite_transactions_not_committed: load(
                &self.counters.sqlite_transactions_not_committed,
            ),
            ingest_only_transactions_started: 0,
            ingest_only_transactions_committed: 0,
            ingest_only_transactions_not_committed: 0,
            retention_invocations: load(&self.counters.retention_invocations),
            retention_failures: load(&self.counters.retention_failures),
            retention_elapsed_ns: load(&self.counters.retention_elapsed_ns),
            retention_only_transactions_started: 0,
            retention_only_transactions_committed: 0,
            retention_only_transactions_not_committed: 0,
            shared_ingest_retention_transactions_started: load(
                &self.counters.shared_ingest_retention_transactions_started,
            ),
            shared_ingest_retention_transactions_committed: load(
                &self.counters.shared_ingest_retention_transactions_committed,
            ),
            shared_ingest_retention_transactions_not_committed: load(
                &self
                    .counters
                    .shared_ingest_retention_transactions_not_committed,
            ),
        }
    }
}

pub(super) struct TransactionObservation<'a> {
    #[cfg(not(any(test, feature = "benchmark-support")))]
    _observer: PhantomData<&'a WriteObserver>,
    #[cfg(any(test, feature = "benchmark-support"))]
    observer: &'a WriteObserver,
    #[cfg(any(test, feature = "benchmark-support"))]
    committed: bool,
}

#[cfg(any(test, feature = "benchmark-support"))]
impl TransactionObservation<'_> {
    #[inline]
    pub(super) fn committed(mut self) {
        self.committed = true;
        saturating_increment(&self.observer.counters.sqlite_transactions_committed);
        saturating_increment(
            &self
                .observer
                .counters
                .shared_ingest_retention_transactions_committed,
        );
    }
}

#[cfg(not(any(test, feature = "benchmark-support")))]
impl TransactionObservation<'_> {
    #[inline]
    pub(super) fn committed(self) {}
}

#[cfg(any(test, feature = "benchmark-support"))]
impl Drop for TransactionObservation<'_> {
    fn drop(&mut self) {
        if self.committed {
            return;
        }
        saturating_increment(&self.observer.counters.sqlite_transactions_not_committed);
        saturating_increment(
            &self
                .observer
                .counters
                .shared_ingest_retention_transactions_not_committed,
        );
    }
}

pub(super) struct RetentionObservation<'a> {
    #[cfg(not(any(test, feature = "benchmark-support")))]
    _observer: PhantomData<&'a WriteObserver>,
    #[cfg(any(test, feature = "benchmark-support"))]
    observer: &'a WriteObserver,
    #[cfg(any(test, feature = "benchmark-support"))]
    started: Instant,
    #[cfg(any(test, feature = "benchmark-support"))]
    succeeded: bool,
}

#[cfg(any(test, feature = "benchmark-support"))]
impl RetentionObservation<'_> {
    #[inline]
    pub(super) fn succeeded(mut self) {
        self.succeeded = true;
    }
}

#[cfg(not(any(test, feature = "benchmark-support")))]
impl RetentionObservation<'_> {
    #[inline]
    pub(super) fn succeeded(self) {}
}

#[cfg(any(test, feature = "benchmark-support"))]
impl Drop for RetentionObservation<'_> {
    fn drop(&mut self) {
        saturating_add(
            &self.observer.counters.retention_elapsed_ns,
            duration_nanos(self.started.elapsed()),
        );
        if !self.succeeded {
            saturating_increment(&self.observer.counters.retention_failures);
        }
    }
}

/// Cumulative writer observations.
///
/// Report schema v1 keeps the ingest-only and retention-only transaction fields. Coalesced
/// writes never open those transaction kinds, so they always read zero.
#[cfg(any(test, feature = "benchmark-support"))]
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct BenchmarkSnapshot {
    pub groups_started: u64,
    pub exports_grouped: u64,
    pub maximum_group_size: u64,
    pub group_size_counts: [u64; OBSERVED_GROUP_SIZES + 1],
    pub sqlite_transactions_started: u64,
    pub sqlite_transactions_committed: u64,
    pub sqlite_transactions_not_committed: u64,
    pub ingest_only_transactions_started: u64,
    pub ingest_only_transactions_committed: u64,
    pub ingest_only_transactions_not_committed: u64,
    pub retention_invocations: u64,
    pub retention_failures: u64,
    pub retention_elapsed_ns: u64,
    pub retention_only_transactions_started: u64,
    pub retention_only_transactions_committed: u64,
    pub retention_only_transactions_not_committed: u64,
    pub shared_ingest_retention_transactions_started: u64,
    pub shared_ingest_retention_transactions_committed: u64,
    pub shared_ingest_retention_transactions_not_committed: u64,
}

#[cfg(any(test, feature = "benchmark-support"))]
#[derive(Default)]
struct BenchmarkCounters {
    groups_started: AtomicU64,
    exports_grouped: AtomicU64,
    maximum_group_size: AtomicU64,
    group_size_counts: [AtomicU64; OBSERVED_GROUP_SIZES + 1],
    sqlite_transactions_started: AtomicU64,
    sqlite_transactions_committed: AtomicU64,
    sqlite_transactions_not_committed: AtomicU64,
    retention_invocations: AtomicU64,
    retention_failures: AtomicU64,
    retention_elapsed_ns: AtomicU64,
    shared_ingest_retention_transactions_started: AtomicU64,
    shared_ingest_retention_transactions_committed: AtomicU64,
    shared_ingest_retention_transactions_not_committed: AtomicU64,
}

#[cfg(any(test, feature = "benchmark-support"))]
fn saturating_increment(counter: &AtomicU64) {
    saturating_add(counter, 1);
}

#[cfg(any(test, feature = "benchmark-support"))]
fn saturating_add(counter: &AtomicU64, value: u64) {
    let _ = counter.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
        Some(current.saturating_add(value))
    });
}

#[cfg(any(test, feature = "benchmark-support"))]
fn load(counter: &AtomicU64) -> u64 {
    counter.load(Ordering::Relaxed)
}

#[cfg(any(test, feature = "benchmark-support"))]
fn duration_nanos(duration: std::time::Duration) -> u64 {
    duration.as_nanos().min(u128::from(u64::MAX)) as u64
}
