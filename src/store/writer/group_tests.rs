use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
        mpsc,
    },
    thread,
    time::Duration,
};

use anyhow::{Result, bail};
use rusqlite::Connection;
use tempfile::{TempDir, tempdir};

use super::GroupLimits;
use crate::store::{
    ingest_weight::IngestWeight,
    writer::{AsyncWriteReceipt, GroupFinish, StoreWriteError, WriterOwner},
};

const WAIT: Duration = Duration::from_secs(2);
const UNHURRIED: GroupLimits = GroupLimits {
    max_transaction_age: Duration::MAX,
    ..GroupLimits::PINNED
};

struct Harness {
    _directory: TempDir,
    path: std::path::PathBuf,
    owner: WriterOwner,
    finishes: Arc<AtomicUsize>,
}

impl Harness {
    fn new(limits: GroupLimits) -> Self {
        let finishes = Arc::new(AtomicUsize::new(0));
        let counted = finishes.clone();
        Self::with_finish(
            limits,
            Box::new(move |_| {
                counted.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }),
            finishes,
        )
    }

    fn with_finish(limits: GroupLimits, finish: GroupFinish, finishes: Arc<AtomicUsize>) -> Self {
        let directory = tempdir().unwrap();
        let path = directory.path().join("group.db");
        let connection = Connection::open(&path).unwrap();
        connection
            .execute("CREATE TABLE writes (export INTEGER NOT NULL)", [])
            .unwrap();
        let owner = WriterOwner::start_for_group_test(connection, limits, finish).unwrap();
        Self {
            _directory: directory,
            path,
            owner,
            finishes,
        }
    }

    /// Blocks the owner on an exclusive job so later submissions are already queued together.
    fn park(&self) -> mpsc::Sender<()> {
        let (entered_sender, entered_receiver) = mpsc::channel();
        let (release_sender, release_receiver) = mpsc::channel::<()>();
        drop(
            self.owner
                .try_execute_async(move |_| {
                    entered_sender.send(()).unwrap();
                    release_receiver.recv().unwrap();
                    Ok(())
                })
                .unwrap(),
        );
        entered_receiver.recv_timeout(WAIT).unwrap();
        release_sender
    }

    fn submit(&self, export: i64, records: usize, bytes: usize) -> AsyncWriteReceipt<usize> {
        self.submit_with(export, records, bytes, |_| Ok(()))
    }

    fn submit_with<F>(
        &self,
        export: i64,
        records: usize,
        bytes: usize,
        before_reply: F,
    ) -> AsyncWriteReceipt<usize>
    where
        F: FnOnce(&Connection) -> Result<()> + Send + 'static,
    {
        self.owner
            .try_execute_ingest_async(weight(records, bytes), move |connection| {
                connection.execute("INSERT INTO writes (export) VALUES (?1)", [export])?;
                before_reply(connection)?;
                Ok(records)
            })
            .unwrap()
    }

    fn group_sizes(&self) -> [u64; 5] {
        self.owner.observer().snapshot().group_size_counts
    }

    fn persisted(&self) -> Vec<i64> {
        let connection = Connection::open(&self.path).unwrap();
        let mut statement = connection
            .prepare("SELECT export FROM writes ORDER BY rowid")
            .unwrap();
        statement
            .query_map([], |row| row.get(0))
            .unwrap()
            .map(Result::unwrap)
            .collect()
    }
}

fn weight(primary_records: usize, canonical_bytes: usize) -> IngestWeight {
    IngestWeight {
        primary_records,
        canonical_bytes,
    }
}

fn wait_all(receipts: Vec<AsyncWriteReceipt<usize>>) -> Vec<Result<usize>> {
    futures::executor::block_on(futures::future::join_all(
        receipts.into_iter().map(AsyncWriteReceipt::wait),
    ))
}

fn accepted(results: Vec<Result<usize>>) -> Vec<usize> {
    results.into_iter().map(Result::unwrap).collect()
}

#[test]
fn ready_exports_share_one_transaction_and_one_finish() {
    let harness = Harness::new(UNHURRIED);
    let release = harness.park();
    let receipts = (0..4).map(|export| harness.submit(export, 1, 1)).collect();
    release.send(()).unwrap();

    assert_eq!(accepted(wait_all(receipts)), vec![1; 4]);
    assert_eq!(harness.finishes.load(Ordering::SeqCst), 1);
    assert_eq!(harness.group_sizes(), [0, 0, 0, 1, 0]);
    assert_eq!(harness.persisted(), vec![0, 1, 2, 3]);
    assert_eq!(harness.owner.reserved_weight_for_test(), IngestWeight::ZERO);
}

#[test]
fn a_lone_export_is_a_group_of_one_without_waiting() {
    let harness = Harness::new(GroupLimits::PINNED);
    assert_eq!(accepted(wait_all(vec![harness.submit(7, 1, 1)])), vec![1]);
    assert_eq!(harness.finishes.load(Ordering::SeqCst), 1);
    assert_eq!(harness.group_sizes(), [1, 0, 0, 0, 0]);
}

#[test]
fn export_cap_starts_a_new_group() {
    let harness = Harness::new(UNHURRIED);
    let release = harness.park();
    let receipts = (0..5).map(|export| harness.submit(export, 1, 1)).collect();
    release.send(()).unwrap();

    assert_eq!(accepted(wait_all(receipts)), vec![1; 5]);
    assert_eq!(harness.finishes.load(Ordering::SeqCst), 2);
    assert_eq!(harness.group_sizes(), [1, 0, 0, 1, 0]);
    assert_eq!(harness.persisted(), vec![0, 1, 2, 3, 4]);
}

#[test]
fn record_and_byte_boundaries_are_inclusive() {
    let harness = Harness::new(GroupLimits {
        max_primary_records: 10,
        max_canonical_bytes: 100,
        ..UNHURRIED
    });
    let release = harness.park();
    let receipts = vec![
        harness.submit(0, 4, 40),
        harness.submit(1, 6, 60),
        // The first two fill both axes exactly, so this export starts the next group.
        harness.submit(2, 1, 1),
        harness.submit(3, 9, 99),
        harness.submit(4, 0, 1),
    ];
    release.send(()).unwrap();

    assert_eq!(accepted(wait_all(receipts)), vec![4, 6, 1, 9, 0]);
    // Groups: [0, 1] at exactly 10/100, [2, 3] at exactly 10/100, then [4].
    assert_eq!(harness.group_sizes(), [1, 2, 0, 0, 0]);
    assert_eq!(harness.persisted(), vec![0, 1, 2, 3, 4]);
}

#[test]
fn an_over_cap_singleton_runs_alone() {
    let harness = Harness::new(GroupLimits {
        max_primary_records: 10,
        ..UNHURRIED
    });
    let release = harness.park();
    let receipts = vec![
        harness.submit(0, 15, 1),
        harness.submit(1, 1, 1),
        harness.submit(2, 1, 1),
    ];
    release.send(()).unwrap();

    assert_eq!(accepted(wait_all(receipts)), vec![15, 1, 1]);
    assert_eq!(harness.group_sizes(), [1, 1, 0, 0, 0]);
}

#[test]
fn transaction_age_is_checked_between_exports() {
    let harness = Harness::new(GroupLimits {
        max_transaction_age: Duration::from_millis(1),
        ..GroupLimits::PINNED
    });
    let release = harness.park();
    let receipts = vec![
        harness.submit_with(0, 1, 1, |_| {
            thread::sleep(Duration::from_millis(5));
            Ok(())
        }),
        harness.submit(1, 1, 1),
    ];
    release.send(()).unwrap();

    assert_eq!(accepted(wait_all(receipts)), vec![1, 1]);
    assert_eq!(harness.group_sizes(), [2, 0, 0, 0, 0]);
}

#[test]
fn exclusive_work_between_exports_keeps_fifo_order() {
    let harness = Harness::new(UNHURRIED);
    let order = Arc::new(Mutex::new(Vec::new()));
    let release = harness.park();
    let first_order = order.clone();
    let first = harness.submit_with(0, 1, 1, move |_| {
        first_order.lock().unwrap().push("first");
        Ok(())
    });
    let exclusive_order = order.clone();
    let exclusive = harness
        .owner
        .try_execute_async(move |_| {
            exclusive_order.lock().unwrap().push("exclusive");
            Ok(())
        })
        .unwrap();
    let last_order = order.clone();
    let last = harness.submit_with(1, 1, 1, move |_| {
        last_order.lock().unwrap().push("last");
        Ok(())
    });
    release.send(()).unwrap();

    futures::executor::block_on(exclusive.wait()).unwrap();
    assert_eq!(accepted(wait_all(vec![first, last])), vec![1, 1]);
    assert_eq!(*order.lock().unwrap(), vec!["first", "exclusive", "last"]);
    assert_eq!(harness.group_sizes(), [2, 0, 0, 0, 0]);
}

#[test]
fn a_failed_export_rolls_back_only_its_own_writes() {
    let harness = Harness::new(UNHURRIED);
    let release = harness.park();
    let receipts = vec![
        harness.submit(0, 1, 1),
        harness.submit_with(1, 1, 1, |_| bail!("projection failed")),
        harness.submit(2, 1, 1),
    ];
    release.send(()).unwrap();

    let results = wait_all(receipts);
    assert_eq!(
        results
            .iter()
            .map(|result| result.as_ref().map_err(ToString::to_string).cloned())
            .collect::<Vec<_>>(),
        vec![Ok(1), Err("projection failed".to_string()), Ok(1)]
    );
    assert_eq!(harness.finishes.load(Ordering::SeqCst), 1);
    assert_eq!(harness.group_sizes(), [0, 0, 1, 0, 0]);
    assert_eq!(harness.persisted(), vec![0, 2]);
}

#[test]
fn finish_failure_rolls_back_the_whole_group_and_the_owner_continues() {
    let failed_once = Arc::new(AtomicBool::new(false));
    let fail = failed_once.clone();
    let harness = Harness::with_finish(
        UNHURRIED,
        Box::new(move |_| {
            if !fail.swap(true, Ordering::SeqCst) {
                bail!("retention failed");
            }
            Ok(())
        }),
        Arc::new(AtomicUsize::new(0)),
    );
    let release = harness.park();
    let receipts = vec![
        harness.submit(0, 1, 1),
        harness.submit_with(1, 1, 1, |_| bail!("projection failed")),
    ];
    release.send(()).unwrap();

    let errors: Vec<_> = wait_all(receipts)
        .into_iter()
        .map(|result| result.unwrap_err().to_string())
        .collect();
    assert_eq!(
        errors,
        vec![
            "coalesced OTLP transaction rolled back: retention failed".to_string(),
            "projection failed".to_string(),
        ]
    );
    assert!(harness.persisted().is_empty());
    assert_eq!(harness.owner.observer().snapshot().retention_failures, 1);

    assert_eq!(accepted(wait_all(vec![harness.submit(2, 1, 1)])), vec![1]);
    assert_eq!(harness.persisted(), vec![2]);
}

#[test]
fn a_panicking_export_fails_the_group_closed() {
    let harness = Harness::new(UNHURRIED);
    let release = harness.park();
    let receipts = vec![
        harness.submit(0, 1, 1),
        harness.submit_with(1, 1, 1, |_| panic!("projection panicked")),
        harness.submit(2, 1, 1),
    ];
    release.send(()).unwrap();

    for result in wait_all(receipts) {
        assert!(matches!(
            result.unwrap_err().downcast_ref(),
            Some(StoreWriteError::OutcomeUnknown)
        ));
    }
    assert!(harness.persisted().is_empty());
    assert_eq!(harness.finishes.load(Ordering::SeqCst), 0);
    let later = harness
        .owner
        .try_execute_ingest_async(weight(1, 1), |_| Ok(1))
        .err()
        .expect("admission closes after a panic");
    assert!(matches!(
        later.downcast_ref(),
        Some(StoreWriteError::Unavailable)
    ));
}
