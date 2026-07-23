use std::{
    num::NonZeroUsize,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
        mpsc,
    },
    thread,
    time::Duration,
};

use anyhow::{Result, bail};
use rusqlite::Connection;
use tempfile::tempdir;

use super::{
    StoreWriteError, WeightedReservation, WriterLimitDimension, WriterLimits, WriterOwner,
};
use crate::store::ingest_weight::IngestWeight;

const WAIT: Duration = Duration::from_secs(2);

fn limits(primary_records: usize, canonical_bytes: usize) -> WriterLimits {
    WriterLimits::new(
        NonZeroUsize::new(primary_records).unwrap(),
        NonZeroUsize::new(canonical_bytes).unwrap(),
    )
}

fn weight(primary_records: usize, canonical_bytes: usize) -> IngestWeight {
    IngestWeight {
        primary_records,
        canonical_bytes,
    }
}

#[test]
fn full_queue_rejects_immediately_with_typed_overload() {
    let owner = WriterOwner::start_with_capacity(Connection::open_in_memory().unwrap(), 1).unwrap();
    let (entered_sender, entered_receiver) = mpsc::channel();
    let (release_sender, release_receiver) = mpsc::channel();
    let active = owner
        .try_execute_async(move |_| {
            entered_sender.send(()).unwrap();
            release_receiver.recv().unwrap();
            Ok(1)
        })
        .unwrap();
    entered_receiver.recv_timeout(WAIT).unwrap();

    let queued = owner.try_execute_async(|_| Ok(2)).unwrap();
    let error = owner.execute(|_| Ok(3)).unwrap_err();

    release_sender.send(()).unwrap();
    assert!(matches!(
        error.downcast_ref(),
        Some(StoreWriteError::Overloaded)
    ));
    assert_eq!(futures::executor::block_on(active.wait()).unwrap(), 1);
    assert_eq!(futures::executor::block_on(queued.wait()).unwrap(), 2);
}

#[test]
fn one_owner_executes_cloned_submissions_in_fifo_order() {
    let owner = WriterOwner::start_with_capacity(Connection::open_in_memory().unwrap(), 8).unwrap();
    let active = Arc::new(AtomicUsize::new(0));
    let maximum_active = Arc::new(AtomicUsize::new(0));
    let order = Arc::new(Mutex::new(Vec::new()));
    let mut receipts = Vec::new();

    for value in 0..4 {
        let clone = owner.clone();
        assert!(owner.shares_owner_with(&clone));
        let active = active.clone();
        let maximum_active = maximum_active.clone();
        let order = order.clone();
        receipts.push(
            clone
                .try_execute_async(move |_| {
                    let current = active.fetch_add(1, Ordering::SeqCst) + 1;
                    maximum_active.fetch_max(current, Ordering::SeqCst);
                    order.lock().unwrap().push(value);
                    active.fetch_sub(1, Ordering::SeqCst);
                    Ok(())
                })
                .unwrap(),
        );
    }

    for receipt in receipts {
        futures::executor::block_on(receipt.wait()).unwrap();
    }
    assert_eq!(maximum_active.load(Ordering::SeqCst), 1);
    assert_eq!(*order.lock().unwrap(), vec![0, 1, 2, 3]);
}

#[test]
fn returned_operation_error_does_not_stop_the_owner() {
    let owner = WriterOwner::start(
        Connection::open_in_memory().unwrap(),
        WriterLimits::default(),
    )
    .unwrap();

    let error = owner
        .execute(|_| -> Result<()> { bail!("ordinary sqlite failure") })
        .unwrap_err();

    assert_eq!(error.to_string(), "ordinary sqlite failure");
    assert_eq!(owner.execute(|_| Ok(7)).unwrap(), 7);
}

#[test]
fn panic_rolls_back_and_fails_closed_for_current_queued_and_later_work() {
    let tempdir = tempdir().unwrap();
    let path = tempdir.path().join("writer.db");
    let connection = Connection::open(&path).unwrap();
    connection
        .execute("CREATE TABLE writes (value INTEGER NOT NULL)", [])
        .unwrap();
    let owner = WriterOwner::start_with_capacity_and_limits(connection, 2, limits(2, 2)).unwrap();
    let (entered_sender, entered_receiver) = mpsc::channel();
    let (release_sender, release_receiver) = mpsc::channel();
    let panicking = owner
        .try_execute_async_weighted(weight(1, 1), move |connection| -> Result<()> {
            let transaction = connection.transaction()?;
            transaction.execute("INSERT INTO writes VALUES (1)", [])?;
            entered_sender.send(()).unwrap();
            release_receiver.recv().unwrap();
            panic!("writer invariant failed");
        })
        .unwrap();
    entered_receiver.recv_timeout(WAIT).unwrap();

    let queued = owner
        .try_execute_async_weighted(weight(1, 1), |_| Ok(7))
        .unwrap();
    release_sender.send(()).unwrap();
    let panic_error = futures::executor::block_on(panicking.wait()).unwrap_err();
    let queued_error = futures::executor::block_on(queued.wait()).unwrap_err();
    let later_error = owner.execute(|_| Ok(9)).unwrap_err();

    assert!(matches!(
        panic_error.downcast_ref(),
        Some(StoreWriteError::OutcomeUnknown)
    ));
    assert!(matches!(
        queued_error.downcast_ref(),
        Some(StoreWriteError::OutcomeUnknown)
    ));
    assert!(matches!(
        later_error.downcast_ref(),
        Some(StoreWriteError::Unavailable)
    ));
    assert_eq!(owner.reserved_weight_for_test(), IngestWeight::ZERO);
    drop(owner);
    let connection = Connection::open(path).unwrap();
    let count: i64 = connection
        .query_row("SELECT COUNT(*) FROM writes", [], |row| row.get(0))
        .unwrap();
    assert_eq!(count, 0);
}

#[test]
fn final_drop_drains_a_full_queue_even_when_receipts_are_dropped() {
    let tempdir = tempdir().unwrap();
    let path = tempdir.path().join("writer.db");
    let connection = Connection::open(&path).unwrap();
    connection
        .execute("CREATE TABLE writes (value INTEGER NOT NULL)", [])
        .unwrap();
    let owner = WriterOwner::start_with_capacity_and_limits(connection, 2, limits(3, 3)).unwrap();
    let (entered_sender, entered_receiver) = mpsc::channel();
    let (release_sender, release_receiver) = mpsc::channel();
    let active = owner
        .try_execute_async_weighted(weight(1, 1), move |connection| {
            entered_sender.send(()).unwrap();
            release_receiver.recv().unwrap();
            connection.execute("INSERT INTO writes VALUES (1)", [])?;
            Ok(())
        })
        .unwrap();
    entered_receiver.recv_timeout(WAIT).unwrap();
    let second = owner
        .try_execute_async_weighted(weight(1, 1), |connection| {
            connection.execute("INSERT INTO writes VALUES (2)", [])?;
            Ok(())
        })
        .unwrap();
    let third = owner
        .try_execute_async_weighted(weight(1, 1), |connection| {
            connection.execute("INSERT INTO writes VALUES (3)", [])?;
            Ok(())
        })
        .unwrap();
    drop((active, second, third));

    let (drop_started_sender, drop_started_receiver) = mpsc::channel();
    let (dropped_sender, dropped_receiver) = mpsc::channel();
    let drop_thread = thread::spawn(move || {
        drop_started_sender.send(()).unwrap();
        drop(owner);
        dropped_sender.send(()).unwrap();
    });
    drop_started_receiver.recv_timeout(WAIT).unwrap();
    assert!(matches!(
        dropped_receiver.recv_timeout(Duration::from_millis(100)),
        Err(mpsc::RecvTimeoutError::Timeout)
    ));
    release_sender.send(()).unwrap();
    dropped_receiver.recv_timeout(WAIT).unwrap();
    drop_thread.join().unwrap();

    let connection = Connection::open(path).unwrap();
    let mut statement = connection
        .prepare("SELECT value FROM writes ORDER BY rowid")
        .unwrap();
    let values = statement
        .query_map([], |row| row.get::<_, i64>(0))
        .unwrap()
        .collect::<rusqlite::Result<Vec<_>>>()
        .unwrap();
    assert_eq!(values, vec![1, 2, 3]);
}

#[test]
fn dropping_the_last_owner_inside_its_worker_does_not_join_itself() {
    let owner = WriterOwner::start(
        Connection::open_in_memory().unwrap(),
        WriterLimits::default(),
    )
    .unwrap();
    let last_worker_owned_clone = owner.clone();
    let receipt = owner
        .try_execute_async(move |_| {
            drop(last_worker_owned_clone);
            Ok(5)
        })
        .unwrap();
    drop(owner);

    assert_eq!(futures::executor::block_on(receipt.wait()).unwrap(), 5);
}

#[test]
fn exact_weight_boundaries_are_admitted() {
    let owner = WriterOwner::start_with_capacity_and_limits(
        Connection::open_in_memory().unwrap(),
        1,
        limits(2, 3),
    )
    .unwrap();

    assert_eq!(owner.execute_weighted(weight(2, 3), |_| Ok(7)).unwrap(), 7);
}

#[test]
fn one_request_over_a_weight_limit_reports_its_dimension() {
    let owner = WriterOwner::start_with_capacity_and_limits(
        Connection::open_in_memory().unwrap(),
        1,
        limits(2, 3),
    )
    .unwrap();

    let records = owner
        .execute_weighted(weight(3, 1), |_| Ok(()))
        .unwrap_err();
    assert!(matches!(
        records.downcast_ref(),
        Some(StoreWriteError::TooLarge {
            dimension: WriterLimitDimension::PrimaryRecords,
            requested: 3,
            limit: 2,
        })
    ));

    let bytes = owner
        .execute_weighted(weight(1, 4), |_| Ok(()))
        .unwrap_err();
    assert!(matches!(
        bytes.downcast_ref(),
        Some(StoreWriteError::TooLarge {
            dimension: WriterLimitDimension::CanonicalBytes,
            requested: 4,
            limit: 3,
        })
    ));
}

#[test]
fn active_job_remains_charged_after_it_leaves_the_queue() {
    let owner = WriterOwner::start_with_capacity_and_limits(
        Connection::open_in_memory().unwrap(),
        2,
        limits(2, 10),
    )
    .unwrap();
    let (entered_sender, entered_receiver) = mpsc::channel();
    let (release_sender, release_receiver) = mpsc::channel();
    let active = owner
        .try_execute_async_weighted(weight(2, 1), move |_| {
            entered_sender.send(()).unwrap();
            release_receiver.recv().unwrap();
            Ok(())
        })
        .unwrap();
    entered_receiver.recv_timeout(WAIT).unwrap();

    let error = owner
        .try_execute_async_weighted(weight(1, 1), |_| Ok(()))
        .err()
        .unwrap();
    assert!(matches!(
        error.downcast_ref(),
        Some(StoreWriteError::Overloaded)
    ));
    assert_eq!(owner.reserved_weight_for_test(), weight(2, 1));

    release_sender.send(()).unwrap();
    futures::executor::block_on(active.wait()).unwrap();
    owner.execute_weighted(weight(2, 10), |_| Ok(())).unwrap();
}

#[test]
fn aggregate_rejection_rolls_back_both_dimensions_atomically() {
    let owner = WriterOwner::start_with_capacity_and_limits(
        Connection::open_in_memory().unwrap(),
        3,
        limits(3, 10),
    )
    .unwrap();
    let (entered_sender, entered_receiver) = mpsc::channel();
    let (release_sender, release_receiver) = mpsc::channel();
    let active = owner
        .try_execute_async_weighted(weight(1, 9), move |_| {
            entered_sender.send(()).unwrap();
            release_receiver.recv().unwrap();
            Ok(())
        })
        .unwrap();
    entered_receiver.recv_timeout(WAIT).unwrap();

    let error = owner
        .try_execute_async_weighted(weight(1, 2), |_| Ok(()))
        .err()
        .unwrap();
    assert!(matches!(
        error.downcast_ref(),
        Some(StoreWriteError::Overloaded)
    ));
    let exact = owner
        .try_execute_async_weighted(weight(2, 1), |_| Ok(11))
        .unwrap();

    release_sender.send(()).unwrap();
    futures::executor::block_on(active.wait()).unwrap();
    assert_eq!(futures::executor::block_on(exact.wait()).unwrap(), 11);
}

#[test]
fn channel_full_rejection_rolls_back_weight_reservation() {
    let owner = WriterOwner::start_with_capacity_and_limits(
        Connection::open_in_memory().unwrap(),
        1,
        limits(3, 3),
    )
    .unwrap();
    let (entered_sender, entered_receiver) = mpsc::channel();
    let (release_sender, release_receiver) = mpsc::channel();
    let active = owner
        .try_execute_async_weighted(weight(1, 1), move |_| {
            entered_sender.send(()).unwrap();
            release_receiver.recv().unwrap();
            Ok(())
        })
        .unwrap();
    entered_receiver.recv_timeout(WAIT).unwrap();
    let queued = owner.try_execute_async(|_| Ok(())).unwrap();

    let error = owner
        .try_execute_async_weighted(weight(2, 2), |_| Ok(()))
        .err()
        .unwrap();
    assert!(matches!(
        error.downcast_ref(),
        Some(StoreWriteError::Overloaded)
    ));
    assert_eq!(owner.reserved_weight_for_test(), weight(1, 1));

    release_sender.send(()).unwrap();
    futures::executor::block_on(active.wait()).unwrap();
    futures::executor::block_on(queued.wait()).unwrap();
    owner.execute_weighted(weight(3, 3), |_| Ok(())).unwrap();
}

#[test]
fn disconnected_channel_rejection_rolls_back_weight_reservation() {
    struct PanicOnDrop(mpsc::Sender<()>);

    impl Drop for PanicOnDrop {
        fn drop(&mut self) {
            self.0.send(()).unwrap();
            panic!("simulate writer acknowledgement failure");
        }
    }

    let owner = WriterOwner::start_with_capacity_and_limits(
        Connection::open_in_memory().unwrap(),
        1,
        limits(1, 1),
    )
    .unwrap();
    let (dropped_sender, dropped_receiver) = mpsc::channel();
    let receipt = owner
        .try_execute_async_weighted(weight(1, 1), move |_| Ok(PanicOnDrop(dropped_sender)))
        .unwrap();
    drop(receipt);
    dropped_receiver.recv_timeout(WAIT).unwrap();
    let deadline = std::time::Instant::now() + WAIT;
    while !owner.worker_finished_for_test() {
        assert!(std::time::Instant::now() < deadline);
        thread::yield_now();
    }

    let error = owner
        .try_execute_async_weighted(weight(1, 1), |_| Ok(()))
        .err()
        .unwrap();
    assert!(matches!(
        error.downcast_ref(),
        Some(StoreWriteError::Unavailable)
    ));
    assert_eq!(owner.reserved_weight_for_test(), IngestWeight::ZERO);
}

#[test]
fn reservation_underflow_never_panics_and_fails_admission_closed() {
    let owner = WriterOwner::start_with_capacity_and_limits(
        Connection::open_in_memory().unwrap(),
        1,
        limits(1, 1),
    )
    .unwrap();
    let corrupt_reservation = WeightedReservation {
        admission: owner.inner.admission.clone(),
        weight: weight(1, 1),
        armed: true,
    };

    drop(corrupt_reservation);

    assert_eq!(owner.reserved_weight_for_test(), IngestWeight::ZERO);
    let error = owner.execute(|_| Ok(())).unwrap_err();
    assert!(matches!(
        error.downcast_ref(),
        Some(StoreWriteError::Unavailable)
    ));
}

#[test]
fn dropped_receipt_does_not_release_weight_before_completion() {
    let owner = WriterOwner::start_with_capacity_and_limits(
        Connection::open_in_memory().unwrap(),
        2,
        limits(1, 1),
    )
    .unwrap();
    let (entered_sender, entered_receiver) = mpsc::channel();
    let (release_sender, release_receiver) = mpsc::channel();
    let active = owner
        .try_execute_async_weighted(weight(1, 1), move |_| {
            entered_sender.send(()).unwrap();
            release_receiver.recv().unwrap();
            Ok(())
        })
        .unwrap();
    entered_receiver.recv_timeout(WAIT).unwrap();
    drop(active);

    let error = owner
        .try_execute_async_weighted(weight(1, 1), |_| Ok(()))
        .err()
        .unwrap();
    assert!(matches!(
        error.downcast_ref(),
        Some(StoreWriteError::Overloaded)
    ));

    release_sender.send(()).unwrap();
    owner.execute(|_| Ok(())).unwrap();
    owner.execute_weighted(weight(1, 1), |_| Ok(())).unwrap();
}

#[test]
fn ordinary_operation_error_releases_weight() {
    let owner = WriterOwner::start_with_capacity_and_limits(
        Connection::open_in_memory().unwrap(),
        1,
        limits(1, 1),
    )
    .unwrap();

    let error = owner
        .execute_weighted(weight(1, 1), |_| -> Result<()> { bail!("write failed") })
        .unwrap_err();
    assert_eq!(error.to_string(), "write failed");
    owner.execute_weighted(weight(1, 1), |_| Ok(())).unwrap();
}

#[test]
fn aggregate_arithmetic_overflow_is_retryable_overload() {
    let owner = WriterOwner::start_with_capacity_and_limits(
        Connection::open_in_memory().unwrap(),
        2,
        limits(usize::MAX, usize::MAX),
    )
    .unwrap();
    let (entered_sender, entered_receiver) = mpsc::channel();
    let (release_sender, release_receiver) = mpsc::channel();
    let active = owner
        .try_execute_async_weighted(weight(usize::MAX - 1, 1), move |_| {
            entered_sender.send(()).unwrap();
            release_receiver.recv().unwrap();
            Ok(())
        })
        .unwrap();
    entered_receiver.recv_timeout(WAIT).unwrap();

    let error = owner
        .try_execute_async_weighted(weight(2, 1), |_| Ok(()))
        .err()
        .unwrap();
    assert!(matches!(
        error.downcast_ref(),
        Some(StoreWriteError::Overloaded)
    ));

    release_sender.send(()).unwrap();
    futures::executor::block_on(active.wait()).unwrap();
}
