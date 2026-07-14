use std::{
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

use super::{StoreWriteError, WriterOwner};

const WAIT: Duration = Duration::from_secs(2);

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
    let owner = WriterOwner::start(Connection::open_in_memory().unwrap()).unwrap();

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
    let owner = WriterOwner::start_with_capacity(connection, 2).unwrap();
    let (entered_sender, entered_receiver) = mpsc::channel();
    let (release_sender, release_receiver) = mpsc::channel();
    let panicking = owner
        .try_execute_async(move |connection| -> Result<()> {
            let transaction = connection.transaction()?;
            transaction.execute("INSERT INTO writes VALUES (1)", [])?;
            entered_sender.send(()).unwrap();
            release_receiver.recv().unwrap();
            panic!("writer invariant failed");
        })
        .unwrap();
    entered_receiver.recv_timeout(WAIT).unwrap();

    let queued = owner.try_execute_async(|_| Ok(7)).unwrap();
    release_sender.send(()).unwrap();
    let panic_error = futures::executor::block_on(panicking.wait()).unwrap_err();
    let queued_error = futures::executor::block_on(queued.wait()).unwrap_err();
    let later_error = owner.execute(|_| Ok(9)).unwrap_err();
    drop(owner);

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
    let owner = WriterOwner::start_with_capacity(connection, 2).unwrap();
    let (entered_sender, entered_receiver) = mpsc::channel();
    let (release_sender, release_receiver) = mpsc::channel();
    let active = owner
        .try_execute_async(move |connection| {
            entered_sender.send(()).unwrap();
            release_receiver.recv().unwrap();
            connection.execute("INSERT INTO writes VALUES (1)", [])?;
            Ok(())
        })
        .unwrap();
    entered_receiver.recv_timeout(WAIT).unwrap();
    let second = owner
        .try_execute_async(|connection| {
            connection.execute("INSERT INTO writes VALUES (2)", [])?;
            Ok(())
        })
        .unwrap();
    let third = owner
        .try_execute_async(|connection| {
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
    let owner = WriterOwner::start(Connection::open_in_memory().unwrap()).unwrap();
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
