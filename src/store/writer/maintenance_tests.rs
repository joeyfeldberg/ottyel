use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
        mpsc,
    },
    time::{Duration, Instant},
};

use anyhow::{Result, bail};
use rusqlite::Connection;
use tempfile::{TempDir, tempdir};

use super::Maintenance;
use crate::store::{
    ingest_weight::IngestWeight,
    writer::{StoreWriteError, WriterOwner, group::GroupLimits},
};

const WAIT: Duration = Duration::from_secs(2);

#[derive(Clone, Copy)]
enum Unit {
    Write,
    Fail,
    Panic,
}

/// Runs the scripted units, one per due slot once armed, recording each in `log`.
struct Scripted {
    armed: Arc<AtomicBool>,
    units: Vec<Unit>,
    log: Arc<Mutex<Vec<String>>>,
    ran: Arc<AtomicUsize>,
}

impl Maintenance for Scripted {
    fn ingested(&mut self, _records: usize) {}

    fn due_in(&self, _now: Instant) -> Duration {
        if self.units.is_empty() || !self.armed.load(Ordering::SeqCst) {
            Duration::MAX
        } else {
            Duration::ZERO
        }
    }

    fn force_pass(&mut self, _now: Instant) {}

    fn in_pass(&self) -> bool {
        !self.units.is_empty()
    }

    fn run_unit(&mut self, transaction: &Connection, _now: Instant) -> Result<()> {
        let unit = self.units.remove(0);
        let index = self.ran.fetch_add(1, Ordering::SeqCst);
        self.log.lock().unwrap().push(format!("unit-{index}"));
        transaction.execute("INSERT INTO maintained (unit) VALUES (?1)", [index as i64])?;
        match unit {
            Unit::Write => Ok(()),
            Unit::Fail => bail!("retention failed"),
            Unit::Panic => panic!("retention panicked"),
        }
    }

    fn failed(&mut self, _now: Instant) {}
}

struct Harness {
    _directory: TempDir,
    path: std::path::PathBuf,
    owner: WriterOwner,
    log: Arc<Mutex<Vec<String>>>,
    ran: Arc<AtomicUsize>,
}

impl Harness {
    /// Starts an owner parked on an exclusive job and arms the script, so every unit is due
    /// the moment the returned sender releases the owner.
    fn parked(units: Vec<Unit>) -> (Self, mpsc::Sender<()>) {
        let directory = tempdir().unwrap();
        let path = directory.path().join("maintenance.db");
        let connection = Connection::open(&path).unwrap();
        connection
            .execute_batch(
                "CREATE TABLE maintained (unit INTEGER NOT NULL);
                 CREATE TABLE writes (export INTEGER NOT NULL);",
            )
            .unwrap();
        let log = Arc::new(Mutex::new(Vec::new()));
        let ran = Arc::new(AtomicUsize::new(0));
        let armed = Arc::new(AtomicBool::new(false));
        let (entered_sender, entered) = mpsc::channel();
        let (release, released) = mpsc::channel::<()>();
        let owner = WriterOwner::start_for_group_test(
            connection,
            GroupLimits::PINNED,
            Box::new(Scripted {
                armed: armed.clone(),
                units,
                log: log.clone(),
                ran: ran.clone(),
            }),
        )
        .unwrap();
        drop(
            owner
                .try_execute_async(move |_| {
                    entered_sender.send(()).unwrap();
                    released.recv().unwrap();
                    Ok(())
                })
                .unwrap(),
        );
        entered.recv_timeout(WAIT).unwrap();
        armed.store(true, Ordering::SeqCst);
        (
            Self {
                _directory: directory,
                path,
                owner,
                log,
                ran,
            },
            release,
        )
    }

    fn submit_export(&self, export: i64) -> crate::store::writer::AsyncWriteReceipt<usize> {
        let log = self.log.clone();
        self.owner
            .try_execute_ingest_async(
                IngestWeight {
                    primary_records: 1,
                    canonical_bytes: 1,
                },
                move |connection| {
                    log.lock().unwrap().push(format!("export-{export}"));
                    connection.execute("INSERT INTO writes (export) VALUES (?1)", [export])?;
                    Ok(1)
                },
            )
            .unwrap()
    }

    fn committed(&self, table: &str) -> Vec<i64> {
        let connection = Connection::open(&self.path).unwrap();
        let column = if table == "maintained" {
            "unit"
        } else {
            "export"
        };
        connection
            .prepare(&format!("SELECT {column} FROM {table} ORDER BY rowid"))
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .map(Result::unwrap)
            .collect()
    }

    fn wait_for_units(&self, units: usize) {
        let started = Instant::now();
        while self.ran.load(Ordering::SeqCst) < units {
            assert!(started.elapsed() < WAIT, "maintenance units never ran");
            std::thread::sleep(Duration::from_millis(1));
        }
    }
}

#[test]
fn due_units_run_while_idle_and_commit_in_their_own_transactions() {
    let (harness, release) = Harness::parked(vec![Unit::Write, Unit::Write]);
    release.send(()).unwrap();
    harness.wait_for_units(2);
    // A zero-weight probe runs after both units because the owner is sequential.
    harness.owner.execute(|_| Ok(())).unwrap();

    assert_eq!(harness.committed("maintained"), vec![0, 1]);
    assert_eq!(harness.owner.observer().snapshot().maintenance_units, 2);
    assert_eq!(
        harness
            .owner
            .observer()
            .snapshot()
            .maintenance_transactions_committed,
        2
    );
}

#[test]
fn queued_exports_interleave_with_due_units_one_per_job() {
    let (harness, release) = Harness::parked(vec![Unit::Write; 3]);
    // Exclusive jobs never coalesce, so each is one job between units.
    let receipts: Vec<_> = (0..3)
        .map(|job| {
            let log = harness.log.clone();
            harness
                .owner
                .try_execute_async(move |_| {
                    log.lock().unwrap().push(format!("job-{job}"));
                    Ok(())
                })
                .unwrap()
        })
        .collect();
    release.send(()).unwrap();
    for receipt in receipts {
        futures::executor::block_on(receipt.wait()).unwrap();
    }
    harness.wait_for_units(3);

    assert_eq!(
        *harness.log.lock().unwrap(),
        // The parking job itself is followed by the first unit.
        vec!["unit-0", "job-0", "unit-1", "job-1", "unit-2", "job-2"]
    );
}

#[test]
fn a_failed_unit_rolls_back_alone_and_the_owner_keeps_ingesting() {
    let (harness, release) = Harness::parked(vec![Unit::Fail, Unit::Write]);
    release.send(()).unwrap();
    harness.wait_for_units(2);
    let accepted = futures::executor::block_on(harness.submit_export(7).wait()).unwrap();

    assert_eq!(accepted, 1);
    assert_eq!(harness.committed("maintained"), vec![1]);
    assert_eq!(harness.committed("writes"), vec![7]);
    assert_eq!(harness.owner.backlog().maintenance_failures, 1);
    let snapshot = harness.owner.observer().snapshot();
    assert_eq!(
        (
            snapshot.maintenance_failures,
            snapshot.maintenance_transactions_not_committed
        ),
        (1, 1)
    );
}

#[test]
fn a_panicking_unit_fails_the_owner_closed() {
    let (harness, release) = Harness::parked(vec![Unit::Panic]);
    release.send(()).unwrap();
    harness.wait_for_units(1);
    let started = Instant::now();
    while !harness.owner.worker_finished_for_test() {
        assert!(started.elapsed() < WAIT, "owner kept running after a panic");
        std::thread::sleep(Duration::from_millis(1));
    }

    assert!(harness.committed("maintained").is_empty());
    let later = harness
        .owner
        .try_execute_ingest_async(IngestWeight::ZERO, |_| Ok(0))
        .err()
        .expect("admission closes after a maintenance panic");
    assert!(matches!(
        later.downcast_ref(),
        Some(StoreWriteError::Unavailable)
    ));
}
