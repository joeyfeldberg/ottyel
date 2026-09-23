//! Deferred writer work, such as retention, that runs between ingest groups.
//!
//! Each unit runs in its own transaction, so a large deletion never holds the writer for longer
//! than one bounded unit, and queued exports interleave with it one job per unit.

use std::{
    panic::{AssertUnwindSafe, catch_unwind},
    sync::Mutex,
    time::{Duration, Instant},
};

use anyhow::Result;
use rusqlite::Connection;

use super::{AdmissionState, WorkerAction, close_admission};
use crate::store::write_observer::WriteObserver;

/// Work the writer owner schedules between ingest groups.
pub(in crate::store) trait Maintenance: Send + 'static {
    /// Notes primary records committed by one ingest group.
    fn ingested(&mut self, records: usize);

    /// Time until the next unit is due, or zero when one should run now.
    fn due_in(&self, now: Instant) -> Duration;

    /// Starts a pass immediately, even if none is due.
    #[cfg(any(test, feature = "benchmark-support"))]
    fn force_pass(&mut self, now: Instant);

    /// Whether a started pass still has units to run.
    #[cfg(any(test, feature = "benchmark-support"))]
    fn in_pass(&self) -> bool;

    /// Runs one bounded unit inside `transaction`. The writer commits on `Ok` and rolls back on
    /// `Err`.
    fn run_unit(&mut self, transaction: &Connection, now: Instant) -> Result<()>;

    /// Recovers after a failed unit, for example by abandoning the pass until the next interval.
    fn failed(&mut self, now: Instant);
}

/// Maintenance for a writer that has none, such as the writer tests' bare connection.
#[cfg(test)]
pub(super) struct NoMaintenance;

#[cfg(test)]
impl Maintenance for NoMaintenance {
    fn ingested(&mut self, _records: usize) {}

    fn due_in(&self, _now: Instant) -> Duration {
        Duration::MAX
    }

    fn force_pass(&mut self, _now: Instant) {}

    fn in_pass(&self) -> bool {
        false
    }

    fn run_unit(&mut self, _transaction: &Connection, _now: Instant) -> Result<()> {
        Ok(())
    }

    fn failed(&mut self, _now: Instant) {}
}

/// The outcome of one unit, for callers that must report failures.
pub(super) enum UnitOutcome {
    Succeeded,
    // Only the explicit flush reports the error; scheduled units record it in admission state.
    #[cfg_attr(not(any(test, feature = "benchmark-support")), allow(dead_code))]
    Failed(anyhow::Error),
    /// The unit panicked; admission is closed and the owner must stop.
    Panicked,
}

pub(super) fn run_unit(
    connection: &mut Connection,
    maintenance: &mut dyn Maintenance,
    observer: &WriteObserver,
    admission: &Mutex<AdmissionState>,
) -> UnitOutcome {
    let now = Instant::now();
    let observation = observer.maintenance();
    // AssertUnwindSafe is valid because a panic closes admission and terminates the owner; the
    // unwound transaction rolls back and the possibly tainted Connection is never reused.
    let result = catch_unwind(AssertUnwindSafe(|| -> Result<()> {
        let transaction = connection.transaction()?;
        let committed = observer.maintenance_transaction();
        maintenance.run_unit(&transaction, now)?;
        transaction.commit()?;
        committed.committed();
        Ok(())
    }));
    match result {
        Ok(Ok(())) => {
            observation.succeeded();
            UnitOutcome::Succeeded
        }
        Ok(Err(error)) => {
            maintenance.failed(now);
            admission
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .note_maintenance_failure(&error);
            UnitOutcome::Failed(error)
        }
        Err(_) => {
            close_admission(admission);
            UnitOutcome::Panicked
        }
    }
}

impl UnitOutcome {
    pub(super) fn action(&self) -> WorkerAction {
        match self {
            Self::Succeeded | Self::Failed(_) => WorkerAction::Continue,
            Self::Panicked => WorkerAction::Stop,
        }
    }
}

#[cfg(test)]
#[path = "maintenance_tests.rs"]
mod tests;
