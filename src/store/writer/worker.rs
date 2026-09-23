//! The writer owner's loop: queued jobs first, one maintenance unit between jobs when due.

use std::{
    sync::{
        Arc, Mutex,
        mpsc::{Receiver, RecvTimeoutError, TryRecvError},
    },
    time::Instant,
};

use rusqlite::Connection;

use super::{
    AdmissionState, Operation, WorkerAction, WriteJob, group,
    maintenance::{self, Maintenance},
};
use crate::store::write_observer::WriteObserver;

pub(super) struct Worker {
    pub(super) connection: Connection,
    pub(super) receiver: Receiver<WriteJob>,
    pub(super) maintenance: Box<dyn Maintenance>,
    pub(super) observer: WriteObserver,
    pub(super) admission: Arc<Mutex<AdmissionState>>,
    pub(super) group_limits: group::GroupLimits,
}

enum Next {
    Job(WriteJob),
    /// No job is ready and a maintenance unit is due.
    Maintain,
    /// The wait for a job ended without one; re-evaluate what is due.
    Idle,
    Closed,
}

impl Worker {
    pub(super) fn run(mut self) {
        // A job pulled while collecting a group that could not join it runs next.
        let mut pending = None;
        loop {
            let job = match pending.take().map_or_else(|| self.next(), Next::Job) {
                Next::Job(job) => job,
                Next::Maintain => {
                    if matches!(self.maintain(), WorkerAction::Stop) {
                        break;
                    }
                    continue;
                }
                Next::Idle => continue,
                Next::Closed => break,
            };
            if matches!(self.execute(job, &mut pending), WorkerAction::Stop) {
                break;
            }
            // Interleave at most one unit per job so maintenance progresses under load without
            // delaying queued exports by more than one bounded unit each.
            if self.maintenance.due_in(Instant::now()).is_zero()
                && matches!(self.maintain(), WorkerAction::Stop)
            {
                break;
            }
        }
    }

    fn next(&self) -> Next {
        let wait = self.maintenance.due_in(Instant::now());
        if wait.is_zero() {
            return match self.receiver.try_recv() {
                Ok(job) => Next::Job(job),
                Err(TryRecvError::Empty) => Next::Maintain,
                Err(TryRecvError::Disconnected) => Next::Closed,
            };
        }
        match self.receiver.recv_timeout(wait) {
            Ok(job) => Next::Job(job),
            Err(RecvTimeoutError::Timeout) => Next::Idle,
            Err(RecvTimeoutError::Disconnected) => Next::Closed,
        }
    }

    fn execute(&mut self, job: WriteJob, pending: &mut Option<WriteJob>) -> WorkerAction {
        match job.into_parts() {
            #[cfg(any(test, feature = "benchmark-support"))]
            (Operation::Exclusive(operation), reservation) => {
                operation(&mut self.connection, reservation)
            }
            #[cfg(any(test, feature = "benchmark-support"))]
            (Operation::FlushMaintenance(reply), _) => {
                let (action, result) = self.flush();
                reply(result);
                action
            }
            (Operation::Ingest(first), reservation) => group::run(
                group::Context {
                    connection: &mut self.connection,
                    receiver: &self.receiver,
                    pending,
                    maintenance: self.maintenance.as_mut(),
                    observer: &self.observer,
                    limits: self.group_limits,
                    admission: &self.admission,
                },
                group::Member::new(first, reservation),
            ),
        }
    }

    fn maintain(&mut self) -> WorkerAction {
        maintenance::run_unit(
            &mut self.connection,
            self.maintenance.as_mut(),
            &self.observer,
            &self.admission,
        )
        .action()
    }

    /// Runs one complete pass now, for callers that need its effects before continuing.
    #[cfg(any(test, feature = "benchmark-support"))]
    fn flush(&mut self) -> (WorkerAction, anyhow::Result<()>) {
        use maintenance::UnitOutcome;

        self.maintenance.force_pass(Instant::now());
        while self.maintenance.in_pass() {
            match maintenance::run_unit(
                &mut self.connection,
                self.maintenance.as_mut(),
                &self.observer,
                &self.admission,
            ) {
                UnitOutcome::Succeeded => {}
                UnitOutcome::Failed(error) => return (WorkerAction::Continue, Err(error)),
                UnitOutcome::Panicked => {
                    return (
                        WorkerAction::Stop,
                        Err(super::StoreWriteError::OutcomeUnknown.into()),
                    );
                }
            }
        }
        (WorkerAction::Continue, Ok(()))
    }
}
