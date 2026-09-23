//! Opportunistic coalescing of adjacent OTLP exports into one SQLite transaction.
//!
//! The owner never waits for more work. After each export it takes only jobs that are already
//! queued, stops at the first job that cannot join, and runs group finish work (retention) once
//! before `COMMIT`. Each export writes inside its own savepoint, so an ordinary export failure
//! rolls back only that export.

use std::{
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{Mutex, mpsc::Receiver},
    time::{Duration, Instant},
};

use anyhow::{Result, anyhow};
use rusqlite::Connection;

use super::{
    AdmissionState, GroupFinish, IngestOperation, IngestReply, IngestWrite, Operation,
    StoreWriteError, WeightedReservation, WorkerAction, WriteJob, close_admission,
};
use crate::store::{ingest_weight::IngestWeight, write_observer::WriteObserver};

/// Inclusive bounds on one coalesced transaction. An admitted export that exceeds them by itself
/// still runs, alone.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct GroupLimits {
    pub(super) max_exports: usize,
    pub(super) max_primary_records: usize,
    pub(super) max_canonical_bytes: usize,
    /// Checked between exports only; one slow export is never interrupted.
    pub(super) max_transaction_age: Duration,
}

impl GroupLimits {
    /// The `opportunistic_adjacent_otlp` v1 policy measured by the writer-coalescing benchmark.
    pub(super) const PINNED: Self = Self {
        max_exports: 4,
        max_primary_records: 10_000,
        max_canonical_bytes: 4 * 1024 * 1024,
        max_transaction_age: Duration::from_millis(25),
    };
}

pub(super) struct Context<'a> {
    pub(super) connection: &'a mut Connection,
    pub(super) receiver: &'a Receiver<WriteJob>,
    pub(super) pending: &'a mut Option<WriteJob>,
    pub(super) finish: &'a mut GroupFinish,
    pub(super) observer: &'a WriteObserver,
    pub(super) limits: GroupLimits,
    pub(super) admission: &'a Mutex<AdmissionState>,
}

pub(super) struct Member {
    weight: IngestWeight,
    write: Option<IngestWrite>,
    reply: IngestReply,
    reservation: Option<WeightedReservation>,
    outcome: Option<Result<usize>>,
}

impl Member {
    pub(super) fn new(
        operation: IngestOperation,
        reservation: Option<WeightedReservation>,
    ) -> Self {
        Self {
            weight: operation.weight,
            write: Some(operation.write),
            reply: operation.reply,
            reservation,
            outcome: None,
        }
    }
}

pub(super) fn run(context: Context<'_>, first: Member) -> WorkerAction {
    let Context {
        connection,
        receiver,
        pending,
        finish,
        observer,
        limits,
        admission,
    } = context;
    let mut members = vec![first];
    // AssertUnwindSafe is valid because a panic closes admission and terminates the owner; the
    // unwound transaction rolls back and the possibly tainted Connection is never reused.
    let executed = catch_unwind(AssertUnwindSafe(|| {
        execute(
            connection,
            receiver,
            pending,
            finish,
            observer,
            limits,
            &mut members,
        )
    }));

    // Every member's weight returns before any acknowledgement becomes observable.
    let finished: Vec<_> = members
        .into_iter()
        .map(|member| {
            drop(member.reservation);
            (member.reply, member.outcome)
        })
        .collect();
    match executed {
        Ok(Ok(())) => {
            for (reply, outcome) in finished {
                reply(outcome.expect("every committed group member has an outcome"));
            }
            WorkerAction::Continue
        }
        Ok(Err(error)) => {
            let message = format!("{error:#}");
            for (reply, outcome) in finished {
                reply(match outcome {
                    Some(Err(own_error)) => Err(own_error),
                    Some(Ok(_)) | None => {
                        Err(anyhow!("coalesced OTLP transaction rolled back: {message}"))
                    }
                });
            }
            WorkerAction::Continue
        }
        Err(_) => {
            close_admission(admission);
            for (reply, _) in finished {
                reply(Err(StoreWriteError::OutcomeUnknown.into()));
            }
            WorkerAction::Stop
        }
    }
}

fn execute(
    connection: &mut Connection,
    receiver: &Receiver<WriteJob>,
    pending: &mut Option<WriteJob>,
    finish: &mut GroupFinish,
    observer: &WriteObserver,
    limits: GroupLimits,
    members: &mut Vec<Member>,
) -> Result<()> {
    let started = Instant::now();
    let mut transaction = connection.transaction()?;
    let observation = observer.shared_ingest_retention_transaction();
    let mut totals = IngestWeight::ZERO;

    loop {
        let member = members.last_mut().expect("a group always has a member");
        totals = IngestWeight {
            primary_records: totals
                .primary_records
                .saturating_add(member.weight.primary_records),
            canonical_bytes: totals
                .canonical_bytes
                .saturating_add(member.weight.canonical_bytes),
        };
        let write = member.write.take().expect("group member writes run once");
        let savepoint = transaction.savepoint()?;
        let outcome = write(&savepoint);
        if outcome.is_ok() {
            savepoint.commit()?;
        } else {
            // The default drop behavior rolls back to the savepoint and releases it.
            savepoint.finish()?;
        }
        member.outcome = Some(outcome);

        match next_member(receiver, pending, limits, members.len(), totals, started) {
            Some(next) => members.push(next),
            None => break,
        }
    }

    observer.group_formed(members.len());
    let retention = observer.retention();
    finish(&transaction)?;
    retention.succeeded();
    transaction.commit()?;
    observation.committed();
    Ok(())
}

fn next_member(
    receiver: &Receiver<WriteJob>,
    pending: &mut Option<WriteJob>,
    limits: GroupLimits,
    exports: usize,
    totals: IngestWeight,
    started: Instant,
) -> Option<Member> {
    if exports >= limits.max_exports || started.elapsed() > limits.max_transaction_age {
        return None;
    }
    let (operation, reservation) = match receiver.try_recv().ok()?.into_ingest() {
        Ok(ingest) => ingest,
        Err(exclusive) => {
            *pending = Some(exclusive);
            return None;
        }
    };
    let joins = totals
        .primary_records
        .checked_add(operation.weight.primary_records)
        .is_some_and(|records| records <= limits.max_primary_records)
        && totals
            .canonical_bytes
            .checked_add(operation.weight.canonical_bytes)
            .is_some_and(|bytes| bytes <= limits.max_canonical_bytes);
    if !joins {
        *pending = Some(WriteJob {
            operation: Some(Operation::Ingest(operation)),
            reservation,
        });
        return None;
    }
    Some(Member::new(operation, reservation))
}

#[cfg(test)]
#[path = "group_tests.rs"]
mod tests;
