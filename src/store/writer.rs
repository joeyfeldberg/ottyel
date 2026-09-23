mod group;

#[cfg(any(test, feature = "benchmark-support"))]
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::{
    fmt,
    num::NonZeroUsize,
    sync::{
        Arc, Mutex,
        mpsc::{self, SyncSender, TrySendError},
    },
    thread::{self, JoinHandle},
};

use anyhow::{Result, anyhow};
use futures::channel::oneshot;
use rusqlite::Connection;
use thiserror::Error;

use super::{ingest_weight::IngestWeight, write_observer::WriteObserver};

const WRITER_QUEUE_CAPACITY: usize = 64;
const DEFAULT_MAX_PRIMARY_RECORDS: usize = 40_000;
const DEFAULT_MAX_CANONICAL_BYTES: usize = 16 * 1024 * 1024;

#[cfg(any(test, feature = "benchmark-support"))]
type WriteOperation =
    Box<dyn FnOnce(&mut Connection, Option<WeightedReservation>) -> WorkerAction + Send + 'static>;
type IngestWrite = Box<dyn FnOnce(&Connection) -> Result<usize> + Send + 'static>;
type IngestReply = Box<dyn FnOnce(Result<usize>) + Send + 'static>;

/// Work that runs once inside every coalesced OTLP transaction, after the last export's writes
/// and before `COMMIT`. A failure rolls back the whole group.
pub(super) type GroupFinish = Box<dyn FnMut(&Connection) -> Result<()> + Send + 'static>;

/// One OTLP export that may share a SQLite transaction with adjacent exports.
struct IngestOperation {
    weight: IngestWeight,
    write: IngestWrite,
    reply: IngestReply,
}

enum Operation {
    /// Arbitrary connection work that always runs alone. Only tests and the benchmark submit it.
    #[cfg(any(test, feature = "benchmark-support"))]
    Exclusive(WriteOperation),
    Ingest(IngestOperation),
}

struct WriteJob {
    operation: Option<Operation>,
    reservation: Option<WeightedReservation>,
}

impl WriteJob {
    fn into_parts(mut self) -> (Operation, Option<WeightedReservation>) {
        let operation = self
            .operation
            .take()
            .expect("writer job operation can only run once");
        (operation, self.reservation.take())
    }

    /// Splits out a coalescible export, or returns an exclusive job unchanged.
    fn into_ingest(mut self) -> Result<(IngestOperation, Option<WeightedReservation>), Self> {
        match self.operation.take() {
            Some(Operation::Ingest(operation)) => Ok((operation, self.reservation.take())),
            other => {
                self.operation = other;
                Err(self)
            }
        }
    }

    fn rollback_reservation(&mut self, admission: &mut AdmissionState) -> Option<SyncSender<Self>> {
        self.reservation
            .take()
            .map(WeightedReservation::disarm)
            .and_then(|weight| admission.release(weight))
    }
}

impl Drop for WriteJob {
    fn drop(&mut self) {
        // OutcomeUnknown must not become observable before the queued weight is returned.
        drop(self.reservation.take());
        drop(self.operation.take());
    }
}

enum WorkerAction {
    Continue,
    Stop,
}

/// Aggregate admission limits for queued and currently executing OTLP SQLite writes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WriterLimits {
    max_primary_records: usize,
    max_canonical_bytes: usize,
}

impl WriterLimits {
    /// Creates nonzero aggregate limits for primary records and canonical protobuf bytes.
    #[must_use]
    pub const fn new(max_primary_records: NonZeroUsize, max_canonical_bytes: NonZeroUsize) -> Self {
        Self {
            max_primary_records: max_primary_records.get(),
            max_canonical_bytes: max_canonical_bytes.get(),
        }
    }

    /// Returns the maximum aggregate primary-record weight.
    #[must_use]
    pub const fn max_primary_records(self) -> usize {
        self.max_primary_records
    }

    /// Returns the maximum aggregate canonical protobuf byte weight.
    #[must_use]
    pub const fn max_canonical_bytes(self) -> usize {
        self.max_canonical_bytes
    }
}

impl Default for WriterLimits {
    fn default() -> Self {
        Self {
            max_primary_records: DEFAULT_MAX_PRIMARY_RECORDS,
            max_canonical_bytes: DEFAULT_MAX_CANONICAL_BYTES,
        }
    }
}

/// OTLP work admitted to the writer but not yet acknowledged, with the configured limits.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WriterBacklog {
    pub primary_records: usize,
    pub canonical_bytes: usize,
    pub max_primary_records: usize,
    pub max_canonical_bytes: usize,
}

/// The writer-admission budget exceeded by one OTLP request.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum WriterLimitDimension {
    /// Spans, log records, or metric data points.
    PrimaryRecords,
    /// The decoded request's canonical Prost-encoded length.
    CanonicalBytes,
}

impl fmt::Display for WriterLimitDimension {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::PrimaryRecords => formatter.write_str("primary records"),
            Self::CanonicalBytes => formatter.write_str("canonical protobuf bytes"),
        }
    }
}

/// Admission and acknowledgement failures from a writable [`Store`](super::Store).
#[derive(Debug, Clone, Copy, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum StoreWriteError {
    /// One OTLP operation exceeds a total writer-admission budget by itself.
    #[error("OTLP write requires {requested} {dimension}, exceeding the writer limit of {limit}")]
    TooLarge {
        dimension: WriterLimitDimension,
        requested: usize,
        limit: usize,
    },
    /// The bounded queue or an aggregate weighted budget could not admit the operation.
    #[error("sqlite writer admission capacity is exhausted")]
    Overloaded,
    /// The writer had stopped before admission, so the operation was not admitted.
    #[error("sqlite writer is unavailable")]
    Unavailable,
    /// The operation was admitted, but its commit outcome cannot be established safely.
    #[error("sqlite writer lost the acknowledgement after admission; commit outcome is unknown")]
    OutcomeUnknown,
}

pub(crate) struct AsyncWriteReceipt<T> {
    receiver: oneshot::Receiver<Result<T>>,
}

impl<T> AsyncWriteReceipt<T> {
    pub(crate) async fn wait(self) -> Result<T> {
        self.receiver
            .await
            .map_err(|_| anyhow!(StoreWriteError::OutcomeUnknown))?
    }
}

#[derive(Clone)]
pub(super) struct WriterOwner {
    inner: Arc<WriterOwnerInner>,
}

struct WriterOwnerInner {
    admission: Arc<Mutex<AdmissionState>>,
    worker: Option<JoinHandle<()>>,
    #[cfg(any(test, feature = "benchmark-support"))]
    observer: WriteObserver,
}

struct AdmissionState {
    sender: Option<SyncSender<WriteJob>>,
    accepting: bool,
    limits: WriterLimits,
    primary_records: usize,
    canonical_bytes: usize,
}

impl WriterOwner {
    pub(super) fn start(
        connection: Connection,
        limits: WriterLimits,
        finish_group: GroupFinish,
    ) -> Result<Self> {
        Self::start_with_finish(
            connection,
            WRITER_QUEUE_CAPACITY,
            limits,
            group::GroupLimits::PINNED,
            finish_group,
        )
    }

    #[cfg(test)]
    fn start_with_capacity(connection: Connection, capacity: usize) -> Result<Self> {
        Self::start_with_capacity_and_limits(connection, capacity, WriterLimits::default())
    }

    #[cfg(test)]
    fn start_with_capacity_and_limits(
        connection: Connection,
        capacity: usize,
        limits: WriterLimits,
    ) -> Result<Self> {
        Self::start_with_finish(
            connection,
            capacity,
            limits,
            group::GroupLimits::PINNED,
            Box::new(|_| Ok(())),
        )
    }

    #[cfg(test)]
    fn start_for_group_test(
        connection: Connection,
        group_limits: group::GroupLimits,
        finish_group: GroupFinish,
    ) -> Result<Self> {
        Self::start_with_finish(
            connection,
            WRITER_QUEUE_CAPACITY,
            WriterLimits::default(),
            group_limits,
            finish_group,
        )
    }

    fn start_with_finish(
        connection: Connection,
        capacity: usize,
        limits: WriterLimits,
        group_limits: group::GroupLimits,
        mut finish_group: GroupFinish,
    ) -> Result<Self> {
        let (sender, receiver) = mpsc::sync_channel::<WriteJob>(capacity);
        let admission = Arc::new(Mutex::new(AdmissionState {
            sender: Some(sender),
            accepting: true,
            limits,
            primary_records: 0,
            canonical_bytes: 0,
        }));
        let observer = WriteObserver::default();
        let worker_admission = admission.clone();
        let worker_observer = observer.clone();
        let worker = thread::Builder::new()
            .name("ottyel-sqlite-writer".to_string())
            .spawn(move || {
                let mut connection = connection;
                // A job pulled while collecting a group that could not join it runs next.
                let mut pending = None;
                while let Some(job) = pending.take().or_else(|| receiver.recv().ok()) {
                    let action = match job.into_parts() {
                        #[cfg(any(test, feature = "benchmark-support"))]
                        (Operation::Exclusive(operation), reservation) => {
                            operation(&mut connection, reservation)
                        }
                        (Operation::Ingest(first), reservation) => group::run(
                            group::Context {
                                connection: &mut connection,
                                receiver: &receiver,
                                pending: &mut pending,
                                finish: &mut finish_group,
                                observer: &worker_observer,
                                limits: group_limits,
                                admission: &worker_admission,
                            },
                            group::Member::new(first, reservation),
                        ),
                    };
                    match action {
                        WorkerAction::Continue => {}
                        WorkerAction::Stop => break,
                    }
                }
            })?;

        Ok(Self {
            inner: Arc::new(WriterOwnerInner {
                admission,
                worker: Some(worker),
                #[cfg(any(test, feature = "benchmark-support"))]
                observer,
            }),
        })
    }

    #[cfg(test)]
    pub(super) fn execute<T, F>(&self, operation: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&mut Connection) -> Result<T> + Send + 'static,
    {
        self.execute_weighted(IngestWeight::ZERO, operation)
    }

    #[cfg(test)]
    pub(super) fn execute_weighted<T, F>(&self, weight: IngestWeight, operation: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&mut Connection) -> Result<T> + Send + 'static,
    {
        let (reply_sender, reply_receiver) = mpsc::sync_channel(1);
        self.try_send(
            weight,
            self.exclusive(operation, move |result| {
                let _ = reply_sender.send(result);
            }),
        )?;

        reply_receiver
            .recv()
            .map_err(|_| anyhow!(StoreWriteError::OutcomeUnknown))?
    }

    /// Admits one OTLP export whose writes may share a transaction with adjacent exports and
    /// waits for its definitive acknowledgement.
    pub(super) fn execute_ingest<F>(&self, weight: IngestWeight, write: F) -> Result<usize>
    where
        F: FnOnce(&Connection) -> Result<usize> + Send + 'static,
    {
        let (reply_sender, reply_receiver) = mpsc::sync_channel(1);
        self.try_send(
            weight,
            ingest_operation(weight, write, move |result| {
                let _ = reply_sender.send(result);
            }),
        )?;

        reply_receiver
            .recv()
            .map_err(|_| anyhow!(StoreWriteError::OutcomeUnknown))?
    }

    /// Admits one coalescible OTLP export and returns its asynchronous acknowledgement.
    pub(super) fn try_execute_ingest_async<F>(
        &self,
        weight: IngestWeight,
        write: F,
    ) -> Result<AsyncWriteReceipt<usize>>
    where
        F: FnOnce(&Connection) -> Result<usize> + Send + 'static,
    {
        let (reply_sender, reply_receiver) = oneshot::channel();
        self.try_send(
            weight,
            ingest_operation(weight, write, move |result| {
                let _ = reply_sender.send(result);
            }),
        )?;
        Ok(AsyncWriteReceipt {
            receiver: reply_receiver,
        })
    }

    #[cfg(any(test, feature = "benchmark-support"))]
    pub(super) fn try_execute_async<T, F>(&self, operation: F) -> Result<AsyncWriteReceipt<T>>
    where
        T: Send + 'static,
        F: FnOnce(&mut Connection) -> Result<T> + Send + 'static,
    {
        self.try_execute_async_weighted(IngestWeight::ZERO, operation)
    }

    #[cfg(any(test, feature = "benchmark-support"))]
    pub(super) fn try_execute_async_weighted<T, F>(
        &self,
        weight: IngestWeight,
        operation: F,
    ) -> Result<AsyncWriteReceipt<T>>
    where
        T: Send + 'static,
        F: FnOnce(&mut Connection) -> Result<T> + Send + 'static,
    {
        let (reply_sender, reply_receiver) = oneshot::channel();
        self.try_send(
            weight,
            self.exclusive(operation, move |result| {
                let _ = reply_sender.send(result);
            }),
        )?;
        Ok(AsyncWriteReceipt {
            receiver: reply_receiver,
        })
    }

    #[cfg(any(test, feature = "benchmark-support"))]
    pub(super) fn observer(&self) -> WriteObserver {
        self.inner.observer.clone()
    }

    #[cfg(any(test, feature = "benchmark-support"))]
    fn exclusive<T, F, S>(&self, operation: F, send_reply: S) -> Operation
    where
        T: Send + 'static,
        F: FnOnce(&mut Connection) -> Result<T> + Send + 'static,
        S: FnOnce(Result<T>) + Send + 'static,
    {
        Operation::Exclusive(wrap_exclusive(
            operation,
            send_reply,
            self.inner.admission.clone(),
        ))
    }

    fn try_send(&self, weight: IngestWeight, operation: Operation) -> Result<()> {
        let mut admission = self
            .inner
            .admission
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if !admission.accepting {
            return Err(StoreWriteError::Unavailable.into());
        }
        if admission.sender.is_none() {
            return Err(StoreWriteError::Unavailable.into());
        }
        admission.reserve(weight)?;
        let reservation = (!weight.is_zero()).then(|| WeightedReservation {
            admission: self.inner.admission.clone(),
            weight,
            armed: true,
        });
        let job = WriteJob {
            operation: Some(operation),
            reservation,
        };
        let result = admission
            .sender
            .as_ref()
            .expect("sender presence checked under the same lock")
            .try_send(job);
        match result {
            Ok(()) => {
                drop(admission);
                Ok(())
            }
            Err(TrySendError::Full(job)) => {
                let mut job = job;
                let sender = job.rollback_reservation(&mut admission);
                drop(admission);
                drop(sender);
                drop(job);
                Err(StoreWriteError::Overloaded.into())
            }
            Err(TrySendError::Disconnected(job)) => {
                let mut job = job;
                let rollback_sender = job.rollback_reservation(&mut admission);
                admission.accepting = false;
                let sender = admission.sender.take();
                drop(admission);
                drop(sender);
                drop(rollback_sender);
                drop(job);
                Err(StoreWriteError::Unavailable.into())
            }
        }
    }

    pub(super) fn backlog(&self) -> WriterBacklog {
        let admission = self
            .inner
            .admission
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        WriterBacklog {
            primary_records: admission.primary_records,
            canonical_bytes: admission.canonical_bytes,
            max_primary_records: admission.limits.max_primary_records,
            max_canonical_bytes: admission.limits.max_canonical_bytes,
        }
    }

    #[cfg(test)]
    pub(super) fn shares_owner_with(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.inner, &other.inner)
    }

    #[cfg(test)]
    fn reserved_weight_for_test(&self) -> IngestWeight {
        let admission = self
            .inner
            .admission
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        IngestWeight {
            primary_records: admission.primary_records,
            canonical_bytes: admission.canonical_bytes,
        }
    }

    #[cfg(test)]
    fn worker_finished_for_test(&self) -> bool {
        self.inner
            .worker
            .as_ref()
            .is_none_or(JoinHandle::is_finished)
    }
}

impl AdmissionState {
    fn reserve(&mut self, weight: IngestWeight) -> Result<()> {
        if weight.primary_records > self.limits.max_primary_records {
            return Err(StoreWriteError::TooLarge {
                dimension: WriterLimitDimension::PrimaryRecords,
                requested: weight.primary_records,
                limit: self.limits.max_primary_records,
            }
            .into());
        }
        if weight.canonical_bytes > self.limits.max_canonical_bytes {
            return Err(StoreWriteError::TooLarge {
                dimension: WriterLimitDimension::CanonicalBytes,
                requested: weight.canonical_bytes,
                limit: self.limits.max_canonical_bytes,
            }
            .into());
        }

        let Some(primary_records) = self
            .primary_records
            .checked_add(weight.primary_records)
            .filter(|total| *total <= self.limits.max_primary_records)
        else {
            return Err(StoreWriteError::Overloaded.into());
        };
        let Some(canonical_bytes) = self
            .canonical_bytes
            .checked_add(weight.canonical_bytes)
            .filter(|total| *total <= self.limits.max_canonical_bytes)
        else {
            return Err(StoreWriteError::Overloaded.into());
        };
        self.primary_records = primary_records;
        self.canonical_bytes = canonical_bytes;
        Ok(())
    }

    fn release(&mut self, weight: IngestWeight) -> Option<SyncSender<WriteJob>> {
        let Some(primary_records) = self.primary_records.checked_sub(weight.primary_records) else {
            self.accepting = false;
            return self.sender.take();
        };
        let Some(canonical_bytes) = self.canonical_bytes.checked_sub(weight.canonical_bytes) else {
            self.accepting = false;
            return self.sender.take();
        };
        self.primary_records = primary_records;
        self.canonical_bytes = canonical_bytes;
        None
    }
}

struct WeightedReservation {
    admission: Arc<Mutex<AdmissionState>>,
    weight: IngestWeight,
    armed: bool,
}

impl WeightedReservation {
    fn disarm(mut self) -> IngestWeight {
        self.armed = false;
        self.weight
    }
}

impl Drop for WeightedReservation {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let mut admission = self
            .admission
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let sender = admission.release(self.weight);
        drop(admission);
        drop(sender);
    }
}

impl fmt::Debug for WriterOwner {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let admission = self
            .inner
            .admission
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        formatter
            .debug_struct("WriterOwner")
            .field("queue_capacity", &WRITER_QUEUE_CAPACITY)
            .field("limits", &admission.limits)
            .field("reserved_primary_records", &admission.primary_records)
            .field("reserved_canonical_bytes", &admission.canonical_bytes)
            .finish_non_exhaustive()
    }
}

impl Drop for WriterOwnerInner {
    fn drop(&mut self) {
        // Closing the last sender lets the owner drain already-admitted jobs before exit.
        let sender = self
            .admission
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .sender
            .take();
        drop(sender);
        if let Some(worker) = self.worker.take()
            && worker.thread().id() != thread::current().id()
        {
            let _ = worker.join();
        }
    }
}

fn ingest_operation<F, S>(weight: IngestWeight, write: F, send_reply: S) -> Operation
where
    F: FnOnce(&Connection) -> Result<usize> + Send + 'static,
    S: FnOnce(Result<usize>) + Send + 'static,
{
    Operation::Ingest(IngestOperation {
        weight,
        write: Box::new(write),
        reply: Box::new(send_reply),
    })
}

#[cfg(any(test, feature = "benchmark-support"))]
fn wrap_exclusive<T, F, S>(
    operation: F,
    send_reply: S,
    admission: Arc<Mutex<AdmissionState>>,
) -> WriteOperation
where
    T: Send + 'static,
    F: FnOnce(&mut Connection) -> Result<T> + Send + 'static,
    S: FnOnce(Result<T>) + Send + 'static,
{
    Box::new(move |connection, reservation| {
        // AssertUnwindSafe is valid because a panic closes admission and terminates the owner;
        // the possibly tainted Connection is dropped instead of being reused.
        match catch_unwind(AssertUnwindSafe(|| operation(connection))) {
            Ok(result) => {
                drop(reservation);
                send_reply(result);
                WorkerAction::Continue
            }
            Err(_) => {
                close_admission(&admission);
                drop(reservation);
                send_reply(Err(StoreWriteError::OutcomeUnknown.into()));
                WorkerAction::Stop
            }
        }
    })
}

/// Stops admission after a panic so no later work reuses a possibly tainted connection.
fn close_admission(admission: &Mutex<AdmissionState>) {
    let mut admission = admission
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    admission.accepting = false;
    let sender = admission.sender.take();
    drop(admission);
    drop(sender);
}

#[cfg(test)]
#[path = "writer_tests.rs"]
mod tests;
