use std::{
    fmt,
    panic::{AssertUnwindSafe, catch_unwind},
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

const WRITER_QUEUE_CAPACITY: usize = 64;

type WriteJob = Box<dyn FnOnce(&mut Connection) -> WorkerAction + Send + 'static>;

enum WorkerAction {
    Continue,
    Stop,
}

/// Queue and acknowledgement failures from a writable [`Store`](super::Store).
#[derive(Debug, Clone, Copy, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum StoreWriteError {
    /// The bounded queue was full, so the operation was not admitted.
    #[error("sqlite writer queue is full")]
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
}

struct AdmissionState {
    sender: Option<SyncSender<WriteJob>>,
    accepting: bool,
}

impl WriterOwner {
    pub(super) fn start(connection: Connection) -> Result<Self> {
        Self::start_with_capacity(connection, WRITER_QUEUE_CAPACITY)
    }

    fn start_with_capacity(connection: Connection, capacity: usize) -> Result<Self> {
        let (sender, receiver) = mpsc::sync_channel::<WriteJob>(capacity);
        let worker = thread::Builder::new()
            .name("ottyel-sqlite-writer".to_string())
            .spawn(move || {
                let mut connection = connection;
                while let Ok(job) = receiver.recv() {
                    match job(&mut connection) {
                        WorkerAction::Continue => {}
                        WorkerAction::Stop => break,
                    }
                }
            })?;

        Ok(Self {
            inner: Arc::new(WriterOwnerInner {
                admission: Arc::new(Mutex::new(AdmissionState {
                    sender: Some(sender),
                    accepting: true,
                })),
                worker: Some(worker),
            }),
        })
    }

    pub(super) fn execute<T, F>(&self, operation: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&mut Connection) -> Result<T> + Send + 'static,
    {
        let (reply_sender, reply_receiver) = mpsc::sync_channel(1);
        self.try_send(wrap_job(
            operation,
            move |result| {
                let _ = reply_sender.send(result);
            },
            self.inner.admission.clone(),
        ))?;

        reply_receiver
            .recv()
            .map_err(|_| anyhow!(StoreWriteError::OutcomeUnknown))?
    }

    pub(super) fn try_execute_async<T, F>(&self, operation: F) -> Result<AsyncWriteReceipt<T>>
    where
        T: Send + 'static,
        F: FnOnce(&mut Connection) -> Result<T> + Send + 'static,
    {
        let (reply_sender, reply_receiver) = oneshot::channel();
        self.try_send(wrap_job(
            operation,
            move |result| {
                let _ = reply_sender.send(result);
            },
            self.inner.admission.clone(),
        ))?;
        Ok(AsyncWriteReceipt {
            receiver: reply_receiver,
        })
    }

    fn try_send(&self, job: WriteJob) -> Result<()> {
        let admission = self
            .inner
            .admission
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if !admission.accepting {
            return Err(StoreWriteError::Unavailable.into());
        }
        let Some(sender) = admission.sender.as_ref() else {
            return Err(StoreWriteError::Unavailable.into());
        };
        match sender.try_send(job) {
            Ok(()) => Ok(()),
            Err(TrySendError::Full(_)) => Err(StoreWriteError::Overloaded.into()),
            Err(TrySendError::Disconnected(_)) => Err(StoreWriteError::Unavailable.into()),
        }
    }

    #[cfg(test)]
    pub(super) fn shares_owner_with(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.inner, &other.inner)
    }
}

impl fmt::Debug for WriterOwner {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("WriterOwner")
            .field("queue_capacity", &WRITER_QUEUE_CAPACITY)
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

fn wrap_job<T, F, S>(operation: F, send_reply: S, admission: Arc<Mutex<AdmissionState>>) -> WriteJob
where
    T: Send + 'static,
    F: FnOnce(&mut Connection) -> Result<T> + Send + 'static,
    S: FnOnce(Result<T>) + Send + 'static,
{
    Box::new(move |connection| {
        // AssertUnwindSafe is valid because a panic closes admission and terminates the owner;
        // the possibly tainted Connection is dropped instead of being reused.
        match catch_unwind(AssertUnwindSafe(|| operation(connection))) {
            Ok(result) => {
                send_reply(result);
                WorkerAction::Continue
            }
            Err(_) => {
                let mut admission = admission
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                admission.accepting = false;
                drop(admission.sender.take());
                drop(admission);
                send_reply(Err(StoreWriteError::OutcomeUnknown.into()));
                WorkerAction::Stop
            }
        }
    })
}

#[cfg(test)]
#[path = "writer_tests.rs"]
mod tests;
