mod helpers;
mod ingest;
mod ingest_weight;
mod queries;
mod reader_pool;
mod retention;
mod schema;
mod write_observer;
mod writer;

#[cfg(feature = "benchmark-support")]
#[doc(hidden)]
pub mod benchmark_support;

use std::{fs, path::Path};

use anyhow::{Context, Result, bail};
pub(crate) use ingest_weight::{MeasureIngest, PreparedIngest};
use reader_pool::{ReaderLease, ReaderPool};
use rusqlite::Connection;
pub(crate) use writer::AsyncWriteReceipt;
use writer::WriterOwner;
pub use writer::{StoreWriteError, WriterBacklog, WriterDrain, WriterLimitDimension, WriterLimits};

#[derive(Debug, Clone, Copy)]
pub(super) struct RetentionPolicy {
    hours: u64,
    maximum_spans: usize,
}

#[derive(Debug, Clone)]
enum StoreAccess {
    ReadWrite { writer: WriterOwner },
    ReadOnly,
}

#[derive(Debug, Clone)]
pub struct Store {
    access: StoreAccess,
    readers: ReaderPool,
}

impl Store {
    /// Opens or creates a filesystem-backed database with one writer and a bounded reader pool.
    pub fn open(path: &Path, retention_hours: u64, max_spans: usize) -> Result<Self> {
        Self::open_with_writer_limits(path, retention_hours, max_spans, WriterLimits::default())
    }

    /// Opens or creates a database with explicit aggregate OTLP writer-admission limits.
    pub fn open_with_writer_limits(
        path: &Path,
        retention_hours: u64,
        max_spans: usize,
        writer_limits: WriterLimits,
    ) -> Result<Self> {
        validate_file_backed_path(path)?;
        if let Some(parent) = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
        {
            fs::create_dir_all(parent)
                .with_context(|| format!("failed to create {}", parent.display()))?;
        }

        let mut conn = Connection::open(path)
            .with_context(|| format!("failed to open sqlite db {}", path.display()))?;
        schema::initialize(&mut conn)
            .with_context(|| format!("failed to initialize sqlite db {}", path.display()))?;
        let retention = RetentionPolicy {
            hours: retention_hours,
            maximum_spans: max_spans,
        };
        let finish_group = Box::new(move |conn: &Connection| retention::enforce(conn, retention));
        let writer = WriterOwner::start(conn, writer_limits, finish_group)
            .with_context(|| format!("failed to start sqlite writer for {}", path.display()))?;
        let readers = ReaderPool::open(path).with_context(|| {
            format!("failed to initialize sqlite readers for {}", path.display())
        })?;

        Ok(Self {
            access: StoreAccess::ReadWrite { writer },
            readers,
        })
    }

    /// Opens an existing compatible filesystem-backed database without migrations or writes to
    /// its schema, data, or persistent settings.
    ///
    /// SQLite may create or update `-wal`/`-shm` coordination sidecars so pooled readers can
    /// observe commits from a live WAL writer.
    pub fn open_read_only(path: &Path) -> Result<Self> {
        validate_file_backed_path(path)?;
        let readers = ReaderPool::open_validated(path, |connection| {
            schema::validate_read_only(connection).with_context(|| {
                format!("failed to validate read-only sqlite db {}", path.display())
            })
        })?;

        Ok(Self {
            access: StoreAccess::ReadOnly,
            readers,
        })
    }

    fn write_access(&self) -> Result<&WriterOwner> {
        match &self.access {
            StoreAccess::ReadWrite { writer } => Ok(writer),
            StoreAccess::ReadOnly => bail!("cannot ingest telemetry through a read-only store"),
        }
    }

    /// Closes writer admission for every clone of this store and waits up to `deadline` for
    /// already-admitted writes. Returns `None` for a read-only store. Reads keep working.
    pub fn close_writer(&self, deadline: std::time::Duration) -> Option<WriterDrain> {
        match &self.access {
            StoreAccess::ReadWrite { writer } => Some(writer.close_and_drain(deadline)),
            StoreAccess::ReadOnly => None,
        }
    }

    /// Returns queued and executing OTLP writer weight, or `None` for a read-only store.
    pub fn writer_backlog(&self) -> Option<WriterBacklog> {
        match &self.access {
            StoreAccess::ReadWrite { writer } => Some(writer.backlog()),
            StoreAccess::ReadOnly => None,
        }
    }

    fn reader(&self) -> ReaderLease {
        self.readers.checkout()
    }

    #[cfg(test)]
    fn has_writer_for_test(&self) -> bool {
        matches!(self.access, StoreAccess::ReadWrite { .. })
    }

    #[cfg(test)]
    pub(crate) fn execute_write_for_test<T, F>(&self, operation: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&mut Connection) -> Result<T> + Send + 'static,
    {
        match &self.access {
            StoreAccess::ReadWrite { writer } => writer.execute(operation),
            StoreAccess::ReadOnly => bail!("cannot write through a read-only store"),
        }
    }

    #[cfg(test)]
    fn reader_connection_for_test(&self) -> ReaderLease {
        self.reader()
    }

    #[cfg(test)]
    fn reader_pool_capacity_for_test(&self) -> usize {
        self.readers.capacity()
    }

    #[cfg(test)]
    fn shares_reader_pool_with_for_test(&self, other: &Self) -> bool {
        self.readers.shares_connections_with(&other.readers)
    }

    #[cfg(test)]
    fn shares_writer_with_for_test(&self, other: &Self) -> bool {
        match (&self.access, &other.access) {
            (StoreAccess::ReadWrite { writer: left }, StoreAccess::ReadWrite { writer: right }) => {
                left.shares_owner_with(right)
            }
            (StoreAccess::ReadWrite { .. }, StoreAccess::ReadOnly)
            | (StoreAccess::ReadOnly, StoreAccess::ReadWrite { .. })
            | (StoreAccess::ReadOnly, StoreAccess::ReadOnly) => false,
        }
    }
}

fn validate_file_backed_path(path: &Path) -> Result<()> {
    let is_uri = path.as_os_str().as_encoded_bytes().starts_with(b"file:");
    if path == Path::new(":memory:") || path.as_os_str().is_empty() || is_uri {
        bail!("pooled Store requires a plain filesystem-backed SQLite database path");
    }
    Ok(())
}

#[cfg(test)]
mod read_only_tests;
#[cfg(test)]
mod reader_pool_tests;
#[cfg(test)]
mod tests;
