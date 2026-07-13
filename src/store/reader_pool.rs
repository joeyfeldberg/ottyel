use std::{
    ops::Deref,
    path::Path,
    sync::{Arc, Condvar, Mutex},
};

use anyhow::{Context, Result, ensure};
use rusqlite::{Connection, MAIN_DB, OpenFlags};

pub(super) const READER_POOL_SIZE: usize = 4;

#[derive(Debug, Clone)]
pub(super) struct ReaderPool {
    inner: Arc<ReaderPoolInner>,
}

#[derive(Debug)]
struct ReaderPoolInner {
    available: Mutex<Vec<Connection>>,
    returned: Condvar,
}

pub(super) struct ReaderLease {
    connection: Option<Connection>,
    pool: Arc<ReaderPoolInner>,
}

impl ReaderPool {
    pub(super) fn open(path: &Path) -> Result<Self> {
        Self::open_with_first_validation(path, |_| Ok(()))
    }

    pub(super) fn open_validated(
        path: &Path,
        validate_first: impl FnOnce(&Connection) -> Result<()>,
    ) -> Result<Self> {
        Self::open_with_first_validation(path, validate_first)
    }

    fn open_with_first_validation(
        path: &Path,
        validate_first: impl FnOnce(&Connection) -> Result<()>,
    ) -> Result<Self> {
        let mut connections = Vec::with_capacity(READER_POOL_SIZE);
        let first = open_reader(path).with_context(|| {
            format!("failed to open read-only sqlite connection 1 of {READER_POOL_SIZE}")
        })?;
        validate_first(&first)?;
        connections.push(first);

        for index in 1..READER_POOL_SIZE {
            let connection = open_reader(path).with_context(|| {
                format!(
                    "failed to open read-only sqlite connection {} of {READER_POOL_SIZE}",
                    index + 1
                )
            })?;
            connections.push(connection);
        }

        Ok(Self {
            inner: Arc::new(ReaderPoolInner {
                available: Mutex::new(connections),
                returned: Condvar::new(),
            }),
        })
    }

    pub(super) fn checkout(&self) -> ReaderLease {
        let mut available = self
            .inner
            .available
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        loop {
            if let Some(connection) = available.pop() {
                return ReaderLease {
                    connection: Some(connection),
                    pool: Arc::clone(&self.inner),
                };
            }
            available = self
                .inner
                .returned
                .wait(available)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
        }
    }

    #[cfg(test)]
    pub(super) fn capacity(&self) -> usize {
        READER_POOL_SIZE
    }

    #[cfg(test)]
    pub(super) fn shares_connections_with(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.inner, &other.inner)
    }
}

impl Deref for ReaderLease {
    type Target = Connection;

    fn deref(&self) -> &Self::Target {
        self.connection
            .as_ref()
            .expect("reader lease must own a connection")
    }
}

impl Drop for ReaderLease {
    fn drop(&mut self) {
        let Some(connection) = self.connection.take() else {
            return;
        };
        let mut available = self
            .pool
            .available
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        available.push(connection);
        drop(available);
        self.pool.returned.notify_one();
    }
}

fn open_reader(path: &Path) -> Result<Connection> {
    let connection = Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .with_context(|| format!("failed to open read-only sqlite db {}", path.display()))?;
    configure_reader(&connection)
        .with_context(|| format!("failed to configure read-only sqlite db {}", path.display()))?;
    Ok(connection)
}

fn configure_reader(connection: &Connection) -> Result<()> {
    let main_is_read_only = connection
        .is_readonly(MAIN_DB)
        .context("failed to verify that the SQLite main database is read-only")?;
    ensure!(
        main_is_read_only,
        "SQLite opened the main database with write access"
    );
    connection
        .pragma_update(None, "query_only", "ON")
        .context("failed to enable SQLite query-only mode")?;
    let query_only: i64 = connection
        .pragma_query_value(None, "query_only", |row| row.get(0))
        .context("failed to verify SQLite query-only mode")?;
    ensure!(
        query_only == 1,
        "SQLite rejected query-only mode and returned {query_only}"
    );
    Ok(())
}
