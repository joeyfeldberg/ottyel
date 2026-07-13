mod helpers;
mod ingest;
mod queries;
mod schema;

use std::{
    fs,
    path::Path,
    sync::{Arc, Mutex},
};

use anyhow::{Context, Result, bail, ensure};
use rusqlite::{Connection, MAIN_DB, OpenFlags};

#[derive(Debug, Clone, Copy)]
pub(super) struct RetentionPolicy {
    hours: u64,
    maximum_spans: usize,
}

#[derive(Debug, Clone, Copy)]
enum StoreAccess {
    ReadWrite(RetentionPolicy),
    ReadOnly,
}

#[derive(Debug, Clone)]
pub struct Store {
    pub(super) conn: Arc<Mutex<Connection>>,
    access: StoreAccess,
}

impl Store {
    pub fn open(path: &Path, retention_hours: u64, max_spans: usize) -> Result<Self> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("failed to create {}", parent.display()))?;
        }

        let mut conn = Connection::open(path)
            .with_context(|| format!("failed to open sqlite db {}", path.display()))?;
        schema::initialize(&mut conn)
            .with_context(|| format!("failed to initialize sqlite db {}", path.display()))?;

        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
            access: StoreAccess::ReadWrite(RetentionPolicy {
                hours: retention_hours,
                maximum_spans: max_spans,
            }),
        })
    }

    /// Opens an existing compatible database without migrations or writes to its schema, data,
    /// or persistent settings.
    ///
    /// SQLite may create or update `-wal`/`-shm` coordination sidecars so this connection can
    /// observe commits from a live WAL writer.
    pub fn open_read_only(path: &Path) -> Result<Self> {
        let conn = Connection::open_with_flags(
            path,
            OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )
        .with_context(|| format!("failed to open read-only sqlite db {}", path.display()))?;
        configure_read_only_connection(&conn).with_context(|| {
            format!("failed to configure read-only sqlite db {}", path.display())
        })?;
        schema::validate_read_only(&conn).with_context(|| {
            format!("failed to validate read-only sqlite db {}", path.display())
        })?;

        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
            access: StoreAccess::ReadOnly,
        })
    }

    pub(super) fn retention_policy(&self) -> Result<RetentionPolicy> {
        match self.access {
            StoreAccess::ReadWrite(policy) => Ok(policy),
            StoreAccess::ReadOnly => bail!("cannot ingest telemetry through a read-only store"),
        }
    }
}

fn configure_read_only_connection(conn: &Connection) -> Result<()> {
    let main_is_read_only = conn
        .is_readonly(MAIN_DB)
        .context("failed to verify that the SQLite main database is read-only")?;
    ensure!(
        main_is_read_only,
        "SQLite opened the main database with write access"
    );
    conn.pragma_update(None, "query_only", "ON")
        .context("failed to enable SQLite query-only mode")?;
    let query_only: i64 = conn
        .pragma_query_value(None, "query_only", |row| row.get(0))
        .context("failed to verify SQLite query-only mode")?;
    ensure!(
        query_only == 1,
        "SQLite rejected query-only mode and returned {query_only}"
    );
    Ok(())
}

#[cfg(test)]
mod read_only_tests;
#[cfg(test)]
mod tests;
