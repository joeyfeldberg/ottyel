mod helpers;
mod ingest;
mod queries;
mod schema;

use std::{
    fs,
    path::Path,
    sync::{Arc, Mutex},
};

use anyhow::{Context, Result};
use rusqlite::Connection;

#[derive(Debug, Clone)]
pub struct Store {
    pub(super) conn: Arc<Mutex<Connection>>,
    pub(super) retention_hours: u64,
    pub(super) max_spans: usize,
}

impl Store {
    pub fn open(path: &Path, retention_hours: u64, max_spans: usize) -> Result<Self> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("failed to create {}", parent.display()))?;
        }

        let conn = Connection::open(path)
            .with_context(|| format!("failed to open sqlite db {}", path.display()))?;
        schema::initialize(&conn)?;

        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
            retention_hours,
            max_spans,
        })
    }
}

#[cfg(test)]
mod tests;
