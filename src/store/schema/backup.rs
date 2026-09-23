//! Pre-migration copies of databases that already hold telemetry.
//!
//! A migration runs in one transaction and rolls back on failure. The copy protects against
//! the cases a rollback cannot cover: a migration that commits but is later found wrong, and
//! a binary that is downgraded after upgrading the schema. Recovery is a manual file swap:
//! stop ottyel, move the copy over the database path, and delete any `-wal` and `-shm` files.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use rusqlite::Connection;

/// Writes a consistent copy of the connection's main database next to it and returns its
/// path, or `None` for an in-memory database. Existing files are never overwritten.
pub(super) fn before_migration(conn: &Connection, from_version: i64) -> Result<Option<PathBuf>> {
    let Some(database) = conn.path().filter(|path| !path.is_empty()) else {
        return Ok(None);
    };
    let target = available_path(Path::new(database), from_version);
    let Some(target_text) = target.to_str() else {
        bail!("backup path {} is not valid UTF-8", target.display());
    };
    conn.execute("VACUUM INTO ?1", [target_text])
        .with_context(|| format!("failed to write {}", target.display()))?;
    Ok(Some(target))
}

fn available_path(database: &Path, from_version: i64) -> PathBuf {
    let base = format!("{}.v{from_version}-backup", database.display());
    std::iter::once(PathBuf::from(&base))
        .chain((1..).map(|attempt| PathBuf::from(format!("{base}.{attempt}"))))
        .find(|candidate| !candidate.exists())
        .expect("an unbounded candidate sequence always yields an unused path")
}
