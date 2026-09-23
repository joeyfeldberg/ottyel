mod backup;
mod v1;
mod v2;
mod v3;

use std::path::PathBuf;

use anyhow::{Context, Result, bail, ensure};
use rusqlite::{Connection, TransactionBehavior};

pub(super) const LATEST_SCHEMA_VERSION: i64 = 3;

#[derive(Debug)]
pub(super) struct Migration {
    from_version: i64,
    to_version: i64,
    name: &'static str,
    sql: &'static str,
    validate: fn(&Connection) -> Result<()>,
}

pub(super) const MIGRATIONS: [Migration; 3] = [
    Migration {
        from_version: 0,
        to_version: 1,
        name: "create v1 telemetry schema",
        sql: v1::DDL,
        validate: v1::validate_strict,
    },
    Migration {
        from_version: 1,
        to_version: 2,
        name: "add retention time indexes",
        sql: v2::DDL,
        validate: v2::validate_strict,
    },
    Migration {
        from_version: 2,
        to_version: 3,
        name: "key spans and projections by trace and span",
        sql: v3::DDL,
        validate: v3::validate_strict,
    },
];

pub(super) fn initialize(conn: &mut Connection) -> Result<()> {
    initialize_with_backup(conn, backup::before_migration)
}

type Backup = fn(&Connection, i64) -> Result<Option<PathBuf>>;

fn initialize_with_backup(conn: &mut Connection, backup: Backup) -> Result<()> {
    let version = schema_version(conn)?;
    ensure_supported_version(version)?;

    let holds_data = has_user_schema(conn)?;
    // Validate the starting schema before copying or migrating anything.
    match version {
        0 if holds_data => v1::validate_strict(conn)
            .context("unversioned database is incompatible with v1 schema")?,
        1 => v1::validate_strict(conn).context("version 1 database has an incompatible schema")?,
        2 => v2::validate_strict(conn).context("version 2 database has an incompatible schema")?,
        _ => {}
    }

    if version < LATEST_SCHEMA_VERSION {
        check_integrity(conn, "before migration")?;
        let copy = if holds_data {
            backup(conn, version).context(
                "could not copy the database before migrating it; the database was not changed",
            )?
        } else {
            None
        };
        run_migrations(conn, version).with_context(|| match &copy {
            Some(copy) => format!(
                "schema migration failed and was rolled back; a pre-migration copy is at {}",
                copy.display()
            ),
            None => "schema migration failed and was rolled back".to_string(),
        })?;
    } else {
        v3::validate_strict(conn).context("version 3 database has an incompatible schema")?;
    }

    configure_connection(conn)
}

pub(super) fn validate_read_only(conn: &Connection) -> Result<()> {
    let version = schema_version(conn)?;
    ensure_supported_version(version)?;

    match version {
        0 if !has_user_schema(conn)? => {
            bail!("unversioned database is empty and cannot be opened read-only")
        }
        0 => {
            v1::validate_strict(conn).context("unversioned database is incompatible with v1 schema")
        }
        // Queries work on every shipped schema, so a reader still opens a database an older
        // writer owns.
        1 => v1::validate_strict(conn).context("version 1 database has an incompatible schema"),
        2 => v2::validate_strict(conn).context("version 2 database has an incompatible schema"),
        LATEST_SCHEMA_VERSION => {
            v3::validate_strict(conn).context("version 3 database has an incompatible schema")
        }
        _ => bail!(
            "database schema version {version} requires migration to version {LATEST_SCHEMA_VERSION} and cannot be opened read-only"
        ),
    }
}

fn ensure_supported_version(version: i64) -> Result<()> {
    if version < 0 {
        bail!("database schema version {version} is invalid");
    }
    if version > LATEST_SCHEMA_VERSION {
        bail!(
            "database schema version {version} is newer than this binary's supported version {LATEST_SCHEMA_VERSION}"
        );
    }
    Ok(())
}

fn has_user_schema(conn: &Connection) -> Result<bool> {
    Ok(conn.query_row(
        "SELECT EXISTS (
             SELECT 1 FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%'
         )",
        [],
        |row| row.get(0),
    )?)
}

fn run_migrations(conn: &mut Connection, starting_version: i64) -> Result<()> {
    let mut current_version = starting_version;

    for migration in &MIGRATIONS {
        if migration.to_version <= current_version {
            continue;
        }
        ensure!(
            migration.from_version == current_version,
            "missing migration from schema version {current_version}"
        );
        apply_migration(conn, migration)?;
        current_version = migration.to_version;
    }

    ensure!(
        current_version == LATEST_SCHEMA_VERSION,
        "migration chain ended at schema version {current_version}, expected {LATEST_SCHEMA_VERSION}"
    );
    Ok(())
}

pub(super) fn apply_migration(conn: &mut Connection, migration: &Migration) -> Result<()> {
    let transaction = conn
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .with_context(|| format!("failed to begin migration {}", migration.name))?;
    let actual_version = schema_version(&transaction)?;
    ensure!(
        actual_version == migration.from_version,
        "migration {} expected schema version {}, found {actual_version}",
        migration.name,
        migration.from_version
    );

    transaction
        .execute_batch(migration.sql)
        .with_context(|| format!("migration {} DDL failed", migration.name))?;
    transaction
        .pragma_update(None, "user_version", migration.to_version)
        .with_context(|| format!("migration {} could not set schema version", migration.name))?;
    let updated_version = schema_version(&transaction)?;
    ensure!(
        updated_version == migration.to_version,
        "migration {} set schema version {updated_version}, expected {}",
        migration.name,
        migration.to_version
    );
    (migration.validate)(&transaction).with_context(|| {
        format!(
            "migration {} produced an incompatible schema",
            migration.name
        )
    })?;
    check_integrity(&transaction, &format!("after migration {}", migration.name))?;
    transaction
        .commit()
        .with_context(|| format!("failed to commit migration {}", migration.name))?;
    Ok(())
}

pub(super) fn schema_version(conn: &Connection) -> Result<i64> {
    Ok(conn.pragma_query_value(None, "user_version", |row| row.get(0))?)
}

fn check_integrity(conn: &Connection, stage: &str) -> Result<()> {
    let mut rows = Vec::new();
    conn.pragma_query(None, "integrity_check", |row| {
        rows.push(row.get(0)?);
        Ok(())
    })
    .with_context(|| format!("could not run SQLite integrity check {stage}"))?;
    validate_integrity_rows(&rows).with_context(|| format!("SQLite integrity check {stage}"))
}

pub(super) fn validate_integrity_rows(rows: &[String]) -> Result<()> {
    if rows.is_empty() {
        bail!("returned no rows");
    }
    ensure!(
        rows.len() == 1 && rows[0] == "ok",
        "expected exactly one `ok` row, reported: {}",
        rows.join("; ")
    );
    Ok(())
}

fn configure_connection(conn: &Connection) -> Result<()> {
    let journal_mode: String = conn
        .pragma_update_and_check(None, "journal_mode", "WAL", |row| row.get(0))
        .context("failed to enable SQLite WAL mode")?;
    ensure!(
        journal_mode.eq_ignore_ascii_case("wal"),
        "SQLite rejected WAL mode and returned {journal_mode}"
    );

    // v3 relies on ON DELETE CASCADE, which SQLite enforces only when enabled per connection.
    conn.pragma_update(None, "foreign_keys", "ON")
        .context("failed to enable SQLite foreign keys")?;
    let foreign_keys: i64 = conn
        .pragma_query_value(None, "foreign_keys", |row| row.get(0))
        .context("failed to verify SQLite foreign keys")?;
    ensure!(foreign_keys == 1, "SQLite rejected foreign key enforcement");

    conn.pragma_update(None, "synchronous", "NORMAL")
        .context("failed to set SQLite synchronous mode")?;
    let synchronous: i64 = conn
        .pragma_query_value(None, "synchronous", |row| row.get(0))
        .context("failed to verify SQLite synchronous mode")?;
    ensure!(
        synchronous == 1,
        "SQLite rejected NORMAL synchronous mode and returned {synchronous}"
    );
    Ok(())
}

#[cfg(test)]
mod tests;
