mod v1;

use anyhow::{Context, Result, bail, ensure};
use rusqlite::{Connection, TransactionBehavior};

pub(super) const LATEST_SCHEMA_VERSION: i64 = 1;

#[derive(Debug)]
pub(super) struct Migration {
    from_version: i64,
    to_version: i64,
    name: &'static str,
    sql: &'static str,
    validate: fn(&Connection) -> Result<()>,
}

pub(super) const MIGRATIONS: [Migration; 1] = [Migration {
    from_version: 0,
    to_version: 1,
    name: "create v1 telemetry schema",
    sql: v1::DDL,
    validate: v1::validate_strict,
}];

pub(super) fn initialize(conn: &mut Connection) -> Result<()> {
    let version = schema_version(conn)?;
    ensure_supported_version(version)?;

    if version == 0 && has_user_schema(conn)? {
        v1::validate_strict(conn).context("unversioned database is incompatible with v1 schema")?;
    }

    if version < LATEST_SCHEMA_VERSION {
        check_integrity(conn, "before migration")?;
        run_migrations(conn, version)?;
    } else {
        v1::validate_strict(conn).context("version 1 database has an incompatible schema")?;
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
        LATEST_SCHEMA_VERSION => {
            v1::validate_strict(conn).context("version 1 database has an incompatible schema")
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
