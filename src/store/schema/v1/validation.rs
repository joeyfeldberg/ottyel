use anyhow::{Context, Result, ensure};
use rusqlite::{Connection, params};

use super::{INDEXES, IndexDefinition, TABLES, TableDefinition};

fn validate(conn: &Connection) -> Result<()> {
    for table in TABLES {
        validate_table(conn, table)?;
    }
    for index in INDEXES {
        validate_index(conn, index)?;
    }

    let mut statement = conn.prepare(
        "SELECT name FROM sqlite_schema
         WHERE type = 'trigger'
           AND tbl_name IN ('spans', 'span_events', 'span_links', 'logs', 'metrics', 'llm_spans')
         ORDER BY name",
    )?;
    let triggers = statement
        .query_map([], |row| row.get::<_, String>(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    ensure!(
        triggers.is_empty(),
        "v1 telemetry tables have unexpected triggers: {}",
        triggers.join(", ")
    );
    Ok(())
}

pub(super) fn validate_strict(conn: &Connection) -> Result<()> {
    validate(conn)?;

    let mut statement = conn.prepare(
        "SELECT type, name, tbl_name
         FROM sqlite_schema
         WHERE name NOT LIKE 'sqlite_%'
         ORDER BY type, name",
    )?;
    let actual = statement
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
            ))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let mut expected = TABLES
        .iter()
        .map(|table| {
            (
                "table".to_string(),
                table.name.to_string(),
                table.name.to_string(),
            )
        })
        .chain(INDEXES.iter().map(|index| {
            (
                "index".to_string(),
                index.name.to_string(),
                index.table.to_string(),
            )
        }))
        .collect::<Vec<_>>();
    expected.sort();
    ensure!(
        actual == expected,
        "non-SQLite schema objects differ: expected {expected:?}, found {actual:?}"
    );
    Ok(())
}

fn validate_table(conn: &Connection, expected: &TableDefinition) -> Result<()> {
    let sql = conn
        .query_row(
            "SELECT sql FROM sqlite_schema WHERE type = 'table' AND name = ?1",
            [expected.name],
            |row| row.get::<_, String>(0),
        )
        .with_context(|| format!("missing table {}", expected.name))?;

    let mut statement = conn.prepare(
        "SELECT name, type, \"notnull\", dflt_value, pk, hidden
         FROM pragma_table_xinfo(?1)
         ORDER BY cid",
    )?;
    let actual = statement
        .query_map([expected.name], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, i64>(2)? != 0,
                row.get::<_, Option<String>>(3)?,
                row.get::<_, i64>(4)?,
                row.get::<_, i64>(5)?,
            ))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let expected_columns = expected
        .columns
        .iter()
        .map(|column| {
            (
                column.name.to_string(),
                column.data_type.to_string(),
                column.not_null,
                None,
                column.primary_key,
                0,
            )
        })
        .collect::<Vec<_>>();
    ensure!(
        actual == expected_columns,
        "table {} columns differ: expected {expected_columns:?}, found {actual:?}",
        expected.name
    );

    let has_autoincrement = sql.to_ascii_uppercase().contains("AUTOINCREMENT");
    ensure!(
        has_autoincrement == expected.autoincrement,
        "table {} AUTOINCREMENT setting differs",
        expected.name
    );
    let actual_create_sql = canonical_create_sql(&sql);
    let expected_create_sql = canonical_create_sql(expected.create_sql);
    ensure!(
        actual_create_sql == expected_create_sql,
        "table {} CREATE TABLE definition differs: expected `{expected_create_sql}`, found `{actual_create_sql}`",
        expected.name
    );
    Ok(())
}

fn validate_index(conn: &Connection, expected: &IndexDefinition) -> Result<()> {
    let table = conn
        .query_row(
            "SELECT tbl_name FROM sqlite_schema WHERE type = 'index' AND name = ?1",
            [expected.name],
            |row| row.get::<_, String>(0),
        )
        .with_context(|| format!("missing index {}", expected.name))?;
    ensure!(
        table == expected.table,
        "index {} belongs to table {table}, expected {}",
        expected.name,
        expected.table
    );

    let mut statement = conn.prepare(
        "SELECT name, \"desc\", coll
         FROM pragma_index_xinfo(?1)
         WHERE key = 1
         ORDER BY seqno",
    )?;
    let actual = statement
        .query_map([expected.name], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, i64>(1)? != 0,
                row.get::<_, String>(2)?,
            ))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let expected_columns = expected
        .columns
        .iter()
        .map(|column| {
            (
                column.name.to_string(),
                column.descending,
                column.collation.to_string(),
            )
        })
        .collect::<Vec<_>>();
    ensure!(
        actual == expected_columns,
        "index {} columns differ: expected {expected_columns:?}, found {actual:?}",
        expected.name
    );

    let (unique, partial) = conn.query_row(
        "SELECT \"unique\", partial
         FROM pragma_index_list(?1)
         WHERE name = ?2",
        params![expected.table, expected.name],
        |row| Ok((row.get::<_, i64>(0)? != 0, row.get::<_, i64>(1)? != 0)),
    )?;
    ensure!(!unique, "index {} is unexpectedly unique", expected.name);
    ensure!(!partial, "index {} is unexpectedly partial", expected.name);
    Ok(())
}

fn canonical_create_sql(sql: &str) -> String {
    let mut canonical = String::with_capacity(sql.len());
    let mut pending_space = false;

    for byte in sql.bytes() {
        if byte.is_ascii_whitespace() {
            pending_space = !canonical.is_empty();
            continue;
        }
        if pending_space {
            canonical.push(' ');
            pending_space = false;
        }
        canonical.push(char::from(byte).to_ascii_lowercase());
    }

    canonical
}
