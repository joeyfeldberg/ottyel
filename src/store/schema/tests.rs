use anyhow::{Result, bail};
use rusqlite::{Connection, types::Value};
use tempfile::tempdir;

use super::{
    LATEST_SCHEMA_VERSION, MIGRATIONS, Migration, apply_migration, schema_version,
    validate_integrity_rows,
};
use crate::store::Store;

const HISTORICAL_V0_SCHEMA: &str = include_str!("fixtures/v0_schema.sql");

#[test]
fn fresh_database_creates_the_v1_schema_and_connection_configuration() {
    let tempdir = tempdir().unwrap();
    let path = tempdir.path().join("ottyel.db");

    let store = Store::open(&path, 24, 1_000).unwrap();
    let conn = store.writer_connection_for_test().unwrap().lock().unwrap();
    assert_eq!(schema_version(&conn).unwrap(), LATEST_SCHEMA_VERSION);
    assert_eq!(user_schema_objects(&conn), expected_v1_objects());
    assert_eq!(pragma_string(&conn, "journal_mode"), "wal");
    assert_eq!(pragma_i64(&conn, "synchronous"), 1);
}

#[test]
fn legacy_unversioned_schema_preserves_rows_and_v1_reopen_is_a_no_op() {
    let tempdir = tempdir().unwrap();
    let path = tempdir.path().join("ottyel.db");
    let conn = Connection::open(&path).unwrap();
    conn.execute_batch(HISTORICAL_V0_SCHEMA).unwrap();
    insert_legacy_rows(&conn);

    assert_eq!(schema_version(&conn).unwrap(), 0);
    let expected_rows = telemetry_snapshot(&conn);
    drop(conn);

    drop(Store::open(&path, 24, 1_000).unwrap());

    let conn = Connection::open(&path).unwrap();
    assert_eq!(schema_version(&conn).unwrap(), LATEST_SCHEMA_VERSION);
    assert_eq!(telemetry_snapshot(&conn), expected_rows);
    let objects_after_upgrade = user_schema_objects(&conn);
    let schema_cookie_after_upgrade = pragma_i64(&conn, "schema_version");
    drop(conn);

    drop(Store::open(&path, 24, 1_000).unwrap());

    let conn = Connection::open(path).unwrap();
    assert_eq!(schema_version(&conn).unwrap(), LATEST_SCHEMA_VERSION);
    assert_eq!(telemetry_snapshot(&conn), expected_rows);
    assert_eq!(user_schema_objects(&conn), objects_after_upgrade);
    assert_eq!(
        pragma_i64(&conn, "schema_version"),
        schema_cookie_after_upgrade
    );
}

#[test]
fn partial_unversioned_database_is_rejected_without_mutation() {
    let tempdir = tempdir().unwrap();
    let path = tempdir.path().join("ottyel.db");
    let conn = Connection::open(&path).unwrap();
    conn.execute_batch(
        "CREATE TABLE unrelated (id INTEGER PRIMARY KEY, value TEXT NOT NULL);\
         INSERT INTO unrelated (value) VALUES ('preserve me');",
    )
    .unwrap();
    let expected_objects = user_schema_objects(&conn);
    let expected_cookie = pragma_i64(&conn, "schema_version");
    let expected_mode = pragma_string(&conn, "journal_mode");
    drop(conn);

    let error = Store::open(&path, 24, 1_000).unwrap_err();
    assert!(
        format!("{error:#}").contains("unversioned database is incompatible"),
        "unexpected error: {error:#}"
    );

    let conn = Connection::open(path).unwrap();
    assert_eq!(schema_version(&conn).unwrap(), 0);
    assert_eq!(user_schema_objects(&conn), expected_objects);
    assert_eq!(pragma_i64(&conn, "schema_version"), expected_cookie);
    assert_eq!(pragma_string(&conn, "journal_mode"), expected_mode);
    assert_eq!(
        conn.query_row("SELECT value FROM unrelated", [], |row| row
            .get::<_, String>(0))
            .unwrap(),
        "preserve me"
    );
}

#[test]
fn otherwise_compatible_unversioned_database_with_extra_schema_is_rejected() {
    let tempdir = tempdir().unwrap();
    let path = tempdir.path().join("ottyel.db");
    let conn = Connection::open(&path).unwrap();
    conn.execute_batch(HISTORICAL_V0_SCHEMA).unwrap();
    conn.execute_batch(
        "CREATE TABLE unrelated (value TEXT NOT NULL);\
         INSERT INTO unrelated VALUES ('preserve me');",
    )
    .unwrap();
    let expected_objects = user_schema_objects(&conn);
    let expected_cookie = pragma_i64(&conn, "schema_version");
    let expected_mode = pragma_string(&conn, "journal_mode");
    drop(conn);

    let error = Store::open(&path, 24, 1_000).unwrap_err();
    assert!(
        format!("{error:#}").contains("non-SQLite schema objects differ"),
        "unexpected error: {error:#}"
    );

    let conn = Connection::open(path).unwrap();
    assert_eq!(schema_version(&conn).unwrap(), 0);
    assert_eq!(user_schema_objects(&conn), expected_objects);
    assert_eq!(pragma_i64(&conn, "schema_version"), expected_cookie);
    assert_eq!(pragma_string(&conn, "journal_mode"), expected_mode);
    assert_eq!(
        conn.query_row("SELECT value FROM unrelated", [], |row| row
            .get::<_, String>(0))
            .unwrap(),
        "preserve me"
    );
}

#[test]
fn unversioned_schema_with_wrong_index_direction_is_rejected_without_mutation() {
    let tempdir = tempdir().unwrap();
    let path = tempdir.path().join("ottyel.db");
    let conn = Connection::open(&path).unwrap();
    conn.execute_batch(HISTORICAL_V0_SCHEMA).unwrap();
    conn.execute_batch(
        "DROP INDEX idx_spans_service_start;
         CREATE INDEX idx_spans_service_start
             ON spans(service_name, start_time_unix_nano ASC);",
    )
    .unwrap();
    let expected_cookie = pragma_i64(&conn, "schema_version");
    let expected_mode = pragma_string(&conn, "journal_mode");
    assert!(!index_column_descending(
        &conn,
        "idx_spans_service_start",
        "start_time_unix_nano"
    ));
    drop(conn);

    let error = Store::open(&path, 24, 1_000).unwrap_err();
    assert!(
        format!("{error:#}").contains("index idx_spans_service_start columns differ"),
        "unexpected error: {error:#}"
    );

    let conn = Connection::open(path).unwrap();
    assert_eq!(schema_version(&conn).unwrap(), 0);
    assert_eq!(pragma_i64(&conn, "schema_version"), expected_cookie);
    assert_eq!(pragma_string(&conn, "journal_mode"), expected_mode);
    assert!(!index_column_descending(
        &conn,
        "idx_spans_service_start",
        "start_time_unix_nano"
    ));
}

#[test]
fn unversioned_schema_with_hidden_unique_constraint_is_rejected_without_mutation() {
    let tempdir = tempdir().unwrap();
    let path = tempdir.path().join("ottyel.db");
    let conn = Connection::open(&path).unwrap();
    conn.execute_batch(HISTORICAL_V0_SCHEMA).unwrap();
    conn.execute_batch(
        "DROP TABLE logs;
         CREATE TABLE logs (
             id INTEGER PRIMARY KEY AUTOINCREMENT,
             service_name TEXT NOT NULL,
             timestamp_unix_nano INTEGER NOT NULL,
             severity TEXT NOT NULL,
             body TEXT NOT NULL,
             trace_id TEXT NOT NULL,
             span_id TEXT NOT NULL,
             resource_attributes_json TEXT NOT NULL,
             attributes_json TEXT NOT NULL,
             UNIQUE(trace_id, span_id)
         );
         CREATE INDEX idx_logs_service_time
             ON logs(service_name, timestamp_unix_nano DESC);
         CREATE INDEX idx_logs_trace ON logs(trace_id, span_id);",
    )
    .unwrap();
    let expected_objects = user_schema_objects(&conn);
    let expected_cookie = pragma_i64(&conn, "schema_version");
    let expected_mode = pragma_string(&conn, "journal_mode");
    assert_eq!(unique_index_count(&conn, "logs"), 1);
    drop(conn);

    let error = Store::open(&path, 24, 1_000).unwrap_err();
    assert!(
        format!("{error:#}").contains("table logs CREATE TABLE definition differs"),
        "unexpected error: {error:#}"
    );

    let conn = Connection::open(path).unwrap();
    assert_eq!(schema_version(&conn).unwrap(), 0);
    assert_eq!(user_schema_objects(&conn), expected_objects);
    assert_eq!(pragma_i64(&conn, "schema_version"), expected_cookie);
    assert_eq!(pragma_string(&conn, "journal_mode"), expected_mode);
    assert_eq!(unique_index_count(&conn, "logs"), 1);
}

#[test]
fn newer_schema_version_is_rejected_before_configuration_or_schema_mutation() {
    let tempdir = tempdir().unwrap();
    let path = tempdir.path().join("ottyel.db");
    let conn = Connection::open(&path).unwrap();
    conn.execute_batch(
        "CREATE TABLE future_data (value TEXT NOT NULL);\
         INSERT INTO future_data VALUES ('preserve me');\
         PRAGMA user_version = 2;",
    )
    .unwrap();
    let expected_objects = user_schema_objects(&conn);
    let expected_cookie = pragma_i64(&conn, "schema_version");
    let expected_mode = pragma_string(&conn, "journal_mode");
    drop(conn);

    let error = Store::open(&path, 24, 1_000).unwrap_err();
    let message = format!("{error:#}");
    assert!(
        message.contains("schema version 2"),
        "unexpected error: {message}"
    );
    assert!(
        message.contains("supported version 1"),
        "unexpected error: {message}"
    );

    let conn = Connection::open(path).unwrap();
    assert_eq!(schema_version(&conn).unwrap(), 2);
    assert_eq!(user_schema_objects(&conn), expected_objects);
    assert_eq!(pragma_i64(&conn, "schema_version"), expected_cookie);
    assert_eq!(pragma_string(&conn, "journal_mode"), expected_mode);
    assert_eq!(
        conn.query_row("SELECT value FROM future_data", [], |row| row
            .get::<_, String>(0))
            .unwrap(),
        "preserve me"
    );
}

#[test]
fn incompatible_v1_schema_is_rejected_before_wal_configuration() {
    let tempdir = tempdir().unwrap();
    let path = tempdir.path().join("ottyel.db");
    let conn = Connection::open(&path).unwrap();
    conn.execute_batch(
        "CREATE TABLE spans (trace_id TEXT NOT NULL);\
         PRAGMA user_version = 1;",
    )
    .unwrap();
    let expected_objects = user_schema_objects(&conn);
    let expected_cookie = pragma_i64(&conn, "schema_version");
    let expected_mode = pragma_string(&conn, "journal_mode");
    drop(conn);

    let error = Store::open(&path, 24, 1_000).unwrap_err();
    assert!(
        format!("{error:#}").contains("version 1 database has an incompatible schema"),
        "unexpected error: {error:#}"
    );

    let conn = Connection::open(path).unwrap();
    assert_eq!(schema_version(&conn).unwrap(), 1);
    assert_eq!(user_schema_objects(&conn), expected_objects);
    assert_eq!(pragma_i64(&conn, "schema_version"), expected_cookie);
    assert_eq!(pragma_string(&conn, "journal_mode"), expected_mode);
}

#[test]
fn negative_schema_version_is_rejected_before_configuration() {
    let tempdir = tempdir().unwrap();
    let path = tempdir.path().join("ottyel.db");
    let conn = Connection::open(&path).unwrap();
    conn.pragma_update(None, "user_version", -1).unwrap();
    let expected_mode = pragma_string(&conn, "journal_mode");
    drop(conn);

    let error = Store::open(&path, 24, 1_000).unwrap_err();
    assert!(
        format!("{error:#}").contains("schema version -1 is invalid"),
        "unexpected error: {error:#}"
    );

    let conn = Connection::open(path).unwrap();
    assert_eq!(schema_version(&conn).unwrap(), -1);
    assert_eq!(pragma_string(&conn, "journal_mode"), expected_mode);
}

#[test]
fn failed_v1_migration_rolls_back_earlier_ddl_and_version_bump() {
    let mut conn = Connection::open_in_memory().unwrap();
    conn.execute_batch(
        "CREATE TABLE logs (
             id INTEGER PRIMARY KEY,
             service_name TEXT NOT NULL,
             timestamp_unix_nano INTEGER NOT NULL,
             trace_id TEXT NOT NULL,
             span_id TEXT NOT NULL
         );",
    )
    .unwrap();

    let error = apply_migration(&mut conn, &MIGRATIONS[0]).unwrap_err();
    assert!(
        format!("{error:#}").contains("table logs columns differ"),
        "unexpected error: {error:#}"
    );

    assert_eq!(schema_version(&conn).unwrap(), 0);
    assert_eq!(
        user_schema_objects(&conn),
        vec![("table".to_string(), "logs".to_string(), "logs".to_string())]
    );
}

#[test]
fn validation_failure_after_version_write_rolls_back_ddl_and_version() {
    let mut conn = Connection::open_in_memory().unwrap();
    let migration = Migration {
        from_version: 0,
        to_version: 1,
        name: "forced validation failure",
        sql: "CREATE TABLE migration_probe (value TEXT NOT NULL);",
        validate: fail_after_version_write,
    };

    let error = apply_migration(&mut conn, &migration).unwrap_err();
    assert!(
        format!("{error:#}").contains("forced target-schema validation failure"),
        "unexpected error: {error:#}"
    );
    assert_eq!(schema_version(&conn).unwrap(), 0);
    assert!(user_schema_objects(&conn).is_empty());
}

#[test]
fn integrity_result_requires_exactly_one_ok_row() {
    assert!(validate_integrity_rows(&["ok".to_string()]).is_ok());

    let repeated = validate_integrity_rows(&["ok".to_string(), "ok".to_string()]).unwrap_err();
    assert!(format!("{repeated:#}").contains("exactly one `ok` row"));

    let empty = validate_integrity_rows(&[]).unwrap_err();
    assert!(format!("{empty:#}").contains("returned no rows"));

    let failed = validate_integrity_rows(&[
        "ok".to_string(),
        "row 7 missing from index idx_logs_trace".to_string(),
    ])
    .unwrap_err();
    assert!(
        format!("{failed:#}").contains("row 7 missing from index idx_logs_trace"),
        "unexpected error: {failed:#}"
    );
}

fn fail_after_version_write(conn: &Connection) -> Result<()> {
    assert_eq!(schema_version(conn)?, 1);
    bail!("forced target-schema validation failure")
}

fn insert_legacy_rows(conn: &Connection) {
    conn.execute_batch(
        r#"
        INSERT INTO spans VALUES (
            'trace-1', 'span-1', '', 'checkout', 'POST /pay', 'SERVER', 'ERROR',
            100, 200, 0.0001, '{"service.name":"checkout"}', '{"cart.id":"c-1"}',
            '{"model":"gpt-test"}'
        );
        INSERT INTO span_events (
            trace_id, span_id, name, timestamp_unix_nano, attributes_json
        ) VALUES ('trace-1', 'span-1', 'exception', 150, '{"type":"timeout"}');
        INSERT INTO span_links (
            trace_id, span_id, linked_trace_id, linked_span_id, trace_state, attributes_json
        ) VALUES ('trace-1', 'span-1', 'trace-2', 'span-2', 'vendor=state', '{"kind":"follows"}');
        INSERT INTO logs (
            service_name, timestamp_unix_nano, severity, body, trace_id, span_id,
            resource_attributes_json, attributes_json
        ) VALUES (
            'checkout', 160, 'ERROR', 'payment failed', 'trace-1', 'span-1',
            '{"service.name":"checkout"}', '{"attempt":1}'
        );
        INSERT INTO metrics (
            service_name, metric_name, instrument_kind, aggregation_temporality,
            timestamp_unix_nano, value, summary, resource_attributes_json, attributes_json
        ) VALUES (
            'checkout', 'request.duration', 'histogram', 'cumulative', 170, 12.5,
            'count=1', '{"service.name":"checkout"}', '{"route":"/pay"}'
        );
        INSERT INTO llm_spans VALUES (
            'span-1', 'trace-1', 'checkout', 'openai', 'gpt-test', 'chat', 10, 20, 30,
            0.01, 50.0, 'ERROR', '{"prompt":"hello"}'
        );
        "#,
    )
    .unwrap();
}

fn telemetry_snapshot(conn: &Connection) -> Vec<(String, Vec<Vec<Value>>)> {
    [
        ("spans", "trace_id, span_id"),
        ("span_events", "id"),
        ("span_links", "id"),
        ("logs", "id"),
        ("metrics", "id"),
        ("llm_spans", "trace_id, span_id"),
    ]
    .into_iter()
    .map(|(table, order)| (table.to_string(), table_rows(conn, table, order)))
    .collect()
}

fn table_rows(conn: &Connection, table: &str, order: &str) -> Vec<Vec<Value>> {
    let mut statement = conn
        .prepare(&format!("SELECT * FROM {table} ORDER BY {order}"))
        .unwrap();
    let column_count = statement.column_count();
    statement
        .query_map([], |row| {
            (0..column_count)
                .map(|column| row.get(column))
                .collect::<rusqlite::Result<Vec<Value>>>()
        })
        .unwrap()
        .collect::<rusqlite::Result<Vec<_>>>()
        .unwrap()
}

fn user_schema_objects(conn: &Connection) -> Vec<(String, String, String)> {
    let mut statement = conn
        .prepare(
            "SELECT type, name, tbl_name
             FROM sqlite_schema
             WHERE name NOT LIKE 'sqlite_%'
             ORDER BY type, name",
        )
        .unwrap();
    statement
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))
        .unwrap()
        .collect::<rusqlite::Result<Vec<_>>>()
        .unwrap()
}

fn expected_v1_objects() -> Vec<(String, String, String)> {
    let mut objects = vec![
        ("table", "spans", "spans"),
        ("table", "span_events", "span_events"),
        ("table", "span_links", "span_links"),
        ("table", "logs", "logs"),
        ("table", "metrics", "metrics"),
        ("table", "llm_spans", "llm_spans"),
        ("index", "idx_spans_trace", "spans"),
        ("index", "idx_spans_service_start", "spans"),
        ("index", "idx_spans_status", "spans"),
        ("index", "idx_span_events_trace", "span_events"),
        ("index", "idx_span_links_trace", "span_links"),
        ("index", "idx_logs_service_time", "logs"),
        ("index", "idx_logs_trace", "logs"),
        ("index", "idx_metrics_service_time", "metrics"),
        ("index", "idx_metrics_name", "metrics"),
        ("index", "idx_llm_trace", "llm_spans"),
        ("index", "idx_llm_service", "llm_spans"),
    ]
    .into_iter()
    .map(|(kind, name, table)| (kind.to_string(), name.to_string(), table.to_string()))
    .collect::<Vec<_>>();
    objects.sort();
    objects
}

fn pragma_string(conn: &Connection, name: &str) -> String {
    conn.pragma_query_value(None, name, |row| row.get(0))
        .unwrap()
}

fn pragma_i64(conn: &Connection, name: &str) -> i64 {
    conn.pragma_query_value(None, name, |row| row.get(0))
        .unwrap()
}

fn index_column_descending(conn: &Connection, index: &str, column: &str) -> bool {
    conn.query_row(
        "SELECT \"desc\"
         FROM pragma_index_xinfo(?1)
         WHERE name = ?2 AND key = 1",
        [index, column],
        |row| Ok(row.get::<_, i64>(0)? != 0),
    )
    .unwrap()
}

fn unique_index_count(conn: &Connection, table: &str) -> i64 {
    conn.query_row(
        "SELECT COUNT(*) FROM pragma_index_list(?1) WHERE \"unique\" = 1",
        [table],
        |row| row.get(0),
    )
    .unwrap()
}
