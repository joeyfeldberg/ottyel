use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::Path,
    time::{SystemTime, UNIX_EPOCH},
};

use opentelemetry_proto::tonic::{
    collector::{
        logs::v1::ExportLogsServiceRequest, metrics::v1::ExportMetricsServiceRequest,
        trace::v1::ExportTraceServiceRequest,
    },
    common::v1::{AnyValue, InstrumentationScope, any_value},
    logs::v1::{LogRecord, ResourceLogs, ScopeLogs},
    resource::v1::Resource,
};
use rusqlite::{Connection, ErrorCode, MAIN_DB, OpenFlags};
use tempfile::tempdir;

use crate::query::LogFilters;

use super::Store;

const HISTORICAL_V0_SCHEMA: &str = include_str!("schema/fixtures/v0_schema.sql");

#[derive(Debug, Eq, PartialEq)]
struct LogicalState {
    user_version: i64,
    schema_version: i64,
    journal_mode: String,
    schema_objects: Vec<(String, String, String, Option<String>)>,
    spans: Vec<(String, String, String)>,
}

type FilesystemSnapshot = BTreeMap<String, Vec<u8>>;

#[test]
fn read_only_open_does_not_create_a_missing_database_or_parent() {
    let tempdir = tempdir().unwrap();
    let parent = tempdir.path().join("missing");
    let path = parent.join("ottyel.db");

    let error = Store::open_read_only(&path).unwrap_err();

    assert!(
        format!("{error:#}").contains("failed to open read-only sqlite db"),
        "unexpected error: {error:#}"
    );
    assert!(!parent.exists());
    assert!(!path.exists());
}

#[test]
fn cold_wal_read_preserves_main_database_and_only_creates_coordination_sidecars() {
    let tempdir = tempdir().unwrap();
    let path = tempdir.path().join("ottyel.db");
    create_compatible_database(&path, 1);
    let writer = Store::open(&path, 24, 1_000).unwrap();
    let expected = {
        let conn = writer.conn.lock().unwrap();
        logical_state(&conn)
    };
    assert_eq!(expected.journal_mode, "wal");
    drop(writer);

    // This is intentionally the first filesystem observer after the WAL writer closes.
    let before = filesystem_snapshot(tempdir.path());
    assert_eq!(before.keys().collect::<Vec<_>>(), vec!["ottyel.db"]);
    let main_before = before["ottyel.db"].clone();

    let reader = Store::open_read_only(&path).unwrap();
    assert_compatible_reader(&reader, &expected);
    let while_open = filesystem_snapshot(tempdir.path());
    assert_eq!(while_open["ottyel.db"], main_before);
    assert_eq!(
        new_file_names(&before, &while_open),
        BTreeSet::from(["ottyel.db-shm".to_string(), "ottyel.db-wal".to_string(),])
    );
    drop(reader);

    let after = filesystem_snapshot(tempdir.path());
    assert_eq!(after["ottyel.db"], main_before);
    assert_only_wal_coordination_files(&after);
}

#[test]
fn exact_legacy_v0_is_queryable_without_database_or_sidecar_content_changes() {
    let tempdir = tempdir().unwrap();
    let path = tempdir.path().join("ottyel.db");
    let conn = create_compatible_database(&path, 0);
    let expected = logical_state(&conn);
    assert_eq!(expected.user_version, 0);
    assert_eq!(expected.journal_mode, "delete");
    drop(conn);
    let before = filesystem_snapshot(tempdir.path());

    let reader = Store::open_read_only(&path).unwrap();
    assert_compatible_reader(&reader, &expected);
    drop(reader);

    assert_eq!(filesystem_snapshot(tempdir.path()), before);
}

#[test]
fn read_only_open_rejects_an_empty_database_without_mutation() {
    let tempdir = tempdir().unwrap();
    let path = tempdir.path().join("empty.db");
    fs::File::create(&path).unwrap();
    let before = filesystem_snapshot(tempdir.path());

    let error = Store::open_read_only(&path).unwrap_err();

    assert!(
        format!("{error:#}").contains("unversioned database is empty"),
        "unexpected error: {error:#}"
    );
    assert_eq!(filesystem_snapshot(tempdir.path()), before);
}

#[test]
fn read_only_open_rejects_future_and_negative_versions_without_mutation() {
    for (version, expected_message) in [
        (2, "schema version 2 is newer"),
        (-1, "schema version -1 is invalid"),
    ] {
        let tempdir = tempdir().unwrap();
        let path = tempdir.path().join("versioned.db");
        let conn = create_compatible_database(&path, version);
        let expected = logical_state(&conn);
        drop(conn);
        let before = filesystem_snapshot(tempdir.path());

        let error = Store::open_read_only(&path).unwrap_err();

        assert!(
            format!("{error:#}").contains(expected_message),
            "unexpected error: {error:#}"
        );
        assert_rejected_delete_database_unchanged(tempdir.path(), &path, &before, &expected);
    }
}

#[test]
fn read_only_open_rejects_incompatible_v0_and_v1_without_mutation() {
    for (version, expected_message) in [
        (0, "unversioned database is incompatible with v1 schema"),
        (1, "version 1 database has an incompatible schema"),
    ] {
        let tempdir = tempdir().unwrap();
        let path = tempdir.path().join("incompatible.db");
        let conn = Connection::open(&path).unwrap();
        conn.execute_batch(
            "CREATE TABLE spans (
                 trace_id TEXT NOT NULL,
                 span_id TEXT NOT NULL,
                 span_name TEXT NOT NULL
             );
             INSERT INTO spans VALUES ('trace-preserve', 'span-preserve', 'preserve me');",
        )
        .unwrap();
        conn.pragma_update(None, "user_version", version).unwrap();
        let expected = logical_state(&conn);
        drop(conn);
        let before = filesystem_snapshot(tempdir.path());

        let error = Store::open_read_only(&path).unwrap_err();

        assert!(
            format!("{error:#}").contains(expected_message),
            "unexpected error: {error:#}"
        );
        assert_rejected_delete_database_unchanged(tempdir.path(), &path, &before, &expected);
    }
}

#[test]
fn read_only_store_rejects_direct_sql_and_every_ingest_entrypoint() {
    let tempdir = tempdir().unwrap();
    let path = tempdir.path().join("ottyel.db");
    drop(create_compatible_database(&path, 1));
    let before = filesystem_snapshot(tempdir.path());
    let store = Store::open_read_only(&path).unwrap();

    {
        let conn = store.conn.lock().unwrap();
        assert!(conn.is_readonly(MAIN_DB).unwrap());
        assert_eq!(pragma_i64(&conn, "query_only"), 1);
        let error = conn
            .execute(
                "INSERT INTO spans (
                     trace_id, span_id, parent_span_id, service_name, span_name, span_kind,
                     status_code, start_time_unix_nano, end_time_unix_nano, duration_ms,
                     resource_attributes_json, attributes_json, llm_json
                 ) VALUES (
                     'trace-direct-write', 'span-direct-write', '', 'test', 'must fail',
                     'INTERNAL', 'STATUS_CODE_OK', 300, 400, 0.0001, '{}', '{}', NULL
                 )",
                [],
            )
            .unwrap_err();
        assert_eq!(error.sqlite_error_code(), Some(ErrorCode::ReadOnly));
    }
    for result in [
        store.ingest_traces(ExportTraceServiceRequest::default()),
        store.ingest_logs(ExportLogsServiceRequest::default()),
        store.ingest_metrics(ExportMetricsServiceRequest::default()),
    ] {
        let error = result.unwrap_err();
        assert!(
            format!("{error:#}").contains("cannot ingest telemetry through a read-only store"),
            "unexpected error: {error:#}"
        );
    }
    drop(store);

    assert_eq!(filesystem_snapshot(tempdir.path()), before);
}

#[test]
fn open_read_only_reader_observes_wal_commits_made_after_it_opens() {
    let tempdir = tempdir().unwrap();
    let path = tempdir.path().join("ottyel.db");
    let writer = Store::open(&path, 24, 1_000).unwrap();
    let reader = Store::open_read_only(&path).unwrap();
    let marker = "committed-after-reader-open";

    assert!(
        reader
            .recent_logs(None, 10, None, Some(marker), &LogFilters::default())
            .unwrap()
            .is_empty()
    );
    writer.ingest_logs(live_log_request(marker)).unwrap();

    let logs = reader
        .recent_logs(None, 10, None, Some(marker), &LogFilters::default())
        .unwrap();
    assert_eq!(logs.len(), 1);
    assert_eq!(logs[0].body, marker);
}

fn assert_compatible_reader(reader: &Store, expected: &LogicalState) {
    assert_eq!(reader.counts(None).unwrap(), (1, 0, 0, 0, 0));
    let spans = reader.trace_detail("trace-preserve").unwrap();
    assert_eq!(spans.len(), 1);
    assert_eq!(spans[0].span_name, "preserve me");
    let conn = reader.conn.lock().unwrap();
    assert!(conn.is_readonly(MAIN_DB).unwrap());
    assert_eq!(pragma_i64(&conn, "query_only"), 1);
    assert_eq!(logical_state(&conn), *expected);
}

fn assert_rejected_delete_database_unchanged(
    directory: &Path,
    path: &Path,
    before: &FilesystemSnapshot,
    expected: &LogicalState,
) {
    assert_eq!(filesystem_snapshot(directory), *before);
    let conn = Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .unwrap();
    assert_eq!(logical_state(&conn), *expected);
    drop(conn);
    assert_eq!(filesystem_snapshot(directory), *before);
}

fn create_compatible_database(path: &Path, version: i64) -> Connection {
    let conn = Connection::open(path).unwrap();
    conn.execute_batch(HISTORICAL_V0_SCHEMA).unwrap();
    insert_span(&conn);
    conn.pragma_update(None, "user_version", version).unwrap();
    conn
}

fn insert_span(conn: &Connection) {
    conn.execute(
        "INSERT INTO spans (
             trace_id, span_id, parent_span_id, service_name, span_name, span_kind,
             status_code, start_time_unix_nano, end_time_unix_nano, duration_ms,
             resource_attributes_json, attributes_json, llm_json
         ) VALUES (
             'trace-preserve', 'span-preserve', '', 'test', 'preserve me', 'INTERNAL',
             'STATUS_CODE_OK', 100, 200, 0.0001, '{}', '{}', NULL
         )",
        [],
    )
    .unwrap();
}

fn live_log_request(body: &str) -> ExportLogsServiceRequest {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64;
    ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            resource: Some(Resource::default()),
            schema_url: String::new(),
            scope_logs: vec![ScopeLogs {
                scope: Some(InstrumentationScope::default()),
                schema_url: String::new(),
                log_records: vec![LogRecord {
                    time_unix_nano: now,
                    observed_time_unix_nano: now,
                    severity_text: "INFO".to_string(),
                    body: Some(AnyValue {
                        value: Some(any_value::Value::StringValue(body.to_string())),
                    }),
                    ..LogRecord::default()
                }],
            }],
        }],
    }
}

fn logical_state(conn: &Connection) -> LogicalState {
    let schema_objects = {
        let mut statement = conn
            .prepare(
                "SELECT type, name, tbl_name, sql
                 FROM sqlite_schema
                 WHERE name NOT LIKE 'sqlite_%'
                 ORDER BY type, name",
            )
            .unwrap();
        statement
            .query_map([], |row| {
                Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
            })
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap()
    };
    let spans = if table_exists(conn, "spans") {
        let mut statement = conn
            .prepare("SELECT trace_id, span_id, span_name FROM spans ORDER BY trace_id, span_id")
            .unwrap();
        statement
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap()
    } else {
        Vec::new()
    };
    LogicalState {
        user_version: pragma_i64(conn, "user_version"),
        schema_version: pragma_i64(conn, "schema_version"),
        journal_mode: pragma_string(conn, "journal_mode"),
        schema_objects,
        spans,
    }
}

fn filesystem_snapshot(directory: &Path) -> FilesystemSnapshot {
    fs::read_dir(directory)
        .unwrap()
        .map(|entry| {
            let entry = entry.unwrap();
            let name = entry.file_name().into_string().unwrap();
            (name, fs::read(entry.path()).unwrap())
        })
        .collect()
}

fn new_file_names(before: &FilesystemSnapshot, after: &FilesystemSnapshot) -> BTreeSet<String> {
    after
        .keys()
        .filter(|name| !before.contains_key(*name))
        .cloned()
        .collect()
}

fn assert_only_wal_coordination_files(snapshot: &FilesystemSnapshot) {
    let allowed = BTreeSet::from([
        "ottyel.db".to_string(),
        "ottyel.db-shm".to_string(),
        "ottyel.db-wal".to_string(),
    ]);
    assert!(snapshot.keys().all(|name| allowed.contains(name)));
}

fn table_exists(conn: &Connection, name: &str) -> bool {
    conn.query_row(
        "SELECT EXISTS (SELECT 1 FROM sqlite_schema WHERE type = 'table' AND name = ?1)",
        [name],
        |row| row.get(0),
    )
    .unwrap()
}

fn pragma_string(conn: &Connection, name: &str) -> String {
    conn.pragma_query_value(None, name, |row| row.get(0))
        .unwrap()
}

fn pragma_i64(conn: &Connection, name: &str) -> i64 {
    conn.pragma_query_value(None, name, |row| row.get(0))
        .unwrap()
}
