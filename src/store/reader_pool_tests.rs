use std::{
    sync::mpsc,
    thread,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use opentelemetry_proto::tonic::{
    collector::logs::v1::ExportLogsServiceRequest,
    common::v1::{AnyValue, InstrumentationScope, any_value},
    logs::v1::{LogRecord, ResourceLogs, ScopeLogs},
    resource::v1::Resource,
};
use rusqlite::{Connection, ErrorCode, MAIN_DB, params};
use tempfile::tempdir;

use super::Store;

const WAIT: Duration = Duration::from_secs(2);

#[test]
fn held_reader_snapshot_does_not_block_ingest_and_refreshes_after_restart() {
    let tempdir = tempdir().unwrap();
    let store = Store::open(&tempdir.path().join("ottyel.db"), 24, 1_000).unwrap();
    let reader = store.reader_connection_for_test();
    reader.execute_batch("BEGIN").unwrap();
    assert_eq!(log_count(&reader), 0);

    let writer = store.clone();
    let (result_tx, result_rx) = mpsc::channel();
    let handle = thread::spawn(move || {
        let result = writer.ingest_logs(log_request("committed during reader snapshot"));
        result_tx.send(result).unwrap();
    });

    let first_result = result_rx.recv_timeout(WAIT);
    if first_result.is_err() {
        reader.execute_batch("ROLLBACK").unwrap();
        let _ = result_rx.recv_timeout(WAIT);
        drop(handle);
        panic!("writer ingest did not complete while a reader snapshot was held");
    }
    assert_eq!(first_result.unwrap().unwrap(), 1);
    handle.join().unwrap();

    assert_eq!(log_count(&reader), 0);
    reader.execute_batch("COMMIT; BEGIN").unwrap();
    assert_eq!(log_count(&reader), 1);
    reader.execute_batch("COMMIT").unwrap();
}

#[test]
fn held_writer_transaction_does_not_block_reader_and_is_visible_after_commit() {
    let tempdir = tempdir().unwrap();
    let store = Store::open(&tempdir.path().join("ottyel.db"), 24, 1_000).unwrap();
    let writer = store.clone();
    let (inserted_tx, inserted_rx) = mpsc::channel();
    let (commit_tx, commit_rx) = mpsc::channel();
    let writer_handle = thread::spawn(move || {
        writer.execute_write_for_test(move |conn| {
            let transaction = conn.transaction()?;
            insert_log(&transaction, "uncommitted writer row");
            inserted_tx.send(()).unwrap();
            commit_rx.recv().unwrap();
            transaction.commit()?;
            Ok(())
        })
    });
    inserted_rx.recv_timeout(WAIT).unwrap();

    let reader = store.clone();
    let (result_tx, result_rx) = mpsc::channel();
    let handle = thread::spawn(move || {
        result_tx.send(reader.counts(None)).unwrap();
    });
    let read_result = result_rx.recv_timeout(WAIT);
    if read_result.is_err() {
        commit_tx.send(()).unwrap();
        let _ = writer_handle.join();
        let _ = result_rx.recv_timeout(WAIT);
        drop(handle);
        panic!("reader did not complete while a writer transaction was held");
    }
    assert_eq!(read_result.unwrap().unwrap().2, 0);
    handle.join().unwrap();

    commit_tx.send(()).unwrap();
    writer_handle.join().unwrap().unwrap();
    assert_eq!(store.counts(None).unwrap().2, 1);
}

#[test]
fn pool_blocks_at_capacity_and_progresses_when_a_reader_is_returned() {
    let tempdir = tempdir().unwrap();
    let store = Store::open(&tempdir.path().join("ottyel.db"), 24, 1_000).unwrap();
    let clone = store.clone();
    let capacity = store.reader_pool_capacity_for_test();
    let mut held = (0..capacity)
        .map(|_| store.reader_connection_for_test())
        .collect::<Vec<_>>();

    let (started_tx, started_rx) = mpsc::channel();
    let (result_tx, result_rx) = mpsc::channel();
    let handle = thread::spawn(move || {
        started_tx.send(()).unwrap();
        let reader = clone.reader_connection_for_test();
        result_tx.send(log_count(&reader)).unwrap();
    });
    started_rx.recv_timeout(WAIT).unwrap();
    let blocked_result = result_rx.recv_timeout(Duration::from_millis(150));

    drop(held.pop());
    let progress_result = result_rx.recv_timeout(WAIT);
    drop(held);

    if progress_result.is_ok() {
        handle.join().unwrap();
    } else {
        drop(handle);
    }
    assert!(
        matches!(blocked_result, Err(mpsc::RecvTimeoutError::Timeout)),
        "checkout unexpectedly progressed while every pooled reader was leased"
    );
    assert_eq!(
        progress_result.expect("checkout did not progress after a reader was returned"),
        0
    );
}

#[test]
fn cloned_stores_share_the_writer_owner_and_reader_bound() {
    let tempdir = tempdir().unwrap();
    let store = Store::open(&tempdir.path().join("ottyel.db"), 24, 1_000).unwrap();
    let clone = store.clone();

    assert!(store.shares_writer_with_for_test(&clone));
    assert!(store.shares_reader_pool_with_for_test(&clone));
    assert_eq!(
        store.reader_pool_capacity_for_test(),
        clone.reader_pool_capacity_for_test()
    );

    drop(store);
    clone
        .ingest_logs(log_request("written after original Store was dropped"))
        .unwrap();
    assert_eq!(clone.counts(None).unwrap().2, 1);

    let reader = clone.reader_connection_for_test();
    drop(clone);
    assert_eq!(log_count(&reader), 1);
    drop(reader);
}

#[test]
fn non_file_backed_paths_are_rejected_before_connections_can_diverge() {
    for result in [
        Store::open(":memory:".as_ref(), 24, 1_000),
        Store::open_read_only(":memory:".as_ref()),
        Store::open("".as_ref(), 24, 1_000),
        Store::open_read_only("".as_ref()),
        Store::open("file::memory:?cache=shared".as_ref(), 24, 1_000),
        Store::open_read_only("file:ottyel.db?mode=ro".as_ref()),
    ] {
        let error = result.unwrap_err();
        assert!(
            format!("{error:#}").contains("requires a plain filesystem-backed SQLite database"),
            "unexpected error: {error:#}"
        );
    }
}

#[cfg(unix)]
#[test]
fn non_utf8_sqlite_uri_path_is_rejected() {
    use std::{ffi::OsString, os::unix::ffi::OsStringExt, path::PathBuf};

    let path = PathBuf::from(OsString::from_vec(b"file:\xff".to_vec()));
    let error = Store::open(&path, 24, 1_000).unwrap_err();

    assert!(
        format!("{error:#}").contains("requires a plain filesystem-backed SQLite database"),
        "unexpected error: {error:#}"
    );
}

#[test]
fn every_pooled_reader_is_physical_read_only_and_query_only() {
    let tempdir = tempdir().unwrap();
    let store = Store::open(&tempdir.path().join("ottyel.db"), 24, 1_000).unwrap();
    let readers = (0..store.reader_pool_capacity_for_test())
        .map(|_| store.reader_connection_for_test())
        .collect::<Vec<_>>();

    for reader in &readers {
        assert!(reader.is_readonly(MAIN_DB).unwrap());
        assert_eq!(
            reader
                .pragma_query_value(None, "query_only", |row| row.get::<_, i64>(0))
                .unwrap(),
            1
        );
        let error = reader
            .execute(
                "INSERT INTO logs (
                     service_name, timestamp_unix_nano, severity, body, trace_id, span_id,
                     resource_attributes_json, attributes_json
                 ) VALUES ('test', 1, 'INFO', 'must fail', '', '', '{}', '{}')",
                [],
            )
            .unwrap_err();
        assert_eq!(error.sqlite_error_code(), Some(ErrorCode::ReadOnly));
    }
}

fn log_count(connection: &Connection) -> i64 {
    connection
        .query_row("SELECT COUNT(*) FROM logs", [], |row| row.get(0))
        .unwrap()
}

fn insert_log(connection: &Connection, body: &str) {
    connection
        .execute(
            "INSERT INTO logs (
                 service_name, timestamp_unix_nano, severity, body, trace_id, span_id,
                 resource_attributes_json, attributes_json
             ) VALUES ('test', ?1, 'INFO', ?2, '', '', '{}', '{}')",
            params![now_nanos(), body],
        )
        .unwrap();
}

fn log_request(body: &str) -> ExportLogsServiceRequest {
    let now = now_nanos() as u64;
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

fn now_nanos() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos() as i64
}
