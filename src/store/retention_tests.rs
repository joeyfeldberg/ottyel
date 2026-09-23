use std::time::{Duration, Instant};

use rusqlite::{Connection, params};
use tempfile::{TempDir, tempdir};

use super::{PASS_INTERVAL, ROWS_PER_UNIT, RetentionMaintenance};
use crate::store::{RetentionPolicy, helpers::now_unix_nanos, schema, writer::Maintenance};

const HOUR_NANOS: i64 = 60 * 60 * 1_000_000_000;

fn database() -> (TempDir, Connection) {
    let directory = tempdir().unwrap();
    let mut connection = Connection::open(directory.path().join("retention.db")).unwrap();
    schema::initialize(&mut connection).unwrap();
    (directory, connection)
}

fn maintenance(hours: u64, maximum_spans: usize) -> RetentionMaintenance {
    RetentionMaintenance::new(
        RetentionPolicy {
            hours,
            maximum_spans,
        },
        Instant::now(),
    )
}

fn run_unit(connection: &mut Connection, retention: &mut RetentionMaintenance) {
    let transaction = connection.transaction().unwrap();
    retention.run_unit(&transaction, Instant::now()).unwrap();
    transaction.commit().unwrap();
}

/// Runs committed units until the pass ends, returning how many ran.
fn run_pass(connection: &mut Connection, retention: &mut RetentionMaintenance) -> usize {
    retention.force_pass(Instant::now());
    let mut units = 0;
    while retention.in_pass() {
        run_unit(connection, retention);
        units += 1;
        assert!(units < 1_000, "retention pass did not terminate");
    }
    units
}

fn insert_log(connection: &Connection, timestamp: i64) {
    connection
        .execute(
            "INSERT INTO logs (
                 service_name, timestamp_unix_nano, severity, body, trace_id, span_id,
                 resource_attributes_json, attributes_json
             ) VALUES ('svc', ?1, 'INFO', 'body', '', '', '{}', '{}')",
            [timestamp],
        )
        .unwrap();
}

fn insert_span(connection: &Connection, trace_id: &str, span_id: &str, end: i64) {
    connection
        .execute(
            "INSERT INTO spans (
                 trace_id, span_id, parent_span_id, service_name, span_name, span_kind,
                 status_code, start_time_unix_nano, end_time_unix_nano, duration_ms,
                 resource_attributes_json, attributes_json, llm_json
             ) VALUES (?1, ?2, '', 'svc', 'op', 'INTERNAL', 'OK', ?3, ?3, 0.0, '{}', '{}', NULL)",
            params![trace_id, span_id, end],
        )
        .unwrap();
}

fn count(connection: &Connection, sql: &str) -> i64 {
    connection.query_row(sql, [], |row| row.get(0)).unwrap()
}

#[test]
fn a_pass_is_due_after_the_interval_or_enough_committed_records() {
    let now = Instant::now();
    let mut retention = RetentionMaintenance::new(
        RetentionPolicy {
            hours: 24,
            maximum_spans: 50_000,
        },
        now,
    );
    assert_eq!(retention.due_in(now), PASS_INTERVAL);
    assert_eq!(retention.due_in(now + PASS_INTERVAL), Duration::ZERO);

    retention.ingested(4_999);
    assert_eq!(retention.due_in(now), PASS_INTERVAL);
    retention.ingested(1);
    assert_eq!(retention.due_in(now), Duration::ZERO, "cap / 10 records");

    let (_directory, mut connection) = database();
    run_pass(&mut connection, &mut retention);
    let finished = Instant::now();
    assert!(!retention.due_in(finished).is_zero());
    assert!(retention.due_in(finished) <= PASS_INTERVAL);
}

#[test]
fn expired_logs_are_deleted_one_bounded_unit_at_a_time() {
    let (_directory, mut connection) = database();
    let now = now_unix_nanos();
    for _ in 0..(2 * ROWS_PER_UNIT + 500) {
        insert_log(&connection, now - 2 * HOUR_NANOS);
    }
    for _ in 0..3 {
        insert_log(&connection, now);
    }
    let mut retention = maintenance(1, 1_000);
    retention.force_pass(Instant::now());

    let remaining: Vec<i64> = (0..3)
        .map(|_| {
            run_unit(&mut connection, &mut retention);
            count(&connection, "SELECT COUNT(*) FROM logs")
        })
        .collect();

    assert_eq!(remaining, vec![(ROWS_PER_UNIT + 503) as i64, 503, 3]);
    run_pass(&mut connection, &mut retention);
    assert_eq!(count(&connection, "SELECT COUNT(*) FROM logs"), 3);
}

#[test]
fn many_old_spans_in_a_live_trace_do_not_stall_expiry_of_later_traces() {
    let (_directory, mut connection) = database();
    let now = now_unix_nanos();
    let old = now - 2 * HOUR_NANOS;
    for span in 0..(ROWS_PER_UNIT + 700) {
        insert_span(
            &connection,
            "live",
            &format!("live-{span}"),
            old + span as i64,
        );
    }
    insert_span(&connection, "live", "live-fresh", now);
    insert_span(&connection, "expired", "expired-1", old + 10_000);
    connection
        .execute(
            "INSERT INTO span_events (trace_id, span_id, name, timestamp_unix_nano, attributes_json)
             VALUES ('expired', 'expired-1', 'e', 1, '{}')",
            [],
        )
        .unwrap();
    let mut retention = maintenance(1, 100_000);

    let units = run_pass(&mut connection, &mut retention);

    assert_eq!(
        count(
            &connection,
            "SELECT COUNT(*) FROM spans WHERE trace_id = 'live'"
        ),
        (ROWS_PER_UNIT + 701) as i64
    );
    assert_eq!(
        count(
            &connection,
            "SELECT COUNT(*) FROM spans WHERE trace_id = 'expired'"
        ),
        0
    );
    assert_eq!(count(&connection, "SELECT COUNT(*) FROM span_events"), 0);
    // Logs, metrics, two span-scan units, and one cap check.
    assert_eq!(units, 5);
}

#[test]
fn the_span_cap_evicts_the_oldest_whole_traces_in_bounded_units() {
    let (_directory, mut connection) = database();
    let now = now_unix_nanos();
    for trace in 0..45 {
        insert_span(
            &connection,
            &format!("trace-{trace:02}"),
            &format!("span-{trace:02}"),
            now + trace,
        );
    }
    let mut retention = maintenance(24, 10);
    retention.force_pass(Instant::now());
    let mut spans_after_unit = Vec::new();
    while retention.in_pass() {
        run_unit(&mut connection, &mut retention);
        spans_after_unit.push(count(&connection, "SELECT COUNT(*) FROM spans"));
    }

    // Logs, metrics, and span expiry find nothing; the cap then evicts 20, then 15 traces.
    assert_eq!(spans_after_unit, vec![45, 45, 45, 25, 10]);
    let kept: Vec<String> = connection
        .prepare("SELECT trace_id FROM spans ORDER BY trace_id")
        .unwrap()
        .query_map([], |row| row.get(0))
        .unwrap()
        .map(Result::unwrap)
        .collect();
    assert_eq!(
        kept,
        (35..45)
            .map(|trace| format!("trace-{trace:02}"))
            .collect::<Vec<_>>()
    );
}

#[test]
fn a_failed_unit_abandons_the_pass_until_the_next_interval() {
    let mut retention = maintenance(24, 1_000);
    let now = Instant::now();
    retention.force_pass(now);
    assert!(retention.in_pass());

    retention.failed(now);

    assert!(!retention.in_pass());
    assert_eq!(retention.due_in(now), PASS_INTERVAL);
}
