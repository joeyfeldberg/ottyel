use opentelemetry_proto::tonic::{
    collector::logs::v1::ExportLogsServiceRequest,
    common::v1::{AnyValue, InstrumentationScope, any_value},
    logs::v1::{LogRecord, ResourceLogs, ScopeLogs, SeverityNumber},
    resource::v1::Resource,
};
use tempfile::tempdir;

use crate::query::{LogFilters, LogSeverityFilter};

use super::{Store, now_nanos, string_attr};

#[test]
fn ingest_uses_event_time_with_observed_time_as_fallback() {
    let tempdir = tempdir().unwrap();
    let store = Store::open(&tempdir.path().join("ottyel.db"), 24, 1000).unwrap();
    let now = now_nanos() as u64;
    let event_time = now;
    let fallback_time = now + 1_000_000;

    store
        .ingest_logs(log_records_request(vec![
            test_log_record(
                event_time,
                now + 5_000_000,
                SeverityNumber::Info as i32,
                "INFO",
                "late arrival",
            ),
            test_log_record(
                0,
                fallback_time,
                SeverityNumber::Info as i32,
                "INFO",
                "missing event time",
            ),
        ]))
        .unwrap();

    let logs = store
        .recent_logs(None, 10, None, None, &LogFilters::default())
        .unwrap();
    let late_arrival = logs.iter().find(|log| log.body == "late arrival").unwrap();
    let missing_event_time = logs
        .iter()
        .find(|log| log.body == "missing event time")
        .unwrap();

    assert_eq!(late_arrival.timestamp_unix_nano, event_time as i64);
    assert_eq!(missing_event_time.timestamp_unix_nano, fallback_time as i64);
}

#[test]
fn ingest_derives_every_numeric_severity_and_preserves_source_text() {
    let tempdir = tempdir().unwrap();
    let store = Store::open(&tempdir.path().join("ottyel.db"), 24, 1000).unwrap();
    let now = now_nanos() as u64;
    let canonical_severities = [
        (SeverityNumber::Unspecified, "UNSPECIFIED"),
        (SeverityNumber::Trace, "TRACE"),
        (SeverityNumber::Trace2, "TRACE2"),
        (SeverityNumber::Trace3, "TRACE3"),
        (SeverityNumber::Trace4, "TRACE4"),
        (SeverityNumber::Debug, "DEBUG"),
        (SeverityNumber::Debug2, "DEBUG2"),
        (SeverityNumber::Debug3, "DEBUG3"),
        (SeverityNumber::Debug4, "DEBUG4"),
        (SeverityNumber::Info, "INFO"),
        (SeverityNumber::Info2, "INFO2"),
        (SeverityNumber::Info3, "INFO3"),
        (SeverityNumber::Info4, "INFO4"),
        (SeverityNumber::Warn, "WARN"),
        (SeverityNumber::Warn2, "WARN2"),
        (SeverityNumber::Warn3, "WARN3"),
        (SeverityNumber::Warn4, "WARN4"),
        (SeverityNumber::Error, "ERROR"),
        (SeverityNumber::Error2, "ERROR2"),
        (SeverityNumber::Error3, "ERROR3"),
        (SeverityNumber::Error4, "ERROR4"),
        (SeverityNumber::Fatal, "FATAL"),
        (SeverityNumber::Fatal2, "FATAL2"),
        (SeverityNumber::Fatal3, "FATAL3"),
        (SeverityNumber::Fatal4, "FATAL4"),
    ];
    let mut records = canonical_severities
        .iter()
        .enumerate()
        .map(|(index, (number, expected))| {
            test_log_record(now + index as u64, now, *number as i32, "", expected)
        })
        .collect::<Vec<_>>();
    records.push(test_log_record(
        now + records.len() as u64,
        now,
        99,
        "",
        "unknown number",
    ));
    records.push(test_log_record(
        now + records.len() as u64,
        now,
        SeverityNumber::Error as i32,
        "source-critical",
        "source text",
    ));
    store.ingest_logs(log_records_request(records)).unwrap();

    let logs = store
        .recent_logs(None, 30, None, None, &LogFilters::default())
        .unwrap();
    for (_, expected) in canonical_severities {
        assert_eq!(severity_for(&logs, expected), expected);
    }
    assert_eq!(severity_for(&logs, "unknown number"), "UNSPECIFIED");
    assert_eq!(severity_for(&logs, "source text"), "source-critical");
}

#[test]
fn severity_filters_match_derived_labels() {
    let tempdir = tempdir().unwrap();
    let store = Store::open(&tempdir.path().join("ottyel.db"), 24, 1000).unwrap();
    let now = now_nanos() as u64;
    let records = [
        (SeverityNumber::Trace2, "trace"),
        (SeverityNumber::Debug4, "debug"),
        (SeverityNumber::Info2, "info"),
        (SeverityNumber::Warn3, "warn"),
        (SeverityNumber::Error2, "error"),
        (SeverityNumber::Fatal4, "fatal"),
    ];
    store
        .ingest_logs(log_records_request(
            records
                .iter()
                .enumerate()
                .map(|(index, (number, body))| {
                    test_log_record(now + index as u64, now, *number as i32, "", body)
                })
                .collect(),
        ))
        .unwrap();

    for (filter, expected_bodies) in [
        (LogSeverityFilter::Debug, vec!["debug", "trace"]),
        (LogSeverityFilter::Info, vec!["info"]),
        (LogSeverityFilter::Warn, vec!["warn"]),
        (LogSeverityFilter::Error, vec!["fatal", "error"]),
    ] {
        let filtered = store
            .recent_logs(
                None,
                20,
                None,
                None,
                &LogFilters {
                    severity: filter,
                    ..LogFilters::default()
                },
            )
            .unwrap();
        assert_eq!(
            filtered
                .iter()
                .map(|log| log.body.as_str())
                .collect::<Vec<_>>(),
            expected_bodies
        );
    }
}

fn severity_for<'a>(logs: &'a [crate::domain::LogSummary], body: &str) -> &'a str {
    logs.iter()
        .find(|log| log.body == body)
        .map(|log| log.severity.as_str())
        .unwrap()
}

fn log_records_request(log_records: Vec<LogRecord>) -> ExportLogsServiceRequest {
    ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            resource: Some(Resource {
                attributes: vec![string_attr("service.name", "api")],
                dropped_attributes_count: 0,
                entity_refs: Vec::new(),
            }),
            schema_url: String::new(),
            scope_logs: vec![ScopeLogs {
                scope: Some(InstrumentationScope::default()),
                schema_url: String::new(),
                log_records,
            }],
        }],
    }
}

fn test_log_record(
    time_unix_nano: u64,
    observed_time_unix_nano: u64,
    severity_number: i32,
    severity_text: &str,
    body: &str,
) -> LogRecord {
    LogRecord {
        time_unix_nano,
        observed_time_unix_nano,
        severity_number,
        severity_text: severity_text.to_string(),
        body: Some(AnyValue {
            value: Some(any_value::Value::StringValue(body.to_string())),
        }),
        attributes: Vec::new(),
        dropped_attributes_count: 0,
        flags: 0,
        trace_id: Vec::new(),
        span_id: Vec::new(),
        event_name: String::new(),
    }
}
