use std::{
    sync::mpsc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use opentelemetry_proto::tonic::{
    collector::logs::v1::{ExportLogsServiceRequest, logs_service_client::LogsServiceClient},
    logs::v1::{LogRecord, ResourceLogs, ScopeLogs},
};
use tempfile::tempdir;

use super::{Receiver, ShutdownReport};
use crate::{
    ingest::IngestLimits,
    store::{Store, StoreWriteError, WriterDrain},
};

fn log_request() -> ExportLogsServiceRequest {
    ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            scope_logs: vec![ScopeLogs {
                log_records: vec![LogRecord {
                    time_unix_nano: SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_nanos() as u64,
                    ..LogRecord::default()
                }],
                ..ScopeLogs::default()
            }],
            ..ResourceLogs::default()
        }],
    }
}

#[tokio::test]
async fn binding_reports_an_occupied_port_before_serving() {
    let directory = tempdir().unwrap();
    let store = Store::open(&directory.path().join("ottyel.db"), 24, 1000).unwrap();
    let occupied = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = occupied.local_addr().unwrap().to_string();

    let error = Receiver::new(store, IngestLimits::default())
        .bind("127.0.0.1:0", &address)
        .await
        .err()
        .expect("an occupied gRPC port must fail to bind");
    assert!(error.to_string().contains("OTLP/gRPC listener"), "{error}");
}

#[tokio::test]
async fn shutdown_commits_accepted_work_and_closes_the_writer() {
    let directory = tempdir().unwrap();
    let store = Store::open(&directory.path().join("ottyel.db"), 24, 1000).unwrap();
    let receiver = Receiver::new(store.clone(), IngestLimits::default())
        .bind("127.0.0.1:0", "127.0.0.1:0")
        .await
        .unwrap();
    let endpoint = format!("http://{}", receiver.grpc_addr().unwrap());
    let (shutdown_sender, shutdown) = tokio::sync::watch::channel(false);
    let server = tokio::spawn(receiver.serve(shutdown));

    LogsServiceClient::connect(endpoint)
        .await
        .unwrap()
        .export(log_request())
        .await
        .unwrap();
    shutdown_sender.send(true).unwrap();
    let report = server.await.unwrap().unwrap();

    assert_eq!(
        report,
        ShutdownReport {
            requests_abandoned: false,
            writer: Some(WriterDrain {
                completed: true,
                unacknowledged_records: 0,
            }),
        }
    );
    assert_eq!(store.counts(None).unwrap().2, 1);
    let later = store.ingest_logs(log_request()).unwrap_err();
    assert!(matches!(
        later.downcast_ref(),
        Some(StoreWriteError::Unavailable)
    ));
}

#[tokio::test]
async fn shutdown_deadline_abandons_stuck_requests_and_writes() {
    let directory = tempdir().unwrap();
    let store = Store::open(&directory.path().join("ottyel.db"), 24, 1000).unwrap();
    let limits = IngestLimits {
        shutdown_timeout: Duration::from_millis(150),
        ..IngestLimits::default()
    };
    let receiver = Receiver::new(store.clone(), limits);
    let probe = receiver.probe();
    let receiver = receiver.bind("127.0.0.1:0", "127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", receiver.grpc_addr().unwrap());
    let (shutdown_sender, shutdown) = tokio::sync::watch::channel(false);
    let server = tokio::spawn(receiver.serve(shutdown));

    let (entered_sender, entered) = mpsc::channel();
    let (release, released) = mpsc::channel::<()>();
    let parking_store = store.clone();
    let parked = std::thread::spawn(move || {
        parking_store.execute_write_for_test(move |_| {
            entered_sender.send(()).unwrap();
            let _ = released.recv();
            Ok(())
        })
    });
    entered.recv_timeout(Duration::from_secs(2)).unwrap();
    let export = tokio::spawn(async move {
        LogsServiceClient::connect(endpoint)
            .await
            .unwrap()
            .export(log_request())
            .await
    });
    let waiting = Instant::now();
    while probe.sample().queued_records == 0 {
        assert!(
            waiting.elapsed() < Duration::from_secs(2),
            "export never queued"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    let stopping = Instant::now();
    shutdown_sender.send(true).unwrap();
    let report = server.await.unwrap().unwrap();
    assert!(stopping.elapsed() < Duration::from_secs(2));
    assert_eq!(
        report,
        ShutdownReport {
            requests_abandoned: true,
            writer: Some(WriterDrain {
                completed: false,
                unacknowledged_records: 1,
            }),
        }
    );
    assert!(!report.is_clean());

    // Shutdown stops waiting rather than killing connection tasks, so once the writer is
    // released the abandoned export may still finish inside this test runtime.
    release.send(()).unwrap();
    parked.join().unwrap().unwrap();
    let _ = export.await.unwrap();
}
