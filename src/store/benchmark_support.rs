//! Opaque controls used by the release benchmark.
//!
//! This module exists only with the `benchmark-support` feature. It is not a supported product API.

use std::{
    sync::mpsc::{SyncSender, sync_channel},
    time::Duration,
};

use anyhow::{Context, Result};
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;

use super::{AsyncWriteReceipt, PreparedIngest, Store};

#[doc(hidden)]
pub use super::write_observer::BenchmarkSnapshot;

const PARK_TIMEOUT: Duration = Duration::from_secs(10);

/// A trace request measured and ready for immediate writer admission.
#[doc(hidden)]
pub struct PreparedTraceExport {
    inner: PreparedIngest<ExportTraceServiceRequest>,
}

/// An opaque definitive SQLite completion receipt.
#[doc(hidden)]
pub struct WriteReceipt<T> {
    inner: AsyncWriteReceipt<T>,
}

impl<T> WriteReceipt<T> {
    /// Waits for the admitted writer operation to finish.
    pub async fn wait(self) -> Result<T> {
        self.inner.wait().await
    }
}

/// A deterministic writer rendezvous used to make subsequent jobs adjacent.
#[doc(hidden)]
pub struct ParkedWriter {
    release: Option<SyncSender<()>>,
    receipt: Option<WriteReceipt<()>>,
}

impl ParkedWriter {
    /// Releases the writer without waiting for the uncounted parking command's receipt.
    pub fn release(mut self) -> Result<()> {
        self.release
            .take()
            .expect("parked writer can only be released once")
            .send(())
            .context("failed to release benchmark writer")?;
        drop(self.receipt.take());
        Ok(())
    }
}

impl Drop for ParkedWriter {
    fn drop(&mut self) {
        if let Some(release) = self.release.take() {
            let _ = release.send(());
        }
        drop(self.receipt.take());
    }
}

impl Store {
    /// Measures a trace request outside the benchmark's admission/acknowledgement interval.
    #[doc(hidden)]
    pub fn prepare_trace_export_for_benchmark(
        request: ExportTraceServiceRequest,
    ) -> PreparedTraceExport {
        PreparedTraceExport {
            inner: PreparedIngest::prepare(request),
        }
    }

    /// Attempts the same asynchronous prepared trace admission used by HTTP and gRPC.
    #[doc(hidden)]
    pub fn try_ingest_traces_for_benchmark(
        &self,
        prepared: PreparedTraceExport,
    ) -> Result<WriteReceipt<usize>> {
        self.try_ingest_traces(prepared.inner)
            .map(|inner| WriteReceipt { inner })
    }

    /// Parks the writer on an uncounted command and waits until that command owns the connection.
    #[doc(hidden)]
    pub fn park_writer_for_benchmark(&self) -> Result<ParkedWriter> {
        let writer = self.write_access()?;
        let (entered_sender, entered_receiver) = sync_channel(1);
        let (release_sender, release_receiver) = sync_channel(1);
        let receipt = writer.try_execute_async(move |_| {
            entered_sender
                .send(())
                .context("failed to report parked benchmark writer")?;
            release_receiver
                .recv()
                .context("benchmark writer release channel disconnected")?;
            Ok(())
        })?;
        entered_receiver
            .recv_timeout(PARK_TIMEOUT)
            .context("timed out waiting for benchmark writer to park")?;
        Ok(ParkedWriter {
            release: Some(release_sender),
            receipt: Some(WriteReceipt { inner: receipt }),
        })
    }

    /// Returns a fixed-size snapshot of feature-gated writer observations.
    #[doc(hidden)]
    pub fn writer_benchmark_snapshot(&self) -> Result<BenchmarkSnapshot> {
        let writer = self.write_access()?;
        Ok(writer.observer().snapshot())
    }
}

#[cfg(test)]
mod tests {
    use opentelemetry_proto::tonic::{
        collector::trace::v1::ExportTraceServiceRequest,
        trace::v1::{ResourceSpans, ScopeSpans, Span},
    };
    use tempfile::tempdir;

    use super::{PreparedTraceExport, Store};

    const EXPORTS: usize = 4;
    const RECORDS_PER_EXPORT: usize = 250;

    #[test]
    fn parked_writer_coalesces_four_real_async_exports_observably() {
        let directory = tempdir().unwrap();
        let store = Store::open(&directory.path().join("benchmark.db"), 24, 2_000).unwrap();
        let initial_rows = span_rows(&store);
        let prepared: [PreparedTraceExport; EXPORTS] = std::array::from_fn(|export| {
            Store::prepare_trace_export_for_benchmark(trace_request(export))
        });
        let parked = store.park_writer_for_benchmark().unwrap();
        let before = store.writer_benchmark_snapshot().unwrap();
        let receipts =
            prepared.map(|request| store.try_ingest_traces_for_benchmark(request).unwrap());

        assert_eq!(store.writer_benchmark_snapshot().unwrap(), before);
        parked.release().unwrap();
        let accepted = futures::executor::block_on(futures::future::join_all(
            receipts.into_iter().map(|receipt| receipt.wait()),
        ));
        assert_eq!(
            accepted.into_iter().map(Result::unwrap).collect::<Vec<_>>(),
            vec![RECORDS_PER_EXPORT; EXPORTS]
        );

        let after = store.writer_benchmark_snapshot().unwrap();
        assert_eq!(span_rows(&store) - initial_rows, 1_000);
        // Debug builds can cross the 25 ms age boundary between exports, so only the release
        // benchmark asserts exactly one group; these invariants hold for any split.
        let groups = after.groups_started - before.groups_started;
        assert!((1..=EXPORTS as u64).contains(&groups), "{groups} groups");
        assert_eq!(after.exports_grouped - before.exports_grouped, 4);
        assert_eq!(
            after.group_size_counts[4] - before.group_size_counts[4],
            0,
            "no group may exceed four exports"
        );
        for (committed, expected) in [
            (
                after.shared_ingest_retention_transactions_committed
                    - before.shared_ingest_retention_transactions_committed,
                groups,
            ),
            (
                after.sqlite_transactions_committed - before.sqlite_transactions_committed,
                groups,
            ),
            (
                after.retention_invocations - before.retention_invocations,
                groups,
            ),
            (
                after.ingest_only_transactions_committed
                    - before.ingest_only_transactions_committed,
                0,
            ),
            (
                after.retention_only_transactions_committed
                    - before.retention_only_transactions_committed,
                0,
            ),
            (
                after.sqlite_transactions_not_committed - before.sqlite_transactions_not_committed,
                0,
            ),
            (after.retention_failures - before.retention_failures, 0),
        ] {
            assert_eq!(committed, expected);
        }
    }

    fn trace_request(export: usize) -> ExportTraceServiceRequest {
        let spans = (0..RECORDS_PER_EXPORT)
            .map(|record| {
                let unique = export * RECORDS_PER_EXPORT + record;
                let mut trace_id = vec![0; 16];
                trace_id[8..].copy_from_slice(&(unique as u64).to_be_bytes());
                Span {
                    trace_id,
                    span_id: (unique as u64).to_be_bytes().to_vec(),
                    name: format!("benchmark-span-{unique}"),
                    start_time_unix_nano: 4_000_000_000_000_000_000 + unique as u64,
                    end_time_unix_nano: 4_000_000_000_001_000_000 + unique as u64,
                    ..Span::default()
                }
            })
            .collect();
        ExportTraceServiceRequest {
            resource_spans: vec![ResourceSpans {
                scope_spans: vec![ScopeSpans {
                    spans,
                    ..ScopeSpans::default()
                }],
                ..ResourceSpans::default()
            }],
        }
    }

    fn span_rows(store: &Store) -> u64 {
        store
            .execute_write_for_test(|connection| {
                connection
                    .query_row("SELECT COUNT(*) FROM spans", [], |row| row.get::<_, i64>(0))
                    .map_err(Into::into)
            })
            .unwrap()
            .try_into()
            .unwrap()
    }
}
