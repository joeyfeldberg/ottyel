//! In-memory receiver statistics by signal and transport.
//!
//! Counters are cumulative for the process lifetime. Consumers derive recent rates and
//! latencies by subtracting two snapshots with [`StreamCounters::since`].

use std::{
    sync::{Arc, Mutex},
    time::{Duration, Instant, SystemTime},
};

use tokio::sync::Semaphore;

use crate::store::Store;

/// Upper bounds of the acknowledgement-latency buckets. A final bucket holds slower requests.
pub const LATENCY_BUCKET_BOUNDS: [Duration; 10] = [
    Duration::from_millis(1),
    Duration::from_millis(5),
    Duration::from_millis(10),
    Duration::from_millis(25),
    Duration::from_millis(50),
    Duration::from_millis(100),
    Duration::from_millis(250),
    Duration::from_millis(500),
    Duration::from_secs(1),
    Duration::from_millis(2_500),
];
const LATENCY_BUCKETS: usize = LATENCY_BUCKET_BOUNDS.len() + 1;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Signal {
    Traces,
    Logs,
    Metrics,
}

impl Signal {
    pub const ALL: [Self; 3] = [Self::Traces, Self::Logs, Self::Metrics];

    pub fn label(self) -> &'static str {
        match self {
            Self::Traces => "traces",
            Self::Logs => "logs",
            Self::Metrics => "metrics",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Transport {
    Http,
    Grpc,
}

impl Transport {
    pub const ALL: [Self; 2] = [Self::Http, Self::Grpc];

    pub fn label(self) -> &'static str {
        match self {
            Self::Http => "http",
            Self::Grpc => "grpc",
        }
    }
}

/// Why an export request did not commit.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Failure {
    /// Malformed envelope or payload; retrying the same request cannot succeed.
    Invalid,
    /// The request exceeded a byte, structure, or work budget.
    TooLarge,
    /// Request capacity, writer capacity, or writer lifecycle; the client may retry.
    Unavailable,
    /// The response deadline passed or the client cancelled first.
    TimedOut,
    Internal,
}

impl Failure {
    pub const ALL: [Self; 5] = [
        Self::Invalid,
        Self::TooLarge,
        Self::Unavailable,
        Self::TimedOut,
        Self::Internal,
    ];

    pub fn label(self) -> &'static str {
        match self {
            Self::Invalid => "invalid",
            Self::TooLarge => "too large",
            Self::Unavailable => "unavailable",
            Self::TimedOut => "timed out",
            Self::Internal => "internal",
        }
    }

    fn index(self) -> usize {
        match self {
            Self::Invalid => 0,
            Self::TooLarge => 1,
            Self::Unavailable => 2,
            Self::TimedOut => 3,
            Self::Internal => 4,
        }
    }

    /// Classifies a gRPC status the receiver returned for an export.
    pub(super) fn from_grpc(code: tonic::Code) -> Option<Self> {
        match code {
            tonic::Code::Ok => None,
            tonic::Code::InvalidArgument | tonic::Code::Unimplemented => Some(Self::Invalid),
            tonic::Code::ResourceExhausted | tonic::Code::OutOfRange => Some(Self::TooLarge),
            tonic::Code::Unavailable => Some(Self::Unavailable),
            tonic::Code::DeadlineExceeded | tonic::Code::Cancelled => Some(Self::TimedOut),
            _ => Some(Self::Internal),
        }
    }

    /// Classifies an HTTP status the receiver returned for an export.
    pub(super) fn from_http(status: axum::http::StatusCode) -> Option<Self> {
        use axum::http::StatusCode;
        match status {
            status if status.is_success() => None,
            StatusCode::PAYLOAD_TOO_LARGE => Some(Self::TooLarge),
            StatusCode::SERVICE_UNAVAILABLE | StatusCode::TOO_MANY_REQUESTS => {
                Some(Self::Unavailable)
            }
            StatusCode::GATEWAY_TIMEOUT | StatusCode::REQUEST_TIMEOUT => Some(Self::TimedOut),
            status if status.is_client_error() => Some(Self::Invalid),
            _ => Some(Self::Internal),
        }
    }
}

/// Cumulative counters for one signal on one transport.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct StreamCounters {
    pub requests_accepted: u64,
    pub records_accepted: u64,
    pub records_rejected: u64,
    pub requests_with_warnings: u64,
    pub failures: [u64; Failure::ALL.len()],
    /// Accepted-request acknowledgement latency, bucketed by [`LATENCY_BUCKET_BOUNDS`].
    pub latency: [u64; LATENCY_BUCKETS],
}

impl StreamCounters {
    pub fn failed(&self, failure: Failure) -> u64 {
        self.failures[failure.index()]
    }

    pub fn failed_requests(&self) -> u64 {
        self.failures.iter().sum()
    }

    /// Counters accumulated after `earlier`, saturating at zero.
    #[must_use]
    pub fn since(&self, earlier: &Self) -> Self {
        Self {
            requests_accepted: self
                .requests_accepted
                .saturating_sub(earlier.requests_accepted),
            records_accepted: self
                .records_accepted
                .saturating_sub(earlier.records_accepted),
            records_rejected: self
                .records_rejected
                .saturating_sub(earlier.records_rejected),
            requests_with_warnings: self
                .requests_with_warnings
                .saturating_sub(earlier.requests_with_warnings),
            failures: std::array::from_fn(|index| {
                self.failures[index].saturating_sub(earlier.failures[index])
            }),
            latency: std::array::from_fn(|index| {
                self.latency[index].saturating_sub(earlier.latency[index])
            }),
        }
    }

    /// The upper bound of the bucket holding the 95th-percentile accepted request, or `None`
    /// when no request was accepted or it fell in the unbounded final bucket.
    pub fn latency_p95_bound(&self) -> Option<Duration> {
        let total: u64 = self.latency.iter().sum();
        if total == 0 {
            return None;
        }
        let rank = total.saturating_mul(95).div_ceil(100);
        let mut seen = 0;
        for (index, count) in self.latency.iter().enumerate() {
            seen += count;
            if seen >= rank {
                return LATENCY_BUCKET_BOUNDS.get(index).copied();
            }
        }
        None
    }

    fn add(&mut self, other: &Self) {
        self.requests_accepted += other.requests_accepted;
        self.records_accepted += other.records_accepted;
        self.records_rejected += other.records_rejected;
        self.requests_with_warnings += other.requests_with_warnings;
        for (total, value) in self.failures.iter_mut().zip(other.failures) {
            *total += value;
        }
        for (total, value) in self.latency.iter_mut().zip(other.latency) {
            *total += value;
        }
    }
}

/// The most recent failed export.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FailureNote {
    pub signal: Signal,
    pub transport: Transport,
    pub failure: Failure,
    pub message: String,
    pub at: SystemTime,
}

/// One accepted export, as recorded by a transport handler.
pub(super) struct Accepted {
    pub(super) records: u64,
    pub(super) rejected: u64,
    pub(super) warned: bool,
    pub(super) latency: Duration,
}

#[derive(Default)]
struct Inner {
    streams: [[StreamCounters; Transport::ALL.len()]; Signal::ALL.len()],
    last_accepted: Option<SystemTime>,
    last_failure: Option<FailureNote>,
}

/// Shared receiver statistics. Clones record into the same counters.
#[derive(Clone, Default)]
pub(super) struct IngestStats {
    inner: Arc<Mutex<Inner>>,
}

impl IngestStats {
    pub(super) fn accepted(&self, signal: Signal, transport: Transport, accepted: Accepted) {
        let mut inner = self.lock();
        let stream = &mut inner.streams[signal_index(signal)][transport_index(transport)];
        stream.requests_accepted += 1;
        stream.records_accepted += accepted.records;
        stream.records_rejected += accepted.rejected;
        stream.requests_with_warnings += u64::from(accepted.warned);
        stream.latency[latency_bucket(accepted.latency)] += 1;
        inner.last_accepted = Some(SystemTime::now());
    }

    pub(super) fn failed(
        &self,
        signal: Signal,
        transport: Transport,
        failure: Failure,
        message: impl Into<String>,
    ) {
        let mut inner = self.lock();
        inner.streams[signal_index(signal)][transport_index(transport)].failures
            [failure.index()] += 1;
        inner.last_failure = Some(FailureNote {
            signal,
            transport,
            failure,
            message: message.into(),
            at: SystemTime::now(),
        });
    }

    /// Starts tracking one export. Dropping the guard unresolved records a timeout, because
    /// the transport only drops an export future on a deadline or client cancellation.
    pub(super) fn pending(&self, signal: Signal, transport: Transport) -> PendingExport {
        PendingExport {
            stats: self.clone(),
            signal,
            transport,
            started: Instant::now(),
            resolved: false,
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        self.inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

pub(super) struct PendingExport {
    stats: IngestStats,
    signal: Signal,
    transport: Transport,
    started: Instant,
    resolved: bool,
}

impl PendingExport {
    pub(super) fn accepted(mut self, records: u64, rejected: u64, warned: bool) {
        self.resolved = true;
        self.stats.accepted(
            self.signal,
            self.transport,
            Accepted {
                records,
                rejected,
                warned,
                latency: self.started.elapsed(),
            },
        );
    }

    pub(super) fn failed(mut self, failure: Failure, message: impl Into<String>) {
        self.resolved = true;
        self.stats
            .failed(self.signal, self.transport, failure, message);
    }

    /// Resolves without recording, for an outcome that an outer layer records.
    pub(super) fn dismiss(mut self) {
        self.resolved = true;
    }
}

impl Drop for PendingExport {
    fn drop(&mut self) {
        if !self.resolved {
            self.stats.failed(
                self.signal,
                self.transport,
                Failure::TimedOut,
                "export dropped before completion (deadline or client cancellation)",
            );
        }
    }
}

/// A point-in-time view of receiver activity and backlog.
#[derive(Clone, Debug, PartialEq)]
pub struct IngestHealth {
    pub taken_at: Instant,
    streams: [[StreamCounters; Transport::ALL.len()]; Signal::ALL.len()],
    pub last_accepted: Option<SystemTime>,
    pub last_failure: Option<FailureNote>,
    pub in_flight_requests: usize,
    pub max_in_flight_requests: usize,
    /// Primary records admitted to the writer but not yet acknowledged.
    pub queued_records: usize,
    pub max_queued_records: usize,
    /// Failed writer maintenance units, such as retention, since the store opened.
    pub maintenance_failures: u64,
}

impl IngestHealth {
    pub fn stream(&self, signal: Signal, transport: Transport) -> StreamCounters {
        self.streams[signal_index(signal)][transport_index(transport)]
    }

    pub fn totals(&self) -> StreamCounters {
        let mut totals = StreamCounters::default();
        for stream in self.streams.iter().flatten() {
            totals.add(stream);
        }
        totals
    }
}

#[cfg(test)]
impl IngestHealth {
    /// Builds a sample whose totals are `totals`, attributed to HTTP traces.
    pub(crate) fn for_test(
        taken_at: Instant,
        totals: StreamCounters,
        queued_records: usize,
        last_failure: Option<FailureNote>,
    ) -> Self {
        let mut streams = [[StreamCounters::default(); Transport::ALL.len()]; Signal::ALL.len()];
        streams[0][0] = totals;
        Self {
            taken_at,
            streams,
            last_accepted: None,
            last_failure,
            in_flight_requests: 0,
            max_in_flight_requests: 4,
            queued_records,
            max_queued_records: 40_000,
            maintenance_failures: 0,
        }
    }
}

/// A cloneable handle for sampling [`IngestHealth`] while the receiver runs.
#[derive(Clone)]
pub struct IngestProbe {
    stats: IngestStats,
    admission: Arc<Semaphore>,
    max_in_flight: usize,
    store: Store,
}

impl IngestProbe {
    pub(super) fn new(
        stats: IngestStats,
        admission: Arc<Semaphore>,
        max_in_flight: usize,
        store: Store,
    ) -> Self {
        Self {
            stats,
            admission,
            max_in_flight,
            store,
        }
    }

    pub fn sample(&self) -> IngestHealth {
        let (streams, last_accepted, last_failure) = {
            let inner = self.stats.lock();
            (
                inner.streams,
                inner.last_accepted,
                inner.last_failure.clone(),
            )
        };
        let backlog = self.store.writer_backlog();
        IngestHealth {
            taken_at: Instant::now(),
            streams,
            last_accepted,
            last_failure,
            in_flight_requests: self
                .max_in_flight
                .saturating_sub(self.admission.available_permits()),
            max_in_flight_requests: self.max_in_flight,
            queued_records: backlog.map_or(0, |backlog| backlog.primary_records),
            max_queued_records: backlog.map_or(0, |backlog| backlog.max_primary_records),
            maintenance_failures: backlog.map_or(0, |backlog| backlog.maintenance_failures),
        }
    }
}

fn signal_index(signal: Signal) -> usize {
    match signal {
        Signal::Traces => 0,
        Signal::Logs => 1,
        Signal::Metrics => 2,
    }
}

fn transport_index(transport: Transport) -> usize {
    match transport {
        Transport::Http => 0,
        Transport::Grpc => 1,
    }
}

fn latency_bucket(latency: Duration) -> usize {
    LATENCY_BUCKET_BOUNDS
        .iter()
        .position(|bound| latency <= *bound)
        .unwrap_or(LATENCY_BUCKET_BOUNDS.len())
}

#[cfg(test)]
#[path = "stats_tests.rs"]
mod tests;
