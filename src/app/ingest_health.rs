//! Turns cumulative receiver samples into the header's recent-window health view.

use std::time::{Duration, SystemTime};

use crate::{
    ingest::stats::IngestHealth,
    ui::{IngestHealthView, RecentIngestFailure},
};

/// How long the header keeps calling out the most recent failed export.
const RECENT_FAILURE: Duration = Duration::from_secs(60);

#[derive(Default)]
pub(super) struct IngestHealthTracker {
    previous: Option<IngestHealth>,
}

impl IngestHealthTracker {
    pub(super) fn update(&mut self, sample: IngestHealth, now: SystemTime) -> IngestHealthView {
        let totals = sample.totals();
        let (window, elapsed) = match &self.previous {
            Some(previous) => (
                totals.since(&previous.totals()),
                sample.taken_at.saturating_duration_since(previous.taken_at),
            ),
            // The first sample has no window, so it reports no recent rate or latency.
            None => (Default::default(), Duration::ZERO),
        };
        let records_per_second = if elapsed.is_zero() {
            0.0
        } else {
            window.records_accepted as f64 / elapsed.as_secs_f64()
        };
        let last_failure = sample.last_failure.as_ref().and_then(|failure| {
            let age = now.duration_since(failure.at).unwrap_or_default();
            (age <= RECENT_FAILURE).then(|| RecentIngestFailure {
                label: format!(
                    "{} {}/{}",
                    failure.failure.label(),
                    failure.transport.label(),
                    failure.signal.label()
                ),
                age,
            })
        });
        let view = IngestHealthView {
            records_per_second,
            recent_ack_p95: window.latency_p95_bound(),
            rejected_records: totals.records_rejected,
            failed_requests: totals.failed_requests(),
            queued_records: sample.queued_records,
            last_failure,
        };
        self.previous = Some(sample);
        view
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant, SystemTime};

    use super::IngestHealthTracker;
    use crate::{
        ingest::stats::{Failure, FailureNote, IngestHealth, Signal, StreamCounters, Transport},
        ui::{IngestHealthView, RecentIngestFailure},
    };

    fn counters(records: u64, fast_requests: u64, failed: u64) -> StreamCounters {
        let mut counters = StreamCounters {
            requests_accepted: fast_requests,
            records_accepted: records,
            records_rejected: 2,
            failures: [0, 0, failed, 0, 0],
            ..StreamCounters::default()
        };
        counters.latency[1] = fast_requests;
        counters
    }

    #[test]
    fn rate_and_latency_describe_only_the_latest_window() {
        let start = Instant::now();
        let now = SystemTime::now();
        let failure = FailureNote {
            signal: Signal::Logs,
            transport: Transport::Grpc,
            failure: Failure::Unavailable,
            message: "ingest at capacity".to_string(),
            at: now - Duration::from_secs(5),
        };
        let mut tracker = IngestHealthTracker::default();

        let first = tracker.update(
            IngestHealth::for_test(start, counters(1_000, 10, 0), 0, None),
            now,
        );
        assert_eq!(first.records_per_second, 0.0);
        assert_eq!(first.recent_ack_p95, None);

        let second = tracker.update(
            IngestHealth::for_test(
                start + Duration::from_secs(2),
                counters(5_000, 14, 1),
                7,
                Some(failure),
            ),
            now,
        );
        assert_eq!(
            second,
            IngestHealthView {
                records_per_second: 2_000.0,
                recent_ack_p95: Some(Duration::from_millis(5)),
                rejected_records: 2,
                failed_requests: 1,
                queued_records: 7,
                last_failure: Some(RecentIngestFailure {
                    label: "unavailable grpc/logs".to_string(),
                    age: Duration::from_secs(5),
                }),
            }
        );
    }

    #[test]
    fn old_failures_are_no_longer_called_out() {
        let now = SystemTime::now();
        let failure = FailureNote {
            signal: Signal::Traces,
            transport: Transport::Http,
            failure: Failure::Invalid,
            message: "bad".to_string(),
            at: now - Duration::from_secs(61),
        };
        let view = IngestHealthTracker::default().update(
            IngestHealth::for_test(Instant::now(), counters(0, 0, 1), 0, Some(failure)),
            now,
        );
        assert_eq!(view.last_failure, None);
        assert_eq!(view.failed_requests, 1);
    }
}
