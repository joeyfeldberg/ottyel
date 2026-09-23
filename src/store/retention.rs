//! Scheduled, bounded retention that runs as writer maintenance.
//!
//! A pass starts on a timer or after enough committed records, then advances one bounded unit
//! at a time: expired logs, expired metric points, wholly expired traces, then whole traces
//! over the span cap. Each unit is its own transaction, so the writer interleaves queued
//! exports between units. Every step seeks on a time or trace index; none scans a table per
//! export.

use std::{
    collections::VecDeque,
    time::{Duration, Instant},
};

use anyhow::Result;
use rusqlite::{Connection, OptionalExtension, params};

use super::{RetentionPolicy, helpers::now_unix_nanos, writer::Maintenance};

/// How often a pass starts when ingest volume alone does not trigger one.
pub(super) const PASS_INTERVAL: Duration = Duration::from_secs(30);
/// Log or metric rows deleted, or span rows examined, by one unit.
pub(super) const ROWS_PER_UNIT: usize = 2_000;
/// Whole traces evicted for the span cap by one unit.
pub(super) const TRACES_PER_UNIT: usize = 20;
/// Committed records that start a pass early, as a fraction of the span cap, so the cap is not
/// overshot by more than about this share between timed passes.
const CAP_SHARE_TRIGGER: usize = 10;
const MIN_RECORD_TRIGGER: usize = 1_000;

pub(super) struct RetentionMaintenance {
    policy: RetentionPolicy,
    record_trigger: usize,
    next_pass: Instant,
    records_since_pass: usize,
    pass: Option<Pass>,
}

struct Pass {
    threshold_nanos: i64,
    phase: Phase,
}

enum Phase {
    Logs,
    Metrics,
    /// The span scan resumes after this `(end_time_unix_nano, rowid)`.
    ExpiredTraces {
        after: (i64, i64),
    },
    /// Traces still to evict for the span cap, computed once when the phase starts.
    SpanCap {
        evict: Option<VecDeque<String>>,
    },
}

impl RetentionMaintenance {
    pub(super) fn new(policy: RetentionPolicy, now: Instant) -> Self {
        Self {
            policy,
            record_trigger: (policy.maximum_spans / CAP_SHARE_TRIGGER).max(MIN_RECORD_TRIGGER),
            // The first pass waits one interval, so opening a store never races its readers.
            next_pass: now + PASS_INTERVAL,
            records_since_pass: 0,
            pass: None,
        }
    }

    fn start_pass(&mut self) {
        let retention_nanos = i64::try_from(self.policy.hours)
            .unwrap_or(i64::MAX)
            .saturating_mul(60 * 60 * 1_000_000_000);
        self.pass = Some(Pass {
            threshold_nanos: now_unix_nanos().saturating_sub(retention_nanos),
            phase: Phase::Logs,
        });
        self.records_since_pass = 0;
    }

    fn finish_pass(&mut self, now: Instant) {
        self.pass = None;
        self.next_pass = now + PASS_INTERVAL;
    }
}

impl Maintenance for RetentionMaintenance {
    fn ingested(&mut self, records: usize) {
        self.records_since_pass = self.records_since_pass.saturating_add(records);
    }

    fn due_in(&self, now: Instant) -> Duration {
        if self.pass.is_some() || self.records_since_pass >= self.record_trigger {
            return Duration::ZERO;
        }
        self.next_pass.saturating_duration_since(now)
    }

    #[cfg(any(test, feature = "benchmark-support"))]
    fn force_pass(&mut self, _now: Instant) {
        self.start_pass();
    }

    #[cfg(any(test, feature = "benchmark-support"))]
    fn in_pass(&self) -> bool {
        self.pass.is_some()
    }

    fn run_unit(&mut self, transaction: &Connection, now: Instant) -> Result<()> {
        if self.pass.is_none() {
            self.start_pass();
        }
        let maximum_spans = self.policy.maximum_spans;
        let pass = self.pass.as_mut().expect("a pass was just started");
        let threshold = pass.threshold_nanos;
        let step = match &mut pass.phase {
            Phase::Logs => advance_when(
                delete_expired_rows(transaction, "logs", threshold)? < ROWS_PER_UNIT,
                Phase::Metrics,
            ),
            Phase::Metrics => advance_when(
                delete_expired_rows(transaction, "metrics", threshold)? < ROWS_PER_UNIT,
                Phase::ExpiredTraces {
                    after: (i64::MIN, i64::MIN),
                },
            ),
            Phase::ExpiredTraces { after } => advance_when(
                !expire_traces(transaction, threshold, after)?,
                Phase::SpanCap { evict: None },
            ),
            Phase::SpanCap { evict } => {
                let queue = match evict {
                    Some(queue) => queue,
                    None => evict.insert(traces_over_cap(transaction, maximum_spans)?),
                };
                for trace_id in queue.drain(..queue.len().min(TRACES_PER_UNIT)) {
                    delete_trace(transaction, &trace_id)?;
                }
                if queue.is_empty() {
                    Step::Finish
                } else {
                    Step::Stay
                }
            }
        };
        match step {
            Step::Advance(next) => pass.phase = next,
            Step::Stay => {}
            Step::Finish => self.finish_pass(now),
        }
        Ok(())
    }

    fn failed(&mut self, now: Instant) {
        // Abandon the pass; the next one recomputes everything from current rows.
        self.finish_pass(now);
    }
}

enum Step {
    Advance(Phase),
    Stay,
    Finish,
}

fn advance_when(done: bool, next: Phase) -> Step {
    if done {
        Step::Advance(next)
    } else {
        Step::Stay
    }
}

/// Deletes up to one unit of rows older than `threshold` from a time-indexed table.
fn delete_expired_rows(transaction: &Connection, table: &str, threshold: i64) -> Result<usize> {
    let sql = format!(
        "DELETE FROM {table} WHERE id IN (
             SELECT id FROM {table}
             WHERE timestamp_unix_nano < ?1
             ORDER BY timestamp_unix_nano
             LIMIT ?2
         )"
    );
    Ok(transaction
        .prepare_cached(&sql)?
        .execute(params![threshold, ROWS_PER_UNIT as i64])?)
}

/// Examines up to one unit of spans that ended before `threshold`, resuming after `after`, and
/// deletes each trace none of whose spans is fresh. Returns whether more spans may remain.
fn expire_traces(transaction: &Connection, threshold: i64, after: &mut (i64, i64)) -> Result<bool> {
    let rows = transaction
        .prepare_cached(
            "SELECT end_time_unix_nano, rowid, trace_id FROM spans
             WHERE end_time_unix_nano < ?1 AND (end_time_unix_nano, rowid) > (?2, ?3)
             ORDER BY end_time_unix_nano, rowid
             LIMIT ?4",
        )?
        .query_map(
            params![threshold, after.0, after.1, ROWS_PER_UNIT as i64],
            |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?, row.get(2)?)),
        )?
        .collect::<rusqlite::Result<Vec<(i64, i64, String)>>>()?;
    let Some(&(end, rowid, _)) = rows.last() else {
        return Ok(false);
    };
    *after = (end, rowid);
    let more = rows.len() == ROWS_PER_UNIT;

    let mut candidates: Vec<String> = rows.into_iter().map(|(_, _, trace_id)| trace_id).collect();
    candidates.sort_unstable();
    candidates.dedup();
    for trace_id in candidates {
        let fresh: Option<i64> = transaction
            .prepare_cached(
                "SELECT 1 FROM spans WHERE trace_id = ?1 AND end_time_unix_nano >= ?2 LIMIT 1",
            )?
            .query_row(params![trace_id, threshold], |row| row.get(0))
            .optional()?;
        if fresh.is_none() {
            delete_trace(transaction, &trace_id)?;
        }
    }
    Ok(more)
}

/// Returns the oldest whole traces, by latest span end then trace ID, whose removal brings the
/// span count within `maximum_spans`. It ranks every trace, so it runs only when over the cap.
fn traces_over_cap(transaction: &Connection, maximum_spans: usize) -> Result<VecDeque<String>> {
    let span_count: i64 =
        transaction.query_row("SELECT COUNT(*) FROM spans", [], |row| row.get(0))?;
    let maximum = i64::try_from(maximum_spans).unwrap_or(i64::MAX);
    if span_count <= maximum {
        return Ok(VecDeque::new());
    }
    let mut statement = transaction.prepare(
        "WITH trace_sizes AS (
             SELECT trace_id, COUNT(*) AS span_count, MAX(end_time_unix_nano) AS latest_end
             FROM spans
             GROUP BY trace_id
         ),
         ranked AS (
             SELECT trace_id, latest_end,
                    SUM(span_count) OVER (
                        ORDER BY latest_end ASC, trace_id ASC
                        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                    ) - span_count AS spans_before
             FROM trace_sizes
         )
         SELECT trace_id FROM ranked
         WHERE spans_before < ?1
         ORDER BY latest_end ASC, trace_id ASC",
    )?;
    let traces = statement
        .query_map([span_count - maximum], |row| row.get(0))?
        .collect::<rusqlite::Result<VecDeque<String>>>()?;
    Ok(traces)
}

/// Deletes one trace and every projection keyed to it, using the per-trace indexes.
fn delete_trace(transaction: &Connection, trace_id: &str) -> Result<()> {
    for table in ["span_events", "span_links", "llm_spans", "spans"] {
        transaction
            .prepare_cached(&format!("DELETE FROM {table} WHERE trace_id = ?1"))?
            .execute([trace_id])?;
    }
    Ok(())
}

#[cfg(test)]
#[path = "retention_tests.rs"]
mod tests;
