//! Time and span-cap retention, run inside the writer's shared ingest transaction.

use anyhow::Result;
use rusqlite::Connection;

use super::{RetentionPolicy, helpers::now_unix_nanos};

pub(super) fn enforce(conn: &Connection, retention: RetentionPolicy) -> Result<()> {
    let retention_nanos = i64::try_from(retention.hours)
        .unwrap_or(i64::MAX)
        .saturating_mul(60 * 60 * 1_000_000_000);
    let threshold_nanos = now_unix_nanos().saturating_sub(retention_nanos);

    conn.execute(
        "DELETE FROM logs WHERE timestamp_unix_nano < ?1",
        [threshold_nanos],
    )?;
    conn.execute(
        "DELETE FROM metrics WHERE timestamp_unix_nano < ?1",
        [threshold_nanos],
    )?;
    let has_expired_spans: bool = conn.query_row(
        r#"
        SELECT EXISTS (
            SELECT 1
            FROM spans
            WHERE end_time_unix_nano < ?1
            LIMIT 1
        )
        "#,
        [threshold_nanos],
        |row| row.get(0),
    )?;
    if has_expired_spans {
        conn.execute(
            r#"
            DELETE FROM spans
            WHERE trace_id IN (
                SELECT trace_id
                FROM spans
                GROUP BY trace_id
                HAVING MAX(end_time_unix_nano) < ?1
            )
            "#,
            [threshold_nanos],
        )?;
    }

    let span_count: i64 = conn.query_row("SELECT COUNT(*) FROM spans", [], |row| row.get(0))?;
    let max_spans = i64::try_from(retention.maximum_spans).unwrap_or(i64::MAX);
    if span_count > max_spans {
        let to_trim = span_count - max_spans;
        conn.execute(
            r#"
            WITH trace_sizes AS (
                SELECT
                    trace_id,
                    COUNT(*) AS span_count,
                    MAX(end_time_unix_nano) AS latest_end_time_unix_nano
                FROM spans
                GROUP BY trace_id
            ),
            ranked_traces AS (
                SELECT
                    trace_id,
                    SUM(span_count) OVER (
                        ORDER BY latest_end_time_unix_nano ASC, trace_id ASC
                        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                    ) - span_count AS spans_before
                FROM trace_sizes
            )
            DELETE FROM spans
            WHERE trace_id IN (
                SELECT trace_id
                FROM ranked_traces
                WHERE spans_before < ?1
            )
            "#,
            [to_trim],
        )?;
    }

    conn.execute(
        r#"
        DELETE FROM span_events
        WHERE NOT EXISTS (
            SELECT 1
            FROM spans
            WHERE spans.trace_id = span_events.trace_id
              AND spans.span_id = span_events.span_id
        )
        "#,
        [],
    )?;
    conn.execute(
        r#"
        DELETE FROM span_links
        WHERE NOT EXISTS (
            SELECT 1
            FROM spans
            WHERE spans.trace_id = span_links.trace_id
              AND spans.span_id = span_links.span_id
        )
        "#,
        [],
    )?;
    conn.execute(
        r#"
        DELETE FROM llm_spans
        WHERE NOT EXISTS (
            SELECT 1
            FROM spans
            WHERE spans.trace_id = llm_spans.trace_id
              AND spans.span_id = llm_spans.span_id
        )
        "#,
        [],
    )?;

    Ok(())
}
