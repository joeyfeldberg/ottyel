//! Schema v2 adds the time indexes that scheduled, bounded retention seeks on.

use anyhow::Result;
use rusqlite::Connection;

use super::v1::{self, IndexDefinition, ascending, index};

// Shipped schema definitions are immutable; future changes require a new versioned migration.
pub(super) const DDL: &str = r#"
CREATE INDEX idx_logs_time ON logs(timestamp_unix_nano);
CREATE INDEX idx_metrics_time ON metrics(timestamp_unix_nano);
CREATE INDEX idx_spans_end ON spans(end_time_unix_nano);
"#;

pub(super) const INDEXES: &[IndexDefinition] = &[
    index("idx_logs_time", "logs", &[ascending("timestamp_unix_nano")]),
    index(
        "idx_metrics_time",
        "metrics",
        &[ascending("timestamp_unix_nano")],
    ),
    index("idx_spans_end", "spans", &[ascending("end_time_unix_nano")]),
];

pub(super) fn validate_strict(conn: &Connection) -> Result<()> {
    v1::validate_strict_with(conn, INDEXES)
}
