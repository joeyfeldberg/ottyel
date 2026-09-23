//! Human-readable values shared by every pane.

use chrono::{DateTime, FixedOffset, Utc};

/// Converts to the machine's local zone. Tests use UTC so checked-in UI snapshots render the
/// same on every machine and CI runner.
pub(crate) fn display_zone(utc: DateTime<Utc>) -> DateTime<FixedOffset> {
    #[cfg(not(test))]
    return utc.with_timezone(&chrono::Local).fixed_offset();
    #[cfg(test)]
    return utc.fixed_offset();
}

fn from_unix_nano(unix_nano: i64) -> Option<DateTime<Utc>> {
    DateTime::<Utc>::from_timestamp(
        unix_nano.div_euclid(1_000_000_000),
        unix_nano.rem_euclid(1_000_000_000) as u32,
    )
}

/// `HH:MM:SS` for today, otherwise `YYYY-MM-DD HH:MM:SS`, in the display zone.
pub(crate) fn local_time(unix_nano: i64) -> String {
    match from_unix_nano(unix_nano) {
        Some(utc) => {
            let local = display_zone(utc);
            if local.date_naive() == display_zone(Utc::now()).date_naive() {
                local.format("%H:%M:%S").to_string()
            } else {
                local.format("%Y-%m-%d %H:%M:%S").to_string()
            }
        }
        None => "invalid time".to_string(),
    }
}

/// `HH:MM:SS` in the display zone, for chart axes.
pub(crate) fn clock(unix_nano: i64) -> String {
    match from_unix_nano(unix_nano) {
        Some(utc) => display_zone(utc).format("%H:%M:%S").to_string(),
        None => "invalid time".to_string(),
    }
}

/// Full date and time with milliseconds, for detail panes.
pub(crate) fn precise_time(unix_nano: i64) -> String {
    match from_unix_nano(unix_nano) {
        Some(utc) => display_zone(utc)
            .format("%Y-%m-%d %H:%M:%S%.3f")
            .to_string(),
        None => "invalid time".to_string(),
    }
}

/// `850µs`, `12.4ms`, `1.29s`, `2.5m`.
pub(crate) fn duration(duration_ms: f64) -> String {
    if !duration_ms.is_finite() || duration_ms < 0.0 {
        return "-".to_string();
    }
    if duration_ms == 0.0 {
        return "0ms".to_string();
    }
    if duration_ms >= 60_000.0 {
        format!("{:.1}m", duration_ms / 60_000.0)
    } else if duration_ms >= 1_000.0 {
        format!("{:.2}s", duration_ms / 1_000.0)
    } else if duration_ms >= 1.0 {
        format!("{duration_ms:.1}ms")
    } else {
        format!("{:.0}µs", duration_ms * 1_000.0)
    }
}

/// `950`, `1.2k`, `3.4M`.
pub(crate) fn count(value: u64) -> String {
    if value >= 1_000_000 {
        format!("{:.1}M", value as f64 / 1_000_000.0)
    } else if value >= 1_000 {
        format!("{:.1}k", value as f64 / 1_000.0)
    } else {
        value.to_string()
    }
}

/// A measurement without trailing zeros: `29`, `12.5`, `0.004`, `1.2e9`.
pub(crate) fn number(value: f64) -> String {
    if !value.is_finite() {
        return value.to_string();
    }
    let magnitude = value.abs();
    if magnitude != 0.0 && !(1e-3..1e9).contains(&magnitude) {
        return format!("{value:.2e}");
    }
    let text = format!("{value:.3}");
    let text = text.trim_end_matches('0').trim_end_matches('.');
    if text == "-0" {
        "0".to_string()
    } else {
        text.to_string()
    }
}

pub(crate) fn optional_number(value: Option<f64>) -> String {
    value.map_or_else(|| "-".to_string(), number)
}

pub(crate) fn cost(value: Option<f64>) -> String {
    value.map_or_else(|| "-".to_string(), |value| format!("${value:.4}"))
}

#[cfg(test)]
mod tests {
    #[test]
    fn values_read_like_a_human_wrote_them() {
        assert_eq!(
            [0.4, 12.44, 1_294.6, 150_000.0].map(super::duration),
            ["400µs", "12.4ms", "1.29s", "2.5m"]
        );
        assert_eq!(
            [950, 1_234, 3_400_000].map(super::count),
            ["950", "1.2k", "3.4M"]
        );
        assert_eq!(
            [29.0, 12.5, 0.0042, -0.0, 2.5e10].map(super::number),
            ["29", "12.5", "0.004", "0", "2.50e10"]
        );
        assert_eq!(super::optional_number(None), "-");
        assert_eq!(
            super::precise_time(1_700_000_001_294_000_000),
            "2023-11-14 22:13:21.294"
        );
    }
}
