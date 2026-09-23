//! Metric points grouped into series, the unit the Metrics tab lists and selects.

use std::collections::HashMap;

use crate::domain::{DashboardSnapshot, MetricSummary};

/// One metric series: every loaded point sharing service, name, and instrument kind.
pub(crate) struct MetricSeries<'a> {
    pub(crate) service_name: &'a str,
    pub(crate) metric_name: &'a str,
    pub(crate) instrument_kind: &'a str,
    /// Oldest first.
    pub(crate) points: Vec<&'a MetricSummary>,
}

impl MetricSeries<'_> {
    pub(crate) fn latest(&self) -> &MetricSummary {
        self.points
            .last()
            .expect("a series always has at least one point")
    }

    pub(crate) fn values(&self) -> Vec<f64> {
        self.points.iter().filter_map(|point| point.value).collect()
    }
}

/// Series ordered by their most recent point, newest first.
pub(crate) fn metric_series(snapshot: &DashboardSnapshot) -> Vec<MetricSeries<'_>> {
    let mut index: HashMap<(&str, &str, &str), usize> = HashMap::new();
    let mut series: Vec<MetricSeries<'_>> = Vec::new();
    for point in &snapshot.metrics {
        let key = (
            point.service_name.as_str(),
            point.metric_name.as_str(),
            point.instrument_kind.as_str(),
        );
        let position = *index.entry(key).or_insert_with(|| {
            series.push(MetricSeries {
                service_name: key.0,
                metric_name: key.1,
                instrument_kind: key.2,
                points: Vec::new(),
            });
            series.len() - 1
        });
        series[position].points.push(point);
    }
    for entry in &mut series {
        entry.points.sort_by_key(|point| point.timestamp_unix_nano);
    }
    series.sort_by(|left, right| {
        right
            .latest()
            .timestamp_unix_nano
            .cmp(&left.latest().timestamp_unix_nano)
            .then_with(|| left.metric_name.cmp(right.metric_name))
            .then_with(|| left.service_name.cmp(right.service_name))
    });
    series
}

/// The selected series index, clamped so a stale selection still shows a series.
pub(crate) fn clamp_selection(selected: usize, series: usize) -> usize {
    selected.min(series.saturating_sub(1))
}

pub fn metric_series_count(snapshot: &DashboardSnapshot) -> usize {
    metric_series(snapshot).len()
}

/// A text sparkline over `values`, scaled to their own range.
pub(crate) fn sparkline(values: &[f64], width: usize) -> String {
    const LEVELS: [char; 8] = ['▁', '▂', '▃', '▄', '▅', '▆', '▇', '█'];
    if values.is_empty() || width == 0 {
        return String::new();
    }
    let recent = &values[values.len().saturating_sub(width)..];
    let min = recent.iter().copied().fold(f64::INFINITY, f64::min);
    let max = recent.iter().copied().fold(f64::NEG_INFINITY, f64::max);
    let spread = max - min;
    recent
        .iter()
        .map(|value| {
            if spread <= f64::EPSILON {
                LEVELS[3]
            } else {
                LEVELS[(((value - min) / spread) * 7.0).round() as usize]
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use crate::domain::{DashboardSnapshot, MetricSummary, OverviewStats};

    fn point(metric: &str, service: &str, at: i64, value: f64) -> MetricSummary {
        MetricSummary {
            service_name: service.to_string(),
            metric_name: metric.to_string(),
            instrument_kind: "gauge".to_string(),
            timestamp_unix_nano: at,
            value: Some(value),
            summary: String::new(),
        }
    }

    #[test]
    fn points_group_into_series_ordered_by_latest_update() {
        let snapshot = DashboardSnapshot {
            services: Vec::new(),
            overview: OverviewStats {
                service_count: 0,
                trace_count: 0,
                error_span_count: 0,
                log_count: 0,
                metric_count: 0,
                llm_count: 0,
            },
            traces: Vec::new(),
            selected_trace: Vec::new(),
            logs: Vec::new(),
            metrics: vec![
                point("queue", "api", 30, 3.0),
                point("cpu", "api", 20, 0.5),
                point("queue", "api", 10, 1.0),
                point("queue", "worker", 5, 9.0),
            ],
            llm: Vec::new(),
            llm_rollups: Vec::new(),
            llm_sessions: Vec::new(),
            llm_model_comparisons: Vec::new(),
            llm_top_calls: Vec::new(),
            selected_llm_timeline: Vec::new(),
        };

        let series = super::metric_series(&snapshot);

        assert_eq!(
            series
                .iter()
                .map(|series| (series.service_name, series.metric_name, series.values()))
                .collect::<Vec<_>>(),
            vec![
                ("api", "queue", vec![1.0, 3.0]),
                ("api", "cpu", vec![0.5]),
                ("worker", "queue", vec![9.0]),
            ]
        );
    }

    #[test]
    fn sparklines_scale_to_their_own_range() {
        assert_eq!(super::sparkline(&[1.0, 2.0, 3.0], 8), "▁▅█");
        assert_eq!(super::sparkline(&[5.0, 5.0], 8), "▄▄");
        assert_eq!(super::sparkline(&[1.0, 2.0, 3.0, 4.0], 2), "▁█");
    }
}
