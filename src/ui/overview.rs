//! The Overview tab: what is arriving, what is failing, and what is slow.

use std::collections::BTreeMap;

use ratatui::{
    Frame,
    layout::{Constraint, Direction, Layout, Rect},
    text::{Line, Span},
    widgets::{Cell, Paragraph, Row, Table},
};

use crate::domain::{DashboardSnapshot, truncate};

use super::{Palette, format, metrics, style, traces::render_empty};

pub(crate) fn render(
    frame: &mut Frame<'_>,
    area: Rect,
    snapshot: &DashboardSnapshot,
    palette: Palette,
) {
    let rows = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(4),
            Constraint::Min(8),
            Constraint::Min(8),
        ])
        .split(area);
    render_tiles(frame, rows[0], snapshot, palette);

    let middle = halves(rows[1]);
    render_services(frame, middle[0], snapshot, palette);
    render_attention(frame, middle[1], snapshot, palette);

    let bottom = halves(rows[2]);
    render_slowest(frame, bottom[0], snapshot, palette);
    render_llm_usage(frame, bottom[1], snapshot, palette);
}

fn halves(area: Rect) -> [Rect; 2] {
    let split = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(55), Constraint::Percentage(45)])
        .split(area);
    [split[0], split[1]]
}

fn render_tiles(frame: &mut Frame<'_>, area: Rect, snapshot: &DashboardSnapshot, palette: Palette) {
    let overview = &snapshot.overview;
    let error_logs = snapshot
        .logs
        .iter()
        .filter(|log| style::severity_label(&log.severity) == "ERROR")
        .count();
    let failing_traces = snapshot
        .traces
        .iter()
        .filter(|trace| trace.error_count > 0)
        .count();
    let tokens: u64 = snapshot
        .llm_model_comparisons
        .iter()
        .map(|model| model.total_tokens)
        .sum();
    let tiles = [
        (
            "Traces",
            format::count(overview.trace_count as u64),
            failure_note(failing_traces, snapshot.traces.len(), "failing", palette),
        ),
        (
            "Logs",
            format::count(overview.log_count as u64),
            if error_logs > 0 {
                style::colored(plural(error_logs, "error"), palette.error)
            } else {
                style::muted("no errors", palette)
            },
        ),
        (
            "Metrics",
            format::count(metrics::metric_series_count(snapshot) as u64),
            style::muted(
                format!(
                    "series · {} points",
                    format::count(overview.metric_count as u64)
                ),
                palette,
            ),
        ),
        (
            "LLM calls",
            format::count(overview.llm_count as u64),
            style::muted(format!("{} tokens", format::count(tokens)), palette),
        ),
        (
            "Services",
            overview.service_count.to_string(),
            style::muted(plural(overview.error_span_count, "error span"), palette),
        ),
    ];
    let columns = Layout::default()
        .direction(Direction::Horizontal)
        .constraints(
            tiles
                .iter()
                .map(|_| Constraint::Ratio(1, tiles.len() as u32)),
        )
        .split(area);
    for ((label, value, note), column) in tiles.into_iter().zip(columns.iter()) {
        frame.render_widget(
            Paragraph::new(vec![
                Line::from(vec![
                    style::strong(value, palette),
                    style::muted(format!("  {label}"), palette),
                ]),
                Line::from(note),
            ])
            .block(style::panel("", false, palette)),
            *column,
        );
    }
}

fn plural(count: usize, noun: &str) -> String {
    if count == 1 {
        format!("1 {noun}")
    } else {
        format!("{} {noun}s", format::count(count as u64))
    }
}

fn failure_note(failing: usize, total: usize, noun: &str, palette: Palette) -> Span<'static> {
    if failing == 0 {
        return style::muted(format!("none {noun}"), palette);
    }
    let share = failing as f64 * 100.0 / total.max(1) as f64;
    style::colored(format!("{failing} {noun} · {share:.0}%"), palette.error)
}

struct ServiceRow {
    traces: usize,
    errors: i64,
    durations: Vec<f64>,
}

fn render_services(
    frame: &mut Frame<'_>,
    area: Rect,
    snapshot: &DashboardSnapshot,
    palette: Palette,
) {
    let mut services: BTreeMap<&str, ServiceRow> = BTreeMap::new();
    for trace in &snapshot.traces {
        let row = services
            .entry(trace.service_name.as_str())
            .or_insert(ServiceRow {
                traces: 0,
                errors: 0,
                durations: Vec::new(),
            });
        row.traces += 1;
        row.errors += trace.error_count;
        row.durations.push(trace.duration_ms);
    }
    let mut ordered: Vec<(&str, ServiceRow)> = services.into_iter().collect();
    ordered.sort_by(|left, right| {
        right
            .1
            .errors
            .cmp(&left.1.errors)
            .then(right.1.traces.cmp(&left.1.traces))
    });
    let rows: Vec<Row<'static>> = ordered
        .into_iter()
        .map(|(service, mut row)| {
            row.durations.sort_by(f64::total_cmp);
            let p50 = row.durations[row.durations.len() / 2];
            let max = row.durations.last().copied().unwrap_or_default();
            Row::new(vec![
                Cell::from(style::colored(truncate(service, 22), palette.accent)),
                Cell::from(Line::from(row.traces.to_string()).right_aligned()),
                Cell::from(
                    Line::from(if row.errors > 0 {
                        style::colored(row.errors.to_string(), palette.error)
                    } else {
                        style::muted("0", palette)
                    })
                    .right_aligned(),
                ),
                Cell::from(Line::from(format::duration(p50)).right_aligned()),
                Cell::from(Line::from(format::duration(max)).right_aligned()),
            ])
        })
        .collect();
    let empty = rows.is_empty();
    frame.render_widget(
        Table::new(
            rows,
            [
                Constraint::Min(14),
                Constraint::Length(6),
                Constraint::Length(6),
                Constraint::Length(8),
                Constraint::Length(8),
            ],
        )
        .column_spacing(2)
        .header(
            Row::new(vec![
                Cell::from("service"),
                Cell::from(Line::from("traces").right_aligned()),
                Cell::from(Line::from("errors").right_aligned()),
                Cell::from(Line::from("p50").right_aligned()),
                Cell::from(Line::from("max").right_aligned()),
            ])
            .style(style::header_row(palette)),
        )
        .block(style::panel_with_context(
            "Services",
            &["recent traces".to_string()],
            false,
            palette,
        )),
        area,
    );
    if empty {
        render_empty(
            frame,
            area,
            "Waiting for telemetry",
            "Send OTLP to http://127.0.0.1:4318 or grpc://127.0.0.1:4317.",
            palette,
        );
    }
}

fn render_attention(
    frame: &mut Frame<'_>,
    area: Rect,
    snapshot: &DashboardSnapshot,
    palette: Palette,
) {
    let mut items: Vec<(i64, Line<'static>)> = snapshot
        .traces
        .iter()
        .filter(|trace| trace.error_count > 0)
        .map(|trace| {
            (
                trace.started_at_unix_nano,
                Line::from(vec![
                    style::muted(
                        format!("{}  ", format::local_time(trace.started_at_unix_nano)),
                        palette,
                    ),
                    style::badge("TRACE", palette.error, palette),
                    Span::raw(" "),
                    style::colored(format!("{} ", trace.service_name), palette.accent),
                    style::plain(trace.root_name.clone(), palette),
                    style::muted(
                        format!(
                            "  {} error{}",
                            trace.error_count,
                            if trace.error_count == 1 { "" } else { "s" }
                        ),
                        palette,
                    ),
                ]),
            )
        })
        .chain(
            snapshot
                .logs
                .iter()
                .filter(|log| {
                    matches!(
                        style::severity_label(&log.severity).as_str(),
                        "ERROR" | "FATAL"
                    )
                })
                .map(|log| {
                    (
                        log.timestamp_unix_nano,
                        Line::from(vec![
                            style::muted(
                                format!("{}  ", format::local_time(log.timestamp_unix_nano)),
                                palette,
                            ),
                            style::badge("LOG", palette.warning, palette),
                            Span::raw(" "),
                            style::colored(format!("{} ", log.service_name), palette.accent),
                            style::plain(
                                log.body.split_whitespace().collect::<Vec<_>>().join(" "),
                                palette,
                            ),
                        ]),
                    )
                }),
        )
        .collect();
    items.sort_by_key(|item| std::cmp::Reverse(item.0));
    let empty = items.is_empty();
    frame.render_widget(
        Paragraph::new(items.into_iter().map(|(_, line)| line).collect::<Vec<_>>())
            .block(style::panel("Needs attention", false, palette)),
        area,
    );
    if empty {
        render_empty(
            frame,
            area,
            "All clear",
            "No failing traces or error logs loaded.",
            palette,
        );
    }
}

fn render_slowest(
    frame: &mut Frame<'_>,
    area: Rect,
    snapshot: &DashboardSnapshot,
    palette: Palette,
) {
    let mut slowest: Vec<_> = snapshot.traces.iter().collect();
    slowest.sort_by(|left, right| right.duration_ms.total_cmp(&left.duration_ms));
    let top = slowest.first().map_or(0.0, |trace| trace.duration_ms);
    let inner_width = area.width.saturating_sub(4) as usize;
    let bar_width = (inner_width / 3).max(6);
    let name_width = inner_width.saturating_sub(bar_width + 10);
    let lines: Vec<Line<'static>> = slowest
        .into_iter()
        .take(area.height.saturating_sub(2) as usize)
        .map(|trace| {
            let share = if top > 0.0 {
                trace.duration_ms / top
            } else {
                0.0
            };
            let name = truncate(
                &format!("{} {}", trace.service_name, trace.root_name),
                name_width,
            );
            Line::from(vec![
                style::plain(format!("{name:<name_width$} "), palette),
                style::colored(
                    format!("{:<bar_width$}", style::bar(share, bar_width)),
                    if trace.error_count > 0 {
                        palette.error
                    } else {
                        palette.accent
                    },
                ),
                style::muted(
                    format!(" {:>8}", format::duration(trace.duration_ms)),
                    palette,
                ),
            ])
        })
        .collect();
    let empty = lines.is_empty();
    frame.render_widget(
        Paragraph::new(lines).block(style::panel("Slowest traces", false, palette)),
        area,
    );
    if empty {
        render_empty(frame, area, "No traces yet", "", palette);
    }
}

fn render_llm_usage(
    frame: &mut Frame<'_>,
    area: Rect,
    snapshot: &DashboardSnapshot,
    palette: Palette,
) {
    let rows: Vec<Row<'static>> = snapshot
        .llm_model_comparisons
        .iter()
        .map(|model| {
            Row::new(vec![
                Cell::from(style::colored(truncate(&model.model, 24), palette.accent)),
                Cell::from(Line::from(model.call_count.to_string()).right_aligned()),
                Cell::from(Line::from(format::count(model.total_tokens)).right_aligned()),
                Cell::from(
                    Line::from(
                        model
                            .avg_latency_ms
                            .map_or_else(|| "-".to_string(), format::duration),
                    )
                    .right_aligned(),
                ),
                Cell::from(Line::from(format::cost(model.cost)).right_aligned()),
            ])
        })
        .collect();
    let empty = rows.is_empty();
    frame.render_widget(
        Table::new(
            rows,
            [
                Constraint::Min(12),
                Constraint::Length(5),
                Constraint::Length(7),
                Constraint::Length(8),
                Constraint::Length(8),
            ],
        )
        .column_spacing(1)
        .header(
            Row::new(vec![
                Cell::from("model"),
                Cell::from(Line::from("calls").right_aligned()),
                Cell::from(Line::from("tokens").right_aligned()),
                Cell::from(Line::from("avg").right_aligned()),
                Cell::from(Line::from("cost").right_aligned()),
            ])
            .style(style::header_row(palette)),
        )
        .block(style::panel("LLM usage", false, palette)),
        area,
    );
    if empty {
        render_empty(frame, area, "No LLM calls yet", "", palette);
    }
}
