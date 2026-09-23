use ratatui::{
    Frame,
    layout::{Constraint, Rect},
    prelude::Style,
    symbols::Marker,
    text::{Line, Span},
    widgets::{Axis, Cell, Chart, Clear, Dataset, GraphType, Paragraph, Row, Table, Wrap},
};

use crate::domain::{DashboardSnapshot, truncate};

use super::{
    LlmFocus, Palette, PaneFocus, UiState, chrome, details, format, geometry, metrics, style,
    traces::render_empty,
};

pub(crate) fn render_logs(
    frame: &mut Frame<'_>,
    area: Rect,
    snapshot: &DashboardSnapshot,
    state: &UiState,
    detail_lines: &[Line<'static>],
    palette: Palette,
) {
    let panels = geometry::log_sections(area, state.log_split_pct);
    let time_width = time_column_width(snapshot.logs.iter().map(|log| log.timestamp_unix_nano));
    let rows: Vec<Row<'_>> = snapshot
        .logs
        .iter()
        .enumerate()
        .skip(state.log_feed_scroll)
        .take(geometry::table_viewport_height(panels[0]))
        .map(|(idx, log)| {
            let level = style::severity_label(&log.severity);
            let correlated = if log.trace_id.is_empty() {
                style::muted(" ", palette)
            } else {
                style::colored("●", palette.accent)
            };
            Row::new(vec![
                Cell::from(style::muted(
                    format::local_time(log.timestamp_unix_nano),
                    palette,
                )),
                Cell::from(style::colored(
                    truncate(&log.service_name, 12),
                    palette.accent,
                )),
                Cell::from(style::colored(
                    level.clone(),
                    style::severity_color(&level, palette),
                )),
                Cell::from(one_line(&log.body)),
                Cell::from(correlated),
            ])
            .style(style::row(idx == state.selected_log, palette))
        })
        .collect();
    let table = Table::new(
        rows,
        [
            Constraint::Length(time_width),
            Constraint::Length(12),
            Constraint::Length(5),
            Constraint::Min(20),
            Constraint::Length(1),
        ],
    )
    .column_spacing(1)
    .header(
        Row::new(vec!["time", "service", "level", "message", ""]).style(style::header_row(palette)),
    )
    .block(style::panel_with_context(
        "Logs",
        &chrome::log_feed_context(state, snapshot.logs.len()),
        state.logs_focus == PaneFocus::Primary,
        palette,
    ));
    frame.render_widget(table, panels[0]);
    if snapshot.logs.is_empty() {
        render_empty(
            frame,
            panels[0],
            "No logs match",
            "Clear filters with v and c, or send OTLP logs.",
            palette,
        );
    }

    frame.render_widget(
        Paragraph::new(detail_lines.to_vec())
            .scroll((state.log_detail_scroll, 0))
            .wrap(Wrap { trim: false })
            .block(style::panel(
                "Log",
                state.logs_focus == PaneFocus::Detail,
                palette,
            )),
        panels[1],
    );
}

pub(crate) fn render_metrics(
    frame: &mut Frame<'_>,
    area: Rect,
    snapshot: &DashboardSnapshot,
    state: &UiState,
    detail_lines: &[Line<'static>],
    palette: Palette,
) {
    let panels = geometry::metric_sections(area, state.metric_split_pct);
    let series = metrics::metric_series(snapshot);
    let selected = metrics::clamp_selection(state.selected_metric, series.len());
    let spark_width = 10;
    let rows: Vec<Row<'_>> = series
        .iter()
        .enumerate()
        .skip(state.metric_feed_scroll)
        .take(geometry::table_viewport_height(panels[0]))
        .map(|(idx, series)| {
            Row::new(vec![
                Cell::from(truncate(series.metric_name, 48)),
                Cell::from(style::colored(
                    truncate(series.service_name, 12),
                    palette.accent,
                )),
                Cell::from(
                    Line::from(format::optional_number(series.latest().value)).right_aligned(),
                ),
                Cell::from(style::colored(
                    metrics::sparkline(&series.values(), spark_width),
                    palette.accent,
                )),
            ])
            .style(style::row(idx == selected, palette))
        })
        .collect();
    let table = Table::new(
        rows,
        [
            Constraint::Min(16),
            Constraint::Length(12),
            Constraint::Length(8),
            Constraint::Length(spark_width as u16),
        ],
    )
    .column_spacing(1)
    .header(
        Row::new(vec![
            Cell::from("metric"),
            Cell::from("service"),
            Cell::from(Line::from("latest").right_aligned()),
            Cell::from("trend"),
        ])
        .style(style::header_row(palette)),
    )
    .block(style::panel_with_context(
        "Metrics",
        &[format!("{} series", series.len())],
        state.metrics_focus == PaneFocus::Primary,
        palette,
    ));
    frame.render_widget(table, panels[0]);
    if series.is_empty() {
        render_empty(
            frame,
            panels[0],
            "No metrics yet",
            "Send OTLP metrics to see series and trends here.",
            palette,
        );
    }

    let right = geometry::metric_right_sections(panels[1]);
    render_metric_chart(frame, right[0], series.get(selected), palette);

    frame.render_widget(
        Paragraph::new(detail_lines.to_vec())
            .scroll((state.metric_detail_scroll, 0))
            .wrap(Wrap { trim: false })
            .block(style::panel(
                "Series",
                state.metrics_focus == PaneFocus::Detail,
                palette,
            )),
        right[1],
    );
}

fn render_metric_chart(
    frame: &mut Frame<'_>,
    area: Rect,
    series: Option<&metrics::MetricSeries<'_>>,
    palette: Palette,
) {
    let Some(series) = series else {
        frame.render_widget(style::panel("Trend", false, palette), area);
        return;
    };
    let block = style::panel_with_context(
        "Trend",
        &[series.instrument_kind.to_string()],
        false,
        palette,
    );
    let first = series.points[0].timestamp_unix_nano;
    let data: Vec<(f64, f64)> = series
        .points
        .iter()
        .filter_map(|point| {
            point
                .value
                .map(|value| ((point.timestamp_unix_nano - first) as f64 / 1e9, value))
        })
        .collect();
    if data.is_empty() {
        frame.render_widget(
            Paragraph::new(style::muted("No numeric points in this series.", palette)).block(block),
            area,
        );
        return;
    }
    let (min_y, max_y) = data
        .iter()
        .fold((f64::INFINITY, f64::NEG_INFINITY), |acc, point| {
            (acc.0.min(point.1), acc.1.max(point.1))
        });
    let pad = ((max_y - min_y) * 0.1).max(max_y.abs() * 0.05).max(1e-9);
    let (low, high) = (min_y - pad, max_y + pad);
    let span_seconds = data.last().map_or(0.0, |point| point.0).max(1.0);
    let label = |text: String| Span::styled(text, Style::default().fg(palette.muted));
    let chart = Chart::new(vec![
        Dataset::default()
            .marker(Marker::Braille)
            .graph_type(GraphType::Line)
            .style(Style::default().fg(palette.accent))
            .data(&data),
    ])
    .block(block)
    .x_axis(
        Axis::default()
            .style(Style::default().fg(palette.border))
            .bounds([0.0, span_seconds])
            .labels(vec![
                label(format::clock(first)),
                label(format::clock(series.latest().timestamp_unix_nano)),
            ]),
    )
    .y_axis(
        Axis::default()
            .style(Style::default().fg(palette.border))
            .bounds([low, high])
            .labels(vec![
                label(format::number(min_y)),
                label(format::number(max_y)),
            ]),
    );
    frame.render_widget(chart, area);
}

pub(crate) fn render_llm(
    frame: &mut Frame<'_>,
    area: Rect,
    snapshot: &DashboardSnapshot,
    state: &UiState,
    detail_lines: &[Line<'static>],
    palette: Palette,
) {
    let panels = geometry::llm_sections(area, state.llm_split_pct);
    let left = geometry::llm_left_sections(panels[0]);
    let right = geometry::llm_detail_sections(panels[1]);

    let rows: Vec<Row<'_>> = snapshot
        .llm
        .iter()
        .enumerate()
        .skip(state.llm_feed_scroll)
        .take(geometry::table_viewport_height(left[2]))
        .map(|(idx, item)| {
            let failed =
                item.status.eq_ignore_ascii_case("error") || item.status == "STATUS_CODE_ERROR";
            Row::new(vec![
                Cell::from(style::colored(
                    "●",
                    if failed {
                        palette.error
                    } else {
                        palette.success
                    },
                )),
                Cell::from(style::muted(
                    format::local_time(item.started_at_unix_nano),
                    palette,
                )),
                Cell::from(truncate(&llm_prompt_name(&item.span_name), 40)),
                Cell::from(style::colored(truncate(&item.model, 16), palette.accent)),
                Cell::from(style::muted(truncate(&item.operation, 8), palette)),
                Cell::from(
                    Line::from(
                        item.total_tokens
                            .map_or_else(|| "-".to_string(), format::count),
                    )
                    .right_aligned(),
                ),
                Cell::from(
                    Line::from(
                        item.latency_ms
                            .map_or_else(|| "-".to_string(), format::duration),
                    )
                    .right_aligned(),
                ),
            ])
            .style(style::row(idx == state.selected_llm, palette))
        })
        .collect();
    let table = Table::new(
        rows,
        [
            Constraint::Length(1),
            Constraint::Length(19),
            Constraint::Min(14),
            Constraint::Length(16),
            Constraint::Length(8),
            Constraint::Length(6),
            Constraint::Length(8),
        ],
    )
    .column_spacing(1)
    .header(
        Row::new(vec![
            Cell::from(""),
            Cell::from("time"),
            Cell::from("call"),
            Cell::from("model"),
            Cell::from("op"),
            Cell::from(Line::from("tokens").right_aligned()),
            Cell::from(Line::from("latency").right_aligned()),
        ])
        .style(style::header_row(palette)),
    )
    .block(style::panel_with_context(
        "Calls",
        &[
            format!("{} calls", snapshot.llm.len()),
            format!("sort {}", state.llm_sort_mode.label()),
        ],
        state.llm_focus == LlmFocus::Feed,
        palette,
    ));
    frame.render_widget(llm_model_panel(snapshot, palette), left[0]);
    frame.render_widget(llm_session_panel(snapshot, palette), left[1]);
    frame.render_widget(table, left[2]);

    frame.render_widget(Clear, right[0]);
    frame.render_widget(
        Paragraph::new(detail_lines.to_vec())
            .scroll((state.llm_detail_scroll, 0))
            .wrap(Wrap { trim: false })
            .block(style::panel(
                "Call",
                state.llm_focus == LlmFocus::Detail,
                palette,
            )),
        right[0],
    );

    frame.render_widget(Clear, right[1]);
    frame.render_widget(
        Paragraph::new(details::llm_timeline_panel_lines(snapshot, state, palette))
            .scroll((state.llm_timeline_scroll, 0))
            .wrap(Wrap { trim: false })
            .block(style::panel(
                "Timeline",
                state.llm_focus == LlmFocus::Timeline,
                palette,
            )),
        right[1],
    );

    if snapshot.llm.is_empty() {
        render_empty(
            frame,
            left[2],
            "No LLM calls yet",
            "Spans with GenAI, OpenInference, or OpenLLMetry attributes such as llm.model_name appear here.",
            palette,
        );
    }
}

fn llm_model_panel(snapshot: &DashboardSnapshot, palette: Palette) -> Table<'static> {
    let rows: Vec<Row<'static>> = snapshot
        .llm_model_comparisons
        .iter()
        .take(4)
        .map(|item| {
            Row::new(vec![
                Cell::from(style::colored(
                    truncate(&llm_model_label(&item.provider, &item.model), 32),
                    palette.accent,
                )),
                Cell::from(Line::from(item.call_count.to_string()).right_aligned()),
                Cell::from(
                    Line::from(if item.error_count > 0 {
                        style::colored(item.error_count.to_string(), palette.error)
                    } else {
                        style::muted("0", palette)
                    })
                    .right_aligned(),
                ),
                Cell::from(Line::from(format::count(item.total_tokens)).right_aligned()),
                Cell::from(
                    Line::from(
                        item.avg_latency_ms
                            .map_or_else(|| "-".to_string(), format::duration),
                    )
                    .right_aligned(),
                ),
                Cell::from(Line::from(format::cost(item.cost)).right_aligned()),
            ])
            .style(Style::default().fg(palette.foreground))
        })
        .collect();

    Table::new(
        rows,
        [
            Constraint::Min(20),
            Constraint::Length(5),
            Constraint::Length(4),
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
            Cell::from(Line::from("errs").right_aligned()),
            Cell::from(Line::from("tokens").right_aligned()),
            Cell::from(Line::from("avg").right_aligned()),
            Cell::from(Line::from("cost").right_aligned()),
        ])
        .style(style::header_row(palette)),
    )
    .block(style::panel("Models", false, palette))
}

fn llm_session_panel(snapshot: &DashboardSnapshot, palette: Palette) -> Table<'static> {
    let rows: Vec<Row<'static>> = snapshot
        .llm_sessions
        .iter()
        .take(4)
        .map(|session| {
            Row::new(vec![
                Cell::from(Line::from(vec![
                    style::muted(format!("{} ", session.correlation_kind), palette),
                    style::plain(truncate(&session.correlation_id, 24), palette),
                ])),
                Cell::from(Line::from(session.call_count.to_string()).right_aligned()),
                Cell::from(
                    Line::from(if session.error_count > 0 {
                        style::colored(session.error_count.to_string(), palette.error)
                    } else {
                        style::muted("0", palette)
                    })
                    .right_aligned(),
                ),
                Cell::from(Line::from(format::count(session.total_tokens)).right_aligned()),
                Cell::from(Line::from(format::duration(session.duration_ms)).right_aligned()),
            ])
            .style(Style::default().fg(palette.foreground))
        })
        .collect();

    Table::new(
        rows,
        [
            Constraint::Min(20),
            Constraint::Length(5),
            Constraint::Length(4),
            Constraint::Length(7),
            Constraint::Length(8),
        ],
    )
    .column_spacing(1)
    .header(
        Row::new(vec![
            Cell::from("session"),
            Cell::from(Line::from("calls").right_aligned()),
            Cell::from(Line::from("errs").right_aligned()),
            Cell::from(Line::from("tokens").right_aligned()),
            Cell::from(Line::from("span").right_aligned()),
        ])
        .style(style::header_row(palette)),
    )
    .block(style::panel("Sessions", false, palette))
}

/// Eight columns when every time is today, otherwise room for the date.
pub(crate) fn time_column_width(times: impl Iterator<Item = i64>) -> u16 {
    let mut width = 8;
    for time in times {
        width = width.max(format::local_time(time).chars().count() as u16);
    }
    width
}

/// The first line of a message with whitespace runs collapsed, for single-row feeds.
fn one_line(text: &str) -> String {
    text.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn llm_model_label(provider: &str, model: &str) -> String {
    let trimmed = model.trim();
    if trimmed.is_empty() {
        return provider.to_string();
    }
    if trimmed.contains('/') {
        return trimmed.to_string();
    }
    format!("{provider}/{trimmed}")
}

fn llm_prompt_name(span_name: &str) -> String {
    let trimmed = span_name.trim();
    if let Some(prompt) = trimmed.strip_prefix("Prompt: ").map(str::trim)
        && !prompt.is_empty()
    {
        return prompt.to_string();
    }
    trimmed.to_string()
}
