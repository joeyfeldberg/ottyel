use ratatui::{
    Frame,
    layout::{Alignment, Constraint, Rect},
    prelude::{Modifier, Style},
    text::Line,
    widgets::{Block, Borders, Cell, Clear, Paragraph, Row, Sparkline, Table, Wrap},
};

use crate::domain::{DashboardSnapshot, truncate};

use super::{LlmFocus, Palette, PaneFocus, UiState, chrome, details, geometry};

pub(crate) fn render_logs(
    frame: &mut Frame<'_>,
    area: Rect,
    snapshot: &DashboardSnapshot,
    state: &UiState,
    detail_lines: &[Line<'static>],
    palette: Palette,
) {
    let panels = geometry::log_sections(area, state.log_split_pct);

    let feed_border = if state.logs_focus == PaneFocus::Primary {
        palette.accent
    } else {
        palette.muted
    };
    let detail_border = if state.logs_focus == PaneFocus::Detail {
        palette.warning
    } else {
        palette.muted
    };

    let rows: Vec<Row<'_>> = snapshot
        .logs
        .iter()
        .enumerate()
        .skip(state.log_feed_scroll)
        .take(geometry::table_viewport_height(panels[0]))
        .map(|(idx, log)| {
            let style = if idx == state.selected_log {
                Style::default().fg(palette.background).bg(palette.accent)
            } else {
                Style::default().fg(palette.foreground)
            };
            Row::new(vec![
                Cell::from(truncate(&log.service_name, 14)),
                Cell::from(truncate(&log.severity, 8)),
                Cell::from(truncate(&log.body, 70)),
                Cell::from(truncate(&log.trace_id, 16)),
            ])
            .style(style)
        })
        .collect();
    let table = Table::new(
        rows,
        [
            Constraint::Length(14),
            Constraint::Length(8),
            Constraint::Min(40),
            Constraint::Length(16),
        ],
    )
    .header(
        Row::new(vec!["service", "lvl", "message", "trace"]).style(
            Style::default()
                .fg(palette.muted)
                .add_modifier(Modifier::BOLD),
        ),
    )
    .block(
        Block::default()
            .title(chrome::log_feed_title(state))
            .borders(Borders::ALL)
            .border_style(Style::default().fg(feed_border)),
    );
    frame.render_widget(table, panels[0]);

    frame.render_widget(
        Paragraph::new(detail_lines.to_vec())
            .scroll((state.log_detail_scroll, 0))
            .wrap(Wrap { trim: false })
            .block(
                Block::default()
                    .title(chrome::detail_title(
                        "Log Detail",
                        state.logs_focus == PaneFocus::Detail,
                    ))
                    .borders(Borders::ALL)
                    .border_style(Style::default().fg(detail_border)),
            ),
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

    let feed_border = if state.metrics_focus == PaneFocus::Primary {
        palette.accent
    } else {
        palette.muted
    };
    let detail_border = if state.metrics_focus == PaneFocus::Detail {
        palette.accent
    } else {
        palette.muted
    };

    let rows: Vec<Row<'_>> = snapshot
        .metrics
        .iter()
        .enumerate()
        .skip(state.metric_feed_scroll)
        .take(geometry::table_viewport_height(panels[0]))
        .map(|(idx, metric)| {
            let style = if idx == state.selected_metric {
                Style::default().fg(palette.background).bg(palette.accent)
            } else {
                Style::default().fg(palette.foreground)
            };
            Row::new(vec![
                Cell::from(truncate(&metric.service_name, 14)),
                Cell::from(truncate(&metric.metric_name, 28)),
                Cell::from(metric.instrument_kind.clone()),
                Cell::from(metric.summary.clone()),
            ])
            .style(style)
        })
        .collect();
    let table = Table::new(
        rows,
        [
            Constraint::Length(14),
            Constraint::Length(28),
            Constraint::Length(14),
            Constraint::Min(16),
        ],
    )
    .header(
        Row::new(vec!["service", "metric", "kind", "summary"]).style(
            Style::default()
                .fg(palette.muted)
                .add_modifier(Modifier::BOLD),
        ),
    )
    .block(
        Block::default()
            .title("Metrics Feed")
            .borders(Borders::ALL)
            .border_style(Style::default().fg(feed_border)),
    );
    frame.render_widget(table, panels[0]);

    let right = geometry::metric_right_sections(panels[1]);
    let series = details::selected_metric_series(snapshot, state.selected_metric);
    let chart_values = details::metric_chart_values(&series);
    let sparkline = Sparkline::default()
        .block(
            Block::default()
                .title("Metric Trend")
                .borders(Borders::ALL)
                .border_style(Style::default().fg(palette.warning)),
        )
        .style(Style::default().fg(palette.warning))
        .data(&chart_values);
    frame.render_widget(sparkline, right[0]);

    frame.render_widget(
        Paragraph::new(detail_lines.to_vec())
            .scroll((state.metric_detail_scroll, 0))
            .wrap(Wrap { trim: false })
            .block(
                Block::default()
                    .title(chrome::detail_title(
                        "Metric Detail",
                        state.metrics_focus == PaneFocus::Detail,
                    ))
                    .borders(Borders::ALL)
                    .border_style(Style::default().fg(detail_border)),
            ),
        right[1],
    );
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

    let feed_border = if state.llm_focus == LlmFocus::Feed {
        palette.warning
    } else {
        palette.muted
    };
    let detail_border = if state.llm_focus == LlmFocus::Detail {
        palette.accent
    } else {
        palette.muted
    };

    let rows: Vec<Row<'_>> = snapshot
        .llm
        .iter()
        .enumerate()
        .skip(state.llm_feed_scroll)
        .take(geometry::table_viewport_height(left[2]))
        .map(|(idx, item)| {
            let style = if idx == state.selected_llm {
                Style::default().fg(palette.background).bg(palette.warning)
            } else {
                Style::default().fg(palette.foreground)
            };
            Row::new(vec![
                Cell::from(super::traces::format_machine_local_time(
                    item.started_at_unix_nano,
                )),
                Cell::from(truncate(&item.service_name, 12)),
                Cell::from(truncate(&item.provider, 10)),
                Cell::from(truncate(&item.model, 16)),
                Cell::from(truncate(&item.operation, 18)),
                Cell::from(item.total_tokens.unwrap_or_default().to_string()),
                Cell::from(format!("{:.1}", item.latency_ms.unwrap_or_default())),
            ])
            .style(style)
        })
        .collect();
    let table = Table::new(
        rows,
        [
            Constraint::Length(19),
            Constraint::Length(12),
            Constraint::Length(10),
            Constraint::Length(16),
            Constraint::Length(18),
            Constraint::Length(10),
            Constraint::Length(8),
        ],
    )
    .header(
        Row::new(vec![
            "time",
            "service",
            "provider",
            "model",
            "operation",
            "tokens",
            "ms",
        ])
        .style(
            Style::default()
                .fg(palette.muted)
                .add_modifier(Modifier::BOLD),
        ),
    )
    .block(
        Block::default()
            .title("LLM Inspector")
            .borders(Borders::ALL)
            .border_style(Style::default().fg(feed_border)),
    );
    frame.render_widget(llm_model_panel(snapshot, palette), left[0]);
    frame.render_widget(llm_session_panel(snapshot, palette), left[1]);
    frame.render_widget(table, left[2]);

    frame.render_widget(Clear, right[0]);
    frame.render_widget(
        Paragraph::new(detail_lines.to_vec())
            .scroll((state.llm_detail_scroll, 0))
            .wrap(Wrap { trim: false })
            .block(
                Block::default()
                    .title(chrome::detail_title(
                        "Model Detail",
                        state.llm_focus == LlmFocus::Detail,
                    ))
                    .borders(Borders::ALL)
                    .border_style(Style::default().fg(detail_border)),
            ),
        right[0],
    );

    frame.render_widget(Clear, right[1]);
    frame.render_widget(
        Paragraph::new(details::llm_timeline_panel_lines(snapshot, state, palette))
            .scroll((state.llm_timeline_scroll, 0))
            .wrap(Wrap { trim: false })
            .block(
                Block::default()
                    .title(chrome::detail_title(
                        "Timeline",
                        state.llm_focus == LlmFocus::Timeline,
                    ))
                    .borders(Borders::ALL)
                    .border_style(Style::default().fg(palette.warning)),
            ),
        right[1],
    );

    if snapshot.llm.is_empty() {
        let popup = geometry::centered_rect(54, 16, area);
        frame.render_widget(Clear, popup);
        frame.render_widget(
            Paragraph::new("No normalized LLM spans yet.\nSend OTLP spans with OpenInference/OpenLLMetry keys like `llm.provider`, `llm.model_name`, or `input.value`.")
                .style(Style::default().fg(palette.foreground))
                .alignment(Alignment::Center)
                .wrap(Wrap { trim: true })
                .block(
                    Block::default()
                        .title("LLM detection")
                        .borders(Borders::ALL)
                        .border_style(Style::default().fg(palette.warning)),
                ),
            popup,
        );
    }
}

fn llm_model_panel(snapshot: &DashboardSnapshot, palette: Palette) -> Table<'static> {
    let mut rows: Vec<Row<'static>> = snapshot
        .llm_model_comparisons
        .iter()
        .take(4)
        .map(|item| {
            let model_label = llm_model_label(&item.provider, &item.model);
            Row::new(vec![
                Cell::from(truncate(&model_label, 28)),
                Cell::from(format!("{:>5}", item.call_count)),
                Cell::from(format!("{:>3}", item.error_count)),
                Cell::from(format!("{:>7}", compact_u64(item.total_tokens))),
                Cell::from(format!("{:>6}", optional_ms(item.avg_latency_ms))),
                Cell::from(format!("{:>8}", optional_cost(item.cost))),
            ])
            .style(Style::default().fg(palette.foreground))
        })
        .collect();

    if rows.is_empty() {
        rows.push(
            Row::new(vec![
                Cell::from("No model comparison data yet."),
                Cell::from(""),
                Cell::from(""),
                Cell::from(""),
                Cell::from(""),
                Cell::from(""),
            ])
            .style(Style::default().fg(palette.foreground)),
        );
    }

    Table::new(
        rows,
        [
            Constraint::Min(24),
            Constraint::Length(5),
            Constraint::Length(3),
            Constraint::Length(7),
            Constraint::Length(6),
            Constraint::Length(8),
        ],
    )
    .header(
        Row::new(vec!["model", "calls", "err", "tokens", "avg", "cost"]).style(
            Style::default()
                .fg(palette.muted)
                .add_modifier(Modifier::BOLD),
        ),
    )
    .block(
        Block::default()
            .title("LLM Models")
            .borders(Borders::ALL)
            .border_style(Style::default().fg(palette.accent)),
    )
}

fn llm_session_panel(snapshot: &DashboardSnapshot, palette: Palette) -> Table<'static> {
    let mut rows: Vec<Row<'static>> = snapshot
        .llm_sessions
        .iter()
        .take(4)
        .map(|session| {
            let id_label = format!(
                "{}:{}",
                session.correlation_kind,
                truncate(&session.correlation_id, 16)
            );
            Row::new(vec![
                Cell::from(id_label),
                Cell::from(format!("{:>5}", session.call_count)),
                Cell::from(format!("{:>3}", session.error_count)),
                Cell::from(format!("{:>7}", compact_u64(session.total_tokens))),
                Cell::from(format!("{:>6}", optional_ms(Some(session.duration_ms)))),
            ])
            .style(Style::default().fg(palette.foreground))
        })
        .collect();

    if rows.is_empty() {
        rows.push(
            Row::new(vec![
                Cell::from("No session/conversation ids found."),
                Cell::from(""),
                Cell::from(""),
                Cell::from(""),
                Cell::from(""),
            ])
            .style(Style::default().fg(palette.foreground)),
        );
    }

    Table::new(
        rows,
        [
            Constraint::Min(24),
            Constraint::Length(5),
            Constraint::Length(3),
            Constraint::Length(7),
            Constraint::Length(6),
        ],
    )
    .header(
        Row::new(vec!["id", "calls", "err", "tokens", "span"]).style(
            Style::default()
                .fg(palette.muted)
                .add_modifier(Modifier::BOLD),
        ),
    )
    .block(
        Block::default()
            .title("LLM Sessions")
            .borders(Borders::ALL)
            .border_style(Style::default().fg(palette.warning)),
    )
}

fn compact_u64(value: u64) -> String {
    if value >= 1_000_000 {
        format!("{:.1}m", value as f64 / 1_000_000.0)
    } else if value >= 1_000 {
        format!("{:.1}k", value as f64 / 1_000.0)
    } else {
        value.to_string()
    }
}

fn optional_ms(value: Option<f64>) -> String {
    value
        .map(|value| format!("{value:.0}ms"))
        .unwrap_or_else(|| "-".to_string())
}

fn optional_cost(value: Option<f64>) -> String {
    value
        .map(|value| format!("${value:.4}"))
        .unwrap_or_else(|| "-".to_string())
}

fn llm_model_label(provider: &str, model: &str) -> String {
    let trimmed = model.trim();
    if trimmed.is_empty() {
        return provider.to_string();
    }

    let provider_prefix = format!("{provider}/");
    if trimmed.starts_with(&provider_prefix) {
        return trimmed.to_string();
    }

    if trimmed.contains('/') {
        return trimmed.to_string();
    }

    format!("{provider}/{trimmed}")
}
