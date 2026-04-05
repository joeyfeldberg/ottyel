use ratatui::{
    Frame,
    layout::{Alignment, Constraint, Rect},
    prelude::{Modifier, Style},
    widgets::{Block, Borders, Cell, Clear, Paragraph, Row, Sparkline, Table, Wrap},
};

use crate::domain::{DashboardSnapshot, truncate};

use super::{Palette, PaneFocus, UiState, chrome, details, geometry};

pub(crate) fn render_logs(
    frame: &mut Frame<'_>,
    area: Rect,
    snapshot: &DashboardSnapshot,
    state: &UiState,
    palette: Palette,
) {
    let panels = geometry::log_sections(area);

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

    let detail = details::log_detail_lines(snapshot, state, palette);
    frame.render_widget(
        Paragraph::new(detail)
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
    palette: Palette,
) {
    let panels = geometry::metric_sections(area);

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

    let detail = details::metric_detail_lines(snapshot, state, palette);
    frame.render_widget(
        Paragraph::new(detail)
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
    palette: Palette,
) {
    let panels = geometry::llm_sections(area);

    let feed_border = if state.llm_focus == PaneFocus::Primary {
        palette.warning
    } else {
        palette.muted
    };
    let detail_border = if state.llm_focus == PaneFocus::Detail {
        palette.accent
    } else {
        palette.muted
    };

    let rows: Vec<Row<'_>> = snapshot
        .llm
        .iter()
        .enumerate()
        .map(|(idx, item)| {
            let style = if idx == state.selected_llm {
                Style::default().fg(palette.background).bg(palette.warning)
            } else {
                Style::default().fg(palette.foreground)
            };
            Row::new(vec![
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
    frame.render_widget(table, panels[0]);

    let detail = details::llm_detail_lines(snapshot, state, palette);
    frame.render_widget(
        Paragraph::new(detail)
            .scroll((state.llm_detail_scroll, 0))
            .wrap(Wrap { trim: false })
            .block(
                Block::default()
                    .title(chrome::detail_title(
                        "Model Detail",
                        state.llm_focus == PaneFocus::Detail,
                    ))
                    .borders(Borders::ALL)
                    .border_style(Style::default().fg(detail_border)),
            ),
        panels[1],
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
