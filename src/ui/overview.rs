use ratatui::{
    Frame,
    layout::{Alignment, Constraint, Direction, Layout, Rect},
    prelude::{Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, List, ListItem, Paragraph},
};

use crate::domain::{DashboardSnapshot, truncate};

use super::Palette;

pub(crate) fn render(
    frame: &mut Frame<'_>,
    area: Rect,
    snapshot: &DashboardSnapshot,
    palette: Palette,
) {
    let rows = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(7), Constraint::Min(10)])
        .split(area);

    let cards = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage(16),
            Constraint::Percentage(16),
            Constraint::Percentage(16),
            Constraint::Percentage(16),
            Constraint::Percentage(16),
            Constraint::Percentage(20),
        ])
        .split(rows[0]);

    let stats = [
        ("Services", snapshot.overview.service_count.to_string()),
        ("Traces", snapshot.overview.trace_count.to_string()),
        ("Errors", snapshot.overview.error_span_count.to_string()),
        ("Logs", snapshot.overview.log_count.to_string()),
        ("Metrics", snapshot.overview.metric_count.to_string()),
        ("LLM", snapshot.overview.llm_count.to_string()),
    ];

    for (idx, (label, value)) in stats.iter().enumerate() {
        frame.render_widget(
            Paragraph::new(vec![
                Line::from(Span::styled(*label, Style::default().fg(palette.muted))),
                Line::from(Span::styled(
                    value.clone(),
                    Style::default()
                        .fg(palette.foreground)
                        .add_modifier(Modifier::BOLD),
                )),
            ])
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .border_style(Style::default().fg(palette.accent)),
            )
            .alignment(Alignment::Center),
            cards[idx],
        );
    }

    let lower = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
        .split(rows[1]);

    let trace_items: Vec<ListItem<'_>> = snapshot
        .traces
        .iter()
        .take(10)
        .map(|trace| {
            ListItem::new(Line::from(vec![
                Span::styled(
                    truncate(&trace.service_name, 14),
                    Style::default().fg(palette.accent),
                ),
                Span::raw(" "),
                Span::styled(
                    truncate(&trace.root_name, 28),
                    Style::default().fg(palette.foreground),
                ),
                Span::raw(format!(" {:.1}ms", trace.duration_ms)),
            ]))
        })
        .collect();
    frame.render_widget(
        List::new(trace_items).block(
            Block::default()
                .title("Recent traces")
                .borders(Borders::ALL)
                .border_style(Style::default().fg(palette.accent)),
        ),
        lower[0],
    );

    let llm_items: Vec<ListItem<'_>> = snapshot
        .llm
        .iter()
        .take(10)
        .map(|row| {
            ListItem::new(Line::from(vec![
                Span::styled(
                    truncate(&row.model, 18),
                    Style::default().fg(palette.warning),
                ),
                Span::raw(" "),
                Span::styled(
                    truncate(&row.operation, 20),
                    Style::default().fg(palette.foreground),
                ),
                Span::raw(format!(" tok={}", row.total_tokens.unwrap_or_default())),
            ]))
        })
        .collect();
    frame.render_widget(
        List::new(llm_items).block(
            Block::default()
                .title("LLM activity")
                .borders(Borders::ALL)
                .border_style(Style::default().fg(palette.warning)),
        ),
        lower[1],
    );
}
