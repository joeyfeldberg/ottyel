mod chrome;
mod details;
mod geometry;
mod overview;
mod panes;
mod state;
mod traces;

pub use state::{Palette, PaneFocus, Tab, TraceFocus, TraceViewMode, UiState};
pub use traces::{
    first_llm_trace_index, next_error_trace_index, parent_trace_index, previous_error_trace_index,
    root_trace_index, selected_trace_tree_span, visible_trace_tree_len,
};

use ratatui::{
    Frame,
    layout::{Constraint, Direction, Layout, Rect},
    prelude::{Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Paragraph, Tabs},
};

use crate::{config::Theme, domain::DashboardSnapshot};

pub fn render(frame: &mut Frame<'_>, snapshot: &DashboardSnapshot, state: &UiState, theme: Theme) {
    let palette = Palette::from_theme(theme);
    let root = frame.area();
    frame.render_widget(
        Block::default().style(Style::default().bg(palette.background)),
        root,
    );

    let layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),
            Constraint::Length(1),
            Constraint::Min(10),
            Constraint::Length(1),
        ])
        .split(root);

    let titles: Vec<Line<'_>> = Tab::ALL
        .iter()
        .map(|tab| {
            Line::from(Span::styled(
                tab.title(),
                Style::default().fg(palette.foreground),
            ))
        })
        .collect();
    let tabs = Tabs::new(titles)
        .select(state.active_tab)
        .divider(" ")
        .highlight_style(
            Style::default()
                .fg(palette.background)
                .bg(palette.accent)
                .add_modifier(Modifier::BOLD),
        )
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title("Ottyel")
                .border_style(Style::default().fg(palette.accent)),
        );
    frame.render_widget(tabs, layout[0]);

    frame.render_widget(
        Paragraph::new(chrome::padded(chrome::global_status_text(snapshot, state)))
            .style(Style::default().fg(palette.muted)),
        layout[1],
    );

    match Tab::ALL[state.active_tab] {
        Tab::Overview => overview::render(frame, layout[2], snapshot, palette),
        Tab::Traces => traces::render(frame, layout[2], snapshot, state, palette),
        Tab::Logs => panes::render_logs(frame, layout[2], snapshot, state, palette),
        Tab::Metrics => panes::render_metrics(frame, layout[2], snapshot, state, palette),
        Tab::Llm => panes::render_llm(frame, layout[2], snapshot, state, palette),
    }

    frame.render_widget(
        Paragraph::new(chrome::padded(chrome::footer_text(state)))
            .style(Style::default().fg(palette.muted)),
        layout[3],
    );

    if state.show_help {
        chrome::render_help_overlay(frame, root, state, palette);
    }
}

pub fn sync_trace_tree_scroll(root: Rect, snapshot: &DashboardSnapshot, state: &mut UiState) {
    if Tab::ALL[state.active_tab] != Tab::Traces || state.trace_view_mode != TraceViewMode::Detail {
        return;
    }

    let viewport_height =
        geometry::trace_tree_viewport_height(geometry::trace_tree_area(geometry::body_area(root)));
    let tree_rows = traces::trace_tree_rows(&snapshot.selected_trace, &state.collapsed_trace_spans);
    let selected_line = traces::trace_tree_selected_line(state, &tree_rows);
    let total_lines = traces::trace_tree_total_lines(&tree_rows, snapshot.traces.is_empty());
    state.trace_tree_scroll = geometry::trace_tree_scroll_offset(
        state.trace_tree_scroll,
        total_lines,
        selected_line,
        viewport_height,
    );
}

pub fn sync_detail_scroll(root: Rect, snapshot: &DashboardSnapshot, state: &mut UiState) {
    if Tab::ALL[state.active_tab] == Tab::Traces && state.trace_view_mode == TraceViewMode::Detail {
        state.trace_detail_scroll = geometry::clamp_scroll(
            state.trace_detail_scroll,
            details::trace_detail_lines(snapshot, state, Palette::from_theme(Theme::Ember)).len(),
            geometry::detail_viewport_height(geometry::trace_detail_area(geometry::body_area(
                root,
            ))),
        );
    } else {
        state.trace_detail_scroll = 0;
    }

    state.log_detail_scroll = geometry::clamp_scroll(
        state.log_detail_scroll,
        details::log_detail_lines(snapshot, state, Palette::from_theme(Theme::Ember)).len(),
        geometry::detail_viewport_height(geometry::log_detail_area(geometry::body_area(root))),
    );
    state.metric_detail_scroll = geometry::clamp_scroll(
        state.metric_detail_scroll,
        details::metric_detail_lines(snapshot, state, Palette::from_theme(Theme::Ember)).len(),
        geometry::detail_viewport_height(geometry::metric_detail_area(geometry::body_area(root))),
    );
    state.llm_detail_scroll = geometry::clamp_scroll(
        state.llm_detail_scroll,
        details::llm_detail_lines(snapshot, state).len(),
        geometry::detail_viewport_height(geometry::llm_detail_area(geometry::body_area(root))),
    );
}

#[cfg(test)]
mod tests;
