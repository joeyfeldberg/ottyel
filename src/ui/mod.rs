mod chrome;
mod details;
pub(crate) mod geometry;
mod overview;
mod panes;
mod state;
mod traces;

pub use state::{
    LayoutPreset, LlmFocus, Palette, PaneFocus, Tab, TraceFocus, TraceViewMode, UiState,
};
pub(crate) use traces::{
    first_llm_trace_index, next_error_trace_index, parent_trace_index, previous_error_trace_index,
    root_trace_index, selected_trace_span_detail, selected_trace_tree_span, trace_tree_hit,
    trace_tree_rows, trace_tree_total_lines, visible_trace_tree_len,
};

use ratatui::{
    Frame,
    layout::{Constraint, Direction, Layout, Rect},
    prelude::{Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Paragraph, Tabs},
};

use crate::domain::DashboardSnapshot;

#[derive(Debug, Default)]
pub struct RenderCache {
    trace_detail: details::TraceDetailLinesCache,
    log_detail: details::LogDetailLinesCache,
    metric_detail: details::MetricDetailLinesCache,
    llm_detail: details::LlmDetailLinesCache,
}

pub fn sync_render_cache(snapshot: &DashboardSnapshot, state: &UiState, cache: &mut RenderCache) {
    let palette = Palette::from_theme(state.theme);
    if Tab::ALL[state.active_tab] == Tab::Traces && state.trace_view_mode == TraceViewMode::Detail {
        details::sync_trace_detail_lines_cache(snapshot, state, palette, &mut cache.trace_detail);
    } else if Tab::ALL[state.active_tab] == Tab::Logs {
        details::sync_log_detail_lines_cache(snapshot, state, palette, &mut cache.log_detail);
    } else if Tab::ALL[state.active_tab] == Tab::Metrics {
        details::sync_metric_detail_lines_cache(snapshot, state, palette, &mut cache.metric_detail);
    } else if Tab::ALL[state.active_tab] == Tab::Llm {
        details::sync_llm_detail_lines_cache(snapshot, state, palette, &mut cache.llm_detail);
    }
}

pub fn render(
    frame: &mut Frame<'_>,
    snapshot: &DashboardSnapshot,
    state: &UiState,
    cache: &RenderCache,
) {
    let palette = Palette::from_theme(state.theme);
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
        Tab::Traces => traces::render(
            frame,
            layout[2],
            snapshot,
            state,
            details::cached_trace_detail_lines(&cache.trace_detail),
            palette,
        ),
        Tab::Logs => panes::render_logs(
            frame,
            layout[2],
            snapshot,
            state,
            details::cached_log_detail_lines(&cache.log_detail),
            palette,
        ),
        Tab::Metrics => panes::render_metrics(
            frame,
            layout[2],
            snapshot,
            state,
            details::cached_metric_detail_lines(&cache.metric_detail),
            palette,
        ),
        Tab::Llm => panes::render_llm(
            frame,
            layout[2],
            snapshot,
            state,
            details::cached_llm_detail_lines(&cache.llm_detail),
            palette,
        ),
    }

    frame.render_widget(
        Paragraph::new(chrome::padded(chrome::footer_text(state)))
            .style(Style::default().fg(palette.muted)),
        layout[3],
    );

    if state.show_command_palette {
        chrome::render_command_palette(frame, root, state, palette);
    } else if state.show_help {
        chrome::render_help_overlay(frame, root, state, palette);
    } else if state.show_context_help {
        chrome::render_context_help_overlay(frame, root, state, palette);
    }
}

pub fn sync_trace_tree_scroll(root: Rect, snapshot: &DashboardSnapshot, state: &mut UiState) {
    if Tab::ALL[state.active_tab] != Tab::Traces || state.trace_view_mode != TraceViewMode::Detail {
        return;
    }

    let viewport_height = geometry::trace_tree_viewport_height(geometry::trace_tree_area(
        geometry::body_area(root),
        state.trace_split_pct,
    ));
    let tree_rows = traces::trace_tree_rows(&snapshot.selected_trace, &state.collapsed_trace_spans);
    let total_lines = traces::trace_tree_total_lines(&tree_rows, snapshot.traces.is_empty());
    if state.trace_tree_follow_selected {
        let selected_line = traces::trace_tree_selected_line(state, &tree_rows);
        state.trace_tree_scroll = geometry::trace_tree_scroll_offset(
            state.trace_tree_scroll,
            total_lines,
            selected_line,
            viewport_height,
        );
    } else {
        state.trace_tree_scroll =
            geometry::clamp_window_offset(state.trace_tree_scroll, total_lines, viewport_height);
    }
}

pub fn sync_detail_scroll(
    root: Rect,
    snapshot: &DashboardSnapshot,
    state: &mut UiState,
    cache: &RenderCache,
) {
    let body = geometry::body_area(root);
    state.trace_list_scroll = geometry::clamp_window_offset(
        state.trace_list_scroll,
        snapshot.traces.len(),
        geometry::table_viewport_height(body),
    );
    if state.trace_list_follow_selected {
        state.trace_list_scroll = geometry::follow_selected_offset(
            state.trace_list_scroll,
            snapshot.traces.len(),
            state.selected_trace,
            geometry::table_viewport_height(body),
        );
    }
    let [log_feed, _] = geometry::log_sections(body, state.log_split_pct);
    state.log_feed_scroll = geometry::clamp_window_offset(
        state.log_feed_scroll,
        snapshot.logs.len(),
        geometry::table_viewport_height(log_feed),
    );
    if state.log_feed_follow_selected {
        state.log_feed_scroll = geometry::follow_selected_offset(
            state.log_feed_scroll,
            snapshot.logs.len(),
            state.selected_log,
            geometry::table_viewport_height(log_feed),
        );
    }
    let [metric_feed, metric_right] = geometry::metric_sections(body, state.metric_split_pct);
    state.metric_feed_scroll = geometry::clamp_window_offset(
        state.metric_feed_scroll,
        snapshot.metrics.len(),
        geometry::table_viewport_height(metric_feed),
    );
    if state.metric_feed_follow_selected {
        state.metric_feed_scroll = geometry::follow_selected_offset(
            state.metric_feed_scroll,
            snapshot.metrics.len(),
            state.selected_metric,
            geometry::table_viewport_height(metric_feed),
        );
    }
    let [llm_left, _] = geometry::llm_sections(body, state.llm_split_pct);
    let [_, _, llm_feed] = geometry::llm_left_sections(llm_left);
    state.llm_feed_scroll = geometry::clamp_window_offset(
        state.llm_feed_scroll,
        snapshot.llm.len(),
        geometry::table_viewport_height(llm_feed),
    );
    if state.llm_feed_follow_selected {
        state.llm_feed_scroll = geometry::follow_selected_offset(
            state.llm_feed_scroll,
            snapshot.llm.len(),
            state.selected_llm,
            geometry::table_viewport_height(llm_feed),
        );
    }

    if Tab::ALL[state.active_tab] == Tab::Traces && state.trace_view_mode == TraceViewMode::Detail {
        state.trace_detail_scroll = geometry::clamp_scroll(
            state.trace_detail_scroll,
            details::cached_trace_detail_lines(&cache.trace_detail).len(),
            geometry::detail_viewport_height(geometry::trace_detail_area(
                body,
                state.trace_split_pct,
            )),
        );
    } else {
        state.trace_detail_scroll = 0;
    }

    if Tab::ALL[state.active_tab] == Tab::Logs {
        state.log_detail_scroll = geometry::clamp_scroll(
            state.log_detail_scroll,
            details::cached_log_detail_lines(&cache.log_detail).len(),
            geometry::detail_viewport_height(geometry::log_detail_area(body, state.log_split_pct)),
        );
    }
    if Tab::ALL[state.active_tab] == Tab::Metrics {
        state.metric_detail_scroll = geometry::clamp_scroll(
            state.metric_detail_scroll,
            details::cached_metric_detail_lines(&cache.metric_detail).len(),
            geometry::detail_viewport_height(geometry::metric_right_sections(metric_right)[1]),
        );
    }
    if Tab::ALL[state.active_tab] == Tab::Llm {
        let llm_detail_sections =
            geometry::llm_detail_sections(geometry::llm_detail_area(body, state.llm_split_pct));
        let llm_detail_lines = details::cached_llm_detail_lines(&cache.llm_detail);
        state.llm_detail_scroll = geometry::clamp_scroll(
            state.llm_detail_scroll,
            details::wrapped_line_count(
                llm_detail_lines,
                geometry::detail_viewport_width(llm_detail_sections[0]),
            ),
            geometry::detail_viewport_height(llm_detail_sections[0]),
        );
        state.llm_timeline_scroll = geometry::clamp_scroll(
            state.llm_timeline_scroll,
            details::llm_timeline_panel_lines(snapshot, state, Palette::from_theme(state.theme))
                .len(),
            geometry::detail_viewport_height(llm_detail_sections[1]),
        );
    }
}

#[cfg(test)]
mod tests;
