use crossterm::event::{KeyCode, KeyModifiers, MouseButton, MouseEvent, MouseEventKind};
use ratatui::layout::Rect;

use crate::{
    commands::{self, PaletteAction},
    config::Theme,
    domain::DashboardSnapshot,
    query::{LogCorrelationFilter, LogFilters, LogSeverityFilter, QueryFilters, TimeWindow},
    ui::{PaneFocus, Tab, TraceFocus, TraceViewMode, UiState},
};

const COMMAND_PALETTE_VISIBLE_ROWS: usize = 8;
const FAST_MOVE_STEP: isize = 5;

pub(super) fn handle_key(
    code: KeyCode,
    modifiers: KeyModifiers,
    state: &mut UiState,
    snapshot: &DashboardSnapshot,
) -> bool {
    if state.show_command_palette {
        return handle_command_palette_key(code, modifiers, state, snapshot);
    }

    if state.show_help {
        return handle_help_key(code, state);
    }

    if state.log_search_mode {
        return handle_log_search_key(code, state);
    }

    if state.search_mode {
        return handle_search_key(code, state);
    }

    let move_step = movement_step(modifiers);

    match code {
        KeyCode::Char(':') => {
            open_command_palette(state);
        }
        KeyCode::Char('p') if modifiers.contains(KeyModifiers::CONTROL) => {
            open_command_palette(state);
        }
        KeyCode::Char('/') => {
            state.search_mode = true;
        }
        KeyCode::Char('?') => {
            state.show_help = true;
        }
        KeyCode::Char('H') => {
            state.show_context_help = !state.show_context_help;
        }
        KeyCode::Char('g') => cycle_theme(state),
        KeyCode::Char('c') if modifiers.contains(KeyModifiers::CONTROL) => return true,
        KeyCode::Char('x') if Tab::ALL[state.active_tab] == Tab::Logs => {
            state.log_search_mode = true;
        }
        KeyCode::Char('i')
            if Tab::ALL[state.active_tab] == Tab::Llm && state.llm_focus == PaneFocus::Detail =>
        {
            state.llm_expand_prompt = !state.llm_expand_prompt;
        }
        KeyCode::Char('o')
            if Tab::ALL[state.active_tab] == Tab::Llm && state.llm_focus == PaneFocus::Detail =>
        {
            state.llm_expand_output = !state.llm_expand_output;
        }
        KeyCode::Char('q') => return true,
        KeyCode::Char('f') => {
            state.log_tail = !state.log_tail;
            if state.log_tail {
                state.selected_log = 0;
            }
        }
        KeyCode::Char('v') if Tab::ALL[state.active_tab] == Tab::Logs => {
            cycle_log_severity_filter(state)
        }
        KeyCode::Char('c') if Tab::ALL[state.active_tab] == Tab::Logs => {
            cycle_log_correlation_filter(state)
        }
        KeyCode::Tab => {
            state.active_tab = (state.active_tab + 1) % Tab::ALL.len();
        }
        KeyCode::BackTab => {
            state.active_tab = (state.active_tab + Tab::ALL.len() - 1) % Tab::ALL.len();
        }
        KeyCode::Enter
            if Tab::ALL[state.active_tab] == Tab::Traces
                && state.trace_view_mode == TraceViewMode::List =>
        {
            open_selected_trace(state)
        }
        KeyCode::Esc => go_back(state),
        KeyCode::Left | KeyCode::Char('h') => move_focus_left(state),
        KeyCode::Right | KeyCode::Char('l') => move_focus_right(state),
        KeyCode::Enter | KeyCode::Char(' ')
            if Tab::ALL[state.active_tab] == Tab::Traces
                && state.trace_view_mode == TraceViewMode::Detail
                && state.trace_focus == TraceFocus::TraceTree =>
        {
            toggle_selected_trace_subtree(state, snapshot)
        }
        KeyCode::Char('e') => state.errors_only = !state.errors_only,
        KeyCode::Char('[') if Tab::ALL[state.active_tab] == Tab::Traces => jump_to_trace_row(
            crate::ui::previous_error_trace_index(snapshot, state),
            state,
        ),
        KeyCode::Char(']') if Tab::ALL[state.active_tab] == Tab::Traces => {
            jump_to_trace_row(crate::ui::next_error_trace_index(snapshot, state), state)
        }
        KeyCode::Char('p') if Tab::ALL[state.active_tab] == Tab::Traces => {
            jump_to_trace_row(crate::ui::parent_trace_index(snapshot, state), state)
        }
        KeyCode::Char('r') if Tab::ALL[state.active_tab] == Tab::Traces => {
            jump_to_trace_row(crate::ui::root_trace_index(snapshot, state), state)
        }
        KeyCode::Char('m') if Tab::ALL[state.active_tab] == Tab::Traces => {
            jump_to_trace_row(crate::ui::first_llm_trace_index(snapshot, state), state)
        }
        KeyCode::Char('t') => cycle_time_window(state),
        KeyCode::Char('s') if !snapshot.services.is_empty() => {
            state.service_filter_index = match state.service_filter_index {
                None => Some(0),
                Some(idx) if idx + 1 >= snapshot.services.len() => None,
                Some(idx) => Some(idx + 1),
            };
        }
        KeyCode::Down | KeyCode::Char('j') => move_selection(move_step, state, snapshot),
        KeyCode::Up | KeyCode::Char('k') => move_selection(-move_step, state, snapshot),
        _ => {}
    }
    false
}

fn movement_step(modifiers: KeyModifiers) -> isize {
    if modifiers.contains(KeyModifiers::SHIFT) {
        FAST_MOVE_STEP
    } else {
        1
    }
}

pub(super) fn handle_mouse(
    event: MouseEvent,
    root: Rect,
    state: &mut UiState,
    snapshot: &DashboardSnapshot,
) -> bool {
    if state.show_command_palette || state.show_help || state.search_mode || state.log_search_mode {
        return false;
    }

    match event.kind {
        MouseEventKind::Down(MouseButton::Left) => {
            handle_left_click(event.column, event.row, root, state, snapshot)
        }
        MouseEventKind::ScrollDown => {
            handle_scroll(1, event.column, event.row, root, state, snapshot)
        }
        MouseEventKind::ScrollUp => {
            handle_scroll(-1, event.column, event.row, root, state, snapshot)
        }
        _ => false,
    }
}

pub(super) fn sync_selection(state: &mut UiState, snapshot: &DashboardSnapshot) {
    state.selected_trace = state
        .selected_trace
        .min(snapshot.traces.len().saturating_sub(1));
    state.trace_list_scroll = state
        .trace_list_scroll
        .min(snapshot.traces.len().saturating_sub(1));
    state.selected_trace_span = state
        .selected_trace_span
        .min(crate::ui::visible_trace_tree_len(snapshot, state).saturating_sub(1));
    if snapshot.selected_trace.is_empty() {
        state.trace_view_mode = TraceViewMode::List;
        state.trace_focus = TraceFocus::TraceList;
        state.trace_tree_scroll = 0;
        state.trace_tree_follow_selected = true;
        state.trace_detail_scroll = 0;
        state.collapsed_trace_spans.clear();
    }
    state.selected_log = state
        .selected_log
        .min(snapshot.logs.len().saturating_sub(1));
    state.log_feed_scroll = state
        .log_feed_scroll
        .min(snapshot.logs.len().saturating_sub(1));
    if state.log_tail && !snapshot.logs.is_empty() {
        state.selected_log = 0;
        state.log_feed_scroll = 0;
    }
    state.selected_metric = state
        .selected_metric
        .min(snapshot.metrics.len().saturating_sub(1));
    state.metric_feed_scroll = state
        .metric_feed_scroll
        .min(snapshot.metrics.len().saturating_sub(1));
    state.selected_llm = state.selected_llm.min(snapshot.llm.len().saturating_sub(1));
    state.llm_feed_scroll = state
        .llm_feed_scroll
        .min(snapshot.llm.len().saturating_sub(1));

    if let Some(idx) = state.service_filter_index
        && idx >= snapshot.services.len()
    {
        state.service_filter_index = None;
    }
}

fn tab_index(tab: Tab) -> usize {
    Tab::ALL
        .iter()
        .position(|candidate| *candidate == tab)
        .unwrap_or(0)
}

pub(super) fn filters(state: &UiState, services: &[String]) -> QueryFilters {
    QueryFilters {
        service: state
            .service_filter_index
            .and_then(|idx| services.get(idx))
            .cloned(),
        errors_only: state.errors_only,
        time_window: state.time_window,
        search_query: (!state.search_query.is_empty()).then(|| state.search_query.clone()),
        log_filters: LogFilters {
            severity: state.log_severity_filter,
            correlation: state.log_correlation_filter,
            search_query: (!state.log_search_query.is_empty())
                .then(|| state.log_search_query.clone()),
        },
    }
}

fn handle_left_click(
    column: u16,
    row: u16,
    root: Rect,
    state: &mut UiState,
    snapshot: &DashboardSnapshot,
) -> bool {
    let [tabs_area, _, body, _] = crate::ui::geometry::root_sections(root);
    if crate::ui::geometry::contains(tabs_area, column, row) {
        click_tab(column, tabs_area, state);
        return true;
    }
    if !crate::ui::geometry::contains(body, column, row) {
        return false;
    }

    match Tab::ALL[state.active_tab] {
        Tab::Overview => false,
        Tab::Traces => handle_trace_click(column, row, body, state, snapshot),
        Tab::Logs => handle_logs_click(column, row, body, state, snapshot),
        Tab::Metrics => handle_metrics_click(column, row, body, state, snapshot),
        Tab::Llm => handle_llm_click(column, row, body, state, snapshot),
    }
}

fn click_tab(column: u16, tabs_area: Rect, state: &mut UiState) {
    let inner_x = tabs_area.x.saturating_add(1);
    let inner_right = tabs_area
        .x
        .saturating_add(tabs_area.width)
        .saturating_sub(1);
    if tabs_area.width <= 2 || column < inner_x || column >= inner_right {
        return;
    }

    let mut x = inner_x;
    for (index, tab) in Tab::ALL.iter().enumerate() {
        let title_width = u16::try_from(tab.title().chars().count()).unwrap_or(u16::MAX);
        let tab_start = x;
        let tab_end = x
            .saturating_add(1)
            .saturating_add(title_width)
            .saturating_add(1);
        if column >= tab_start && column < tab_end {
            state.active_tab = index;
            break;
        }

        x = tab_end;
        if index + 1 < Tab::ALL.len() {
            x = x.saturating_add(1);
        }
        if x >= inner_right {
            break;
        }
    }
}

fn handle_scroll(
    delta: isize,
    column: u16,
    row: u16,
    root: Rect,
    state: &mut UiState,
    snapshot: &DashboardSnapshot,
) -> bool {
    let body = crate::ui::geometry::body_area(root);
    if !crate::ui::geometry::contains(body, column, row) {
        return false;
    }

    match Tab::ALL[state.active_tab] {
        Tab::Overview => false,
        Tab::Traces => handle_trace_scroll(delta, column, row, body, state, snapshot),
        Tab::Logs => handle_logs_scroll(delta, column, row, body, state, snapshot),
        Tab::Metrics => handle_metrics_scroll(delta, column, row, body, state, snapshot),
        Tab::Llm => handle_llm_scroll(delta, column, row, body, state, snapshot),
    }
}

fn handle_trace_click(
    column: u16,
    row: u16,
    body: Rect,
    state: &mut UiState,
    snapshot: &DashboardSnapshot,
) -> bool {
    if state.trace_view_mode == TraceViewMode::List {
        if let Some(index) = table_row_at(body, row, snapshot.traces.len(), state.trace_list_scroll)
        {
            let was_selected = index == state.selected_trace;
            select_trace(index, state);
            if was_selected {
                open_selected_trace(state);
            }
            return true;
        }
        return false;
    }

    let [tree_area, detail_area] = crate::ui::geometry::trace_detail_sections(body);
    if crate::ui::geometry::contains(detail_area, column, row) {
        state.trace_focus = TraceFocus::TraceDetail;
        return false;
    }
    if !crate::ui::geometry::contains(tree_area, column, row) {
        return false;
    }

    state.trace_focus = TraceFocus::TraceTree;
    if let Some((index, clicked_disclosure)) =
        crate::ui::trace_tree_hit(snapshot, state, tree_area, column, row)
    {
        state.selected_trace_span = index;
        state.trace_tree_follow_selected = true;
        state.trace_detail_scroll = 0;
        if clicked_disclosure {
            toggle_selected_trace_subtree(state, snapshot);
        }
        return true;
    }
    false
}

fn handle_logs_click(
    column: u16,
    row: u16,
    body: Rect,
    state: &mut UiState,
    snapshot: &DashboardSnapshot,
) -> bool {
    let [feed_area, detail_area] = crate::ui::geometry::log_sections(body);
    if crate::ui::geometry::contains(detail_area, column, row) {
        state.logs_focus = PaneFocus::Detail;
        return false;
    }
    if !crate::ui::geometry::contains(feed_area, column, row) {
        return false;
    }

    state.logs_focus = PaneFocus::Primary;
    state.log_tail = false;
    if let Some(index) = table_row_at(feed_area, row, snapshot.logs.len(), state.log_feed_scroll) {
        state.selected_log = index;
        state.log_detail_scroll = 0;
        return true;
    }
    false
}

fn handle_metrics_click(
    column: u16,
    row: u16,
    body: Rect,
    state: &mut UiState,
    snapshot: &DashboardSnapshot,
) -> bool {
    let [feed_area, right_area] = crate::ui::geometry::metric_sections(body);
    let detail_area = crate::ui::geometry::metric_right_sections(right_area)[1];
    if crate::ui::geometry::contains(detail_area, column, row) {
        state.metrics_focus = PaneFocus::Detail;
        return false;
    }
    if !crate::ui::geometry::contains(feed_area, column, row) {
        return false;
    }

    state.metrics_focus = PaneFocus::Primary;
    if let Some(index) = table_row_at(
        feed_area,
        row,
        snapshot.metrics.len(),
        state.metric_feed_scroll,
    ) {
        state.selected_metric = index;
        state.metric_detail_scroll = 0;
        return true;
    }
    false
}

fn handle_llm_click(
    column: u16,
    row: u16,
    body: Rect,
    state: &mut UiState,
    snapshot: &DashboardSnapshot,
) -> bool {
    let [left_area, detail_area] = crate::ui::geometry::llm_sections(body);
    let [_, _, _, feed_area] = crate::ui::geometry::llm_left_sections(left_area);
    if crate::ui::geometry::contains(detail_area, column, row) {
        state.llm_focus = PaneFocus::Detail;
        return false;
    }
    if !crate::ui::geometry::contains(left_area, column, row) {
        return false;
    }

    state.llm_focus = PaneFocus::Primary;
    if let Some(index) = table_row_at(feed_area, row, snapshot.llm.len(), state.llm_feed_scroll) {
        state.selected_llm = index;
        state.llm_detail_scroll = 0;
        state.llm_expand_prompt = false;
        state.llm_expand_output = false;
        return true;
    }
    false
}

fn handle_trace_scroll(
    delta: isize,
    column: u16,
    row: u16,
    body: Rect,
    state: &mut UiState,
    snapshot: &DashboardSnapshot,
) -> bool {
    if state.trace_view_mode == TraceViewMode::List {
        if crate::ui::geometry::contains(body, column, row) {
            state.trace_focus = TraceFocus::TraceList;
            let previous = state.selected_trace;
            move_index(&mut state.selected_trace, snapshot.traces.len(), delta);
            if state.selected_trace != previous {
                state.selected_trace_span = 0;
                state.trace_tree_scroll = 0;
                state.trace_tree_follow_selected = true;
                state.trace_detail_scroll = 0;
                state.collapsed_trace_spans.clear();
            }
        }
        return false;
    }

    let [tree_area, detail_area] = crate::ui::geometry::trace_detail_sections(body);
    if crate::ui::geometry::contains(detail_area, column, row) {
        state.trace_focus = TraceFocus::TraceDetail;
        scroll_detail(delta as i16, state);
        return false;
    }
    if crate::ui::geometry::contains(tree_area, column, row) {
        state.trace_focus = TraceFocus::TraceTree;
        let previous = state.selected_trace_span;
        let visible_len = crate::ui::visible_trace_tree_len(snapshot, state);
        move_index(&mut state.selected_trace_span, visible_len, delta);
        if state.selected_trace_span != previous {
            state.trace_tree_follow_selected = true;
            state.trace_detail_scroll = 0;
        }
    }
    false
}

fn handle_logs_scroll(
    delta: isize,
    column: u16,
    row: u16,
    body: Rect,
    state: &mut UiState,
    snapshot: &DashboardSnapshot,
) -> bool {
    let [feed_area, detail_area] = crate::ui::geometry::log_sections(body);
    if crate::ui::geometry::contains(detail_area, column, row) {
        state.logs_focus = PaneFocus::Detail;
        scroll_detail(delta as i16, state);
        return false;
    }
    if crate::ui::geometry::contains(feed_area, column, row) {
        state.logs_focus = PaneFocus::Primary;
        state.log_tail = false;
        let previous = state.selected_log;
        move_index(&mut state.selected_log, snapshot.logs.len(), delta);
        if state.selected_log != previous {
            state.log_detail_scroll = 0;
        }
    }
    false
}

fn handle_metrics_scroll(
    delta: isize,
    column: u16,
    row: u16,
    body: Rect,
    state: &mut UiState,
    snapshot: &DashboardSnapshot,
) -> bool {
    let [feed_area, right_area] = crate::ui::geometry::metric_sections(body);
    let detail_area = crate::ui::geometry::metric_right_sections(right_area)[1];
    if crate::ui::geometry::contains(detail_area, column, row) {
        state.metrics_focus = PaneFocus::Detail;
        scroll_detail(delta as i16, state);
        return false;
    }
    if crate::ui::geometry::contains(feed_area, column, row) {
        state.metrics_focus = PaneFocus::Primary;
        let previous = state.selected_metric;
        move_index(&mut state.selected_metric, snapshot.metrics.len(), delta);
        if state.selected_metric != previous {
            state.metric_detail_scroll = 0;
        }
    }
    false
}

fn handle_llm_scroll(
    delta: isize,
    column: u16,
    row: u16,
    body: Rect,
    state: &mut UiState,
    snapshot: &DashboardSnapshot,
) -> bool {
    let [left_area, detail_area] = crate::ui::geometry::llm_sections(body);
    let [_, _, _, feed_area] = crate::ui::geometry::llm_left_sections(left_area);
    if crate::ui::geometry::contains(detail_area, column, row) {
        state.llm_focus = PaneFocus::Detail;
        scroll_detail(delta as i16, state);
        return false;
    }
    if crate::ui::geometry::contains(feed_area, column, row) {
        state.llm_focus = PaneFocus::Primary;
        let previous = state.selected_llm;
        move_index(&mut state.selected_llm, snapshot.llm.len(), delta);
        if state.selected_llm != previous {
            state.llm_detail_scroll = 0;
            state.llm_expand_prompt = false;
            state.llm_expand_output = false;
            return true;
        }
    }
    false
}

fn table_row_at(area: Rect, row: u16, item_count: usize, scroll_offset: usize) -> Option<usize> {
    let content_top = area.y.saturating_add(2);
    let content_bottom = area.y.saturating_add(area.height).saturating_sub(1);
    if row < content_top || row >= content_bottom {
        return None;
    }

    let index = scroll_offset.saturating_add(usize::from(row - content_top));
    (index < item_count).then_some(index)
}

fn select_trace(index: usize, state: &mut UiState) {
    if state.selected_trace == index {
        return;
    }

    state.selected_trace = index;
    state.selected_trace_span = 0;
    state.trace_tree_scroll = 0;
    state.trace_tree_follow_selected = true;
    state.trace_detail_scroll = 0;
    state.collapsed_trace_spans.clear();
}

fn move_selection(delta: isize, state: &mut UiState, snapshot: &DashboardSnapshot) {
    match Tab::ALL[state.active_tab] {
        Tab::Overview => {}
        Tab::Traces => match state.trace_focus {
            TraceFocus::TraceList => {
                if state.trace_view_mode != TraceViewMode::List {
                    return;
                }
                let previous = state.selected_trace;
                move_index(&mut state.selected_trace, snapshot.traces.len(), delta);
                if state.selected_trace != previous {
                    state.selected_trace_span = 0;
                    state.trace_tree_scroll = 0;
                    state.trace_detail_scroll = 0;
                    state.collapsed_trace_spans.clear();
                }
            }
            TraceFocus::TraceTree => {
                if state.trace_view_mode != TraceViewMode::Detail {
                    return;
                }
                let previous = state.selected_trace_span;
                let visible_len = crate::ui::visible_trace_tree_len(snapshot, state);
                move_index(&mut state.selected_trace_span, visible_len, delta);
                if state.selected_trace_span != previous {
                    state.trace_tree_follow_selected = true;
                    state.trace_detail_scroll = 0;
                }
            }
            TraceFocus::TraceDetail if state.trace_view_mode == TraceViewMode::Detail => {
                scroll_detail(delta as i16, state)
            }
            TraceFocus::TraceDetail => {}
        },
        Tab::Logs => match state.logs_focus {
            PaneFocus::Primary => {
                let previous = state.selected_log;
                move_index(&mut state.selected_log, snapshot.logs.len(), delta);
                if delta != 0 {
                    state.log_tail = false;
                }
                if state.selected_log != previous {
                    state.log_detail_scroll = 0;
                }
            }
            PaneFocus::Detail => scroll_detail(delta as i16, state),
        },
        Tab::Metrics => match state.metrics_focus {
            PaneFocus::Primary => {
                let previous = state.selected_metric;
                move_index(&mut state.selected_metric, snapshot.metrics.len(), delta);
                if state.selected_metric != previous {
                    state.metric_detail_scroll = 0;
                }
            }
            PaneFocus::Detail => scroll_detail(delta as i16, state),
        },
        Tab::Llm => match state.llm_focus {
            PaneFocus::Primary => {
                let previous = state.selected_llm;
                move_index(&mut state.selected_llm, snapshot.llm.len(), delta);
                if state.selected_llm != previous {
                    state.llm_detail_scroll = 0;
                    state.llm_expand_prompt = false;
                    state.llm_expand_output = false;
                }
            }
            PaneFocus::Detail => scroll_detail(delta as i16, state),
        },
    }
}

fn move_index(selection: &mut usize, max: usize, delta: isize) {
    if max == 0 {
        return;
    }

    let next = (*selection as isize + delta).clamp(0, max.saturating_sub(1) as isize) as usize;
    *selection = next;
}

fn cycle_time_window(state: &mut UiState) {
    let current = TimeWindow::ALL
        .iter()
        .position(|window| *window == state.time_window)
        .unwrap_or(0);
    state.time_window = TimeWindow::ALL[(current + 1) % TimeWindow::ALL.len()];
}

fn cycle_theme(state: &mut UiState) {
    let current = Theme::ALL
        .iter()
        .position(|theme| *theme == state.theme)
        .unwrap_or(0);
    state.theme = Theme::ALL[(current + 1) % Theme::ALL.len()];
}

fn cycle_log_severity_filter(state: &mut UiState) {
    let current = LogSeverityFilter::ALL
        .iter()
        .position(|filter| *filter == state.log_severity_filter)
        .unwrap_or(0);
    state.log_severity_filter =
        LogSeverityFilter::ALL[(current + 1) % LogSeverityFilter::ALL.len()];
}

fn cycle_log_correlation_filter(state: &mut UiState) {
    let current = LogCorrelationFilter::ALL
        .iter()
        .position(|filter| *filter == state.log_correlation_filter)
        .unwrap_or(0);
    state.log_correlation_filter =
        LogCorrelationFilter::ALL[(current + 1) % LogCorrelationFilter::ALL.len()];
}

fn toggle_selected_trace_subtree(state: &mut UiState, snapshot: &DashboardSnapshot) {
    let Some((span_id, has_children)) = crate::ui::selected_trace_tree_span(snapshot, state) else {
        return;
    };
    if !has_children {
        return;
    }

    if !state.collapsed_trace_spans.insert(span_id.clone()) {
        state.collapsed_trace_spans.remove(&span_id);
    }
}

fn scroll_detail(delta: i16, state: &mut UiState) {
    let scroll = match Tab::ALL[state.active_tab] {
        Tab::Overview => return,
        Tab::Traces => &mut state.trace_detail_scroll,
        Tab::Logs => &mut state.log_detail_scroll,
        Tab::Metrics => &mut state.metric_detail_scroll,
        Tab::Llm => &mut state.llm_detail_scroll,
    };

    *scroll = scroll.saturating_add_signed(delta);
}

fn jump_to_trace_row(target: Option<usize>, state: &mut UiState) {
    if let Some(index) = target {
        if state.trace_view_mode == TraceViewMode::List {
            return;
        }
        state.selected_trace_span = index;
        state.trace_tree_follow_selected = true;
        state.trace_detail_scroll = 0;
    }
}

fn move_focus_left(state: &mut UiState) {
    match Tab::ALL[state.active_tab] {
        Tab::Overview => {}
        Tab::Traces => match state.trace_view_mode {
            TraceViewMode::List => {
                state.trace_focus = TraceFocus::TraceList;
            }
            TraceViewMode::Detail => {
                state.trace_focus = match state.trace_focus {
                    TraceFocus::TraceDetail => TraceFocus::TraceTree,
                    TraceFocus::TraceTree => TraceFocus::TraceTree,
                    TraceFocus::TraceList => TraceFocus::TraceList,
                };
            }
        },
        Tab::Logs => state.logs_focus = PaneFocus::Primary,
        Tab::Metrics => state.metrics_focus = PaneFocus::Primary,
        Tab::Llm => state.llm_focus = PaneFocus::Primary,
    }
}

fn move_focus_right(state: &mut UiState) {
    match Tab::ALL[state.active_tab] {
        Tab::Overview => {}
        Tab::Traces => {
            if state.trace_view_mode == TraceViewMode::Detail {
                state.trace_focus = match state.trace_focus {
                    TraceFocus::TraceTree => TraceFocus::TraceDetail,
                    TraceFocus::TraceDetail => TraceFocus::TraceDetail,
                    TraceFocus::TraceList => TraceFocus::TraceTree,
                };
            }
        }
        Tab::Logs => state.logs_focus = PaneFocus::Detail,
        Tab::Metrics => state.metrics_focus = PaneFocus::Detail,
        Tab::Llm => state.llm_focus = PaneFocus::Detail,
    }
}

fn open_selected_trace(state: &mut UiState) {
    state.trace_view_mode = TraceViewMode::Detail;
    state.trace_focus = TraceFocus::TraceTree;
    state.selected_trace_span = 0;
    state.trace_tree_scroll = 0;
    state.trace_tree_follow_selected = true;
    state.trace_detail_scroll = 0;
}

fn go_back(state: &mut UiState) {
    match Tab::ALL[state.active_tab] {
        Tab::Overview => {}
        Tab::Traces => match state.trace_view_mode {
            TraceViewMode::List => {
                state.trace_focus = TraceFocus::TraceList;
            }
            TraceViewMode::Detail => {
                state.trace_view_mode = TraceViewMode::List;
                state.trace_focus = TraceFocus::TraceList;
            }
        },
        Tab::Logs => state.logs_focus = PaneFocus::Primary,
        Tab::Metrics => state.metrics_focus = PaneFocus::Primary,
        Tab::Llm => state.llm_focus = PaneFocus::Primary,
    }
}

fn handle_search_key(code: KeyCode, state: &mut UiState) -> bool {
    match code {
        KeyCode::Esc | KeyCode::Enter => {
            state.search_mode = false;
        }
        KeyCode::Backspace => {
            state.search_query.pop();
        }
        KeyCode::Char('u') if state.search_query.is_empty() => {
            state.search_mode = false;
        }
        KeyCode::Char(character) => {
            state.search_query.push(character);
        }
        _ => {}
    }
    false
}

fn handle_log_search_key(code: KeyCode, state: &mut UiState) -> bool {
    match code {
        KeyCode::Esc | KeyCode::Enter => {
            state.log_search_mode = false;
        }
        KeyCode::Backspace => {
            state.log_search_query.pop();
        }
        KeyCode::Char('u') if state.log_search_query.is_empty() => {
            state.log_search_mode = false;
        }
        KeyCode::Char(character) => {
            state.log_search_query.push(character);
        }
        _ => {}
    }
    false
}

fn handle_help_key(code: KeyCode, state: &mut UiState) -> bool {
    match code {
        KeyCode::Esc | KeyCode::Enter | KeyCode::Char('?') => {
            state.show_help = false;
        }
        _ => {}
    }
    false
}

fn open_command_palette(state: &mut UiState) {
    state.show_help = false;
    state.show_context_help = false;
    state.search_mode = false;
    state.log_search_mode = false;
    state.show_command_palette = true;
    state.command_query.clear();
    state.selected_command = 0;
    state.command_palette_scroll = 0;
}

fn close_command_palette(state: &mut UiState) {
    state.show_command_palette = false;
    state.command_query.clear();
    state.selected_command = 0;
    state.command_palette_scroll = 0;
}

fn handle_command_palette_key(
    code: KeyCode,
    modifiers: KeyModifiers,
    state: &mut UiState,
    snapshot: &DashboardSnapshot,
) -> bool {
    match code {
        KeyCode::Esc => {
            close_command_palette(state);
        }
        KeyCode::Enter => {
            let commands = commands::matching_commands(&state.command_query);
            if let Some(command) = commands.get(state.selected_command).copied() {
                close_command_palette(state);
                return execute_palette_action(command.action, state, snapshot);
            }
        }
        KeyCode::Backspace => {
            state.command_query.pop();
            clamp_palette_selection(state);
        }
        KeyCode::Down | KeyCode::Char('j') => move_palette_selection(1, state),
        KeyCode::Up | KeyCode::Char('k') => move_palette_selection(-1, state),
        KeyCode::Char('p') if modifiers.contains(KeyModifiers::CONTROL) => {
            close_command_palette(state);
        }
        KeyCode::Char(character) if !modifiers.contains(KeyModifiers::CONTROL) => {
            state.command_query.push(character);
            clamp_palette_selection(state);
        }
        _ => {}
    }
    false
}

fn move_palette_selection(delta: isize, state: &mut UiState) {
    let commands = commands::matching_commands(&state.command_query);
    let previous = state.selected_command;
    move_index(&mut state.selected_command, commands.len(), delta);
    if state.selected_command != previous {
        sync_palette_scroll(state, commands.len());
    }
}

fn clamp_palette_selection(state: &mut UiState) {
    let len = commands::matching_commands(&state.command_query).len();
    state.selected_command = state.selected_command.min(len.saturating_sub(1));
    sync_palette_scroll(state, len);
}

fn sync_palette_scroll(state: &mut UiState, total: usize) {
    if total <= COMMAND_PALETTE_VISIBLE_ROWS {
        state.command_palette_scroll = 0;
        return;
    }

    state.command_palette_scroll = crate::ui::geometry::trace_tree_scroll_offset(
        state.command_palette_scroll,
        total,
        state.selected_command,
        COMMAND_PALETTE_VISIBLE_ROWS,
    );
}

fn execute_palette_action(
    action: PaletteAction,
    state: &mut UiState,
    snapshot: &DashboardSnapshot,
) -> bool {
    match action {
        PaletteAction::SwitchTab(tab) => {
            state.active_tab = tab_index(tab);
        }
        PaletteAction::SetTheme(theme) => {
            state.theme = theme;
        }
        PaletteAction::CycleTheme => cycle_theme(state),
        PaletteAction::ToggleHelp => {
            state.show_help = true;
        }
        PaletteAction::CycleService => {
            if !snapshot.services.is_empty() {
                state.service_filter_index = match state.service_filter_index {
                    None => Some(0),
                    Some(idx) if idx + 1 >= snapshot.services.len() => None,
                    Some(idx) => Some(idx + 1),
                };
            }
        }
        PaletteAction::ClearService => {
            state.service_filter_index = None;
        }
        PaletteAction::CycleTimeWindow => cycle_time_window(state),
        PaletteAction::ToggleTraceErrors => {
            state.errors_only = !state.errors_only;
        }
        PaletteAction::ReturnToTraceList => {
            state.active_tab = tab_index(Tab::Traces);
            state.trace_view_mode = TraceViewMode::List;
            state.trace_focus = TraceFocus::TraceList;
        }
        PaletteAction::ToggleLogTail => {
            state.active_tab = tab_index(Tab::Logs);
            state.log_tail = !state.log_tail;
            if state.log_tail {
                state.selected_log = 0;
            }
        }
        PaletteAction::ClearGlobalSearch => {
            state.search_query.clear();
            state.search_mode = false;
        }
        PaletteAction::ClearLogSearch => {
            state.active_tab = tab_index(Tab::Logs);
            state.log_search_query.clear();
            state.log_search_mode = false;
        }
        PaletteAction::Quit => return true,
    }
    false
}

#[cfg(test)]
mod tests {
    use crossterm::event::{KeyCode, KeyModifiers, MouseButton, MouseEvent, MouseEventKind};
    use ratatui::layout::Rect;

    use super::handle_mouse;
    use crate::{
        domain::{DashboardSnapshot, LogSummary, OverviewStats, SpanDetail, TraceSummary},
        query::TimeWindow,
        ui::{PaneFocus, Tab, TraceFocus, TraceViewMode, UiState},
    };

    #[test]
    fn clicking_selected_trace_opens_trace_detail() {
        let mut state = UiState::default();
        state.active_tab = Tab::Traces as usize;
        let snapshot = snapshot_with_trace();

        handle_mouse(
            MouseEvent {
                kind: MouseEventKind::Down(MouseButton::Left),
                column: 4,
                row: 6,
                modifiers: KeyModifiers::empty(),
            },
            Rect::new(0, 0, 120, 40),
            &mut state,
            &snapshot,
        );

        assert_eq!(state.trace_view_mode, TraceViewMode::Detail);
        assert_eq!(state.trace_focus, TraceFocus::TraceTree);
    }

    #[test]
    fn clicking_tab_switches_active_tab() {
        let mut state = UiState::default();
        let snapshot = DashboardSnapshot {
            services: Vec::new(),
            overview: empty_overview(),
            traces: Vec::new(),
            selected_trace: Vec::new(),
            logs: Vec::new(),
            metrics: Vec::new(),
            llm: Vec::new(),
            llm_rollups: Vec::new(),
            llm_sessions: Vec::new(),
            llm_model_comparisons: Vec::new(),
            llm_top_calls: Vec::new(),
            selected_llm_timeline: Vec::new(),
        };

        handle_mouse(
            MouseEvent {
                kind: MouseEventKind::Down(MouseButton::Left),
                column: 38,
                row: 1,
                modifiers: KeyModifiers::empty(),
            },
            Rect::new(0, 0, 120, 40),
            &mut state,
            &snapshot,
        );

        assert_eq!(state.active_tab, Tab::Metrics as usize);
    }

    #[test]
    fn time_window_cycle_wraps_at_twenty_four_hours() {
        let mut state = UiState {
            time_window: TimeWindow::TwentyFourHours,
            ..UiState::default()
        };

        super::cycle_time_window(&mut state);

        assert_eq!(state.time_window, TimeWindow::FifteenMinutes);
        assert!(!TimeWindow::ALL.iter().any(|window| window.label() == "all"));
    }

    #[test]
    fn shift_j_moves_selection_faster() {
        let mut state = UiState {
            active_tab: Tab::Logs as usize,
            logs_focus: PaneFocus::Primary,
            ..UiState::default()
        };
        let snapshot = DashboardSnapshot {
            services: Vec::new(),
            overview: OverviewStats {
                log_count: 20,
                ..empty_overview()
            },
            traces: Vec::new(),
            selected_trace: Vec::new(),
            logs: (0..20)
                .map(|index| LogSummary {
                    service_name: "api".to_string(),
                    timestamp_unix_nano: index,
                    severity: "INFO".to_string(),
                    body: format!("log {index}"),
                    trace_id: String::new(),
                    span_id: String::new(),
                    resource_attributes: Default::default(),
                    attributes: Default::default(),
                })
                .collect(),
            metrics: Vec::new(),
            llm: Vec::new(),
            llm_rollups: Vec::new(),
            llm_sessions: Vec::new(),
            llm_model_comparisons: Vec::new(),
            llm_top_calls: Vec::new(),
            selected_llm_timeline: Vec::new(),
        };

        let should_quit = super::handle_key(
            KeyCode::Char('j'),
            KeyModifiers::SHIFT,
            &mut state,
            &snapshot,
        );

        assert!(!should_quit);
        assert_eq!(state.selected_log, super::FAST_MOVE_STEP as usize);
    }

    #[test]
    fn scrolling_llm_detail_uses_mouse_wheel() {
        let mut state = UiState {
            active_tab: Tab::Llm as usize,
            llm_focus: PaneFocus::Detail,
            ..UiState::default()
        };
        let snapshot = DashboardSnapshot {
            services: Vec::new(),
            overview: OverviewStats {
                llm_count: 1,
                ..empty_overview()
            },
            traces: Vec::new(),
            selected_trace: Vec::new(),
            logs: Vec::new(),
            metrics: Vec::new(),
            llm: vec![crate::domain::LlmSummary {
                trace_id: "trace-1".to_string(),
                span_id: "span-1".to_string(),
                service_name: "api".to_string(),
                provider: "openai".to_string(),
                model: "gpt-5.4".to_string(),
                operation: "chat".to_string(),
                span_kind: None,
                session_id: None,
                conversation_id: None,
                prompt_preview: Some(
                    (1..=20)
                        .map(|i| format!("line {i}"))
                        .collect::<Vec<_>>()
                        .join("\n"),
                ),
                output_preview: None,
                tool_name: None,
                tool_args: None,
                input_tokens: None,
                output_tokens: None,
                total_tokens: None,
                cost: None,
                latency_ms: None,
                status: "STATUS_CODE_UNSET".to_string(),
                raw_json: serde_json::json!({}),
            }],
            llm_rollups: Vec::new(),
            llm_sessions: Vec::new(),
            llm_model_comparisons: Vec::new(),
            llm_top_calls: Vec::new(),
            selected_llm_timeline: Vec::new(),
        };

        handle_mouse(
            MouseEvent {
                kind: MouseEventKind::ScrollDown,
                column: 90,
                row: 10,
                modifiers: KeyModifiers::empty(),
            },
            Rect::new(0, 0, 120, 40),
            &mut state,
            &snapshot,
        );

        assert_eq!(state.llm_focus, PaneFocus::Detail);
        assert_eq!(state.llm_detail_scroll, 1);
    }

    #[test]
    fn scrolling_log_feed_moves_selection_not_window() {
        let mut state = UiState {
            active_tab: Tab::Logs as usize,
            logs_focus: PaneFocus::Primary,
            selected_log: 7,
            log_tail: true,
            ..UiState::default()
        };
        let snapshot = DashboardSnapshot {
            services: Vec::new(),
            overview: OverviewStats {
                log_count: 40,
                ..empty_overview()
            },
            traces: Vec::new(),
            selected_trace: Vec::new(),
            logs: (0..40)
                .map(|index| LogSummary {
                    service_name: "api".to_string(),
                    timestamp_unix_nano: index,
                    severity: "INFO".to_string(),
                    body: format!("log {index}"),
                    trace_id: String::new(),
                    span_id: String::new(),
                    resource_attributes: Default::default(),
                    attributes: Default::default(),
                })
                .collect(),
            metrics: Vec::new(),
            llm: Vec::new(),
            llm_rollups: Vec::new(),
            llm_sessions: Vec::new(),
            llm_model_comparisons: Vec::new(),
            llm_top_calls: Vec::new(),
            selected_llm_timeline: Vec::new(),
        };

        let needs_refresh = handle_mouse(
            MouseEvent {
                kind: MouseEventKind::ScrollDown,
                column: 5,
                row: 10,
                modifiers: KeyModifiers::empty(),
            },
            Rect::new(0, 0, 120, 40),
            &mut state,
            &snapshot,
        );

        assert!(!needs_refresh);
        assert_eq!(state.logs_focus, PaneFocus::Primary);
        assert_eq!(state.selected_log, 8);
        assert_eq!(state.log_feed_scroll, 0);
        assert!(!state.log_tail);
    }

    #[test]
    fn scrolling_trace_tree_moves_selection_not_window() {
        let mut state = UiState {
            active_tab: Tab::Traces as usize,
            trace_view_mode: TraceViewMode::Detail,
            trace_focus: TraceFocus::TraceTree,
            selected_trace_span: 7,
            ..UiState::default()
        };
        let mut selected_trace = vec![span("root", "", 1)];
        selected_trace.extend((0..40).map(|index| span(&format!("child-{index}"), "root", index)));
        let snapshot = DashboardSnapshot {
            services: Vec::new(),
            overview: OverviewStats {
                trace_count: 1,
                ..empty_overview()
            },
            traces: vec![TraceSummary {
                trace_id: "trace-1".to_string(),
                service_name: "api".to_string(),
                root_name: "request".to_string(),
                span_count: 41,
                error_count: 0,
                duration_ms: 42.0,
                started_at_unix_nano: 1,
            }],
            selected_trace,
            logs: Vec::new(),
            metrics: Vec::new(),
            llm: Vec::new(),
            llm_rollups: Vec::new(),
            llm_sessions: Vec::new(),
            llm_model_comparisons: Vec::new(),
            llm_top_calls: Vec::new(),
            selected_llm_timeline: Vec::new(),
        };

        let needs_refresh = handle_mouse(
            MouseEvent {
                kind: MouseEventKind::ScrollDown,
                column: 5,
                row: 10,
                modifiers: KeyModifiers::empty(),
            },
            Rect::new(0, 0, 120, 40),
            &mut state,
            &snapshot,
        );

        assert!(!needs_refresh);
        assert_eq!(state.trace_focus, TraceFocus::TraceTree);
        assert_eq!(state.selected_trace_span, 8);
        assert_eq!(state.trace_tree_scroll, 0);
        assert!(state.trace_tree_follow_selected);
    }

    #[test]
    fn clicking_trace_disclosure_toggles_collapsed_subtree() {
        let mut state = UiState {
            active_tab: Tab::Traces as usize,
            trace_view_mode: TraceViewMode::Detail,
            trace_focus: TraceFocus::TraceTree,
            ..UiState::default()
        };
        let snapshot = DashboardSnapshot {
            services: Vec::new(),
            overview: OverviewStats {
                trace_count: 1,
                ..empty_overview()
            },
            traces: vec![TraceSummary {
                trace_id: "trace-1".to_string(),
                service_name: "api".to_string(),
                root_name: "request".to_string(),
                span_count: 2,
                error_count: 0,
                duration_ms: 42.0,
                started_at_unix_nano: 1,
            }],
            selected_trace: vec![
                crate::domain::SpanDetail {
                    trace_id: "trace-1".to_string(),
                    span_id: "root".to_string(),
                    parent_span_id: String::new(),
                    service_name: "api".to_string(),
                    span_name: "request".to_string(),
                    span_kind: "SERVER".to_string(),
                    status_code: "STATUS_CODE_OK".to_string(),
                    start_time_unix_nano: 1,
                    end_time_unix_nano: 10,
                    duration_ms: 9.0,
                    resource_attributes: Default::default(),
                    attributes: Default::default(),
                    events: Vec::new(),
                    links: Vec::new(),
                    llm: None,
                },
                crate::domain::SpanDetail {
                    trace_id: "trace-1".to_string(),
                    span_id: "child".to_string(),
                    parent_span_id: "root".to_string(),
                    service_name: "api".to_string(),
                    span_name: "child".to_string(),
                    span_kind: "INTERNAL".to_string(),
                    status_code: "STATUS_CODE_OK".to_string(),
                    start_time_unix_nano: 2,
                    end_time_unix_nano: 9,
                    duration_ms: 7.0,
                    resource_attributes: Default::default(),
                    attributes: Default::default(),
                    events: Vec::new(),
                    links: Vec::new(),
                    llm: None,
                },
            ],
            logs: Vec::new(),
            metrics: Vec::new(),
            llm: Vec::new(),
            llm_rollups: Vec::new(),
            llm_sessions: Vec::new(),
            llm_model_comparisons: Vec::new(),
            llm_top_calls: Vec::new(),
            selected_llm_timeline: Vec::new(),
        };

        handle_mouse(
            MouseEvent {
                kind: MouseEventKind::Down(MouseButton::Left),
                column: 1,
                row: 8,
                modifiers: KeyModifiers::empty(),
            },
            Rect::new(0, 0, 120, 40),
            &mut state,
            &snapshot,
        );

        assert!(state.collapsed_trace_spans.contains("root"));
    }

    fn empty_overview() -> OverviewStats {
        OverviewStats {
            service_count: 0,
            trace_count: 0,
            error_span_count: 0,
            log_count: 0,
            metric_count: 0,
            llm_count: 0,
        }
    }

    fn snapshot_with_trace() -> DashboardSnapshot {
        DashboardSnapshot {
            services: Vec::new(),
            overview: OverviewStats {
                trace_count: 1,
                ..empty_overview()
            },
            traces: vec![TraceSummary {
                trace_id: "trace-1".to_string(),
                service_name: "api".to_string(),
                root_name: "request".to_string(),
                span_count: 3,
                error_count: 0,
                duration_ms: 42.0,
                started_at_unix_nano: 1,
            }],
            selected_trace: Vec::new(),
            logs: Vec::new(),
            metrics: Vec::new(),
            llm: Vec::new(),
            llm_rollups: Vec::new(),
            llm_sessions: Vec::new(),
            llm_model_comparisons: Vec::new(),
            llm_top_calls: Vec::new(),
            selected_llm_timeline: Vec::new(),
        }
    }

    fn span(span_id: &str, parent_span_id: &str, start_time_unix_nano: i64) -> SpanDetail {
        SpanDetail {
            trace_id: "trace-1".to_string(),
            span_id: span_id.to_string(),
            parent_span_id: parent_span_id.to_string(),
            service_name: "api".to_string(),
            span_name: span_id.to_string(),
            span_kind: "INTERNAL".to_string(),
            status_code: "STATUS_CODE_OK".to_string(),
            start_time_unix_nano,
            end_time_unix_nano: start_time_unix_nano + 10,
            duration_ms: 10.0,
            resource_attributes: Default::default(),
            attributes: Default::default(),
            events: Vec::new(),
            links: Vec::new(),
            llm: None,
        }
    }
}
