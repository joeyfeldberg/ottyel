use crossterm::event::{KeyCode, KeyModifiers};

use crate::{
    commands::{self, PaletteAction},
    domain::DashboardSnapshot,
    query::{LogCorrelationFilter, LogFilters, LogSeverityFilter, QueryFilters, TimeWindow},
    ui::{PaneFocus, Tab, TraceFocus, TraceViewMode, UiState},
};

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
        KeyCode::Char('c') if modifiers.contains(KeyModifiers::CONTROL) => return true,
        KeyCode::Char('x') if Tab::ALL[state.active_tab] == Tab::Logs => {
            state.log_search_mode = true;
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
        KeyCode::Down | KeyCode::Char('j') => move_selection(1, state, snapshot),
        KeyCode::Up | KeyCode::Char('k') => move_selection(-1, state, snapshot),
        _ => {}
    }
    false
}

pub(super) fn sync_selection(state: &mut UiState, snapshot: &DashboardSnapshot) {
    state.selected_trace = state
        .selected_trace
        .min(snapshot.traces.len().saturating_sub(1));
    state.selected_trace_span = state
        .selected_trace_span
        .min(crate::ui::visible_trace_tree_len(snapshot, state).saturating_sub(1));
    if snapshot.selected_trace.is_empty() {
        state.trace_view_mode = TraceViewMode::List;
        state.trace_focus = TraceFocus::TraceList;
        state.trace_tree_scroll = 0;
        state.trace_detail_scroll = 0;
        state.collapsed_trace_spans.clear();
    }
    state.selected_log = state
        .selected_log
        .min(snapshot.logs.len().saturating_sub(1));
    if state.log_tail && !snapshot.logs.is_empty() {
        state.selected_log = 0;
    }
    state.selected_metric = state
        .selected_metric
        .min(snapshot.metrics.len().saturating_sub(1));
    state.selected_llm = state.selected_llm.min(snapshot.llm.len().saturating_sub(1));

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
    state.search_mode = false;
    state.log_search_mode = false;
    state.show_command_palette = true;
    state.command_query.clear();
    state.selected_command = 0;
}

fn close_command_palette(state: &mut UiState) {
    state.show_command_palette = false;
    state.command_query.clear();
    state.selected_command = 0;
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
    move_index(&mut state.selected_command, commands.len(), delta);
}

fn clamp_palette_selection(state: &mut UiState) {
    let len = commands::matching_commands(&state.command_query).len();
    state.selected_command = state.selected_command.min(len.saturating_sub(1));
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
