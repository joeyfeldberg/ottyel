use ratatui::{
    Frame,
    layout::{Alignment, Rect},
    prelude::{Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Clear, Paragraph, Wrap},
};

use crate::{
    commands,
    domain::{DashboardSnapshot, truncate},
    query::{LogCorrelationFilter, LogSeverityFilter},
};

use super::{Palette, PaneFocus, Tab, TraceFocus, TraceViewMode, UiState, geometry};

pub(crate) const COMMAND_PALETTE_VISIBLE_ROWS: usize = 8;

pub(crate) fn render_help_overlay(
    frame: &mut Frame<'_>,
    area: Rect,
    state: &UiState,
    palette: Palette,
) {
    let popup = geometry::centered_rect(70, 58, area);
    frame.render_widget(Clear, popup);
    frame.render_widget(
        Paragraph::new(help_lines(state))
            .wrap(Wrap { trim: false })
            .block(
                Block::default()
                    .title(help_title(state))
                    .borders(Borders::ALL)
                    .border_style(Style::default().fg(palette.warning)),
            )
            .style(Style::default().fg(palette.foreground))
            .alignment(Alignment::Left),
        popup,
    );
}

pub(crate) fn render_context_help_overlay(
    frame: &mut Frame<'_>,
    area: Rect,
    state: &UiState,
    palette: Palette,
) {
    let width = 38_u16.min(area.width.saturating_sub(4)).max(24);
    let height = (context_help_lines(state).len() as u16)
        .saturating_add(2)
        .min(area.height.saturating_sub(4))
        .max(6);
    let popup = Rect {
        x: area.x + area.width.saturating_sub(width + 2),
        y: area.y + 4,
        width,
        height,
    };
    frame.render_widget(Clear, popup);
    frame.render_widget(
        Paragraph::new(context_help_lines(state))
            .wrap(Wrap { trim: false })
            .block(
                Block::default()
                    .title(context_help_title(state))
                    .borders(Borders::ALL)
                    .border_style(Style::default().fg(palette.accent)),
            )
            .style(Style::default().fg(palette.foreground))
            .alignment(Alignment::Left),
        popup,
    );
}

pub(crate) fn render_wheel_debug_overlay(
    frame: &mut Frame<'_>,
    area: Rect,
    state: &UiState,
    palette: Palette,
) {
    let width = 88_u16.min(area.width.saturating_sub(4)).max(42);
    let height = 18_u16.min(area.height.saturating_sub(4)).max(8);
    let popup = Rect {
        x: area.x + area.width.saturating_sub(width + 2),
        y: area.y + area.height.saturating_sub(height + 2),
        width,
        height,
    };
    let mut lines = vec![Line::from(Span::styled(
        "/tmp/ottyel-wheel.log",
        Style::default().fg(palette.muted),
    ))];
    lines.push(Line::raw(""));
    if state.wheel_debug_events.is_empty() {
        lines.push(Line::raw("No wheel events captured yet."));
    } else {
        lines.extend(state.wheel_debug_events.iter().cloned().map(Line::raw));
    }

    frame.render_widget(Clear, popup);
    frame.render_widget(
        Paragraph::new(lines)
            .wrap(Wrap { trim: false })
            .block(
                Block::default()
                    .title("Wheel Debug")
                    .borders(Borders::ALL)
                    .border_style(Style::default().fg(palette.warning)),
            )
            .style(Style::default().fg(palette.foreground))
            .alignment(Alignment::Left),
        popup,
    );
}

pub(crate) fn render_command_palette(
    frame: &mut Frame<'_>,
    area: Rect,
    state: &UiState,
    palette: Palette,
) {
    let popup = geometry::centered_rect(68, 52, area);
    let commands = commands::matching_commands(&state.command_query);
    let mut lines = vec![Line::from(vec![
        Span::styled("query ", Style::default().fg(palette.muted)),
        Span::styled(
            if state.command_query.is_empty() {
                ":".to_string()
            } else {
                format!(":{}", state.command_query)
            },
            Style::default()
                .fg(palette.foreground)
                .add_modifier(Modifier::BOLD),
        ),
    ])];
    lines.push(Line::raw(""));

    if commands.is_empty() {
        lines.push(Line::raw("No matching commands."));
    } else {
        let (start, end) = command_palette_window(
            state.command_palette_scroll,
            state.selected_command,
            commands.len(),
        );
        for (index, command) in commands.iter().enumerate().skip(start).take(end - start) {
            let selected = index == state.selected_command;
            let style = if selected {
                Style::default().fg(palette.background).bg(palette.accent)
            } else {
                Style::default().fg(palette.foreground)
            };
            lines.push(Line::from(vec![
                Span::styled(
                    if selected { " > " } else { "   " },
                    Style::default().fg(palette.muted).patch(style),
                ),
                Span::styled(command.title, style),
            ]));
        }
    }

    lines.push(Line::raw(""));
    lines.push(Line::from(Span::styled(
        "enter run  j/k move  esc close",
        Style::default().fg(palette.muted),
    )));

    frame.render_widget(Clear, popup);
    frame.render_widget(
        Paragraph::new(lines).wrap(Wrap { trim: false }).block(
            Block::default()
                .title("Command Palette")
                .borders(Borders::ALL)
                .border_style(Style::default().fg(palette.accent)),
        ),
        popup,
    );
}

pub(crate) fn command_palette_window(
    current_offset: usize,
    selected: usize,
    total: usize,
) -> (usize, usize) {
    if total <= COMMAND_PALETTE_VISIBLE_ROWS {
        return (0, total);
    }

    let start = current_offset.min(total.saturating_sub(COMMAND_PALETTE_VISIBLE_ROWS));
    let end = (start + COMMAND_PALETTE_VISIBLE_ROWS).min(total);
    if selected < start {
        let adjusted_start = selected;
        return (
            adjusted_start,
            (adjusted_start + COMMAND_PALETTE_VISIBLE_ROWS).min(total),
        );
    }
    if selected >= end {
        let adjusted_end = (selected + 1).min(total);
        let adjusted_start = adjusted_end.saturating_sub(COMMAND_PALETTE_VISIBLE_ROWS);
        return (adjusted_start, adjusted_end);
    }
    (start, end)
}

pub(crate) fn global_status_text(snapshot: &DashboardSnapshot, state: &UiState) -> String {
    let mut parts = vec![
        format!("theme={}", state.theme.label()),
        format!("[t]ime={}", state.time_window.label()),
        format!(
            "[s]ervice={}",
            current_service(snapshot, state).unwrap_or("all")
        ),
    ];
    if state.search_mode || !state.search_query.is_empty() {
        parts.push(format!("search={}", search_label(state)));
    }
    if state.show_wheel_debug {
        parts.push("wheel-debug".to_string());
    }
    parts.push(format!(
        "panes traces={} logs={} metrics={} llm={}",
        snapshot.overview.trace_count,
        snapshot.overview.log_count,
        snapshot.overview.metric_count,
        snapshot.overview.llm_count,
    ));
    parts.join(" | ")
}

pub(crate) fn footer_text(state: &UiState) -> String {
    if state.show_command_palette {
        return "command palette: type to filter | enter run | j/k move | esc close".to_string();
    }
    if state.show_help {
        return "help: esc/?/enter close".to_string();
    }
    if state.search_mode {
        return "global search: type to filter | enter/esc close | backspace delete".to_string();
    }
    if state.log_search_mode {
        return "log search: type to filter logs | enter/esc close | backspace delete".to_string();
    }

    match Tab::ALL[state.active_tab] {
        Tab::Overview => {
            "overview: tab switch panes | : commands | ? help | H hints | / global search"
                .to_string()
        }
        Tab::Traces => match state.trace_focus {
            TraceFocus::TraceList => {
                "traces: j/k select trace | enter open | : commands | ? help | e errors".to_string()
            }
            TraceFocus::TraceTree => {
                "trace tree: j/k move | l/right detail | esc list | : commands | ? help | e errors"
                    .to_string()
            }
            TraceFocus::TraceDetail => {
                "span detail: j/k scroll | h/left tree | esc list | : commands | ? help | e errors"
                    .to_string()
            }
        },
        Tab::Logs => {
            if state.logs_focus == PaneFocus::Primary {
                "logs: j/k move | l/right detail | f tail | x log search | v severity | c correlation | : commands"
                    .to_string()
            } else {
                "log detail: j/k scroll | esc/h/left feed | : commands".to_string()
            }
        }
        Tab::Metrics => {
            if state.metrics_focus == PaneFocus::Primary {
                "metrics: j/k move | l/right detail | : commands".to_string()
            } else {
                "metric detail: j/k scroll | esc/h/left feed | : commands".to_string()
            }
        }
        Tab::Llm => {
            if state.llm_focus == PaneFocus::Primary {
                "llm: j/k move | l/right detail | : commands".to_string()
            } else {
                "model detail: j/k scroll | i/o toggle blocks | esc/h/left feed | : commands"
                    .to_string()
            }
        }
    }
}

pub(crate) fn help_title(state: &UiState) -> String {
    match Tab::ALL[state.active_tab] {
        Tab::Overview => "Help: Overview".to_string(),
        Tab::Traces => match state.trace_focus {
            TraceFocus::TraceList => "Help: Trace List".to_string(),
            TraceFocus::TraceTree => "Help: Trace Tree".to_string(),
            TraceFocus::TraceDetail => "Help: Span Detail".to_string(),
        },
        Tab::Logs => {
            if state.logs_focus == PaneFocus::Primary {
                "Help: Logs Feed".to_string()
            } else {
                "Help: Log Detail".to_string()
            }
        }
        Tab::Metrics => {
            if state.metrics_focus == PaneFocus::Primary {
                "Help: Metrics Feed".to_string()
            } else {
                "Help: Metric Detail".to_string()
            }
        }
        Tab::Llm => {
            if state.llm_focus == PaneFocus::Primary {
                "Help: LLM Inspector".to_string()
            } else {
                "Help: Model Detail".to_string()
            }
        }
    }
}

pub(crate) fn help_lines(state: &UiState) -> Vec<Line<'static>> {
    let mut lines = vec![
        Line::raw("global"),
        Line::raw("  tab / shift-tab  switch panes"),
        Line::raw("  : / ctrl-p       open command palette"),
        Line::raw("  /                global search"),
        Line::raw("  g                cycle theme"),
        Line::raw("  s                cycle service filter"),
        Line::raw("  t                cycle time window"),
        Line::raw("  ?                open/close help"),
        Line::raw("  H                toggle contextual hints"),
        Line::raw("  mouse            click to focus/select, wheel to scroll"),
        Line::raw("  q                quit"),
        Line::raw(""),
    ];

    match Tab::ALL[state.active_tab] {
        Tab::Overview => {
            lines.push(Line::raw("overview"));
            lines.push(Line::raw("  use tab to move to investigation panes"));
        }
        Tab::Traces => match state.trace_focus {
            TraceFocus::TraceList => {
                lines.push(Line::raw("trace list"));
                lines.push(Line::raw("  j / k            move traces"));
                lines.push(Line::raw("  enter            open selected trace"));
                lines.push(Line::raw("  e                toggle errors-only traces"));
            }
            TraceFocus::TraceTree => {
                lines.push(Line::raw("trace tree"));
                lines.push(Line::raw("  j / k            move visible spans"));
                lines.push(Line::raw("  [ / ]            previous/next error span"));
                lines.push(Line::raw("  p                jump to parent span"));
                lines.push(Line::raw("  r                jump to root span"));
                lines.push(Line::raw("  m                jump to first llm span"));
                lines.push(Line::raw("  esc              back to trace list"));
                lines.push(Line::raw("  l / right        focus span detail"));
                lines.push(Line::raw("  space / enter    collapse or expand subtree"));
                lines.push(Line::raw("  e                toggle errors-only traces"));
            }
            TraceFocus::TraceDetail => {
                lines.push(Line::raw("span detail"));
                lines.push(Line::raw("  j / k            scroll detail"));
                lines.push(Line::raw("  [ / ]            previous/next error span"));
                lines.push(Line::raw("  p                jump to parent span"));
                lines.push(Line::raw("  r                jump to root span"));
                lines.push(Line::raw("  m                jump to first llm span"));
                lines.push(Line::raw("  h / left         focus trace tree"));
                lines.push(Line::raw("  esc              back to trace list"));
            }
        },
        Tab::Logs => {
            if state.logs_focus == PaneFocus::Primary {
                lines.push(Line::raw("logs feed"));
                lines.push(Line::raw("  j / k            move logs and disable tail"));
                lines.push(Line::raw("  l / right        focus log detail"));
                lines.push(Line::raw("  f                toggle tail/follow mode"));
                lines.push(Line::raw("  x                log-only text search"));
                lines.push(Line::raw("  v                cycle severity filter"));
                lines.push(Line::raw("  c                cycle correlation filter"));
            } else {
                lines.push(Line::raw("log detail"));
                lines.push(Line::raw("  j / k            scroll detail"));
                lines.push(Line::raw("  esc / h / left   back to logs feed"));
            }
        }
        Tab::Metrics => {
            if state.metrics_focus == PaneFocus::Primary {
                lines.push(Line::raw("metrics feed"));
                lines.push(Line::raw("  j / k            move metric selection"));
                lines.push(Line::raw("  l / right        focus metric detail"));
                lines.push(Line::raw(
                    "  right pane       shows trend and stats for selection",
                ));
            } else {
                lines.push(Line::raw("metric detail"));
                lines.push(Line::raw("  j / k            scroll detail"));
                lines.push(Line::raw("  esc / h / left   back to metrics feed"));
            }
        }
        Tab::Llm => {
            if state.llm_focus == PaneFocus::Primary {
                lines.push(Line::raw("llm inspector"));
                lines.push(Line::raw("  j / k            move normalized llm spans"));
                lines.push(Line::raw("  l / right        focus model detail"));
                lines.push(Line::raw(
                    "  right pane       shows model/provider/token detail",
                ));
            } else {
                lines.push(Line::raw("model detail"));
                lines.push(Line::raw("  j / k            scroll detail"));
                lines.push(Line::raw("  i                expand or collapse prompt"));
                lines.push(Line::raw("  o                expand or collapse output"));
                lines.push(Line::raw("  esc / h / left   back to llm inspector"));
            }
        }
    }

    if state.search_mode {
        lines.push(Line::raw(""));
        lines.push(Line::raw("global search mode is active"));
        lines.push(Line::raw(
            "  type to edit, backspace to delete, enter/esc to close",
        ));
    }
    if state.log_search_mode {
        lines.push(Line::raw(""));
        lines.push(Line::raw("log search mode is active"));
        lines.push(Line::raw(
            "  type to edit, backspace to delete, enter/esc to close",
        ));
    }
    if state.show_command_palette {
        lines.push(Line::raw(""));
        lines.push(Line::raw("command palette is active"));
        lines.push(Line::raw(
            "  type to edit, j/k to move, enter to run, esc to close",
        ));
    }

    lines
}

pub(crate) fn context_help_title(state: &UiState) -> String {
    match Tab::ALL[state.active_tab] {
        Tab::Overview => "Hints: Overview".to_string(),
        Tab::Traces => match state.trace_focus {
            TraceFocus::TraceList => "Hints: Trace List".to_string(),
            TraceFocus::TraceTree => "Hints: Trace Tree".to_string(),
            TraceFocus::TraceDetail => "Hints: Span Detail".to_string(),
        },
        Tab::Logs => {
            if state.logs_focus == PaneFocus::Primary {
                "Hints: Logs Feed".to_string()
            } else {
                "Hints: Log Detail".to_string()
            }
        }
        Tab::Metrics => {
            if state.metrics_focus == PaneFocus::Primary {
                "Hints: Metrics Feed".to_string()
            } else {
                "Hints: Metric Detail".to_string()
            }
        }
        Tab::Llm => {
            if state.llm_focus == PaneFocus::Primary {
                "Hints: LLM Inspector".to_string()
            } else {
                "Hints: Model Detail".to_string()
            }
        }
    }
}

pub(crate) fn context_help_lines(state: &UiState) -> Vec<Line<'static>> {
    let mut lines = Vec::new();
    match Tab::ALL[state.active_tab] {
        Tab::Overview => {
            lines.push(Line::raw("Start from a signal pane:"));
            lines.push(Line::raw("  tab     switch panes"));
            lines.push(Line::raw("  s/t     service/window"));
            lines.push(Line::raw("  /       global search"));
        }
        Tab::Traces => match state.trace_focus {
            TraceFocus::TraceList => {
                lines.push(Line::raw("Pick a trace to inspect:"));
                lines.push(Line::raw("  j/k     select trace"));
                lines.push(Line::raw("  enter   open tree"));
                lines.push(Line::raw("  e       errors only"));
            }
            TraceFocus::TraceTree => {
                lines.push(Line::raw("Navigate visible spans:"));
                lines.push(Line::raw("  j/k     move"));
                lines.push(Line::raw("  space   collapse subtree"));
                lines.push(Line::raw("  l       span detail"));
                lines.push(Line::raw("  esc     trace list"));
            }
            TraceFocus::TraceDetail => {
                lines.push(Line::raw("Inspect selected span:"));
                lines.push(Line::raw("  j/k     scroll detail"));
                lines.push(Line::raw("  h       back to tree"));
                lines.push(Line::raw("  [/]     prev/next error"));
            }
        },
        Tab::Logs => {
            if state.logs_focus == PaneFocus::Primary {
                lines.push(Line::raw("Filter and correlate logs:"));
                lines.push(Line::raw("  v       severity"));
                lines.push(Line::raw("  c       correlation"));
                lines.push(Line::raw("  f       tail mode"));
                lines.push(Line::raw("  l       detail"));
            } else {
                lines.push(Line::raw("Inspect log payload:"));
                lines.push(Line::raw("  j/k     scroll"));
                lines.push(Line::raw("  h/esc   feed"));
            }
        }
        Tab::Metrics => {
            if state.metrics_focus == PaneFocus::Primary {
                lines.push(Line::raw("Select a metric series:"));
                lines.push(Line::raw("  j/k     move"));
                lines.push(Line::raw("  l       trend/detail"));
                lines.push(Line::raw("  s/t     service/window"));
            } else {
                lines.push(Line::raw("Read trend and stats:"));
                lines.push(Line::raw("  j/k     scroll"));
                lines.push(Line::raw("  h/esc   feed"));
            }
        }
        Tab::Llm => {
            if state.llm_focus == PaneFocus::Primary {
                lines.push(Line::raw("Compare LLM activity:"));
                lines.push(Line::raw("  j/k     select call"));
                lines.push(Line::raw("  l       detail"));
                lines.push(Line::raw("  s/t     service/window"));
            } else {
                lines.push(Line::raw("Inspect prompt/output:"));
                lines.push(Line::raw("  j/k     scroll"));
                lines.push(Line::raw("  i/o     expand blocks"));
                lines.push(Line::raw("  h/esc   feed"));
            }
        }
    }
    lines.push(Line::raw(""));
    lines.push(Line::raw("H closes hints  ? full help"));
    lines
}

pub(crate) fn log_feed_title(state: &UiState) -> String {
    let mut parts = vec!["Logs Feed".to_string()];
    if state.log_tail {
        parts.push("tail".to_string());
    }
    if state.log_severity_filter != LogSeverityFilter::All {
        parts.push(format!("sev={}", state.log_severity_filter.label()));
    }
    if state.log_correlation_filter != LogCorrelationFilter::All {
        parts.push(format!("corr={}", state.log_correlation_filter.label()));
    }
    if !state.log_search_query.is_empty() {
        parts.push(format!("text={}", truncate(&state.log_search_query, 18)));
    }

    titled(parts)
}

pub(crate) fn trace_list_title(state: &UiState) -> String {
    let mut parts = vec!["Trace Explorer".to_string()];
    if state.errors_only {
        parts.push("errors-only".to_string());
    }
    if state.trace_view_mode == TraceViewMode::List {
        parts.push("focus".to_string());
        parts.push("enter=open".to_string());
    }
    titled(parts)
}

pub(crate) fn trace_tree_title(state: &UiState) -> String {
    let mut parts = vec!["Trace Tree".to_string()];
    if state.trace_focus == TraceFocus::TraceTree {
        parts.push("focus".to_string());
    }
    if !state.collapsed_trace_spans.is_empty() {
        parts.push(format!("collapsed={}", state.collapsed_trace_spans.len()));
    }
    titled(parts)
}

pub(crate) fn trace_detail_title(state: &UiState) -> String {
    detail_title("Span Detail", state.trace_focus == TraceFocus::TraceDetail)
}

pub(crate) fn detail_title(base: &str, focused: bool) -> String {
    if focused {
        format!("{base} [focus]")
    } else {
        base.to_string()
    }
}

pub(crate) fn titled(mut parts: Vec<String>) -> String {
    if parts.len() == 1 {
        parts.remove(0)
    } else {
        format!("{} [{}]", parts.remove(0), parts.join(" | "))
    }
}

pub(crate) fn padded(text: String) -> String {
    format!(" {text} ")
}

fn current_service<'a>(snapshot: &'a DashboardSnapshot, state: &UiState) -> Option<&'a str> {
    state
        .service_filter_index
        .and_then(|idx| snapshot.services.get(idx))
        .map(String::as_str)
}

fn search_label(state: &UiState) -> String {
    if state.search_query.is_empty() {
        if state.search_mode {
            "/".to_string()
        } else {
            "-".to_string()
        }
    } else if state.search_mode {
        format!("/{}/", state.search_query)
    } else {
        state.search_query.clone()
    }
}
