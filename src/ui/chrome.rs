use ratatui::{
    Frame,
    layout::{Alignment, Rect},
    prelude::{Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Clear, Paragraph, Wrap},
};

use crate::{
    commands,
    domain::{DashboardSnapshot, truncate},
    query::{LogCorrelationFilter, LogSeverityFilter},
};

use super::{IngestHealthView, LlmFocus, Palette, PaneFocus, Tab, TraceFocus, UiState, geometry};

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
            .block(super::style::panel(help_title(state), true, palette))
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
            .block(super::style::panel(
                context_help_title(state),
                true,
                palette,
            ))
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
        Paragraph::new(lines)
            .wrap(Wrap { trim: false })
            .block(super::style::panel("Command Palette", true, palette)),
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

/// The header's right-side status as `(hotkey, value)` pairs.
fn status_parts(snapshot: &DashboardSnapshot, state: &UiState) -> Vec<(&'static str, String)> {
    let mut parts = vec![
        ("t", state.time_window.label().to_string()),
        (
            "s",
            current_service(snapshot, state)
                .map_or_else(|| "all services".to_string(), str::to_string),
        ),
    ];
    if state.search_mode || !state.search_query.is_empty() {
        parts.push(("/", search_label(state)));
    }
    parts
}

/// The header status as plain text.
#[cfg(test)]
pub(crate) fn global_status_text(snapshot: &DashboardSnapshot, state: &UiState) -> String {
    let mut parts: Vec<String> = status_parts(snapshot, state)
        .into_iter()
        .map(|(_, value)| value)
        .collect();
    if let Some(health) = &state.ingest_health {
        parts.push(ingest_health_text(health));
    }
    parts.join(" · ")
}

/// One header row: brand, numbered tabs, and right-aligned status.
pub(crate) fn render_header(
    frame: &mut Frame<'_>,
    area: Rect,
    snapshot: &DashboardSnapshot,
    state: &UiState,
    palette: Palette,
) {
    frame.render_widget(
        Block::default().style(Style::default().bg(palette.surface)),
        area,
    );
    let mut left = vec![
        Span::styled(
            geometry::BRAND,
            Style::default()
                .fg(palette.background)
                .bg(palette.accent)
                .add_modifier(Modifier::BOLD),
        ),
        Span::raw(" "),
    ];
    for (index, tab) in Tab::ALL.into_iter().enumerate() {
        let label = geometry::tab_label(index, tab);
        left.push(if index == state.active_tab {
            Span::styled(
                label,
                Style::default()
                    .fg(palette.accent)
                    .bg(palette.selection)
                    .add_modifier(Modifier::BOLD),
            )
        } else {
            Span::styled(label, Style::default().fg(palette.muted))
        });
    }
    let left_width: usize = left.iter().map(|span| span.content.chars().count()).sum();

    let mut right = Vec::new();
    for (key, value) in status_parts(snapshot, state) {
        right.push(Span::styled(
            format!("{key} "),
            Style::default().fg(palette.muted),
        ));
        right.push(Span::styled(value, Style::default().fg(palette.foreground)));
        right.push(Span::raw("   "));
    }
    if let Some(health) = &state.ingest_health {
        right.push(Span::styled(
            ingest_health_text(health),
            Style::default().fg(ingest_health_color(health, palette)),
        ));
        right.push(Span::raw(" "));
    }
    let right_width: usize = right.iter().map(|span| span.content.chars().count()).sum();

    frame.render_widget(
        Paragraph::new(Line::from(left)).style(Style::default().bg(palette.surface)),
        area,
    );
    if left_width + right_width < area.width as usize {
        // Draw only over the status's own columns so the tabs keep their styling.
        let width = right_width as u16;
        frame.render_widget(
            Paragraph::new(Line::from(right)).style(Style::default().bg(palette.surface)),
            Rect::new(area.x + area.width - width, area.y, width, 1),
        );
    }
}

fn ingest_health_color(health: &IngestHealthView, palette: Palette) -> ratatui::prelude::Color {
    if health.retention_failures > 0 || health.last_failure.is_some() {
        palette.error
    } else if health.rejected_records > 0 || health.queued_records > 0 {
        palette.warning
    } else if health.records_per_second >= 0.5 {
        palette.success
    } else {
        palette.muted
    }
}

/// One footer row of styled, contextual key hints.
pub(crate) fn render_footer(frame: &mut Frame<'_>, area: Rect, state: &UiState, palette: Palette) {
    frame.render_widget(
        Paragraph::new(super::style::key_hints(&footer_text(state), palette))
            .style(Style::default().bg(palette.surface)),
        area,
    );
}

pub(crate) fn ingest_health_text(health: &IngestHealthView) -> String {
    let mut text = if health.records_per_second < 0.5 && health.queued_records == 0 {
        "ingest idle".to_string()
    } else {
        format!("ingest {}/s", compact_rate(health.records_per_second))
    };
    if let Some(p95) = health.recent_ack_p95 {
        text.push_str(&format!(" ack<={}ms", p95.as_millis()));
    }
    if health.queued_records > 0 {
        text.push_str(&format!(" queued={}", health.queued_records));
    }
    if health.rejected_records > 0 {
        text.push_str(&format!(" rejected={}", health.rejected_records));
    }
    if health.failed_requests > 0 {
        text.push_str(&format!(" failed={}", health.failed_requests));
    }
    if health.retention_failures > 0 {
        text.push_str(&format!(" retention_failed={}", health.retention_failures));
    }
    if let Some(failure) = &health.last_failure {
        text.push_str(&format!(
            " last: {} {}s ago",
            failure.label,
            failure.age.as_secs()
        ));
    }
    text
}

fn compact_rate(rate: f64) -> String {
    if rate >= 1_000.0 {
        format!("{:.1}k", rate / 1_000.0)
    } else {
        format!("{rate:.0}")
    }
}

pub(crate) fn footer_text(state: &UiState) -> String {
    if state.show_command_palette {
        return "palette: type filter | enter run | j/k move | esc close".to_string();
    }
    if state.show_help {
        return "help: esc close".to_string();
    }
    if state.search_mode {
        return "search: type filter | enter done | esc done | backspace delete".to_string();
    }
    if state.log_search_mode {
        return "log search: type filter | enter done | esc done | backspace delete".to_string();
    }

    let pinned = state.log_pinned_trace_id.is_some() || state.log_pinned_span_id.is_some();
    match Tab::ALL[state.active_tab] {
        Tab::Overview => {
            "overview: 1-5 tabs | / search | t window | s service | : commands | ? help".to_string()
        }
        Tab::Traces => match state.trace_focus {
            TraceFocus::TraceList => {
                "traces: j/k select | enter open | e next error | / search | : commands | ? help"
                    .to_string()
            }
            TraceFocus::TraceTree => {
                "trace: j/k move | l span detail | L logs | e next error | esc back | ? help"
                    .to_string()
            }
            TraceFocus::TraceDetail => {
                "span: j/k scroll | h tree | L logs | esc back | ? help".to_string()
            }
        },
        Tab::Logs => {
            let mut text = if state.logs_focus == PaneFocus::Primary {
                "logs: j/k move | l detail | f tail | x search | v severity | c correlation"
                    .to_string()
            } else {
                "log: j/k scroll | h feed | esc back".to_string()
            };
            if pinned {
                text.push_str(" | u unpin");
            }
            text.push_str(" | ? help");
            text
        }
        Tab::Metrics => {
            if state.metrics_focus == PaneFocus::Primary {
                "metrics: j/k select series | l detail | : commands | ? help".to_string()
            } else {
                "metric: j/k scroll | h series | esc back | ? help".to_string()
            }
        }
        Tab::Llm => match state.llm_focus {
            LlmFocus::Feed => "llm: j/k move | l detail | : commands | ? help".to_string(),
            LlmFocus::Detail => {
                "llm call: j/k scroll | l timeline | i prompt | o output | esc back | ? help"
                    .to_string()
            }
            LlmFocus::Timeline => "timeline: j/k scroll | h call | esc back | ? help".to_string(),
        },
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
        Tab::Llm => match state.llm_focus {
            LlmFocus::Feed => "Help: LLM Inspector".to_string(),
            LlmFocus::Detail => "Help: Model Detail".to_string(),
            LlmFocus::Timeline => "Help: Timeline".to_string(),
        },
    }
}

pub(crate) fn help_lines(state: &UiState) -> Vec<Line<'static>> {
    let mut lines = vec![
        Line::raw("global"),
        Line::raw("  tab / shift-tab  switch panes"),
        Line::raw("  : / ctrl-p       open command palette"),
        Line::raw("  /                global search"),
        Line::raw("  g                cycle theme"),
        Line::raw("  w                cycle layout preset"),
        Line::raw("  = / -            grow or shrink focused split"),
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
                lines.push(Line::raw("  shift-l          pivot to correlated logs"));
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
                lines.push(Line::raw("  shift-l          pivot to correlated logs"));
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
                if state.log_pinned_trace_id.is_some() || state.log_pinned_span_id.is_some() {
                    lines.push(Line::raw("  u                clear trace/span pin"));
                }
            } else {
                lines.push(Line::raw("log detail"));
                lines.push(Line::raw("  j / k            scroll detail"));
                lines.push(Line::raw("  esc / h / left   back to logs feed"));
                if state.log_pinned_trace_id.is_some() || state.log_pinned_span_id.is_some() {
                    lines.push(Line::raw("  u                clear trace/span pin"));
                }
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
        Tab::Llm => match state.llm_focus {
            LlmFocus::Feed => {
                lines.push(Line::raw("llm inspector"));
                lines.push(Line::raw("  j / k            move normalized llm spans"));
                lines.push(Line::raw(
                    "  S                cycle sort: time/tokens/cost/latency",
                ));
                lines.push(Line::raw("  l / right        focus model detail"));
                lines.push(Line::raw(
                    "  right pane       shows model/provider/token detail",
                ));
            }
            LlmFocus::Detail => {
                lines.push(Line::raw("model detail"));
                lines.push(Line::raw("  j / k            scroll detail"));
                lines.push(Line::raw("  l / right        focus timeline"));
                lines.push(Line::raw("  i                collapse or expand prompt"));
                lines.push(Line::raw("  o                expand or collapse output"));
                lines.push(Line::raw("  esc              back to llm inspector"));
                lines.push(Line::raw("  h / left         back to llm inspector"));
            }
            LlmFocus::Timeline => {
                lines.push(Line::raw("timeline"));
                lines.push(Line::raw("  j / k            scroll timeline"));
                lines.push(Line::raw("  h / left         focus model detail"));
                lines.push(Line::raw("  esc              back to llm inspector"));
            }
        },
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
        Tab::Llm => match state.llm_focus {
            LlmFocus::Feed => "Hints: LLM Inspector".to_string(),
            LlmFocus::Detail => "Hints: Model Detail".to_string(),
            LlmFocus::Timeline => "Hints: Timeline".to_string(),
        },
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
        Tab::Llm => match state.llm_focus {
            LlmFocus::Feed => {
                lines.push(Line::raw("Compare LLM activity:"));
                lines.push(Line::raw("  j/k     select call"));
                lines.push(Line::raw("  S       sort mode"));
                lines.push(Line::raw("  l       detail"));
                lines.push(Line::raw("  s/t     service/window"));
            }
            LlmFocus::Detail => {
                lines.push(Line::raw("Inspect prompt/output:"));
                lines.push(Line::raw("  j/k     scroll"));
                lines.push(Line::raw("  l       timeline"));
                lines.push(Line::raw("  i/o     toggle blocks"));
                lines.push(Line::raw("  h/esc   feed"));
            }
            LlmFocus::Timeline => {
                lines.push(Line::raw("Inspect timeline:"));
                lines.push(Line::raw("  j/k     scroll"));
                lines.push(Line::raw("  h       detail"));
                lines.push(Line::raw("  esc     feed"));
            }
        },
    }
    lines.push(Line::raw(""));
    lines.push(Line::raw("H closes hints  ? full help"));
    lines
}

/// Active log filters, shown after the feed title.
pub(crate) fn log_feed_context(state: &UiState, rows: usize) -> Vec<String> {
    let mut parts = vec![format!("{rows} rows")];
    if state.log_tail {
        parts.push("tailing".to_string());
    }
    if state.log_severity_filter != LogSeverityFilter::All {
        parts.push(format!("severity {}", state.log_severity_filter.label()));
    }
    if state.log_correlation_filter != LogCorrelationFilter::All {
        parts.push(state.log_correlation_filter.label().to_string());
    }
    if let Some(trace_id) = state.log_pinned_trace_id.as_deref() {
        parts.push(format!("trace {}", truncate(trace_id, 12)));
    }
    if let Some(span_id) = state.log_pinned_span_id.as_deref() {
        parts.push(format!("span {}", truncate(span_id, 12)));
    }
    if !state.log_search_query.is_empty() {
        parts.push(format!("\"{}\"", truncate(&state.log_search_query, 18)));
    }
    parts
}

pub(crate) fn trace_list_context(state: &UiState, rows: usize) -> Vec<String> {
    let mut parts = vec![format!("{rows} traces")];
    if state.errors_only {
        parts.push("errors only".to_string());
    }
    parts
}

pub(crate) fn trace_tree_context(state: &UiState) -> Vec<String> {
    if state.collapsed_trace_spans.is_empty() {
        Vec::new()
    } else {
        vec![format!("{} collapsed", state.collapsed_trace_spans.len())]
    }
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
