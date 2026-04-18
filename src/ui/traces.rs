use std::collections::{HashMap, HashSet};

use chrono::{DateTime, Local, Utc};
use ratatui::{
    Frame,
    layout::{Constraint, Rect},
    prelude::Style,
    text::{Line, Span},
    widgets::{Block, Borders, Cell, Paragraph, Row, Table, Wrap},
};
use serde_json::Value;

use crate::domain::{DashboardSnapshot, SpanDetail, truncate};

use super::{Palette, TraceFocus, TraceViewMode, UiState, chrome, geometry};

pub(crate) fn render(
    frame: &mut Frame<'_>,
    area: Rect,
    snapshot: &DashboardSnapshot,
    state: &UiState,
    trace_detail_lines: &[Line<'static>],
    palette: Palette,
) {
    let rows: Vec<Row<'_>> = snapshot
        .traces
        .iter()
        .enumerate()
        .skip(state.trace_list_scroll)
        .take(geometry::table_viewport_height(area))
        .map(|(idx, trace)| {
            let style = if idx == state.selected_trace {
                Style::default().fg(palette.background).bg(palette.accent)
            } else {
                Style::default().fg(palette.foreground)
            };
            Row::new(vec![
                Cell::from(format_trace_timestamp(trace.started_at_unix_nano)),
                Cell::from(truncate(&trace.service_name, 12)),
                Cell::from(truncate(&simplify_wrapper_name(&trace.root_name), 24)),
                Cell::from(trace.span_count.to_string()),
                Cell::from(trace.error_count.to_string()),
                Cell::from(format!("{:.1}", trace.duration_ms)),
            ])
            .style(style)
        })
        .collect();
    let table = Table::new(
        rows,
        [
            Constraint::Length(19),
            Constraint::Length(12),
            Constraint::Min(20),
            Constraint::Length(6),
            Constraint::Length(6),
            Constraint::Length(8),
        ],
    )
    .header(
        Row::new(vec!["time", "service", "root", "spans", "errs", "ms"]).style(
            Style::default()
                .fg(palette.muted)
                .add_modifier(ratatui::prelude::Modifier::BOLD),
        ),
    )
    .block(
        Block::default()
            .title(chrome::trace_list_title(state))
            .borders(Borders::ALL)
            .border_style(
                Style::default().fg(if state.trace_view_mode == TraceViewMode::List {
                    palette.accent
                } else {
                    palette.muted
                }),
            ),
    );
    if state.trace_view_mode == TraceViewMode::List {
        frame.render_widget(table, area);
        return;
    }

    let [tree_area, detail_area] = geometry::trace_detail_sections(area, state.trace_split_pct);
    let tree_border = if state.trace_focus == TraceFocus::TraceTree {
        palette.warning
    } else {
        palette.muted
    };

    let tree_rows = trace_tree_rows(&snapshot.selected_trace, &state.collapsed_trace_spans);
    let window = trace_window(&snapshot.selected_trace);
    let tree_line_width = tree_area.width.saturating_sub(2) as usize;
    let tree_lines = snapshot
        .traces
        .get(state.selected_trace)
        .map(|trace| {
            vec![
                Line::from(vec![
                    Span::styled(
                        truncate(&trace.trace_id, 18),
                        Style::default().fg(palette.accent),
                    ),
                    Span::raw(" "),
                    Span::styled(
                        truncate(&trace.root_name, 30),
                        Style::default().fg(palette.foreground),
                    ),
                ]),
                Line::from(format!(
                    "service={} duration={:.1}ms errors={}",
                    trace.service_name, trace.duration_ms, trace.error_count
                )),
                Line::raw(""),
            ]
            .into_iter()
            .chain(build_trace_tree_lines(
                &tree_rows,
                state.selected_trace_span,
                state.trace_focus == TraceFocus::TraceTree,
                window,
                tree_line_width,
                palette,
            ))
            .collect::<Vec<_>>()
        })
        .unwrap_or_else(|| vec![Line::raw("No trace selected yet.")]);
    frame.render_widget(
        Paragraph::new(tree_lines)
            .scroll((
                u16::try_from(state.trace_tree_scroll).unwrap_or(u16::MAX),
                0,
            ))
            .wrap(Wrap { trim: false })
            .block(
                Block::default()
                    .title(chrome::trace_tree_title(state))
                    .borders(Borders::ALL)
                    .border_style(Style::default().fg(tree_border)),
            ),
        tree_area,
    );

    let detail_border = if state.trace_focus == TraceFocus::TraceDetail {
        palette.accent
    } else {
        palette.muted
    };
    frame.render_widget(
        Paragraph::new(trace_detail_lines.to_vec())
            .scroll((state.trace_detail_scroll, 0))
            .wrap(Wrap { trim: false })
            .block(
                Block::default()
                    .title(chrome::trace_detail_title(state))
                    .borders(Borders::ALL)
                    .border_style(Style::default().fg(detail_border)),
            ),
        detail_area,
    );
}

pub(crate) fn format_trace_timestamp(started_at_unix_nano: i64) -> String {
    format_machine_local_time(started_at_unix_nano)
}

pub(crate) fn format_machine_local_time(unix_nano: i64) -> String {
    let seconds = unix_nano.div_euclid(1_000_000_000);
    let nanos = unix_nano.rem_euclid(1_000_000_000) as u32;
    match DateTime::<Utc>::from_timestamp(seconds, nanos) {
        Some(utc) => {
            let local = utc.with_timezone(&Local);
            if local.date_naive() == Local::now().date_naive() {
                local.format("%H:%M:%S").to_string()
            } else {
                local.format("%Y-%m-%d %H:%M:%S").to_string()
            }
        }
        None => "invalid-time".to_string(),
    }
}

pub(crate) fn trace_tree_rows(
    spans: &[SpanDetail],
    collapsed_span_ids: &HashSet<String>,
) -> Vec<TraceTreeRow> {
    if spans.is_empty() {
        return Vec::new();
    }

    let mut spans_by_id = HashMap::with_capacity(spans.len());
    let mut children_by_parent: HashMap<String, Vec<usize>> = HashMap::new();
    let mut roots = Vec::new();

    for (index, span) in spans.iter().enumerate() {
        spans_by_id.insert(span.span_id.clone(), index);
    }

    for (index, span) in spans.iter().enumerate() {
        if span.parent_span_id.is_empty() || !spans_by_id.contains_key(&span.parent_span_id) {
            roots.push(index);
        } else {
            children_by_parent
                .entry(span.parent_span_id.clone())
                .or_default()
                .push(index);
        }
    }

    sort_span_indexes(&mut roots, spans);
    for children in children_by_parent.values_mut() {
        sort_span_indexes(children, spans);
    }

    let critical_path_span_ids = critical_path_span_ids(&roots, spans, &children_by_parent);

    let mut rows = Vec::with_capacity(spans.len());
    for root_index in &roots {
        push_tree_rows(
            *root_index,
            spans,
            &children_by_parent,
            collapsed_span_ids,
            &critical_path_span_ids,
            &mut rows,
            0,
        );
    }

    rows
}

pub fn visible_trace_tree_len(snapshot: &DashboardSnapshot, state: &UiState) -> usize {
    trace_tree_rows(&snapshot.selected_trace, &state.collapsed_trace_spans).len()
}

pub fn selected_trace_tree_span(
    snapshot: &DashboardSnapshot,
    state: &UiState,
) -> Option<(String, bool)> {
    let rows = trace_tree_rows(&snapshot.selected_trace, &state.collapsed_trace_spans);
    selected_trace_row(&rows, state.selected_trace_span)
        .map(|row| (row.span.span_id.clone(), row.has_children))
}

pub(crate) fn selected_trace_span_detail(
    snapshot: &DashboardSnapshot,
    state: &UiState,
) -> Option<SpanDetail> {
    let rows = trace_tree_rows(&snapshot.selected_trace, &state.collapsed_trace_spans);
    selected_trace_row(&rows, state.selected_trace_span).map(|row| row.span.clone())
}

pub fn previous_error_trace_index(snapshot: &DashboardSnapshot, state: &UiState) -> Option<usize> {
    let rows = trace_tree_rows(&snapshot.selected_trace, &state.collapsed_trace_spans);
    let current = state.selected_trace_span.min(rows.len().saturating_sub(1));
    (0..current)
        .rev()
        .find(|index| rows[*index].span.status_code == "STATUS_CODE_ERROR")
}

pub fn next_error_trace_index(snapshot: &DashboardSnapshot, state: &UiState) -> Option<usize> {
    let rows = trace_tree_rows(&snapshot.selected_trace, &state.collapsed_trace_spans);
    let start = state.selected_trace_span.saturating_add(1);
    (start..rows.len()).find(|index| rows[*index].span.status_code == "STATUS_CODE_ERROR")
}

pub fn parent_trace_index(snapshot: &DashboardSnapshot, state: &UiState) -> Option<usize> {
    let rows = trace_tree_rows(&snapshot.selected_trace, &state.collapsed_trace_spans);
    let row = selected_trace_row(&rows, state.selected_trace_span)?;
    if row.span.parent_span_id.is_empty() {
        return None;
    }

    rows.iter()
        .position(|candidate| candidate.span.span_id == row.span.parent_span_id)
}

pub fn root_trace_index(snapshot: &DashboardSnapshot, state: &UiState) -> Option<usize> {
    let rows = trace_tree_rows(&snapshot.selected_trace, &state.collapsed_trace_spans);
    let row = selected_trace_row(&rows, state.selected_trace_span)?;

    let spans_by_id = snapshot
        .selected_trace
        .iter()
        .map(|span| (span.span_id.as_str(), span))
        .collect::<HashMap<_, _>>();

    let mut current = row.span.span_id.as_str();
    let mut root = current;
    while let Some(span) = spans_by_id.get(current) {
        if span.parent_span_id.is_empty() {
            root = span.span_id.as_str();
            break;
        }
        root = span.parent_span_id.as_str();
        current = span.parent_span_id.as_str();
    }

    rows.iter()
        .position(|candidate| candidate.span.span_id == root)
}

pub fn first_llm_trace_index(snapshot: &DashboardSnapshot, state: &UiState) -> Option<usize> {
    let rows = trace_tree_rows(&snapshot.selected_trace, &state.collapsed_trace_spans);
    rows.iter().position(|row| row.span.llm.is_some())
}

pub(crate) fn trace_tree_hit(
    snapshot: &DashboardSnapshot,
    state: &UiState,
    area: Rect,
    column: u16,
    row: u16,
) -> Option<(usize, bool)> {
    let content_top = area.y.saturating_add(1);
    let content_bottom = area.y.saturating_add(area.height).saturating_sub(1);
    if row < content_top || row >= content_bottom {
        return None;
    }

    let content_row = usize::from(row - content_top);
    let tree_index = state
        .trace_tree_scroll
        .saturating_add(content_row)
        .saturating_sub(3);
    let rows = trace_tree_rows(&snapshot.selected_trace, &state.collapsed_trace_spans);
    let tree_row = rows.get(tree_index)?;
    let disclosure_end = area
        .x
        .saturating_add(1)
        .saturating_add(u16::try_from(tree_row.depth.saturating_mul(2) + 2).unwrap_or(u16::MAX));
    let clicked_disclosure = tree_row.has_children && column < disclosure_end;
    Some((tree_index, clicked_disclosure))
}

pub(crate) fn trace_tree_selected_line(state: &UiState, tree_rows: &[TraceTreeRow]) -> usize {
    const TREE_HEADER_LINES: usize = 3;
    if tree_rows.is_empty() {
        return 0;
    }
    TREE_HEADER_LINES
        + state
            .selected_trace_span
            .min(tree_rows.len().saturating_sub(1))
}

pub(crate) fn trace_tree_total_lines(tree_rows: &[TraceTreeRow], no_trace_selected: bool) -> usize {
    if no_trace_selected {
        1
    } else {
        3 + tree_rows.len()
    }
}

pub(crate) fn selected_trace_row(
    rows: &[TraceTreeRow],
    selected_index: usize,
) -> Option<&TraceTreeRow> {
    rows.get(selected_index)
}

pub(crate) fn trace_window(spans: &[SpanDetail]) -> TraceWindow {
    let start_unix_nano = spans
        .iter()
        .map(|span| span.start_time_unix_nano)
        .min()
        .unwrap_or_default();
    let end_unix_nano = spans
        .iter()
        .map(|span| span.end_time_unix_nano)
        .max()
        .unwrap_or(start_unix_nano);

    TraceWindow {
        start_unix_nano,
        end_unix_nano,
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct TraceWindow {
    start_unix_nano: i64,
    end_unix_nano: i64,
}

#[derive(Debug, Clone)]
pub(crate) struct TraceTreeRow {
    pub(crate) depth: usize,
    pub(crate) has_children: bool,
    pub(crate) is_collapsed: bool,
    pub(crate) is_critical: bool,
    pub(crate) span: SpanDetail,
}

impl TraceTreeRow {
    fn disclosure(&self) -> &'static str {
        if self.has_children {
            if self.is_collapsed { "▸ " } else { "▾ " }
        } else {
            "    "
        }
    }
}

fn build_trace_tree_lines(
    rows: &[TraceTreeRow],
    selected_index: usize,
    tree_focused: bool,
    trace_window: TraceWindow,
    line_width: usize,
    palette: Palette,
) -> Vec<Line<'static>> {
    if rows.is_empty() {
        return vec![Line::raw("No spans recorded for this trace.")];
    }

    let timeline_width = if line_width >= 72 {
        18
    } else if line_width >= 56 {
        14
    } else {
        10
    };
    let duration_width = 8;

    rows.iter()
        .enumerate()
        .map(|(index, row)| {
            let selection_style = if index == selected_index {
                let color = if tree_focused {
                    palette.warning
                } else {
                    palette.muted
                };
                Style::default().fg(palette.background).bg(color)
            } else {
                Style::default()
            };
            let indent = "  ".repeat(row.depth);
            let prefix = format!("{indent}{}", row.disclosure());
            let duration = format_duration_compact(row.span.duration_ms);
            let display_name = trace_row_display_name(&row.span);
            let mut badges = trace_row_badges(&row.span);
            if row.is_critical {
                badges.push(TraceRowBadge {
                    label: "Hot Path".to_string(),
                });
            }
            let name_style = if is_low_signal_wrapper_span(&row.span) {
                let mut style = Style::default().fg(palette.muted).patch(selection_style);
                if row.is_critical {
                    style = style.add_modifier(ratatui::prelude::Modifier::BOLD);
                }
                style
            } else {
                let mut style = Style::default()
                    .fg(palette.foreground)
                    .patch(selection_style);
                if row.is_critical {
                    style = style.add_modifier(ratatui::prelude::Modifier::BOLD);
                }
                style
            };
            let badge_width = badges
                .iter()
                .map(|badge| badge.label.chars().count() + 3)
                .sum::<usize>();
            let name_width = line_width
                .saturating_sub(
                    prefix.chars().count() + timeline_width + duration_width + badge_width + 2,
                )
                .max(8);
            let name = truncate(&display_name, name_width);
            let rendered_width = prefix.chars().count()
                + name.chars().count()
                + badge_width
                + timeline_width
                + duration_width
                + 1;
            let spacer = " ".repeat(line_width.saturating_sub(rendered_width));
            let timeline = waterfall_bar(trace_window, row, timeline_width);

            Line::from(vec![
                Span::styled(
                    prefix,
                    Style::default().fg(palette.muted).patch(selection_style),
                ),
                Span::styled(name, name_style),
                render_badges(&badges, selection_style),
                Span::styled(spacer, selection_style),
                Span::styled(
                    timeline.before,
                    Style::default().fg(palette.muted).patch(selection_style),
                ),
                Span::styled(
                    timeline.active,
                    Style::default()
                        .fg(if row.is_critical {
                            palette.warning
                        } else {
                            palette.accent
                        })
                        .patch(selection_style),
                ),
                Span::styled(
                    timeline.after,
                    Style::default().fg(palette.muted).patch(selection_style),
                ),
                Span::raw(" "),
                Span::styled(
                    format!("{duration:>duration_width$}"),
                    Style::default()
                        .fg(palette.foreground)
                        .patch(selection_style),
                ),
            ])
        })
        .collect()
}

#[derive(Debug, Clone)]
pub(crate) struct TraceRowBadge {
    pub(crate) label: String,
}

fn render_badges(badges: &[TraceRowBadge], selection_style: Style) -> Span<'static> {
    let rendered = badges
        .iter()
        .map(|badge| format!(" [{}]", badge.label))
        .collect::<String>();
    Span::styled(rendered, selection_style)
}

pub(crate) fn trace_row_display_name(span: &SpanDetail) -> String {
    if let Some(tool_name) = span_tool_name(span)
        && is_generic_tool_wrapper_name(&span.span_name)
    {
        return tool_name;
    }

    simplify_wrapper_name(&span.span_name)
}

pub(crate) fn trace_row_badges(span: &SpanDetail) -> Vec<TraceRowBadge> {
    let mut badges = Vec::new();

    if span.status_code == "STATUS_CODE_ERROR" {
        badges.push(TraceRowBadge {
            label: "ERR".to_string(),
        });
    }

    if let Some(tool_name) = span_tool_name(span) {
        badges.push(TraceRowBadge {
            label: format!("tool {tool_name}"),
        });
    }

    if let Some(llm) = &span.llm {
        if let Some(model) = llm.model.as_deref().filter(|model| !model.is_empty()) {
            badges.push(TraceRowBadge {
                label: format!("LLM {model}"),
            });
        } else if !is_low_signal_wrapper_span(span) {
            badges.push(TraceRowBadge {
                label: "LLM".to_string(),
            });
        }
    }

    badges
}

fn span_tool_name(span: &SpanDetail) -> Option<String> {
    span.attributes
        .get("tool.name")
        .and_then(Value::as_str)
        .filter(|name| !name.is_empty())
        .map(ToString::to_string)
        .or_else(|| {
            span.llm
                .as_ref()
                .and_then(|llm| llm.tool_name.as_deref())
                .filter(|name| !name.is_empty())
                .map(ToString::to_string)
        })
}

fn is_generic_tool_wrapper_name(name: &str) -> bool {
    let normalized = name.to_ascii_lowercase();
    normalized.contains("running tool")
        || normalized.contains("running tools")
        || normalized == "tool"
        || normalized.contains("tool call")
}

fn simplify_wrapper_name(name: &str) -> String {
    if let Some(prefix) = name.strip_suffix(" {name}") {
        return prefix.to_string();
    }
    if let Some(prefix) = name.strip_suffix(" {task}") {
        return prefix.to_string();
    }
    if let Some(prefix) = name.strip_prefix("case: {")
        && prefix.ends_with('}')
    {
        return "case".to_string();
    }
    if let Some(prefix) = name.strip_prefix("evaluator: {")
        && prefix.ends_with('}')
    {
        return "evaluator".to_string();
    }
    if let Some(prefix) = name.strip_prefix("Prompt: ") {
        return prefix.to_string();
    }

    name.to_string()
}

fn is_low_signal_wrapper_span(span: &SpanDetail) -> bool {
    let normalized = simplify_wrapper_name(&span.span_name).to_ascii_lowercase();
    matches!(
        normalized.as_str(),
        "evaluate" | "case" | "execute" | "agent run" | "running output function" | "evaluator"
    ) || span.span_name.starts_with("Prompt: ")
}

fn push_tree_rows(
    span_index: usize,
    spans: &[SpanDetail],
    children_by_parent: &HashMap<String, Vec<usize>>,
    collapsed_span_ids: &HashSet<String>,
    critical_path_span_ids: &HashSet<String>,
    rows: &mut Vec<TraceTreeRow>,
    depth: usize,
) {
    let has_children = children_by_parent.contains_key(&spans[span_index].span_id);
    let is_collapsed = has_children && collapsed_span_ids.contains(&spans[span_index].span_id);
    rows.push(TraceTreeRow {
        depth,
        has_children,
        is_collapsed,
        is_critical: critical_path_span_ids.contains(&spans[span_index].span_id),
        span: spans[span_index].clone(),
    });

    if is_collapsed {
        return;
    }

    if let Some(children) = children_by_parent.get(&spans[span_index].span_id) {
        for child_index in children {
            push_tree_rows(
                *child_index,
                spans,
                children_by_parent,
                collapsed_span_ids,
                critical_path_span_ids,
                rows,
                depth + 1,
            );
        }
    }
}

fn critical_path_span_ids(
    roots: &[usize],
    spans: &[SpanDetail],
    children_by_parent: &HashMap<String, Vec<usize>>,
) -> HashSet<String> {
    let mut path = HashSet::new();
    if spans.is_empty() {
        return path;
    }
    let mut memo = vec![None; spans.len()];
    let Some(root_index) = select_critical_child(roots, spans, children_by_parent, &mut memo)
    else {
        return path;
    };
    mark_critical_path(root_index, spans, children_by_parent, &mut memo, &mut path);

    path
}

fn mark_critical_path(
    span_index: usize,
    spans: &[SpanDetail],
    children_by_parent: &HashMap<String, Vec<usize>>,
    memo: &mut [Option<i64>],
    path: &mut HashSet<String>,
) {
    path.insert(spans[span_index].span_id.clone());
    let Some(children) = children_by_parent.get(&spans[span_index].span_id) else {
        return;
    };
    let Some(child_index) = select_critical_child(children, spans, children_by_parent, memo) else {
        return;
    };
    mark_critical_path(child_index, spans, children_by_parent, memo, path);
}

fn select_critical_child(
    indexes: &[usize],
    spans: &[SpanDetail],
    children_by_parent: &HashMap<String, Vec<usize>>,
    memo: &mut [Option<i64>],
) -> Option<usize> {
    indexes.iter().copied().max_by(|left, right| {
        critical_path_cost(*left, spans, children_by_parent, memo)
            .cmp(&critical_path_cost(*right, spans, children_by_parent, memo))
            .then(
                spans[*left]
                    .end_time_unix_nano
                    .cmp(&spans[*right].end_time_unix_nano),
            )
            .then(
                spans[*left]
                    .duration_ms
                    .total_cmp(&spans[*right].duration_ms),
            )
            .then(
                spans[*right]
                    .start_time_unix_nano
                    .cmp(&spans[*left].start_time_unix_nano),
            )
            .then(spans[*left].span_name.cmp(&spans[*right].span_name))
    })
}

fn critical_path_cost(
    span_index: usize,
    spans: &[SpanDetail],
    children_by_parent: &HashMap<String, Vec<usize>>,
    memo: &mut [Option<i64>],
) -> i64 {
    if let Some(cost) = memo[span_index] {
        return cost;
    }

    let child_cost = children_by_parent
        .get(&spans[span_index].span_id)
        .and_then(|children| {
            children
                .iter()
                .map(|child| critical_path_cost(*child, spans, children_by_parent, memo))
                .max()
        })
        .unwrap_or(0);
    let cost =
        exclusive_span_nanos(span_index, spans, children_by_parent).saturating_add(child_cost);
    memo[span_index] = Some(cost);
    cost
}

fn exclusive_span_nanos(
    span_index: usize,
    spans: &[SpanDetail],
    children_by_parent: &HashMap<String, Vec<usize>>,
) -> i64 {
    let span = &spans[span_index];
    let span_start = span.start_time_unix_nano;
    let span_end = span.end_time_unix_nano.max(span_start);
    let span_duration = span_end.saturating_sub(span_start);
    let Some(children) = children_by_parent.get(&span.span_id) else {
        return span_duration;
    };

    let mut covered = 0_i64;
    let mut current_start = None;
    let mut current_end = 0_i64;

    for child_index in children {
        let child = &spans[*child_index];
        let child_start = child.start_time_unix_nano.max(span_start);
        let child_end = child.end_time_unix_nano.min(span_end);
        if child_end <= child_start {
            continue;
        }

        match current_start {
            None => {
                current_start = Some(child_start);
                current_end = child_end;
            }
            Some(start) if child_start > current_end => {
                covered = covered.saturating_add(current_end.saturating_sub(start));
                current_start = Some(child_start);
                current_end = child_end;
            }
            Some(_) => {
                current_end = current_end.max(child_end);
            }
        }
    }

    if let Some(start) = current_start {
        covered = covered.saturating_add(current_end.saturating_sub(start));
    }

    span_duration.saturating_sub(covered)
}

fn sort_span_indexes(indexes: &mut [usize], spans: &[SpanDetail]) {
    indexes.sort_by(|left, right| {
        let left_span = &spans[*left];
        let right_span = &spans[*right];
        left_span
            .start_time_unix_nano
            .cmp(&right_span.start_time_unix_nano)
            .then(
                left_span
                    .end_time_unix_nano
                    .cmp(&right_span.end_time_unix_nano),
            )
            .then(left_span.span_name.cmp(&right_span.span_name))
    });
}

pub(crate) struct WaterfallBar {
    pub(crate) before: String,
    pub(crate) active: String,
    pub(crate) after: String,
}

pub(crate) fn waterfall_bar(
    trace_window: TraceWindow,
    row: &TraceTreeRow,
    width: usize,
) -> WaterfallBar {
    if width == 0 {
        return WaterfallBar {
            before: String::new(),
            active: String::new(),
            after: String::new(),
        };
    }

    let total = (trace_window.end_unix_nano - trace_window.start_unix_nano).max(1) as f64;
    let start = (row.span.start_time_unix_nano - trace_window.start_unix_nano).max(0) as f64;
    let end = (row.span.end_time_unix_nano - trace_window.start_unix_nano).max(0) as f64;

    let left = ((start / total) * width as f64).floor() as usize;
    let mut right = ((end / total) * width as f64).ceil() as usize;
    right = right.clamp(left.saturating_add(1), width);

    WaterfallBar {
        before: "·".repeat(left),
        active: "━".repeat(right.saturating_sub(left)),
        after: "·".repeat(width.saturating_sub(right)),
    }
}

pub(crate) fn format_duration_compact(duration_ms: f64) -> String {
    if duration_ms >= 60_000.0 {
        format!("{:.1}m", duration_ms / 60_000.0)
    } else if duration_ms >= 1_000.0 {
        format!("{:.2}s", duration_ms / 1_000.0)
    } else {
        format!("{duration_ms:.1}ms")
    }
}
