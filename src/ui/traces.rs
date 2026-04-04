use std::collections::{HashMap, HashSet};

use ratatui::{
    Frame,
    layout::{Constraint, Rect},
    prelude::Style,
    text::{Line, Span},
    widgets::{Block, Borders, Cell, Paragraph, Row, Table, Wrap},
};

use crate::domain::{DashboardSnapshot, SpanDetail, truncate};

use super::{Palette, TraceFocus, TraceViewMode, UiState, chrome, details, geometry};

pub(crate) fn render(
    frame: &mut Frame<'_>,
    area: Rect,
    snapshot: &DashboardSnapshot,
    state: &UiState,
    palette: Palette,
) {
    let rows: Vec<Row<'_>> = snapshot
        .traces
        .iter()
        .enumerate()
        .map(|(idx, trace)| {
            let style = if idx == state.selected_trace {
                Style::default().fg(palette.background).bg(palette.accent)
            } else {
                Style::default().fg(palette.foreground)
            };
            Row::new(vec![
                Cell::from(truncate(&trace.service_name, 12)),
                Cell::from(truncate(&trace.root_name, 24)),
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
            Constraint::Length(12),
            Constraint::Min(20),
            Constraint::Length(6),
            Constraint::Length(6),
            Constraint::Length(8),
        ],
    )
    .header(
        Row::new(vec!["service", "root", "spans", "errs", "ms"]).style(
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

    let [tree_area, detail_area] = geometry::trace_detail_sections(area);
    let tree_border = if state.trace_focus == TraceFocus::TraceTree {
        palette.warning
    } else {
        palette.muted
    };

    let tree_rows = trace_tree_rows(&snapshot.selected_trace, &state.collapsed_trace_spans);
    let window = trace_window(&snapshot.selected_trace);
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
    let span_detail = details::trace_detail_lines(snapshot, state, palette);
    frame.render_widget(
        Paragraph::new(span_detail)
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

    let mut rows = Vec::with_capacity(spans.len());
    for (idx, root_index) in roots.iter().enumerate() {
        push_tree_rows(
            *root_index,
            spans,
            &children_by_parent,
            collapsed_span_ids,
            &mut rows,
            Vec::new(),
            false,
            idx + 1 == roots.len(),
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

pub(crate) fn waterfall_bar(trace_window: TraceWindow, row: &TraceTreeRow, width: usize) -> String {
    if width == 0 {
        return String::new();
    }

    let total = (trace_window.end_unix_nano - trace_window.start_unix_nano).max(1) as f64;
    let start = (row.span.start_time_unix_nano - trace_window.start_unix_nano).max(0) as f64;
    let end = (row.span.end_time_unix_nano - trace_window.start_unix_nano).max(0) as f64;

    let mut cells = vec!['.'; width];
    let left = ((start / total) * width as f64).floor() as usize;
    let mut right = ((end / total) * width as f64).ceil() as usize;
    right = right.clamp(left.saturating_add(1), width);

    for cell in &mut cells[left.min(width.saturating_sub(1))..right] {
        *cell = '=';
    }

    format!("[{}]", cells.into_iter().collect::<String>())
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct TraceWindow {
    start_unix_nano: i64,
    end_unix_nano: i64,
}

#[derive(Debug, Clone)]
pub(crate) struct TraceTreeRow {
    pub(crate) prefix: String,
    pub(crate) has_children: bool,
    pub(crate) is_collapsed: bool,
    pub(crate) span: SpanDetail,
}

impl TraceTreeRow {
    fn branch_marker(&self) -> &'static str {
        if self.has_children {
            if self.is_collapsed { "[+] " } else { "[-] " }
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
    palette: Palette,
) -> Vec<Line<'static>> {
    if rows.is_empty() {
        return vec![Line::raw("No spans recorded for this trace.")];
    }

    rows.iter()
        .enumerate()
        .map(|(index, row)| {
            let llm_suffix = row
                .span
                .llm
                .as_ref()
                .and_then(|llm| llm.model.as_deref())
                .map(|model| format!(" [{model}]"))
                .unwrap_or_default();
            let status_style = match row.span.status_code.as_str() {
                "STATUS_CODE_ERROR" => Style::default().fg(palette.warning),
                "STATUS_CODE_OK" => Style::default().fg(palette.success),
                _ => Style::default().fg(palette.muted),
            };
            let status_text = visible_status_badge(&row.span.status_code);
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

            Line::from(vec![
                Span::styled(
                    row.prefix.clone(),
                    Style::default().fg(palette.muted).patch(selection_style),
                ),
                Span::styled(
                    row.branch_marker(),
                    Style::default().fg(palette.muted).patch(selection_style),
                ),
                Span::styled(
                    format!("{:>8.1}ms", row.span.duration_ms),
                    Style::default()
                        .fg(palette.foreground)
                        .patch(selection_style),
                ),
                Span::raw(" "),
                Span::styled(
                    waterfall_bar(trace_window, row, 16),
                    Style::default().fg(palette.accent).patch(selection_style),
                ),
                Span::raw(" "),
                Span::styled(
                    truncate(&row.span.span_name, 28),
                    Style::default()
                        .fg(palette.foreground)
                        .patch(selection_style),
                ),
                Span::styled(
                    llm_suffix,
                    Style::default().fg(palette.warning).patch(selection_style),
                ),
                Span::styled(
                    status_text
                        .map(|text| format!(" {text}"))
                        .unwrap_or_default(),
                    status_style.patch(selection_style),
                ),
            ])
        })
        .collect()
}

fn push_tree_rows(
    span_index: usize,
    spans: &[SpanDetail],
    children_by_parent: &HashMap<String, Vec<usize>>,
    collapsed_span_ids: &HashSet<String>,
    rows: &mut Vec<TraceTreeRow>,
    ancestor_has_more_siblings: Vec<bool>,
    show_branch: bool,
    is_last: bool,
) {
    let has_children = children_by_parent.contains_key(&spans[span_index].span_id);
    let is_collapsed = has_children && collapsed_span_ids.contains(&spans[span_index].span_id);
    rows.push(TraceTreeRow {
        prefix: tree_prefix(&ancestor_has_more_siblings, show_branch, is_last),
        has_children,
        is_collapsed,
        span: spans[span_index].clone(),
    });

    if is_collapsed {
        return;
    }

    if let Some(children) = children_by_parent.get(&spans[span_index].span_id) {
        for (child_idx, child_index) in children.iter().enumerate() {
            let mut child_ancestors = ancestor_has_more_siblings.clone();
            if show_branch {
                child_ancestors.push(!is_last);
            }
            push_tree_rows(
                *child_index,
                spans,
                children_by_parent,
                collapsed_span_ids,
                rows,
                child_ancestors,
                true,
                child_idx + 1 == children.len(),
            );
        }
    }
}

fn tree_prefix(ancestor_has_more_siblings: &[bool], show_branch: bool, is_last: bool) -> String {
    let mut prefix = String::new();
    for has_more in ancestor_has_more_siblings {
        prefix.push_str(if *has_more { "| " } else { "  " });
    }
    if show_branch {
        prefix.push_str(if is_last { "`- " } else { "+- " });
    }
    prefix
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

fn visible_status_badge(status_code: &str) -> Option<&'static str> {
    match status_code {
        "STATUS_CODE_ERROR" => Some("error"),
        "STATUS_CODE_OK" => Some("ok"),
        _ => None,
    }
}
