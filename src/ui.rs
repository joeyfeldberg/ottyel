use std::collections::{HashMap, HashSet};

use ratatui::{
    Frame,
    layout::{Alignment, Constraint, Direction, Layout, Rect},
    prelude::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{
        Block, Borders, Cell, Clear, List, ListItem, Paragraph, Row, Sparkline, Table, Tabs, Wrap,
    },
};

use crate::{
    config::Theme,
    domain::{DashboardSnapshot, SpanDetail, truncate},
    query::{LogCorrelationFilter, LogSeverityFilter, TimeWindow},
};

#[derive(Debug, Clone, Copy)]
pub struct Palette {
    pub background: Color,
    pub foreground: Color,
    pub accent: Color,
    pub muted: Color,
    pub warning: Color,
    pub success: Color,
}

impl Palette {
    pub fn from_theme(theme: Theme) -> Self {
        match theme {
            Theme::Ember => Self {
                background: Color::Rgb(16, 12, 10),
                foreground: Color::Rgb(245, 226, 208),
                accent: Color::Rgb(255, 126, 56),
                muted: Color::Rgb(139, 116, 98),
                warning: Color::Rgb(255, 210, 74),
                success: Color::Rgb(92, 214, 154),
            },
            Theme::Tidal => Self {
                background: Color::Rgb(10, 18, 24),
                foreground: Color::Rgb(220, 240, 245),
                accent: Color::Rgb(39, 196, 245),
                muted: Color::Rgb(108, 141, 153),
                warning: Color::Rgb(255, 192, 92),
                success: Color::Rgb(100, 230, 190),
            },
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum Tab {
    Overview,
    Traces,
    Logs,
    Metrics,
    Llm,
}

impl Tab {
    pub const ALL: [Self; 5] = [
        Self::Overview,
        Self::Traces,
        Self::Logs,
        Self::Metrics,
        Self::Llm,
    ];

    pub fn title(self) -> &'static str {
        match self {
            Self::Overview => "Overview",
            Self::Traces => "Trace Explorer",
            Self::Logs => "Logs",
            Self::Metrics => "Metrics",
            Self::Llm => "LLM Inspector",
        }
    }
}

#[derive(Debug, Clone)]
pub struct UiState {
    pub active_tab: usize,
    pub selected_trace: usize,
    pub selected_trace_span: usize,
    pub trace_tree_scroll: usize,
    pub collapsed_trace_spans: HashSet<String>,
    pub selected_log: usize,
    pub selected_metric: usize,
    pub selected_llm: usize,
    pub service_filter_index: Option<usize>,
    pub errors_only: bool,
    pub trace_focus: TraceFocus,
    pub time_window: TimeWindow,
    pub search_query: String,
    pub search_mode: bool,
    pub log_search_query: String,
    pub log_search_mode: bool,
    pub log_severity_filter: LogSeverityFilter,
    pub log_correlation_filter: LogCorrelationFilter,
    pub log_tail: bool,
}

impl Default for UiState {
    fn default() -> Self {
        Self {
            active_tab: 0,
            selected_trace: 0,
            selected_trace_span: 0,
            trace_tree_scroll: 0,
            collapsed_trace_spans: HashSet::new(),
            selected_log: 0,
            selected_metric: 0,
            selected_llm: 0,
            service_filter_index: None,
            errors_only: false,
            trace_focus: TraceFocus::TraceList,
            time_window: TimeWindow::TwentyFourHours,
            search_query: String::new(),
            search_mode: false,
            log_search_query: String::new(),
            log_search_mode: false,
            log_severity_filter: LogSeverityFilter::All,
            log_correlation_filter: LogCorrelationFilter::All,
            log_tail: false,
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum TraceFocus {
    TraceList,
    TraceTree,
}

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
        Paragraph::new(padded(global_status_text(snapshot, state)))
            .style(Style::default().fg(palette.muted))
            .alignment(Alignment::Left),
        layout[1],
    );

    match Tab::ALL[state.active_tab] {
        Tab::Overview => render_overview(frame, layout[2], snapshot, palette),
        Tab::Traces => render_traces(frame, layout[2], snapshot, state, palette),
        Tab::Logs => render_logs(frame, layout[2], snapshot, state, palette),
        Tab::Metrics => render_metrics(frame, layout[2], snapshot, state, palette),
        Tab::Llm => render_llm(frame, layout[2], snapshot, state, palette),
    }

    frame.render_widget(
        Paragraph::new(padded(footer_text(state))).style(Style::default().fg(palette.muted)),
        layout[3],
    );
}

pub fn sync_trace_tree_scroll(root: Rect, snapshot: &DashboardSnapshot, state: &mut UiState) {
    if Tab::ALL[state.active_tab] != Tab::Traces {
        return;
    }

    let viewport_height = trace_tree_viewport_height(trace_tree_area(root));
    let tree_rows = trace_tree_rows(&snapshot.selected_trace, &state.collapsed_trace_spans);
    let selected_line = trace_tree_selected_line(state, &tree_rows);
    let total_lines = trace_tree_total_lines(&tree_rows, snapshot.traces.is_empty());
    state.trace_tree_scroll = trace_tree_scroll_offset(
        state.trace_tree_scroll,
        total_lines,
        selected_line,
        viewport_height,
    );
}

fn render_overview(
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

fn render_traces(
    frame: &mut Frame<'_>,
    area: Rect,
    snapshot: &DashboardSnapshot,
    state: &UiState,
    palette: Palette,
) {
    let panels = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(48), Constraint::Percentage(52)])
        .split(area);

    let trace_list_border = if state.trace_focus == TraceFocus::TraceList {
        palette.accent
    } else {
        palette.muted
    };

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
                .add_modifier(Modifier::BOLD),
        ),
    )
    .block(
        Block::default()
            .title(trace_list_title(state))
            .borders(Borders::ALL)
            .border_style(Style::default().fg(trace_list_border)),
    );
    frame.render_widget(table, panels[0]);

    let right = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Percentage(58), Constraint::Percentage(42)])
        .split(panels[1]);

    let trace_tree_border = if state.trace_focus == TraceFocus::TraceTree {
        palette.warning
    } else {
        palette.muted
    };

    let tree_rows = trace_tree_rows(&snapshot.selected_trace, &state.collapsed_trace_spans);
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
                    .title(trace_tree_title(state))
                    .borders(Borders::ALL)
                    .border_style(Style::default().fg(trace_tree_border)),
            ),
        right[0],
    );

    let span_detail = selected_trace_row(&tree_rows, state.selected_trace_span)
        .map(|row| build_span_detail_lines(row, palette))
        .unwrap_or_else(|| {
            vec![Line::raw(
                "Select a trace and move focus to the tree to inspect spans.",
            )]
        });

    frame.render_widget(
        Paragraph::new(span_detail)
            .wrap(Wrap { trim: false })
            .block(
                Block::default()
                    .title("Span Detail")
                    .borders(Borders::ALL)
                    .border_style(Style::default().fg(palette.accent)),
            ),
        right[1],
    );
}

fn render_logs(
    frame: &mut Frame<'_>,
    area: Rect,
    snapshot: &DashboardSnapshot,
    state: &UiState,
    palette: Palette,
) {
    let panels = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(58), Constraint::Percentage(42)])
        .split(area);

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
            .title(log_feed_title(state))
            .borders(Borders::ALL)
            .border_style(Style::default().fg(palette.accent)),
    );
    frame.render_widget(table, panels[0]);

    let detail = snapshot
        .logs
        .get(state.selected_log)
        .map(|log| build_log_detail_lines(log, palette))
        .unwrap_or_else(|| vec![Line::raw("No log selected.")]);

    frame.render_widget(
        Paragraph::new(detail).wrap(Wrap { trim: false }).block(
            Block::default()
                .title("Log Detail")
                .borders(Borders::ALL)
                .border_style(Style::default().fg(palette.warning)),
        ),
        panels[1],
    );
}

fn render_metrics(
    frame: &mut Frame<'_>,
    area: Rect,
    snapshot: &DashboardSnapshot,
    state: &UiState,
    palette: Palette,
) {
    let panels = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(52), Constraint::Percentage(48)])
        .split(area);

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
            .border_style(Style::default().fg(palette.accent)),
    );
    frame.render_widget(table, panels[0]);

    let right = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(9), Constraint::Min(10)])
        .split(panels[1]);

    let series = selected_metric_series(snapshot, state.selected_metric);
    let chart_values = metric_chart_values(&series);
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

    let detail = build_metric_detail_lines(snapshot, state.selected_metric, &series, palette);
    frame.render_widget(
        Paragraph::new(detail).wrap(Wrap { trim: false }).block(
            Block::default()
                .title("Metric Detail")
                .borders(Borders::ALL)
                .border_style(Style::default().fg(palette.accent)),
        ),
        right[1],
    );
}

fn render_llm(
    frame: &mut Frame<'_>,
    area: Rect,
    snapshot: &DashboardSnapshot,
    state: &UiState,
    palette: Palette,
) {
    let panels = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(55), Constraint::Percentage(45)])
        .split(area);

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
            .border_style(Style::default().fg(palette.warning)),
    );
    frame.render_widget(table, panels[0]);

    let detail = snapshot
        .llm
        .get(state.selected_llm)
        .map(|item| {
            vec![
                Line::from(format!("trace {}", truncate(&item.trace_id, 24))),
                Line::from(format!("service {}", item.service_name)),
                Line::from(format!("provider {}", item.provider)),
                Line::from(format!("model {}", item.model)),
                Line::from(format!("operation {}", item.operation)),
                Line::from(format!("status {}", item.status)),
                Line::from(format!(
                    "tokens in={} out={} total={}",
                    item.input_tokens.unwrap_or_default(),
                    item.output_tokens.unwrap_or_default(),
                    item.total_tokens.unwrap_or_default()
                )),
                Line::from(format!("cost {:?}", item.cost)),
                Line::from(format!("latency_ms {:?}", item.latency_ms)),
            ]
        })
        .unwrap_or_else(|| vec![Line::raw("No LLM spans yet.")]);
    frame.render_widget(
        Paragraph::new(detail).wrap(Wrap { trim: false }).block(
            Block::default()
                .title("Model detail")
                .borders(Borders::ALL)
                .border_style(Style::default().fg(palette.accent)),
        ),
        panels[1],
    );

    if snapshot.llm.is_empty() {
        let popup = centered_rect(54, 16, area);
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

fn build_trace_tree_lines(
    rows: &[TraceTreeRow],
    selected_index: usize,
    tree_focused: bool,
    palette: Palette,
) -> Vec<Line<'static>> {
    if rows.is_empty() {
        return vec![Line::raw("No spans recorded for this trace.")];
    }

    rows.into_iter()
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
                    format!("{:<9}", row.span.span_kind),
                    Style::default().fg(palette.accent).patch(selection_style),
                ),
                Span::raw(" "),
                Span::styled(
                    format!("{:>8.1}ms", row.span.duration_ms),
                    Style::default()
                        .fg(palette.foreground)
                        .patch(selection_style),
                ),
                Span::raw(" "),
                Span::styled(
                    truncate(&row.span.span_name, 44),
                    Style::default()
                        .fg(palette.foreground)
                        .patch(selection_style),
                ),
                Span::styled(
                    llm_suffix,
                    Style::default().fg(palette.warning).patch(selection_style),
                ),
                Span::raw(" "),
                Span::styled(
                    status_badge(&row.span.status_code),
                    status_style.patch(selection_style),
                ),
            ])
        })
        .collect()
}

#[derive(Debug, Clone)]
struct TraceTreeRow {
    prefix: String,
    has_children: bool,
    is_collapsed: bool,
    span: SpanDetail,
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

fn selected_trace_row(rows: &[TraceTreeRow], selected_index: usize) -> Option<&TraceTreeRow> {
    rows.get(selected_index)
}

fn trace_tree_rows(
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
        let is_last_root = idx + 1 == roots.len();
        push_tree_rows(
            *root_index,
            spans,
            &children_by_parent,
            collapsed_span_ids,
            &mut rows,
            Vec::new(),
            false,
            is_last_root,
        );
    }

    rows
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

fn status_badge(status_code: &str) -> &'static str {
    match status_code {
        "STATUS_CODE_ERROR" => "error",
        "STATUS_CODE_OK" => "ok",
        _ => "unset",
    }
}

fn selected_metric_series(
    snapshot: &DashboardSnapshot,
    selected_index: usize,
) -> Vec<crate::domain::MetricSummary> {
    let Some(selected) = snapshot.metrics.get(selected_index) else {
        return Vec::new();
    };

    let mut series = snapshot
        .metrics
        .iter()
        .filter(|metric| {
            metric.service_name == selected.service_name
                && metric.metric_name == selected.metric_name
                && metric.instrument_kind == selected.instrument_kind
        })
        .cloned()
        .collect::<Vec<_>>();
    series.sort_by_key(|metric| metric.timestamp_unix_nano);
    series
}

fn metric_chart_values(series: &[crate::domain::MetricSummary]) -> Vec<u64> {
    if series.is_empty() {
        return vec![0];
    }

    let numeric = series
        .iter()
        .filter_map(|metric| metric.value)
        .collect::<Vec<_>>();
    if numeric.is_empty() {
        return vec![0; series.len().max(1)];
    }

    let min = numeric.iter().copied().fold(f64::INFINITY, f64::min);
    let max = numeric.iter().copied().fold(f64::NEG_INFINITY, f64::max);
    let spread = (max - min).max(1.0);

    series
        .iter()
        .map(|metric| {
            metric
                .value
                .map(|value| (((value - min) / spread) * 100.0).round() as u64 + 1)
                .unwrap_or(0)
        })
        .collect()
}

fn build_metric_detail_lines(
    snapshot: &DashboardSnapshot,
    selected_index: usize,
    series: &[crate::domain::MetricSummary],
    palette: Palette,
) -> Vec<Line<'static>> {
    let Some(selected) = snapshot.metrics.get(selected_index) else {
        return vec![Line::raw("No metric selected.")];
    };

    let numeric = series
        .iter()
        .filter_map(|metric| metric.value)
        .collect::<Vec<_>>();
    let latest = series.last().and_then(|metric| metric.value);
    let min = numeric.iter().copied().reduce(f64::min);
    let max = numeric.iter().copied().reduce(f64::max);
    let avg = if numeric.is_empty() {
        None
    } else {
        Some(numeric.iter().sum::<f64>() / numeric.len() as f64)
    };

    let mut lines = vec![
        Line::from(Span::styled(
            truncate(&selected.metric_name, 42),
            Style::default()
                .fg(palette.foreground)
                .add_modifier(Modifier::BOLD),
        )),
        Line::from(format!("service {}", selected.service_name)),
        Line::from(format!("kind {}", selected.instrument_kind)),
        Line::from(format!("samples {}", series.len())),
        Line::from(format!("latest {:?}", latest)),
        Line::from(format!("min {:?}  max {:?}", min, max)),
        Line::from(format!("avg {:?}", avg)),
        Line::raw(""),
        Line::from(Span::styled(
            "recent points",
            Style::default()
                .fg(palette.accent)
                .add_modifier(Modifier::BOLD),
        )),
    ];

    for metric in series.iter().rev().take(6) {
        lines.push(Line::from(format!(
            "{} -> {}",
            metric.timestamp_unix_nano, metric.summary
        )));
    }

    lines
}

fn build_span_detail_lines(row: &TraceTreeRow, palette: Palette) -> Vec<Line<'static>> {
    let span = &row.span;
    let mut lines = vec![
        Line::from(vec![
            Span::styled(
                truncate(&span.span_name, 48),
                Style::default()
                    .fg(palette.foreground)
                    .add_modifier(Modifier::BOLD),
            ),
            Span::raw(" "),
            Span::styled(
                status_badge(&span.status_code),
                match span.status_code.as_str() {
                    "STATUS_CODE_ERROR" => Style::default().fg(palette.warning),
                    "STATUS_CODE_OK" => Style::default().fg(palette.success),
                    _ => Style::default().fg(palette.muted),
                },
            ),
        ]),
        Line::from(format!("service {}", span.service_name)),
        Line::from(format!("span_id {}", span.span_id)),
        Line::from(format!(
            "parent {}",
            if span.parent_span_id.is_empty() {
                "<root>"
            } else {
                span.parent_span_id.as_str()
            }
        )),
        Line::from(format!(
            "kind {}  duration {:.1}ms",
            span.span_kind, span.duration_ms
        )),
        Line::from(format!(
            "events {}  links {}",
            span.events.len(),
            span.links.len()
        )),
    ];

    if let Some(llm) = &span.llm {
        lines.push(Line::raw(""));
        lines.push(Line::from(Span::styled(
            "llm",
            Style::default()
                .fg(palette.warning)
                .add_modifier(Modifier::BOLD),
        )));
        if let Some(provider) = &llm.provider {
            lines.push(Line::from(format!("provider {provider}")));
        }
        if let Some(model) = &llm.model {
            lines.push(Line::from(format!("model {model}")));
        }
        if let Some(operation) = &llm.operation {
            lines.push(Line::from(format!("operation {operation}")));
        }
        if llm.input_tokens.is_some() || llm.output_tokens.is_some() || llm.total_tokens.is_some() {
            lines.push(Line::from(format!(
                "tokens in={} out={} total={}",
                llm.input_tokens.unwrap_or_default(),
                llm.output_tokens.unwrap_or_default(),
                llm.total_tokens.unwrap_or_default()
            )));
        }
        if let Some(cost) = llm.cost {
            lines.push(Line::from(format!("cost {cost:.6}")));
        }
    }

    if !span.resource_attributes.is_empty() {
        lines.push(Line::raw(""));
        lines.push(Line::from(Span::styled(
            "resource",
            Style::default()
                .fg(palette.success)
                .add_modifier(Modifier::BOLD),
        )));
        for (key, value) in span.resource_attributes.iter().take(4) {
            lines.push(Line::from(format!(
                "{} = {}",
                truncate(key, 28),
                truncate(&attribute_value_text(value), 64)
            )));
        }
    }

    if !span.attributes.is_empty() {
        lines.push(Line::raw(""));
        lines.push(Line::from(Span::styled(
            "attributes",
            Style::default()
                .fg(palette.accent)
                .add_modifier(Modifier::BOLD),
        )));
        for (key, value) in span.attributes.iter().take(8) {
            lines.push(Line::from(format!(
                "{} = {}",
                truncate(key, 28),
                truncate(&attribute_value_text(value), 64)
            )));
        }
        if span.attributes.len() > 8 {
            lines.push(Line::from(format!(
                "... {} more attributes",
                span.attributes.len() - 8
            )));
        }
    }

    if !span.events.is_empty() {
        lines.push(Line::raw(""));
        lines.push(Line::from(Span::styled(
            "events",
            Style::default()
                .fg(palette.warning)
                .add_modifier(Modifier::BOLD),
        )));
        for event in span.events.iter().take(3) {
            lines.push(Line::from(format!(
                "{} @ {}",
                truncate(&event.name, 28),
                event.timestamp_unix_nano
            )));
            for (key, value) in event.attributes.iter().take(2) {
                lines.push(Line::from(format!(
                    "  {} = {}",
                    truncate(key, 24),
                    truncate(&attribute_value_text(value), 56)
                )));
            }
        }
    }

    if !span.links.is_empty() {
        lines.push(Line::raw(""));
        lines.push(Line::from(Span::styled(
            "links",
            Style::default()
                .fg(palette.muted)
                .add_modifier(Modifier::BOLD),
        )));
        for link in span.links.iter().take(3) {
            lines.push(Line::from(format!(
                "{} / {}",
                truncate(&link.trace_id, 16),
                truncate(&link.span_id, 16)
            )));
            if !link.trace_state.is_empty() {
                lines.push(Line::from(format!(
                    "  state {}",
                    truncate(&link.trace_state, 52)
                )));
            }
            for (key, value) in link.attributes.iter().take(2) {
                lines.push(Line::from(format!(
                    "  {} = {}",
                    truncate(key, 24),
                    truncate(&attribute_value_text(value), 56)
                )));
            }
        }
    }

    lines
}

fn build_log_detail_lines(log: &crate::domain::LogSummary, palette: Palette) -> Vec<Line<'static>> {
    let mut lines = vec![
        Line::from(Span::styled(
            truncate(&log.body, 72),
            Style::default()
                .fg(palette.foreground)
                .add_modifier(Modifier::BOLD),
        )),
        Line::from(format!("service {}", log.service_name)),
        Line::from(format!("severity {}", log.severity)),
        Line::from(format!("timestamp {}", log.timestamp_unix_nano)),
        Line::from(format!(
            "trace {}",
            if log.trace_id.is_empty() {
                "<none>"
            } else {
                log.trace_id.as_str()
            }
        )),
        Line::from(format!(
            "span {}",
            if log.span_id.is_empty() {
                "<none>"
            } else {
                log.span_id.as_str()
            }
        )),
    ];

    if !log.resource_attributes.is_empty() {
        lines.push(Line::raw(""));
        lines.push(Line::from(Span::styled(
            "resource",
            Style::default()
                .fg(palette.success)
                .add_modifier(Modifier::BOLD),
        )));
        for (key, value) in log.resource_attributes.iter().take(6) {
            lines.push(Line::from(format!(
                "{} = {}",
                truncate(key, 28),
                truncate(&attribute_value_text(value), 64)
            )));
        }
        if log.resource_attributes.len() > 6 {
            lines.push(Line::from(format!(
                "... {} more resource attributes",
                log.resource_attributes.len() - 6
            )));
        }
    }

    if !log.attributes.is_empty() {
        lines.push(Line::raw(""));
        lines.push(Line::from(Span::styled(
            "attributes",
            Style::default()
                .fg(palette.accent)
                .add_modifier(Modifier::BOLD),
        )));
        for (key, value) in log.attributes.iter().take(8) {
            lines.push(Line::from(format!(
                "{} = {}",
                truncate(key, 28),
                truncate(&attribute_value_text(value), 64)
            )));
        }
        if log.attributes.len() > 8 {
            lines.push(Line::from(format!(
                "... {} more attributes",
                log.attributes.len() - 8
            )));
        }
    }

    lines.push(Line::raw(""));
    lines.push(Line::from(Span::styled(
        "message",
        Style::default()
            .fg(palette.warning)
            .add_modifier(Modifier::BOLD),
    )));
    lines.extend(format_log_body(&log.body).into_iter().map(Line::from));

    lines
}

fn attribute_value_text(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::String(text) => text.clone(),
        _ => value.to_string(),
    }
}

fn format_log_body(body: &str) -> Vec<String> {
    if let Ok(value) = serde_json::from_str::<serde_json::Value>(body) {
        if let Ok(pretty) = serde_json::to_string_pretty(&value) {
            return pretty.lines().map(ToString::to_string).collect();
        }
    }

    body.lines().map(ToString::to_string).collect()
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

fn log_feed_title(state: &UiState) -> String {
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

    if parts.len() == 1 {
        parts.remove(0)
    } else {
        format!("{} [{}]", parts.remove(0), parts.join(" | "))
    }
}

fn trace_list_title(state: &UiState) -> String {
    let mut parts = vec!["Trace Explorer".to_string()];
    if state.errors_only {
        parts.push("errors-only".to_string());
    }
    if state.trace_focus == TraceFocus::TraceList {
        parts.push("focus".to_string());
    }
    titled(parts)
}

fn trace_tree_title(state: &UiState) -> String {
    let mut parts = vec!["Trace Tree".to_string()];
    if state.trace_focus == TraceFocus::TraceTree {
        parts.push("focus".to_string());
    }
    if !state.collapsed_trace_spans.is_empty() {
        parts.push(format!("collapsed={}", state.collapsed_trace_spans.len()));
    }
    titled(parts)
}

fn global_status_text(snapshot: &DashboardSnapshot, state: &UiState) -> String {
    format!(
        "window={} | service={} | search={} | panes traces={} logs={} metrics={} llm={}",
        state.time_window.label(),
        current_service(snapshot, state).unwrap_or("all"),
        search_label(state),
        snapshot.overview.trace_count,
        snapshot.overview.log_count,
        snapshot.overview.metric_count,
        snapshot.overview.llm_count,
    )
}

fn footer_text(state: &UiState) -> String {
    if state.search_mode {
        return "global search: type to filter | enter/esc close | backspace delete".to_string();
    }
    if state.log_search_mode {
        return "log search: type to filter logs | enter/esc close | backspace delete".to_string();
    }

    match Tab::ALL[state.active_tab] {
        Tab::Overview => {
            "overview: tab switch panes | / global search | s service | t window | q quit"
                .to_string()
        }
        Tab::Traces => match state.trace_focus {
            TraceFocus::TraceList => {
                "traces: j/k select trace | l tree focus | e errors | s service | t window | / search | q quit"
                    .to_string()
            }
            TraceFocus::TraceTree => {
                "trace tree: j/k move | h list focus | space/enter toggle subtree | e errors | / search | q quit"
                    .to_string()
            }
        },
        Tab::Logs => {
            "logs: j/k move | f tail | x log search | v severity | c correlation | s service | t window | / global search | q quit"
                .to_string()
        }
        Tab::Metrics => {
            "metrics: j/k move | s service | t window | / global search | q quit".to_string()
        }
        Tab::Llm => {
            "llm: j/k move | s service | t window | / global search | q quit".to_string()
        }
    }
}

fn titled(mut parts: Vec<String>) -> String {
    if parts.len() == 1 {
        parts.remove(0)
    } else {
        format!("{} [{}]", parts.remove(0), parts.join(" | "))
    }
}

fn padded(text: String) -> String {
    format!(" {text} ")
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

fn trace_tree_selected_line(state: &UiState, tree_rows: &[TraceTreeRow]) -> usize {
    const TREE_HEADER_LINES: usize = 3;
    if tree_rows.is_empty() {
        return 0;
    }
    TREE_HEADER_LINES
        + state
            .selected_trace_span
            .min(tree_rows.len().saturating_sub(1))
}

fn trace_tree_viewport_height(area: Rect) -> usize {
    area.height.saturating_sub(2) as usize
}

fn trace_tree_total_lines(tree_rows: &[TraceTreeRow], no_trace_selected: bool) -> usize {
    if no_trace_selected {
        1
    } else {
        3 + tree_rows.len()
    }
}

fn trace_tree_area(root: Rect) -> Rect {
    let layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),
            Constraint::Length(1),
            Constraint::Min(10),
            Constraint::Length(1),
        ])
        .split(root);
    let body = layout[2];
    let panels = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(48), Constraint::Percentage(52)])
        .split(body);
    Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Percentage(58), Constraint::Percentage(42)])
        .split(panels[1])[0]
}

fn trace_tree_scroll_offset(
    current_offset: usize,
    total_lines: usize,
    selected_line: usize,
    viewport_height: usize,
) -> usize {
    if total_lines == 0 || viewport_height == 0 || total_lines <= viewport_height {
        return 0;
    }

    let max_offset = total_lines.saturating_sub(viewport_height);
    let offset = current_offset.min(max_offset);

    if selected_line < offset {
        return selected_line;
    }

    let visible_end = offset.saturating_add(viewport_height);
    if selected_line >= visible_end {
        return (selected_line + 1)
            .saturating_sub(viewport_height)
            .min(max_offset);
    }

    offset
}

fn centered_rect(percent_x: u16, percent_y: u16, area: Rect) -> Rect {
    let vertical = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Percentage((100 - percent_y) / 2),
            Constraint::Percentage(percent_y),
            Constraint::Percentage((100 - percent_y) / 2),
        ])
        .split(area);

    Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage((100 - percent_x) / 2),
            Constraint::Percentage(percent_x),
            Constraint::Percentage((100 - percent_x) / 2),
        ])
        .split(vertical[1])[1]
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::{
        TraceFocus, UiState, build_log_detail_lines, format_log_body, metric_chart_values,
        selected_trace_row, trace_tree_rows, trace_tree_scroll_offset,
    };
    use crate::domain::{AttributeMap, LogSummary, MetricSummary, SpanDetail};
    use crate::query::TimeWindow;
    use serde_json::json;

    #[test]
    fn trace_tree_rows_follow_parent_child_structure() {
        let rows = trace_tree_rows(
            &[
                span_with_parent("trace", "root", "", "request", 0, 100),
                span_with_parent("trace", "cache", "http", "cache.get", 20, 30),
                span_with_parent("trace", "http", "root", "http.call", 10, 70),
                span_with_parent("trace", "db", "root", "db.query", 75, 95),
            ],
            &HashSet::new(),
        );

        let rendered = rows
            .into_iter()
            .map(|row| format!("{}{}", row.prefix, row.span.span_name))
            .collect::<Vec<_>>();

        assert_eq!(
            rendered,
            vec!["request", "+- http.call", "| `- cache.get", "`- db.query",]
        );
    }

    #[test]
    fn selected_trace_row_uses_tree_order() {
        let rows = trace_tree_rows(
            &[
                span_with_parent("trace", "root", "", "request", 0, 100),
                span_with_parent("trace", "child-a", "root", "http.call", 10, 70),
                span_with_parent("trace", "child-b", "root", "db.query", 75, 95),
            ],
            &HashSet::new(),
        );

        assert_eq!(
            selected_trace_row(&rows, 1).map(|row| row.span.span_name.as_str()),
            Some("http.call")
        );
    }

    #[test]
    fn trace_tree_rows_hide_collapsed_subtrees() {
        let rows = trace_tree_rows(
            &[
                span_with_parent("trace", "root", "", "request", 0, 100),
                span_with_parent("trace", "http", "root", "http.call", 10, 70),
                span_with_parent("trace", "cache", "http", "cache.get", 20, 30),
                span_with_parent("trace", "db", "root", "db.query", 75, 95),
            ],
            &HashSet::from(["http".to_string()]),
        );

        let rendered = rows
            .into_iter()
            .map(|row| row.span.span_name)
            .collect::<Vec<_>>();

        assert_eq!(rendered, vec!["request", "http.call", "db.query"]);
    }

    #[test]
    fn ui_state_defaults_to_trace_list_focus() {
        let state = UiState::default();
        assert_eq!(state.trace_focus, TraceFocus::TraceList);
        assert_eq!(state.selected_trace_span, 0);
        assert_eq!(state.trace_tree_scroll, 0);
        assert!(state.collapsed_trace_spans.is_empty());
        assert_eq!(state.time_window, TimeWindow::TwentyFourHours);
        assert!(!state.search_mode);
        assert!(state.search_query.is_empty());
        assert!(!state.log_search_mode);
        assert!(state.log_search_query.is_empty());
        assert_eq!(
            state.log_severity_filter,
            crate::query::LogSeverityFilter::All
        );
        assert_eq!(
            state.log_correlation_filter,
            crate::query::LogCorrelationFilter::All
        );
        assert!(!state.log_tail);
    }

    #[test]
    fn trace_tree_scroll_offset_keeps_selected_line_visible() {
        assert_eq!(trace_tree_scroll_offset(0, 30, 0, 8), 0);
        assert_eq!(trace_tree_scroll_offset(0, 30, 7, 8), 0);
        assert_eq!(trace_tree_scroll_offset(0, 30, 8, 8), 1);
        assert_eq!(trace_tree_scroll_offset(22, 30, 28, 8), 22);
        assert_eq!(trace_tree_scroll_offset(22, 30, 21, 8), 21);
        assert_eq!(trace_tree_scroll_offset(22, 30, 20, 8), 20);
        assert_eq!(trace_tree_scroll_offset(22, 30, 29, 8), 22);
    }

    #[test]
    fn metric_chart_values_normalize_numeric_series() {
        let values = metric_chart_values(&[
            metric("latency", Some(10.0), 1),
            metric("latency", Some(15.0), 2),
            metric("latency", Some(20.0), 3),
        ]);

        assert_eq!(values.len(), 3);
        assert!(values[0] < values[1]);
        assert!(values[1] < values[2]);
    }

    #[test]
    fn format_log_body_pretty_prints_json() {
        let lines = format_log_body(r#"{"status":"ok","tokens":12}"#);

        assert!(lines.len() > 1);
        assert_eq!(lines[0], "{");
        assert!(lines.iter().any(|line| line.contains(r#""status": "ok""#)));
    }

    #[test]
    fn build_log_detail_lines_include_attributes() {
        let log = LogSummary {
            service_name: "api".to_string(),
            timestamp_unix_nano: 42,
            severity: "INFO".to_string(),
            body: r#"{"message":"done"}"#.to_string(),
            trace_id: "abc".to_string(),
            span_id: "def".to_string(),
            resource_attributes: AttributeMap::from([("service.name".to_string(), json!("api"))]),
            attributes: AttributeMap::from([
                ("http.status_code".to_string(), json!(200)),
                ("user.id".to_string(), json!("123")),
            ]),
        };

        let lines = build_log_detail_lines(
            &log,
            super::Palette::from_theme(crate::config::Theme::Ember),
        )
        .into_iter()
        .map(|line| line.to_string())
        .collect::<Vec<_>>();

        assert!(lines.iter().any(|line| line.contains("resource")));
        assert!(lines.iter().any(|line| line.contains("service.name = api")));
        assert!(lines.iter().any(|line| line.contains("attributes")));
        assert!(
            lines
                .iter()
                .any(|line| line.contains("http.status_code = 200"))
        );
        assert!(
            lines
                .iter()
                .any(|line| line.contains(r#""message": "done""#))
        );
    }

    fn span_with_parent(
        trace_id: &str,
        span_id: &str,
        parent_span_id: &str,
        name: &str,
        start: i64,
        end: i64,
    ) -> SpanDetail {
        SpanDetail {
            trace_id: trace_id.to_string(),
            span_id: span_id.to_string(),
            parent_span_id: parent_span_id.to_string(),
            service_name: "api".to_string(),
            span_name: name.to_string(),
            span_kind: "SERVER".to_string(),
            status_code: "STATUS_CODE_OK".to_string(),
            start_time_unix_nano: start,
            end_time_unix_nano: end,
            duration_ms: (end - start) as f64,
            resource_attributes: Default::default(),
            attributes: Default::default(),
            events: Vec::new(),
            links: Vec::new(),
            llm: None,
        }
    }

    fn metric(metric_name: &str, value: Option<f64>, timestamp: i64) -> MetricSummary {
        MetricSummary {
            service_name: "api".to_string(),
            metric_name: metric_name.to_string(),
            instrument_kind: "gauge".to_string(),
            timestamp_unix_nano: timestamp,
            value,
            summary: value
                .map(|value| format!("gauge={value}"))
                .unwrap_or_default(),
        }
    }
}
