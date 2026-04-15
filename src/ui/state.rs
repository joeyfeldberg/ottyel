use std::collections::HashSet;

use ratatui::prelude::Color;

use crate::{
    config::Theme,
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
            Theme::Grove => Self {
                background: Color::Rgb(11, 18, 13),
                foreground: Color::Rgb(229, 240, 212),
                accent: Color::Rgb(112, 204, 92),
                muted: Color::Rgb(102, 132, 98),
                warning: Color::Rgb(245, 194, 74),
                success: Color::Rgb(108, 224, 178),
            },
            Theme::Paper => Self {
                background: Color::Rgb(244, 236, 224),
                foreground: Color::Rgb(48, 38, 30),
                accent: Color::Rgb(184, 96, 46),
                muted: Color::Rgb(136, 118, 98),
                warning: Color::Rgb(184, 126, 28),
                success: Color::Rgb(54, 138, 92),
            },
            Theme::Neon => Self {
                background: Color::Rgb(8, 9, 18),
                foreground: Color::Rgb(225, 235, 255),
                accent: Color::Rgb(255, 94, 184),
                muted: Color::Rgb(109, 120, 155),
                warning: Color::Rgb(255, 202, 64),
                success: Color::Rgb(86, 240, 196),
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
            Self::Overview => "[1] Overview",
            Self::Traces => "[2] Trace Explorer",
            Self::Logs => "[3] Logs",
            Self::Metrics => "[4] Metrics",
            Self::Llm => "[5] LLM Inspector",
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct UiState {
    pub theme: Theme,
    pub active_tab: usize,
    pub trace_view_mode: TraceViewMode,
    pub selected_trace: usize,
    pub trace_list_scroll: usize,
    pub trace_list_follow_selected: bool,
    pub selected_trace_span: usize,
    pub trace_tree_scroll: usize,
    pub trace_tree_follow_selected: bool,
    pub trace_detail_scroll: u16,
    pub collapsed_trace_spans: HashSet<String>,
    pub show_help: bool,
    pub show_context_help: bool,
    pub show_command_palette: bool,
    pub command_query: String,
    pub selected_command: usize,
    pub command_palette_scroll: usize,
    pub logs_focus: PaneFocus,
    pub selected_log: usize,
    pub log_feed_scroll: usize,
    pub log_feed_follow_selected: bool,
    pub log_detail_scroll: u16,
    pub metrics_focus: PaneFocus,
    pub selected_metric: usize,
    pub metric_feed_scroll: usize,
    pub metric_feed_follow_selected: bool,
    pub metric_detail_scroll: u16,
    pub llm_focus: PaneFocus,
    pub selected_llm: usize,
    pub llm_feed_scroll: usize,
    pub llm_feed_follow_selected: bool,
    pub llm_detail_scroll: u16,
    pub llm_expand_prompt: bool,
    pub llm_expand_output: bool,
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
    pub log_pinned_trace_id: Option<String>,
    pub log_pinned_span_id: Option<String>,
    pub log_tail: bool,
}

impl Default for UiState {
    fn default() -> Self {
        Self {
            theme: Theme::Ember,
            active_tab: 0,
            trace_view_mode: TraceViewMode::List,
            selected_trace: 0,
            trace_list_scroll: 0,
            trace_list_follow_selected: true,
            selected_trace_span: 0,
            trace_tree_scroll: 0,
            trace_tree_follow_selected: true,
            trace_detail_scroll: 0,
            collapsed_trace_spans: HashSet::new(),
            show_help: false,
            show_context_help: false,
            show_command_palette: false,
            command_query: String::new(),
            selected_command: 0,
            command_palette_scroll: 0,
            logs_focus: PaneFocus::Primary,
            selected_log: 0,
            log_feed_scroll: 0,
            log_feed_follow_selected: true,
            log_detail_scroll: 0,
            metrics_focus: PaneFocus::Primary,
            selected_metric: 0,
            metric_feed_scroll: 0,
            metric_feed_follow_selected: true,
            metric_detail_scroll: 0,
            llm_focus: PaneFocus::Primary,
            selected_llm: 0,
            llm_feed_scroll: 0,
            llm_feed_follow_selected: true,
            llm_detail_scroll: 0,
            llm_expand_prompt: false,
            llm_expand_output: false,
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
            log_pinned_trace_id: None,
            log_pinned_span_id: None,
            log_tail: false,
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum TraceFocus {
    TraceList,
    TraceTree,
    TraceDetail,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum TraceViewMode {
    List,
    Detail,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum PaneFocus {
    Primary,
    Detail,
}
