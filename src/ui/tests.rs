use std::collections::HashSet;

use chrono::{Duration, Local};
use serde_json::json;

use crate::{
    config::Theme,
    domain::{
        AttributeMap, DashboardSnapshot, LlmAttributes, LlmSummary, LogSummary, MetricSummary,
        OverviewStats, SpanDetail,
    },
    query::TimeWindow,
};

use super::{
    LayoutPreset, LlmFocus, Palette, Tab, TraceFocus, TraceViewMode, UiState,
    chrome::{
        command_palette_window, context_help_lines, context_help_title, footer_text,
        global_status_text, help_lines, help_title,
    },
    details::{
        build_log_detail_lines, format_log_body, llm_detail_lines, llm_timeline_panel_lines,
        metric_chart_values,
    },
    geometry::trace_tree_scroll_offset,
    traces::{
        first_llm_trace_index, format_duration_compact, format_machine_local_time,
        format_trace_timestamp, next_error_trace_index, parent_trace_index,
        previous_error_trace_index, root_trace_index, selected_trace_row, trace_row_badges,
        trace_row_display_name, trace_tree_rows, trace_window, waterfall_bar,
    },
};

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
        .map(|row| format!("{}:{}", row.depth, row.span.span_name))
        .collect::<Vec<_>>();

    assert_eq!(
        rendered,
        vec!["0:request", "1:http.call", "2:cache.get", "1:db.query",]
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
fn trace_tree_rows_mark_latest_finishing_path_as_critical() {
    let rows = trace_tree_rows(
        &[
            span_with_parent("trace", "root", "", "request", 0, 100),
            span_with_parent("trace", "fast", "root", "fast.step", 10, 30),
            span_with_parent("trace", "slow", "root", "slow.step", 20, 90),
            span_with_parent("trace", "slow-leaf", "slow", "slow.leaf", 40, 95),
            span_with_parent("trace", "other", "root", "other.step", 50, 80),
        ],
        &HashSet::new(),
    );

    let critical = rows
        .iter()
        .filter(|row| row.is_critical)
        .map(|row| row.span.span_id.as_str())
        .collect::<Vec<_>>();

    assert_eq!(critical, vec!["root", "slow", "slow-leaf"]);
}

#[test]
fn critical_path_uses_exclusive_time_not_only_latest_finish() {
    let rows = trace_tree_rows(
        &[
            span_with_parent("trace", "root", "", "request", 0, 100),
            span_with_parent("trace", "heavy", "root", "heavy.branch", 0, 90),
            span_with_parent("trace", "heavy-leaf", "heavy", "heavy.leaf", 80, 90),
            span_with_parent("trace", "late", "root", "late.branch", 91, 100),
        ],
        &HashSet::new(),
    );

    let critical = rows
        .iter()
        .filter(|row| row.is_critical)
        .map(|row| row.span.span_id.as_str())
        .collect::<Vec<_>>();

    assert_eq!(critical, vec!["root", "heavy", "heavy-leaf"]);
}

#[test]
fn trace_tree_rows_support_multiple_roots() {
    let rows = trace_tree_rows(
        &[
            span_with_parent("trace", "root-b", "", "background.job", 40, 80),
            span_with_parent("trace", "root-a", "", "http.request", 0, 100),
            span_with_parent("trace", "child-a", "root-a", "db.query", 10, 20),
            span_with_parent("trace", "child-b", "root-b", "cache.get", 50, 60),
        ],
        &HashSet::new(),
    );

    let rendered = rows
        .iter()
        .map(|row| format!("{}:{}", row.depth, row.span.span_name))
        .collect::<Vec<_>>();

    assert_eq!(
        rendered,
        vec![
            "0:http.request",
            "1:db.query",
            "0:background.job",
            "1:cache.get",
        ]
    );
}

#[test]
fn trace_tree_rows_treat_missing_parent_as_root() {
    let rows = trace_tree_rows(
        &[
            span_with_parent("trace", "orphan", "missing-parent", "orphan.step", 0, 10),
            span_with_parent("trace", "root", "", "request", 20, 40),
        ],
        &HashSet::new(),
    );

    let rendered = rows
        .iter()
        .map(|row| format!("{}:{}", row.depth, row.span.span_name))
        .collect::<Vec<_>>();

    assert_eq!(rendered, vec!["0:orphan.step", "0:request"]);
}

#[test]
fn trace_tree_rows_sort_out_of_order_arrival_by_start_time() {
    let rows = trace_tree_rows(
        &[
            span_with_parent("trace", "child-late", "root", "third", 70, 80),
            span_with_parent("trace", "root", "", "request", 0, 100),
            span_with_parent("trace", "child-early", "root", "first", 10, 20),
            span_with_parent("trace", "child-mid", "root", "second", 40, 50),
        ],
        &HashSet::new(),
    );

    let rendered = rows
        .iter()
        .map(|row| row.span.span_name.as_str())
        .collect::<Vec<_>>();

    assert_eq!(rendered, vec!["request", "first", "second", "third"]);
}

#[test]
fn trace_tree_rows_preserve_deep_nesting_depths() {
    let rows = trace_tree_rows(
        &[
            span_with_parent("trace", "level-4", "level-3", "level-4", 40, 50),
            span_with_parent("trace", "level-2", "level-1", "level-2", 20, 60),
            span_with_parent("trace", "level-0", "", "level-0", 0, 100),
            span_with_parent("trace", "level-3", "level-2", "level-3", 30, 55),
            span_with_parent("trace", "level-1", "level-0", "level-1", 10, 70),
        ],
        &HashSet::new(),
    );

    let rendered = rows
        .iter()
        .map(|row| format!("{}:{}", row.depth, row.span.span_name))
        .collect::<Vec<_>>();

    assert_eq!(
        rendered,
        vec![
            "0:level-0",
            "1:level-1",
            "2:level-2",
            "3:level-3",
            "4:level-4",
        ]
    );
}

#[test]
fn trace_navigation_helpers_follow_visible_tree_rows() {
    let snapshot = crate::domain::DashboardSnapshot {
        services: Vec::new(),
        overview: crate::domain::OverviewStats {
            service_count: 0,
            trace_count: 0,
            error_span_count: 0,
            log_count: 0,
            metric_count: 0,
            llm_count: 0,
        },
        traces: Vec::new(),
        selected_trace: vec![
            span_with_parent("trace", "root", "", "request", 0, 100),
            span_with_parent("trace", "http", "root", "http.call", 10, 70),
            span_with_parent("trace", "cache", "http", "cache.get", 20, 30),
            SpanDetail {
                status_code: "STATUS_CODE_ERROR".to_string(),
                ..span_with_parent("trace", "db", "root", "db.query", 75, 95)
            },
            SpanDetail {
                llm: Some(LlmAttributes {
                    model: Some("gpt-5.4".to_string()),
                    ..LlmAttributes::default()
                }),
                ..span_with_parent("trace", "llm", "root", "chat.completion", 96, 110)
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
    let state = UiState {
        selected_trace_span: 2,
        ..UiState::default()
    };

    assert_eq!(previous_error_trace_index(&snapshot, &state), None);
    assert_eq!(next_error_trace_index(&snapshot, &state), Some(3));
    assert_eq!(parent_trace_index(&snapshot, &state), Some(1));
    assert_eq!(root_trace_index(&snapshot, &state), Some(0));
    assert_eq!(first_llm_trace_index(&snapshot, &state), Some(4));
}

#[test]
fn waterfall_bar_uses_relative_trace_timing() {
    let span = span_with_parent("trace", "child", "root", "db.query", 25, 75);
    let rows = trace_tree_rows(std::slice::from_ref(&span), &HashSet::new());
    let bar = waterfall_bar(
        trace_window(&[
            span_with_parent("trace", "root", "", "request", 0, 100),
            span.clone(),
        ]),
        &rows[0],
        8,
    );

    assert_eq!(bar.before, "··");
    assert_eq!(bar.active, "━━━━");
    assert_eq!(bar.after, "··");
}

#[test]
fn duration_format_compacts_long_values() {
    assert_eq!(format_duration_compact(58.6), "58.6ms");
    assert_eq!(format_duration_compact(1_101.7), "1.10s");
    assert_eq!(format_duration_compact(62_500.0), "1.0m");
}

#[test]
fn trace_timestamp_uses_time_only_for_today() {
    let now = Local::now();
    let rendered = format_trace_timestamp(now.timestamp_nanos_opt().unwrap_or_default());
    assert_eq!(rendered.len(), 8);
    assert_eq!(&rendered[2..3], ":");
    assert_eq!(&rendered[5..6], ":");
}

#[test]
fn machine_local_time_includes_calendar_date_for_older_events() {
    let older = Local::now() - Duration::days(1);
    let rendered = format_machine_local_time(older.timestamp_nanos_opt().unwrap_or_default());
    assert_eq!(rendered.len(), 19);
    assert_eq!(&rendered[4..5], "-");
    assert_eq!(&rendered[7..8], "-");
    assert_eq!(&rendered[10..11], " ");
    assert_eq!(&rendered[13..14], ":");
    assert_eq!(&rendered[16..17], ":");
}

#[test]
fn trace_row_display_name_prefers_tool_name_for_generic_wrappers() {
    let mut span = span_with_parent("trace", "tool", "root", "running tool", 0, 100);
    span.attributes
        .insert("tool.name".to_string(), json!("product_tool"));

    assert_eq!(trace_row_display_name(&span), "product_tool");
}

#[test]
fn trace_row_display_name_simplifies_template_wrappers() {
    assert_eq!(
        trace_row_display_name(&span_with_parent(
            "trace",
            "a",
            "root",
            "evaluate {name}",
            0,
            1
        )),
        "evaluate"
    );
    assert_eq!(
        trace_row_display_name(&span_with_parent(
            "trace",
            "b",
            "root",
            "case: {case_name}",
            0,
            1
        )),
        "case"
    );
    assert_eq!(
        trace_row_display_name(&span_with_parent(
            "trace",
            "c",
            "root",
            "Prompt: Synthesizer:conversation",
            0,
            1
        )),
        "Synthesizer:conversation"
    );
}

#[test]
fn trace_list_root_names_use_wrapper_simplification() {
    let snapshot = DashboardSnapshot {
        services: Vec::new(),
        overview: OverviewStats {
            service_count: 0,
            trace_count: 1,
            error_span_count: 0,
            log_count: 0,
            metric_count: 0,
            llm_count: 0,
        },
        traces: vec![crate::domain::TraceSummary {
            trace_id: "trace-1".to_string(),
            service_name: "api".to_string(),
            root_name: "evaluate {name}".to_string(),
            span_count: 1,
            error_count: 0,
            duration_ms: 10.0,
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
    };

    assert_eq!(
        trace_row_display_name(&SpanDetail {
            span_name: snapshot.traces[0].root_name.clone(),
            ..span_with_parent("trace", "root", "", "ignored", 0, 1)
        }),
        "evaluate"
    );
}

#[test]
fn trace_row_badges_include_error_tool_and_llm_metadata() {
    let mut span = span_with_parent("trace", "tool", "root", "chat", 0, 100);
    span.status_code = "STATUS_CODE_ERROR".to_string();
    span.attributes
        .insert("tool.name".to_string(), json!("product_tool"));
    span.llm = Some(LlmAttributes {
        model: Some("gpt-4o".to_string()),
        ..LlmAttributes::default()
    });

    let badges = trace_row_badges(&span)
        .into_iter()
        .map(|badge| badge.label)
        .collect::<Vec<_>>();

    assert_eq!(badges, vec!["ERR", "tool product_tool", "LLM gpt-4o"]);
}

#[test]
fn trace_row_badges_skip_plain_llm_badge_for_low_signal_wrappers() {
    let span = SpanDetail {
        llm: Some(LlmAttributes::default()),
        ..span_with_parent("trace", "llm", "root", "agent run", 0, 1)
    };

    assert!(trace_row_badges(&span).is_empty());
}

#[test]
fn ui_state_defaults_to_trace_list_focus() {
    let state = UiState::default();
    assert_eq!(state.theme, Theme::Ember);
    assert_eq!(state.layout_preset, LayoutPreset::Balanced);
    assert_eq!(state.trace_split_pct, 62);
    assert_eq!(state.log_split_pct, 58);
    assert_eq!(state.metric_split_pct, 52);
    assert_eq!(state.llm_split_pct, 55);
    assert_eq!(state.trace_view_mode, TraceViewMode::List);
    assert_eq!(state.trace_focus, TraceFocus::TraceList);
    assert_eq!(state.selected_trace_span, 0);
    assert_eq!(state.trace_tree_scroll, 0);
    assert_eq!(state.trace_detail_scroll, 0);
    assert!(state.collapsed_trace_spans.is_empty());
    assert!(!state.show_help);
    assert!(!state.show_context_help);
    assert!(!state.show_command_palette);
    assert!(state.command_query.is_empty());
    assert_eq!(state.selected_command, 0);
    assert_eq!(state.command_palette_scroll, 0);
    assert_eq!(state.log_detail_scroll, 0);
    assert_eq!(state.metric_detail_scroll, 0);
    assert_eq!(state.llm_detail_scroll, 0);
    assert!(!state.llm_expand_prompt);
    assert!(!state.llm_expand_output);
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
fn help_title_and_footer_follow_active_pane() {
    let mut state = UiState::default();
    state.active_tab = Tab::Logs as usize;
    state.show_help = true;

    assert_eq!(help_title(&state), "Help: Logs Feed");
    assert_eq!(footer_text(&state), "help: esc/?/enter close");
}

#[test]
fn global_status_owns_service_and_time_key_hints() {
    let mut state = UiState {
        active_tab: Tab::Logs as usize,
        ..UiState::default()
    };
    let snapshot = DashboardSnapshot {
        services: Vec::new(),
        overview: OverviewStats {
            service_count: 0,
            trace_count: 1,
            error_span_count: 0,
            log_count: 2,
            metric_count: 3,
            llm_count: 4,
        },
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

    let status = global_status_text(&snapshot, &state);
    assert!(status.contains("[s]ervice=all"));
    assert!(status.contains("[t]ime=24h"));
    assert!(!status.contains("search=none"));
    assert!(!status.contains("search=-"));
    assert!(!footer_text(&state).contains("s service"));
    assert!(!footer_text(&state).contains("t window"));

    state.search_query = "latency".to_string();
    assert!(global_status_text(&snapshot, &state).contains("search=latency"));
}

#[test]
fn context_help_lines_follow_active_focus() {
    let state = UiState {
        active_tab: Tab::Llm as usize,
        llm_focus: LlmFocus::Detail,
        ..UiState::default()
    };

    let rendered = context_help_lines(&state)
        .into_iter()
        .map(|line| line.to_string())
        .collect::<Vec<_>>();

    assert_eq!(context_help_title(&state), "Hints: Model Detail");
    assert!(rendered.iter().any(|line| line.contains("i/o")));
    assert!(rendered.iter().any(|line| line.contains("? full help")));
}

#[test]
fn help_lines_include_trace_tree_commands() {
    let mut state = UiState {
        active_tab: Tab::Traces as usize,
        trace_focus: TraceFocus::TraceTree,
        ..UiState::default()
    };
    state.show_help = true;

    let rendered = help_lines(&state)
        .into_iter()
        .map(|line| line.to_string())
        .collect::<Vec<_>>();

    assert!(rendered.iter().any(|line| line.contains("trace tree")));
    assert!(
        rendered
            .iter()
            .any(|line| line.contains("space / enter    collapse or expand subtree"))
    );
    assert!(
        rendered
            .iter()
            .any(|line| line.contains("?                open/close help"))
    );
    assert!(
        rendered
            .iter()
            .any(|line| line.contains("g                cycle theme"))
    );
    assert!(
        rendered
            .iter()
            .any(|line| line.contains("w                cycle layout preset"))
    );
    assert!(
        rendered
            .iter()
            .any(|line| line.contains("= / -            grow or shrink focused split"))
    );
}

#[test]
fn command_palette_window_tracks_selected_command() {
    assert_eq!(command_palette_window(0, 0, 3), (0, 3));
    assert_eq!(command_palette_window(0, 0, 12), (0, 8));
    assert_eq!(command_palette_window(4, 10, 12), (4, 12));
    assert_eq!(command_palette_window(4, 9, 12), (4, 12));
    assert_eq!(command_palette_window(4, 4, 12), (4, 12));
    assert_eq!(command_palette_window(4, 3, 12), (3, 11));
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

    let lines = build_log_detail_lines(&log, Palette::from_theme(Theme::Ember))
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

#[test]
fn llm_detail_lines_show_prompt_output_tool_and_normalized_json() {
    let snapshot = crate::domain::DashboardSnapshot {
        services: Vec::new(),
        overview: crate::domain::OverviewStats {
            service_count: 0,
            trace_count: 0,
            error_span_count: 0,
            log_count: 0,
            metric_count: 0,
            llm_count: 1,
        },
        traces: Vec::new(),
        selected_trace: Vec::new(),
        logs: Vec::new(),
        metrics: Vec::new(),
        llm: vec![LlmSummary {
            trace_id: "trace-1".to_string(),
            span_id: "span-1".to_string(),
            started_at_unix_nano: 65_956_000_000_000,
            service_name: "api".to_string(),
            provider: "openai".to_string(),
            model: "gpt-5.4".to_string(),
            operation: "chat".to_string(),
            span_kind: Some("chain".to_string()),
            session_id: None,
            conversation_id: None,
            prompt_preview: Some("{\"prompt\":\"hello\"}".to_string()),
            output_preview: Some("world".to_string()),
            tool_name: Some("lookup_customer".to_string()),
            tool_args: Some("{\"customer_id\":\"123\"}".to_string()),
            input_tokens: Some(11),
            output_tokens: Some(7),
            total_tokens: Some(18),
            cost: Some(0.0042),
            latency_ms: Some(42.5),
            status: "STATUS_CODE_OK".to_string(),
            raw_json: json!({
                "provider": "openai",
                "model": "gpt-5.4",
                "tool_name": "lookup_customer"
            }),
        }],
        llm_rollups: Vec::new(),
        llm_sessions: Vec::new(),
        llm_model_comparisons: Vec::new(),
        llm_top_calls: Vec::new(),
        selected_llm_timeline: vec![
            crate::domain::LlmTimelineItem {
                kind: crate::domain::LlmTimelineKind::Prompt,
                label: "input".to_string(),
                detail: Some("hello".to_string()),
                offset_ms: 0.0,
                duration_ms: None,
                status: None,
            },
            crate::domain::LlmTimelineItem {
                kind: crate::domain::LlmTimelineKind::Tool,
                label: "lookup_customer".to_string(),
                detail: Some("{\"customer_id\":\"123\"}".to_string()),
                offset_ms: 12.0,
                duration_ms: Some(8.0),
                status: Some("STATUS_CODE_OK".to_string()),
            },
            crate::domain::LlmTimelineItem {
                kind: crate::domain::LlmTimelineKind::Output,
                label: "output".to_string(),
                detail: Some("world".to_string()),
                offset_ms: 42.5,
                duration_ms: None,
                status: None,
            },
        ],
    };

    let rendered = llm_detail_lines(
        &snapshot,
        &UiState::default(),
        Palette::from_theme(Theme::Ember),
    )
    .into_iter()
    .map(|line| line.to_string())
    .collect::<Vec<_>>();

    assert!(rendered.iter().any(|line| line.contains("prompt")));
    assert!(
        rendered
            .iter()
            .any(|line| line.contains("\"prompt\": \"hello\""))
    );
    assert!(rendered.iter().any(|line| line.contains("output")));
    assert!(rendered.iter().any(|line| line.contains("world")));
    assert!(rendered.iter().any(|line| line.contains("lookup_customer")));
    assert!(rendered.iter().any(|line| line.contains("normalized")));
    assert!(
        rendered
            .iter()
            .any(|line| line.contains("\"provider\": \"openai\""))
    );
}

#[test]
fn llm_timeline_panel_lines_show_timeline_steps() {
    let snapshot = crate::domain::DashboardSnapshot {
        services: Vec::new(),
        overview: crate::domain::OverviewStats {
            service_count: 0,
            trace_count: 0,
            error_span_count: 0,
            log_count: 0,
            metric_count: 0,
            llm_count: 1,
        },
        traces: Vec::new(),
        selected_trace: Vec::new(),
        logs: Vec::new(),
        metrics: Vec::new(),
        llm: vec![LlmSummary {
            trace_id: "trace-1".to_string(),
            span_id: "span-1".to_string(),
            started_at_unix_nano: 65_956_000_000_000,
            service_name: "api".to_string(),
            provider: "openai".to_string(),
            model: "gpt-5.4".to_string(),
            operation: "chat".to_string(),
            span_kind: None,
            session_id: None,
            conversation_id: None,
            prompt_preview: None,
            output_preview: None,
            tool_name: None,
            tool_args: None,
            input_tokens: None,
            output_tokens: None,
            total_tokens: None,
            cost: None,
            latency_ms: None,
            status: "STATUS_CODE_OK".to_string(),
            raw_json: json!({}),
        }],
        llm_rollups: Vec::new(),
        llm_sessions: Vec::new(),
        llm_model_comparisons: Vec::new(),
        llm_top_calls: Vec::new(),
        selected_llm_timeline: vec![
            crate::domain::LlmTimelineItem {
                kind: crate::domain::LlmTimelineKind::Prompt,
                label: "input".to_string(),
                detail: Some("hello".to_string()),
                offset_ms: 0.0,
                duration_ms: None,
                status: None,
            },
            crate::domain::LlmTimelineItem {
                kind: crate::domain::LlmTimelineKind::Tool,
                label: "lookup_customer".to_string(),
                detail: Some("{\"customer_id\":\"123\"}".to_string()),
                offset_ms: 12.0,
                duration_ms: Some(8.0),
                status: Some("STATUS_CODE_OK".to_string()),
            },
        ],
    };

    let rendered = llm_timeline_panel_lines(
        &snapshot,
        &UiState::default(),
        Palette::from_theme(Theme::Ember),
    )
    .into_iter()
    .map(|line| line.to_string())
    .collect::<Vec<_>>();

    assert!(rendered.iter().any(|line| line.contains("prompt input")));
    assert!(
        rendered
            .iter()
            .any(|line| line.contains("tool lookup_customer"))
    );
}

#[test]
fn llm_detail_lines_truncate_prompt_and_output_by_default() {
    let long_prompt = (1..=12)
        .map(|index| format!("prompt line {index}"))
        .collect::<Vec<_>>()
        .join("\n");
    let long_output = (1..=11)
        .map(|index| format!("output line {index}"))
        .collect::<Vec<_>>()
        .join("\n");
    let snapshot = crate::domain::DashboardSnapshot {
        services: Vec::new(),
        overview: crate::domain::OverviewStats {
            service_count: 0,
            trace_count: 0,
            error_span_count: 0,
            log_count: 0,
            metric_count: 0,
            llm_count: 1,
        },
        traces: Vec::new(),
        selected_trace: Vec::new(),
        logs: Vec::new(),
        metrics: Vec::new(),
        llm: vec![LlmSummary {
            trace_id: "trace-1".to_string(),
            span_id: "span-1".to_string(),
            started_at_unix_nano: 65_956_000_000_000,
            service_name: "api".to_string(),
            provider: "openai".to_string(),
            model: "gpt-5.4".to_string(),
            operation: "chat".to_string(),
            span_kind: None,
            session_id: None,
            conversation_id: None,
            prompt_preview: Some(long_prompt),
            output_preview: Some(long_output),
            tool_name: None,
            tool_args: None,
            input_tokens: Some(11),
            output_tokens: Some(7),
            total_tokens: Some(18),
            cost: None,
            latency_ms: Some(42.5),
            status: "STATUS_CODE_OK".to_string(),
            raw_json: json!({}),
        }],
        llm_rollups: Vec::new(),
        llm_sessions: Vec::new(),
        llm_model_comparisons: Vec::new(),
        llm_top_calls: Vec::new(),
        selected_llm_timeline: Vec::new(),
    };

    let rendered = llm_detail_lines(
        &snapshot,
        &UiState::default(),
        Palette::from_theme(Theme::Ember),
    )
    .into_iter()
    .map(|line| line.to_string())
    .collect::<Vec<_>>();

    assert!(rendered.iter().any(|line| line.contains("prompt line 8")));
    assert!(!rendered.iter().any(|line| line.contains("prompt line 9")));
    assert!(
        rendered
            .iter()
            .any(|line| line.contains("press i to expand"))
    );
    assert!(rendered.iter().any(|line| line.contains("output line 8")));
    assert!(!rendered.iter().any(|line| line.contains("output line 9")));
    assert!(
        rendered
            .iter()
            .any(|line| line.contains("press o to expand"))
    );
}

#[test]
fn llm_detail_lines_expand_prompt_and_output_when_toggled() {
    let long_prompt = (1..=12)
        .map(|index| format!("prompt line {index}"))
        .collect::<Vec<_>>()
        .join("\n");
    let long_output = (1..=11)
        .map(|index| format!("output line {index}"))
        .collect::<Vec<_>>()
        .join("\n");
    let snapshot = crate::domain::DashboardSnapshot {
        services: Vec::new(),
        overview: crate::domain::OverviewStats {
            service_count: 0,
            trace_count: 0,
            error_span_count: 0,
            log_count: 0,
            metric_count: 0,
            llm_count: 1,
        },
        traces: Vec::new(),
        selected_trace: Vec::new(),
        logs: Vec::new(),
        metrics: Vec::new(),
        llm: vec![LlmSummary {
            trace_id: "trace-1".to_string(),
            span_id: "span-1".to_string(),
            started_at_unix_nano: 65_956_000_000_000,
            service_name: "api".to_string(),
            provider: "openai".to_string(),
            model: "gpt-5.4".to_string(),
            operation: "chat".to_string(),
            span_kind: None,
            session_id: None,
            conversation_id: None,
            prompt_preview: Some(long_prompt),
            output_preview: Some(long_output),
            tool_name: None,
            tool_args: None,
            input_tokens: Some(11),
            output_tokens: Some(7),
            total_tokens: Some(18),
            cost: None,
            latency_ms: Some(42.5),
            status: "STATUS_CODE_OK".to_string(),
            raw_json: json!({}),
        }],
        llm_rollups: Vec::new(),
        llm_sessions: Vec::new(),
        llm_model_comparisons: Vec::new(),
        llm_top_calls: Vec::new(),
        selected_llm_timeline: Vec::new(),
    };
    let state = UiState {
        llm_expand_prompt: true,
        llm_expand_output: true,
        ..UiState::default()
    };

    let rendered = llm_detail_lines(&snapshot, &state, Palette::from_theme(Theme::Ember))
        .into_iter()
        .map(|line| line.to_string())
        .collect::<Vec<_>>();

    assert!(rendered.iter().any(|line| line.contains("prompt line 12")));
    assert!(rendered.iter().any(|line| line.contains("output line 11")));
    assert!(
        rendered
            .iter()
            .any(|line| line.contains("press i to collapse"))
    );
    assert!(
        rendered
            .iter()
            .any(|line| line.contains("press o to collapse"))
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
