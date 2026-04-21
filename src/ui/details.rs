use ratatui::{
    prelude::{Modifier, Style},
    text::{Line, Span},
};

use crate::domain::{
    DashboardSnapshot, LlmTimelineItem, LlmTimelineKind, LogSummary, MetricSummary, SpanDetail,
    truncate,
};

use super::{Palette, UiState, traces};

const LLM_PREVIEW_LINE_LIMIT: usize = 8;
const LLM_PREVIEW_WRAP_WIDTH_ESTIMATE: usize = 100;

#[derive(Debug, Default)]
pub(crate) struct TraceDetailLinesCache {
    key: Option<TraceDetailLinesKey>,
    lines: Vec<Line<'static>>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
struct TraceDetailLinesKey {
    trace_id: String,
    span_id: String,
    end_time_unix_nano: i64,
    attribute_count: usize,
    resource_attribute_count: usize,
    event_count: usize,
    link_count: usize,
}

#[derive(Debug, Default)]
pub(crate) struct LogDetailLinesCache {
    key: Option<crate::domain::LogSummary>,
    lines: Vec<Line<'static>>,
}

#[derive(Debug, Default)]
pub(crate) struct MetricDetailLinesCache {
    key: Option<MetricDetailLinesKey>,
    lines: Vec<Line<'static>>,
}

#[derive(Debug, Clone, PartialEq)]
struct MetricDetailLinesKey {
    selected: MetricSummary,
    series: Vec<MetricSummary>,
}

#[derive(Debug, Default)]
pub(crate) struct LlmDetailLinesCache {
    key: Option<LlmDetailLinesKey>,
    lines: Vec<Line<'static>>,
}

#[derive(Debug, Clone, PartialEq)]
struct LlmDetailLinesKey {
    item: crate::domain::LlmSummary,
    timeline: Vec<LlmTimelineItem>,
    expand_prompt: bool,
    expand_output: bool,
}

pub(crate) fn selected_metric_series(
    snapshot: &DashboardSnapshot,
    selected_index: usize,
) -> Vec<MetricSummary> {
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

pub(crate) fn metric_chart_values(series: &[MetricSummary]) -> Vec<u64> {
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

pub(crate) fn sync_trace_detail_lines_cache(
    snapshot: &DashboardSnapshot,
    state: &UiState,
    palette: Palette,
    cache: &mut TraceDetailLinesCache,
) {
    let Some(span) = traces::selected_trace_span_detail(snapshot, state) else {
        cache.key = None;
        cache.lines = vec![Line::raw(
            "Select a trace and move focus to the tree to inspect spans.",
        )];
        return;
    };

    let next_key = TraceDetailLinesKey {
        trace_id: span.trace_id.clone(),
        span_id: span.span_id.clone(),
        end_time_unix_nano: span.end_time_unix_nano,
        attribute_count: span.attributes.len(),
        resource_attribute_count: span.resource_attributes.len(),
        event_count: span.events.len(),
        link_count: span.links.len(),
    };

    if cache.key.as_ref() != Some(&next_key) {
        cache.lines = build_span_detail_lines(&span, palette);
        cache.key = Some(next_key);
    }
}

pub(crate) fn cached_trace_detail_lines(cache: &TraceDetailLinesCache) -> &[Line<'static>] {
    &cache.lines
}

#[cfg(test)]
#[allow(dead_code)]
pub(crate) fn log_detail_lines(
    snapshot: &DashboardSnapshot,
    state: &UiState,
    palette: Palette,
) -> Vec<Line<'static>> {
    snapshot
        .logs
        .get(state.selected_log)
        .map(|log| build_log_detail_lines(log, palette))
        .unwrap_or_else(|| vec![Line::raw("No log selected.")])
}

pub(crate) fn sync_log_detail_lines_cache(
    snapshot: &DashboardSnapshot,
    state: &UiState,
    palette: Palette,
    cache: &mut LogDetailLinesCache,
) {
    let Some(log) = snapshot.logs.get(state.selected_log) else {
        cache.key = None;
        cache.lines = vec![Line::raw("No log selected.")];
        return;
    };

    if cache.key.as_ref() != Some(log) {
        cache.lines = build_log_detail_lines(log, palette);
        cache.key = Some(log.clone());
    }
}

pub(crate) fn cached_log_detail_lines(cache: &LogDetailLinesCache) -> &[Line<'static>] {
    &cache.lines
}

#[cfg(test)]
#[allow(dead_code)]
pub(crate) fn metric_detail_lines(
    snapshot: &DashboardSnapshot,
    state: &UiState,
    palette: Palette,
) -> Vec<Line<'static>> {
    let series = selected_metric_series(snapshot, state.selected_metric);
    build_metric_detail_lines(snapshot, state.selected_metric, &series, palette)
}

pub(crate) fn sync_metric_detail_lines_cache(
    snapshot: &DashboardSnapshot,
    state: &UiState,
    palette: Palette,
    cache: &mut MetricDetailLinesCache,
) {
    let Some(selected) = snapshot.metrics.get(state.selected_metric) else {
        cache.key = None;
        cache.lines = vec![Line::raw("No metric selected.")];
        return;
    };

    let series = selected_metric_series(snapshot, state.selected_metric);
    let next_key = MetricDetailLinesKey {
        selected: selected.clone(),
        series: series.clone(),
    };

    if cache.key.as_ref() != Some(&next_key) {
        cache.lines = build_metric_detail_lines(snapshot, state.selected_metric, &series, palette);
        cache.key = Some(next_key);
    }
}

pub(crate) fn cached_metric_detail_lines(cache: &MetricDetailLinesCache) -> &[Line<'static>] {
    &cache.lines
}

#[cfg(test)]
#[allow(dead_code)]
pub(crate) fn llm_detail_lines(
    snapshot: &DashboardSnapshot,
    state: &UiState,
    palette: Palette,
) -> Vec<Line<'static>> {
    snapshot
        .llm
        .get(state.selected_llm)
        .map(|item| build_llm_detail_lines(item, state, palette))
        .unwrap_or_else(|| vec![Line::raw("No LLM spans yet.")])
}

pub(crate) fn sync_llm_detail_lines_cache(
    snapshot: &DashboardSnapshot,
    state: &UiState,
    palette: Palette,
    cache: &mut LlmDetailLinesCache,
) {
    let Some(item) = snapshot.llm.get(state.selected_llm) else {
        cache.key = None;
        cache.lines = vec![Line::raw("No LLM spans yet.")];
        return;
    };

    let next_key = LlmDetailLinesKey {
        item: item.clone(),
        timeline: snapshot.selected_llm_timeline.clone(),
        expand_prompt: state.llm_expand_prompt,
        expand_output: state.llm_expand_output,
    };

    // Rebuild LLM detail lines on each sync instead of relying on the cache key.
    // The selected row can change while the timeline refresh is still catching up,
    // and a stale cached block is more noticeable here than the rebuild cost.
    cache.lines = build_llm_detail_lines(item, state, palette);
    cache.key = Some(next_key);
}

pub(crate) fn cached_llm_detail_lines(cache: &LlmDetailLinesCache) -> &[Line<'static>] {
    &cache.lines
}

pub(crate) fn llm_timeline_panel_lines(
    snapshot: &DashboardSnapshot,
    state: &UiState,
    palette: Palette,
) -> Vec<Line<'static>> {
    let Some(_) = snapshot.llm.get(state.selected_llm) else {
        return vec![Line::raw("No LLM spans yet.")];
    };

    if snapshot.selected_llm_timeline.is_empty() {
        return vec![Line::raw("")];
    }

    llm_timeline_lines(&snapshot.selected_llm_timeline, palette)
}

pub(crate) fn build_log_detail_lines(log: &LogSummary, palette: Palette) -> Vec<Line<'static>> {
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

pub(crate) fn format_log_body(body: &str) -> Vec<String> {
    if let Ok(value) = serde_json::from_str::<serde_json::Value>(body) {
        if let Ok(pretty) = serde_json::to_string_pretty(&value) {
            return pretty.lines().map(ToString::to_string).collect();
        }
    }

    body.lines().map(ToString::to_string).collect()
}

fn build_llm_detail_lines(
    item: &crate::domain::LlmSummary,
    state: &UiState,
    palette: Palette,
) -> Vec<Line<'static>> {
    let mut lines = vec![
        Line::from(Span::styled(
            truncate(&item.model, 48),
            Style::default()
                .fg(palette.foreground)
                .add_modifier(Modifier::BOLD),
        )),
        Line::from(format!("service {}", item.service_name)),
        Line::from(format!("trace {}", item.trace_id)),
        Line::from(format!("span {}", item.span_id)),
        Line::from(format!("provider {}", item.provider)),
        Line::from(format!("operation {}", item.operation)),
        Line::from(format!(
            "kind {}",
            item.span_kind.as_deref().unwrap_or("<unset>")
        )),
        Line::from(format!(
            "session {}",
            item.session_id.as_deref().unwrap_or("<none>")
        )),
        Line::from(format!(
            "conversation {}",
            item.conversation_id.as_deref().unwrap_or("<none>")
        )),
        Line::from(format!("status {}", item.status)),
        Line::from(format!(
            "tokens in={} out={} total={}",
            item.input_tokens.unwrap_or_default(),
            item.output_tokens.unwrap_or_default(),
            item.total_tokens.unwrap_or_default()
        )),
        Line::from(format!(
            "latency {} ms  cost {}",
            optional_number(item.latency_ms, 3),
            optional_number(item.cost, 6)
        )),
    ];

    if let Some(prompt) = item
        .prompt_preview
        .as_deref()
        .filter(|value| !value.is_empty())
    {
        lines.push(Line::raw(""));
        lines.push(section_header("prompt", palette.accent));
        lines.extend(
            truncated_block(
                prompt,
                state.llm_expand_prompt,
                LLM_PREVIEW_LINE_LIMIT,
                'i',
                palette.muted,
            )
            .into_iter()
            .map(Line::from),
        );
    }

    if let Some(output) = item
        .output_preview
        .as_deref()
        .filter(|value| !value.is_empty())
    {
        lines.push(Line::raw(""));
        lines.push(section_header("output", palette.success));
        lines.extend(
            truncated_block(
                output,
                state.llm_expand_output,
                LLM_PREVIEW_LINE_LIMIT,
                'o',
                palette.muted,
            )
            .into_iter()
            .map(Line::from),
        );
    }

    if item.tool_name.is_some() || item.tool_args.is_some() {
        lines.push(Line::raw(""));
        lines.push(section_header("tool", palette.warning));
        if let Some(name) = &item.tool_name {
            lines.push(Line::from(format!("name {name}")));
        }
        if let Some(args) = item.tool_args.as_deref().filter(|value| !value.is_empty()) {
            lines.push(Line::from("args"));
            lines.extend(multiline_block(args).into_iter().map(Line::from));
        }
    }

    lines
}

fn section_header(label: &str, color: ratatui::prelude::Color) -> Line<'static> {
    Line::from(Span::styled(
        label.to_string(),
        Style::default().fg(color).add_modifier(Modifier::BOLD),
    ))
}

fn multiline_block(text: &str) -> Vec<String> {
    if let Ok(mut value) = serde_json::from_str::<serde_json::Value>(text) {
        decode_embedded_json_strings(&mut value);
        return format_json_value(&value);
    }

    text.lines().map(ToString::to_string).collect()
}

fn decode_embedded_json_strings(value: &mut serde_json::Value) {
    match value {
        serde_json::Value::Array(items) => {
            for item in items {
                decode_embedded_json_strings(item);
            }
        }
        serde_json::Value::Object(object) => {
            for child in object.values_mut() {
                decode_embedded_json_strings(child);
            }
        }
        serde_json::Value::String(text) => {
            let Ok(mut parsed) = serde_json::from_str::<serde_json::Value>(text) else {
                return;
            };
            if !matches!(
                parsed,
                serde_json::Value::Array(_) | serde_json::Value::Object(_)
            ) {
                return;
            }
            decode_embedded_json_strings(&mut parsed);
            *value = parsed;
        }
        _ => {}
    }
}

fn llm_timeline_lines(items: &[LlmTimelineItem], palette: Palette) -> Vec<Line<'static>> {
    let total_ms = items
        .iter()
        .map(|item| item.offset_ms + item.duration_ms.unwrap_or(0.0))
        .fold(0.0, f64::max)
        .max(1.0);

    let mut lines = Vec::new();
    for item in items {
        let lane = timeline_lane(item, total_ms, 18);
        let color = match item.kind {
            LlmTimelineKind::Prompt => palette.accent,
            LlmTimelineKind::Tool => palette.warning,
            LlmTimelineKind::Output => palette.success,
            LlmTimelineKind::Step => palette.muted,
        };
        let duration = item
            .duration_ms
            .map(|value| format!(" {value:.1}ms"))
            .unwrap_or_default();
        lines.push(Line::from(vec![
            Span::styled(
                format!("{:>6.1}ms ", item.offset_ms),
                Style::default().fg(palette.muted),
            ),
            Span::styled(lane, Style::default().fg(color)),
            Span::raw(" "),
            Span::styled(
                format!("{} {}", item.kind.label(), item.label),
                Style::default()
                    .fg(palette.foreground)
                    .add_modifier(Modifier::BOLD),
            ),
            Span::styled(duration, Style::default().fg(palette.muted)),
        ]));
        if let Some(detail) = item.detail.as_deref().filter(|detail| !detail.is_empty()) {
            lines.push(Line::from(vec![
                Span::raw("        "),
                Span::styled(
                    truncate(&detail.replace('\n', " "), 72),
                    Style::default().fg(palette.muted),
                ),
            ]));
        }
    }
    lines
}

fn timeline_lane(item: &LlmTimelineItem, total_ms: f64, width: usize) -> String {
    if width == 0 {
        return String::new();
    }

    let start = ((item.offset_ms / total_ms) * width as f64).floor() as usize;
    let duration = item.duration_ms.unwrap_or(0.0);
    let extent = if duration <= 0.0 {
        1
    } else {
        ((duration / total_ms) * width as f64).ceil().max(1.0) as usize
    };
    let end = (start + extent).min(width);
    (0..width)
        .map(|index| {
            if index >= start && index < end {
                if matches!(item.kind, LlmTimelineKind::Prompt | LlmTimelineKind::Output) {
                    '●'
                } else {
                    '━'
                }
            } else {
                '·'
            }
        })
        .collect()
}

fn truncated_block(
    text: &str,
    expanded: bool,
    line_limit: usize,
    toggle_key: char,
    muted: ratatui::prelude::Color,
) -> Vec<Line<'static>> {
    let lines = multiline_block(text);
    let rows = estimated_wrapped_rows(&lines, LLM_PREVIEW_WRAP_WIDTH_ESTIMATE);
    if expanded || rows.len() <= line_limit {
        let mut rendered = lines.into_iter().map(Line::from).collect::<Vec<_>>();
        if expanded && rows.len() > line_limit {
            rendered.push(Line::from(Span::styled(
                format!("press {toggle_key} to collapse"),
                Style::default().fg(muted),
            )));
        }
        return rendered;
    }

    let hidden_count = rows.len() - line_limit;
    let mut rendered = vec![Line::from(Span::styled(
        format!("... {hidden_count} more lines (press {toggle_key} to expand)"),
        Style::default().fg(muted),
    ))];
    rendered.extend(rows.into_iter().take(line_limit).map(Line::from));
    rendered
}

fn estimated_wrapped_rows(lines: &[String], wrap_width: usize) -> Vec<String> {
    if wrap_width == 0 {
        return lines.to_vec();
    }

    let mut rows = Vec::new();
    for line in lines {
        if line.is_empty() {
            rows.push(String::new());
            continue;
        }
        let chars = line.chars().collect::<Vec<_>>();
        for chunk in chars.chunks(wrap_width) {
            rows.push(chunk.iter().collect());
        }
    }
    rows
}

fn format_json_value(value: &serde_json::Value) -> Vec<String> {
    serde_json::to_string_pretty(value)
        .map(|pretty| pretty.lines().map(ToString::to_string).collect())
        .unwrap_or_else(|_| vec![value.to_string()])
}

fn optional_number(value: Option<f64>, precision: usize) -> String {
    value
        .map(|number| format!("{number:.precision$}"))
        .unwrap_or_else(|| "-".to_string())
}

pub(crate) fn wrapped_line_count(lines: &[Line<'static>], viewport_width: usize) -> usize {
    if viewport_width == 0 {
        return lines.len();
    }

    lines
        .iter()
        .map(|line| {
            let width = line.to_string().chars().count();
            width.div_ceil(viewport_width).max(1)
        })
        .sum()
}

fn build_metric_detail_lines(
    snapshot: &DashboardSnapshot,
    selected_index: usize,
    series: &[MetricSummary],
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

fn build_span_detail_lines(span: &SpanDetail, palette: Palette) -> Vec<Line<'static>> {
    let mut header_spans = vec![Span::styled(
        truncate(&span.span_name, 48),
        Style::default()
            .fg(palette.foreground)
            .add_modifier(Modifier::BOLD),
    )];

    if let Some(status_badge) = status_badge(&span.status_code) {
        header_spans.push(Span::raw(" "));
        header_spans.push(Span::styled(
            status_badge,
            match span.status_code.as_str() {
                "STATUS_CODE_ERROR" => Style::default().fg(palette.warning),
                "STATUS_CODE_OK" => Style::default().fg(palette.success),
                _ => Style::default().fg(palette.muted),
            },
        ));
    }

    let mut lines = vec![
        Line::from(header_spans),
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
        for (key, value) in &span.resource_attributes {
            lines.push(Line::from(format!(
                "{key} = {}",
                attribute_value_text(value)
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
        for (key, value) in &span.attributes {
            lines.push(Line::from(format!(
                "{key} = {}",
                attribute_value_text(value)
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
        for event in &span.events {
            lines.push(Line::from(format!(
                "{} @ {}",
                event.name, event.timestamp_unix_nano
            )));
            for (key, value) in &event.attributes {
                lines.push(Line::from(format!(
                    "  {key} = {}",
                    attribute_value_text(value)
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
        for link in &span.links {
            lines.push(Line::from(format!("{} / {}", link.trace_id, link.span_id)));
            if !link.trace_state.is_empty() {
                lines.push(Line::from(format!("  state {}", link.trace_state)));
            }
            for (key, value) in &link.attributes {
                lines.push(Line::from(format!(
                    "  {key} = {}",
                    attribute_value_text(value)
                )));
            }
        }
    }

    lines
}

fn attribute_value_text(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::String(text) => text.clone(),
        _ => value.to_string(),
    }
}

fn status_badge(status_code: &str) -> Option<&'static str> {
    match status_code {
        "STATUS_CODE_ERROR" => Some("error"),
        "STATUS_CODE_OK" => Some("ok"),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use serde_json::json;

    use crate::{config::Theme, domain::SpanDetail, ui::Palette};

    use super::build_span_detail_lines;

    #[test]
    fn span_detail_header_hides_unset_status() {
        let span = SpanDetail {
            trace_id: "trace".to_string(),
            span_id: "span".to_string(),
            parent_span_id: String::new(),
            service_name: "svc".to_string(),
            span_name: "Prompt: DASv2 AIClient Completion".to_string(),
            span_kind: "INTERNAL".to_string(),
            status_code: "STATUS_CODE_UNSET".to_string(),
            duration_ms: 12.3,
            start_time_unix_nano: 1,
            end_time_unix_nano: 2,
            attributes: BTreeMap::from([(String::from("foo"), json!("bar"))]),
            resource_attributes: BTreeMap::new(),
            events: Vec::new(),
            links: Vec::new(),
            llm: None,
        };

        let lines = build_span_detail_lines(&span, Palette::from_theme(Theme::Ember));
        let header = lines[0]
            .spans
            .iter()
            .map(|span| span.content.as_ref())
            .collect::<String>();

        assert_eq!(header, "Prompt: DASv2 AIClient Completion");
    }
}
