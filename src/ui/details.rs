use ratatui::{
    prelude::Style,
    text::{Line, Span},
};

use crate::domain::{
    DashboardSnapshot, LlmTimelineItem, LlmTimelineKind, LogSummary, MetricSummary, SpanDetail,
    truncate,
};

use super::{Palette, UiState, format, metrics, style, traces};

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
    root_span_id: Option<String>,
    root_span_name: Option<String>,
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
    let series = metrics::metric_series(snapshot);
    let selected = metrics::clamp_selection(selected_index, series.len());
    series
        .get(selected)
        .map(|series| series.points.iter().map(|point| (*point).clone()).collect())
        .unwrap_or_default()
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
    let root_span = root_span_detail(&snapshot.selected_trace);

    let next_key = TraceDetailLinesKey {
        trace_id: span.trace_id.clone(),
        span_id: span.span_id.clone(),
        root_span_id: root_span.map(|span| span.span_id.clone()),
        root_span_name: root_span.map(|span| span.span_name.clone()),
        end_time_unix_nano: span.end_time_unix_nano,
        attribute_count: span.attributes.len(),
        resource_attribute_count: span.resource_attributes.len(),
        event_count: span.events.len(),
        link_count: span.links.len(),
    };

    if cache.key.as_ref() != Some(&next_key) {
        cache.lines = build_span_detail_lines(&span, root_span, palette);
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
    build_metric_detail_lines(
        &selected_metric_series(snapshot, state.selected_metric),
        palette,
    )
}

pub(crate) fn sync_metric_detail_lines_cache(
    snapshot: &DashboardSnapshot,
    state: &UiState,
    palette: Palette,
    cache: &mut MetricDetailLinesCache,
) {
    let series = selected_metric_series(snapshot, state.selected_metric);
    let Some(selected) = series.last().cloned() else {
        cache.key = None;
        cache.lines = vec![Line::from(style::muted(
            "No metric series selected.",
            palette,
        ))];
        return;
    };
    let next_key = MetricDetailLinesKey {
        selected,
        series: series.clone(),
    };
    if cache.key.as_ref() != Some(&next_key) {
        cache.lines = build_metric_detail_lines(&series, palette);
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
    let level = style::severity_label(&log.severity);
    let first_line = log_headline(&log.body);
    let mut lines = vec![
        Line::from(vec![
            style::badge(&level, style::severity_color(&level, palette), palette),
            Span::raw("  "),
            style::muted(format::precise_time(log.timestamp_unix_nano), palette),
        ]),
        Line::from(style::strong(truncate(&first_line, 120), palette)),
        Line::raw(""),
        style::field(
            "service",
            style::colored(log.service_name.clone(), palette.accent),
            8,
            palette,
        ),
        style::field(
            "severity",
            style::plain(log.severity.clone(), palette),
            8,
            palette,
        ),
        style::field(
            "trace",
            id_value(&log.trace_id, "none", palette),
            8,
            palette,
        ),
        style::field("span", id_value(&log.span_id, "none", palette), 8, palette),
        Line::raw(""),
        style::section("Message", palette),
    ];
    lines.extend(
        format_log_body(&log.body)
            .into_iter()
            .map(|line| Line::from(style::plain(line, palette))),
    );
    push_attributes(&mut lines, "Attributes", &log.attributes, palette);
    push_attributes(&mut lines, "Resource", &log.resource_attributes, palette);
    lines
}

/// A structured body's `message`-like field, or the body's first line.
fn log_headline(body: &str) -> String {
    if let Ok(serde_json::Value::Object(fields)) = serde_json::from_str(body) {
        for key in ["message", "msg", "event", "error"] {
            if let Some(serde_json::Value::String(text)) = fields.get(key) {
                return text.clone();
            }
        }
    }
    body.lines().next().unwrap_or_default().to_string()
}

/// An ID, or a muted placeholder when it is empty.
fn id_value(id: &str, empty: &str, palette: Palette) -> Span<'static> {
    if id.is_empty() {
        style::muted(empty.to_string(), palette)
    } else {
        style::plain(id.to_string(), palette)
    }
}

/// Appends a titled, aligned attribute table when `attributes` is not empty.
fn push_attributes(
    lines: &mut Vec<Line<'static>>,
    title: &str,
    attributes: &crate::domain::AttributeMap,
    palette: Palette,
) {
    if attributes.is_empty() {
        return;
    }
    lines.push(Line::raw(""));
    lines.push(style::section(title, palette));
    let width = attributes
        .keys()
        .map(|key| key.chars().count())
        .max()
        .unwrap_or_default()
        .min(36);
    for (key, value) in attributes {
        lines.push(style::field(
            &truncate(key, width),
            style::plain(attribute_value_text(value), palette),
            width,
            palette,
        ));
    }
}

pub(crate) fn format_log_body(body: &str) -> Vec<String> {
    if let Ok(value) = serde_json::from_str::<serde_json::Value>(body)
        && let Ok(pretty) = serde_json::to_string_pretty(&value)
    {
        return pretty.lines().map(ToString::to_string).collect();
    }

    body.lines().map(ToString::to_string).collect()
}

fn build_llm_detail_lines(
    item: &crate::domain::LlmSummary,
    state: &UiState,
    palette: Palette,
) -> Vec<Line<'static>> {
    let failed = item.status.eq_ignore_ascii_case("error") || item.status == "STATUS_CODE_ERROR";
    let mut lines = vec![
        Line::from(vec![
            style::strong(truncate(&llm_prompt_name(&item.span_name), 80), palette),
            Span::raw("  "),
            if failed {
                style::badge("ERROR", palette.error, palette)
            } else {
                style::badge("OK", palette.success, palette)
            },
        ]),
        Line::from(vec![
            style::colored(item.model.clone(), palette.accent),
            style::muted(format!("  {} · {}", item.provider, item.operation), palette),
        ]),
        Line::raw(""),
        style::field_spans(
            "tokens",
            vec![
                style::plain(
                    item.input_tokens
                        .map_or_else(|| "-".to_string(), format::count),
                    palette,
                ),
                style::muted(" in  ", palette),
                style::plain(
                    item.output_tokens
                        .map_or_else(|| "-".to_string(), format::count),
                    palette,
                ),
                style::muted(" out  ", palette),
                style::strong(
                    item.total_tokens
                        .map_or_else(|| "-".to_string(), format::count),
                    palette,
                ),
                style::muted(" total", palette),
            ],
            12,
            palette,
        ),
        style::field(
            "latency",
            style::plain(
                item.latency_ms
                    .map_or_else(|| "-".to_string(), format::duration),
                palette,
            ),
            12,
            palette,
        ),
        style::field(
            "cost",
            style::plain(format::cost(item.cost), palette),
            12,
            palette,
        ),
        style::field(
            "service",
            style::colored(item.service_name.clone(), palette.accent),
            12,
            palette,
        ),
        style::field(
            "session",
            id_value(
                item.session_id.as_deref().unwrap_or_default(),
                "none",
                palette,
            ),
            12,
            palette,
        ),
        style::field(
            "conversation",
            id_value(
                item.conversation_id.as_deref().unwrap_or_default(),
                "none",
                palette,
            ),
            12,
            palette,
        ),
        style::field(
            "trace",
            style::muted(item.trace_id.clone(), palette),
            12,
            palette,
        ),
        style::field(
            "span",
            style::muted(item.span_id.clone(), palette),
            12,
            palette,
        ),
    ];

    if let Some(prompt) = item
        .prompt_preview
        .as_deref()
        .filter(|value| !value.is_empty())
    {
        lines.push(Line::raw(""));
        lines.push(style::section("Prompt", palette));
        lines.extend(truncated_block(
            prompt,
            state.llm_expand_prompt,
            LLM_PREVIEW_LINE_LIMIT,
            'i',
            palette.muted,
        ));
    }

    if let Some(output) = item
        .output_preview
        .as_deref()
        .filter(|value| !value.is_empty())
    {
        lines.push(Line::raw(""));
        lines.push(style::section("Output", palette));
        lines.extend(truncated_block(
            output,
            state.llm_expand_output,
            LLM_PREVIEW_LINE_LIMIT,
            'o',
            palette.muted,
        ));
    }

    if item.tool_name.is_some() || item.tool_args.is_some() {
        lines.push(Line::raw(""));
        lines.push(style::section("Tool", palette));
        if let Some(name) = &item.tool_name {
            lines.push(style::field(
                "name",
                style::plain(name.clone(), palette),
                4,
                palette,
            ));
        }
        if let Some(args) = item.tool_args.as_deref().filter(|value| !value.is_empty()) {
            lines.extend(multiline_block(args).into_iter().map(Line::from));
        }
    }

    lines
}

fn llm_prompt_name(span_name: &str) -> String {
    let trimmed = span_name.trim();
    if let Some(prompt) = trimmed.strip_prefix("Prompt: ").map(str::trim)
        && !prompt.is_empty()
    {
        return prompt.to_string();
    }
    trimmed.to_string()
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
        let color = match item.kind {
            LlmTimelineKind::Prompt => palette.accent,
            LlmTimelineKind::Tool => palette.warning,
            LlmTimelineKind::Output => palette.success,
            LlmTimelineKind::Step => palette.muted,
        };
        let marker = if matches!(item.kind, LlmTimelineKind::Prompt | LlmTimelineKind::Output) {
            "●"
        } else {
            "━"
        };
        let (start, end) = timeline_lane(item, total_ms, TIMELINE_WIDTH);
        let duration = item
            .duration_ms
            .filter(|value| *value > 0.0)
            .map(|value| format!("  {}", format::duration(value)))
            .unwrap_or_default();
        lines.push(Line::from(vec![
            style::muted(format!("{:>8} ", format::duration(item.offset_ms)), palette),
            style::colored("─".repeat(start), palette.border),
            style::colored(marker.repeat(end - start), color),
            style::colored("─".repeat(TIMELINE_WIDTH - end), palette.border),
            Span::raw(" "),
            style::colored(format!("{} ", item.kind.label()), color),
            style::strong(
                item.label
                    .strip_prefix(item.kind.label())
                    .map_or(item.label.as_str(), str::trim_start)
                    .to_string(),
                palette,
            ),
            style::muted(duration, palette),
        ]));
        if let Some(detail) = item.detail.as_deref().filter(|detail| !detail.is_empty()) {
            lines.push(Line::from(vec![
                Span::raw(" ".repeat(10)),
                style::muted(truncate(&detail.replace('\n', " "), 96), palette),
            ]));
        }
    }
    lines
}

const TIMELINE_WIDTH: usize = 20;

/// The `[start, end)` cells an item occupies on a lane `width` cells wide.
fn timeline_lane(item: &LlmTimelineItem, total_ms: f64, width: usize) -> (usize, usize) {
    if width == 0 {
        return (0, 0);
    }
    let start = (((item.offset_ms / total_ms) * width as f64).floor() as usize).min(width - 1);
    let duration = item.duration_ms.unwrap_or(0.0);
    let extent = if duration <= 0.0 {
        1
    } else {
        ((duration / total_ms) * width as f64).ceil().max(1.0) as usize
    };
    (start, (start + extent).min(width))
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

fn build_metric_detail_lines(series: &[MetricSummary], palette: Palette) -> Vec<Line<'static>> {
    let (Some(first), Some(latest)) = (series.first(), series.last()) else {
        return vec![Line::from(style::muted(
            "No metric series selected.",
            palette,
        ))];
    };
    let numeric = series
        .iter()
        .filter_map(|metric| metric.value)
        .collect::<Vec<_>>();
    let min = numeric.iter().copied().reduce(f64::min);
    let max = numeric.iter().copied().reduce(f64::max);
    let avg = (!numeric.is_empty()).then(|| numeric.iter().sum::<f64>() / numeric.len() as f64);

    let mut lines = vec![
        Line::from(style::strong(latest.metric_name.clone(), palette)),
        Line::from(vec![
            style::colored(latest.service_name.clone(), palette.accent),
            style::muted(format!("  {}", latest.instrument_kind), palette),
        ]),
        Line::raw(""),
        style::field(
            "latest",
            style::strong(format::optional_number(latest.value), palette),
            7,
            palette,
        ),
        style::field(
            "min",
            style::plain(format::optional_number(min), palette),
            7,
            palette,
        ),
        style::field(
            "max",
            style::plain(format::optional_number(max), palette),
            7,
            palette,
        ),
        style::field(
            "avg",
            style::plain(format::optional_number(avg), palette),
            7,
            palette,
        ),
        style::field(
            "samples",
            style::plain(series.len().to_string(), palette),
            7,
            palette,
        ),
        style::field(
            "window",
            style::plain(
                format!(
                    "{} → {}",
                    format::local_time(first.timestamp_unix_nano),
                    format::local_time(latest.timestamp_unix_nano)
                ),
                palette,
            ),
            7,
            palette,
        ),
        Line::raw(""),
        style::section("Recent points", palette),
    ];
    for metric in series.iter().rev().take(12) {
        let value = metric
            .value
            .map_or_else(|| metric.summary.clone(), format::number);
        lines.push(Line::from(vec![
            style::muted(
                format!("{:<19}  ", format::local_time(metric.timestamp_unix_nano)),
                palette,
            ),
            style::plain(value, palette),
        ]));
    }
    lines
}

fn root_span_detail(spans: &[SpanDetail]) -> Option<&SpanDetail> {
    spans
        .iter()
        .find(|span| span.parent_span_id.is_empty())
        .or_else(|| spans.first())
}

fn build_span_detail_lines(
    span: &SpanDetail,
    root_span: Option<&SpanDetail>,
    palette: Palette,
) -> Vec<Line<'static>> {
    let mut header = vec![style::strong(truncate(&span.span_name, 80), palette)];
    if let Some(status) = status_badge(&span.status_code) {
        header.push(Span::raw("  "));
        header.push(style::badge(
            status,
            style::status_color(&span.status_code, palette),
            palette,
        ));
    }

    let mut lines = vec![
        Line::from(header),
        Line::from(vec![
            style::colored(span.service_name.clone(), palette.accent),
            style::muted(
                format!(
                    "  {} · {}",
                    span.span_kind.to_lowercase(),
                    format::duration(span.duration_ms)
                ),
                palette,
            ),
        ]),
        Line::raw(""),
        style::field(
            "started",
            style::plain(format::precise_time(span.start_time_unix_nano), palette),
            9,
            palette,
        ),
        style::field(
            "duration",
            style::plain(format::duration(span.duration_ms), palette),
            9,
            palette,
        ),
        style::field(
            "trace id",
            style::plain(span.trace_id.clone(), palette),
            9,
            palette,
        ),
        style::field(
            "span id",
            style::plain(span.span_id.clone(), palette),
            9,
            palette,
        ),
        style::field(
            "parent",
            id_value(&span.parent_span_id, "root", palette),
            9,
            palette,
        ),
    ];
    if let Some(root_span) = root_span {
        lines.push(style::field_spans(
            "root",
            vec![
                style::plain(root_span.span_name.clone(), palette),
                style::muted(format!("  {}", root_span.span_id), palette),
            ],
            9,
            palette,
        ));
    }

    if let Some(llm) = &span.llm {
        lines.push(Line::raw(""));
        lines.push(style::section("LLM", palette));
        for (label, value) in [
            ("provider", llm.provider.clone()),
            ("model", llm.model.clone()),
            ("operation", llm.operation.clone()),
        ] {
            if let Some(value) = value {
                lines.push(style::field(
                    label,
                    style::plain(value, palette),
                    9,
                    palette,
                ));
            }
        }
        if llm.input_tokens.is_some() || llm.output_tokens.is_some() || llm.total_tokens.is_some() {
            lines.push(style::field(
                "tokens",
                style::plain(
                    format!(
                        "{} in · {} out · {} total",
                        llm.input_tokens
                            .map_or_else(|| "-".to_string(), format::count),
                        llm.output_tokens
                            .map_or_else(|| "-".to_string(), format::count),
                        llm.total_tokens
                            .map_or_else(|| "-".to_string(), format::count),
                    ),
                    palette,
                ),
                9,
                palette,
            ));
        }
        if llm.cost.is_some() {
            lines.push(style::field(
                "cost",
                style::plain(format::cost(llm.cost), palette),
                9,
                palette,
            ));
        }
    }

    push_attributes(&mut lines, "Attributes", &span.attributes, palette);

    if !span.events.is_empty() {
        lines.push(Line::raw(""));
        lines.push(style::section("Events", palette));
        for event in &span.events {
            lines.push(Line::from(vec![
                style::muted(
                    format!("{}  ", format::precise_time(event.timestamp_unix_nano)),
                    palette,
                ),
                style::strong(event.name.clone(), palette),
            ]));
            for (key, value) in &event.attributes {
                lines.push(Line::from(vec![
                    style::muted(format!("    {key}  "), palette),
                    style::plain(attribute_value_text(value), palette),
                ]));
            }
        }
    }

    if !span.links.is_empty() {
        lines.push(Line::raw(""));
        lines.push(style::section("Links", palette));
        for link in &span.links {
            lines.push(Line::from(vec![
                style::plain(link.trace_id.clone(), palette),
                style::muted(" / ", palette),
                style::plain(link.span_id.clone(), palette),
            ]));
            if !link.trace_state.is_empty() {
                lines.push(Line::from(style::muted(
                    format!("    state {}", link.trace_state),
                    palette,
                )));
            }
            for (key, value) in &link.attributes {
                lines.push(Line::from(vec![
                    style::muted(format!("    {key}  "), palette),
                    style::plain(attribute_value_text(value), palette),
                ]));
            }
        }
    }

    push_attributes(&mut lines, "Resource", &span.resource_attributes, palette);
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

        let lines = build_span_detail_lines(&span, Some(&span), Palette::from_theme(Theme::Ember));
        let header = lines[0]
            .spans
            .iter()
            .map(|span| span.content.as_ref())
            .collect::<String>();

        assert_eq!(header, "Prompt: DASv2 AIClient Completion");
    }

    #[test]
    fn span_detail_includes_full_untruncated_ids() {
        let root = SpanDetail {
            trace_id: "019e2d0a6fb7e5337b121ed7562519cb".to_string(),
            span_id: "0000000000000001".to_string(),
            parent_span_id: String::new(),
            service_name: "dialog-agent-service".to_string(),
            span_name: "DAS Client".to_string(),
            span_kind: "INTERNAL".to_string(),
            status_code: "STATUS_CODE_OK".to_string(),
            duration_ms: 12.3,
            start_time_unix_nano: 1,
            end_time_unix_nano: 2,
            attributes: BTreeMap::new(),
            resource_attributes: BTreeMap::new(),
            events: Vec::new(),
            links: Vec::new(),
            llm: None,
        };
        let child = SpanDetail {
            trace_id: root.trace_id.clone(),
            span_id: "3b52c4d4c1612aff".to_string(),
            parent_span_id: root.span_id.clone(),
            service_name: root.service_name.clone(),
            span_name: "GraphQL Operation".to_string(),
            span_kind: "CLIENT".to_string(),
            status_code: "STATUS_CODE_UNSET".to_string(),
            duration_ms: 5.0,
            start_time_unix_nano: 2,
            end_time_unix_nano: 3,
            attributes: BTreeMap::new(),
            resource_attributes: BTreeMap::new(),
            events: Vec::new(),
            links: Vec::new(),
            llm: None,
        };

        let rendered =
            build_span_detail_lines(&child, Some(&root), Palette::from_theme(Theme::Ember))
                .into_iter()
                .map(|line| {
                    line.spans
                        .into_iter()
                        .map(|span| span.content.into_owned())
                        .collect::<String>()
                })
                .collect::<Vec<_>>();

        for expected in [
            format!("trace id   {}", child.trace_id),
            format!("span id    {}", child.span_id),
            format!("parent     {}", child.parent_span_id),
            format!("root       {}  {}", root.span_name, root.span_id),
        ] {
            assert!(
                rendered.contains(&expected),
                "missing {expected:?} in {rendered:#?}"
            );
        }
        assert!(rendered[1].starts_with(&child.service_name));
    }
}
