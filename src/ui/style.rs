//! Shared visual vocabulary: panels, rows, sections, fields, and badges.
//!
//! Every pane builds its chrome from these helpers so focus, selection, and severity look the
//! same everywhere.

use ratatui::{
    prelude::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, BorderType, Padding},
};

use super::Palette;

/// A rounded panel. The focused panel gets an accent border and a marked, bold title.
pub(crate) fn panel(title: impl Into<String>, focused: bool, palette: Palette) -> Block<'static> {
    let title = title.into();
    let (border, title_style, marker) = if focused {
        (
            palette.accent,
            Style::default()
                .fg(palette.accent)
                .add_modifier(Modifier::BOLD),
            "▸ ",
        )
    } else {
        (palette.border, Style::default().fg(palette.foreground), "")
    };
    let block = Block::bordered()
        .border_type(BorderType::Rounded)
        .border_style(Style::default().fg(border))
        .padding(Padding::horizontal(1));
    if title.is_empty() {
        return block;
    }
    block.title(Line::from(vec![
        Span::raw(" "),
        Span::styled(format!("{marker}{title}"), title_style),
        Span::raw(" "),
    ]))
}

/// A panel title with trailing muted context, such as active filters.
pub(crate) fn panel_with_context(
    title: &str,
    context: &[String],
    focused: bool,
    palette: Palette,
) -> Block<'static> {
    let block = panel(title, focused, palette);
    if context.is_empty() {
        return block;
    }
    block.title(Line::from(Span::styled(
        format!(" {} ", context.join(" · ")),
        Style::default().fg(palette.muted),
    )))
}

pub(crate) fn header_row(palette: Palette) -> Style {
    Style::default()
        .fg(palette.muted)
        .add_modifier(Modifier::BOLD)
}

/// Selected rows use a quiet background rather than an inverted accent bar.
pub(crate) fn row(selected: bool, palette: Palette) -> Style {
    if selected {
        Style::default()
            .fg(palette.foreground)
            .bg(palette.selection)
            .add_modifier(Modifier::BOLD)
    } else {
        Style::default().fg(palette.foreground)
    }
}

/// A bold accent heading that separates detail sections.
pub(crate) fn section(label: &str, palette: Palette) -> Line<'static> {
    Line::from(Span::styled(
        label.to_string(),
        Style::default()
            .fg(palette.accent)
            .add_modifier(Modifier::BOLD),
    ))
}

/// One aligned `label  value` line with a muted label column.
pub(crate) fn field(
    label: &str,
    value: impl Into<Span<'static>>,
    label_width: usize,
    palette: Palette,
) -> Line<'static> {
    Line::from(vec![
        Span::styled(
            format!("{label:<label_width$}  "),
            Style::default().fg(palette.muted),
        ),
        value.into(),
    ])
}

/// A field whose value is several styled spans.
pub(crate) fn field_spans(
    label: &str,
    value: Vec<Span<'static>>,
    label_width: usize,
    palette: Palette,
) -> Line<'static> {
    let mut spans = vec![Span::styled(
        format!("{label:<label_width$}  "),
        Style::default().fg(palette.muted),
    )];
    spans.extend(value);
    Line::from(spans)
}

pub(crate) fn muted(text: impl Into<String>, palette: Palette) -> Span<'static> {
    Span::styled(text.into(), Style::default().fg(palette.muted))
}

pub(crate) fn plain(text: impl Into<String>, palette: Palette) -> Span<'static> {
    Span::styled(text.into(), Style::default().fg(palette.foreground))
}

pub(crate) fn colored(text: impl Into<String>, color: Color) -> Span<'static> {
    Span::styled(text.into(), Style::default().fg(color))
}

pub(crate) fn strong(text: impl Into<String>, palette: Palette) -> Span<'static> {
    Span::styled(
        text.into(),
        Style::default()
            .fg(palette.foreground)
            .add_modifier(Modifier::BOLD),
    )
}

/// A compact filled label, such as ` ERROR `.
pub(crate) fn badge(text: &str, color: Color, palette: Palette) -> Span<'static> {
    Span::styled(
        format!(" {text} "),
        Style::default()
            .fg(palette.background)
            .bg(color)
            .add_modifier(Modifier::BOLD),
    )
}

/// The color for an OTLP log severity label.
pub(crate) fn severity_color(severity: &str, palette: Palette) -> Color {
    match severity.to_ascii_uppercase().as_str() {
        level if level.starts_with("FATAL") || level.starts_with("ERROR") => palette.error,
        level if level.starts_with("WARN") => palette.warning,
        level if level.starts_with("INFO") => palette.success,
        _ => palette.muted,
    }
}

/// Short fixed-width severity text for feeds.
pub(crate) fn severity_label(severity: &str) -> String {
    let upper = severity.to_ascii_uppercase();
    let label = match upper.as_str() {
        level if level.starts_with("FATAL") => "FATAL",
        level if level.starts_with("ERROR") => "ERROR",
        level if level.starts_with("WARN") => "WARN",
        level if level.starts_with("INFO") => "INFO",
        level if level.starts_with("DEBUG") => "DEBUG",
        level if level.starts_with("TRACE") => "TRACE",
        "" | "UNSPECIFIED" | "SEVERITY_NUMBER_UNSPECIFIED" => "-",
        other => other,
    };
    label.to_string()
}

/// A span status label without the OTLP enum prefix.
pub(crate) fn status_label(status_code: &str) -> &str {
    match status_code {
        "STATUS_CODE_ERROR" | "ERROR" => "error",
        "STATUS_CODE_OK" | "OK" => "ok",
        _ => "unset",
    }
}

pub(crate) fn status_color(status_code: &str, palette: Palette) -> Color {
    match status_label(status_code) {
        "error" => palette.error,
        "ok" => palette.success,
        _ => palette.muted,
    }
}

/// Parses `mode: key description | key description` into styled footer spans.
pub(crate) fn key_hints(text: &str, palette: Palette) -> Line<'static> {
    let (mode, hints) = match text.split_once(": ") {
        Some((mode, rest)) if !mode.contains(' ') || mode.split(' ').count() <= 2 => {
            (Some(mode), rest)
        }
        _ => (None, text),
    };
    let mut spans = vec![Span::raw(" ")];
    if let Some(mode) = mode {
        spans.push(badge(&mode.to_uppercase(), palette.accent, palette));
        spans.push(Span::raw("  "));
    }
    for (index, hint) in hints.split(" | ").enumerate() {
        if index > 0 {
            spans.push(muted("  ", palette));
        }
        let (key, description) = hint.split_once(' ').unwrap_or((hint, ""));
        spans.push(Span::styled(
            key.to_string(),
            Style::default()
                .fg(palette.accent)
                .add_modifier(Modifier::BOLD),
        ));
        if !description.is_empty() {
            spans.push(muted(format!(" {description}"), palette));
        }
    }
    Line::from(spans)
}

/// A horizontal bar of `fraction` (0..=1) of `width` cells, with eighth-cell resolution.
pub(crate) fn bar(fraction: f64, width: usize) -> String {
    const EIGHTHS: [&str; 8] = ["", "▏", "▎", "▍", "▌", "▋", "▊", "▉"];
    let fraction = if fraction.is_finite() {
        fraction.clamp(0.0, 1.0)
    } else {
        0.0
    };
    let eighths = (fraction * width as f64 * 8.0).round() as usize;
    let mut text = "█".repeat(eighths / 8);
    text.push_str(EIGHTHS[eighths % 8]);
    if eighths == 0 && fraction > 0.0 {
        text.push('▏');
    }
    text
}
