use ratatui::{Terminal, backend::TestBackend, buffer::Buffer, layout::Rect};
use std::{env, fs, path::Path};

use crate::{config::Theme, testing};

use super::{
    LlmFocus, RenderCache, Tab, TraceFocus, TraceViewMode, UiState, render, sync_detail_scroll,
    sync_render_cache, sync_trace_tree_scroll,
};

#[test]
fn snapshot_overview_120x40() {
    let state = UiState {
        theme: Theme::Ember,
        active_tab: Tab::Overview.index(),
        ..UiState::default()
    };

    assert_screen_snapshot("overview_120x40", &rendered_screen(120, 40, state));
}

#[test]
fn snapshot_trace_list_120x40() {
    let state = UiState {
        theme: Theme::Ember,
        active_tab: Tab::Traces.index(),
        trace_view_mode: TraceViewMode::List,
        ..UiState::default()
    };

    assert_screen_snapshot("trace_list_120x40", &rendered_screen(120, 40, state));
}

#[test]
fn snapshot_trace_detail_120x40() {
    let state = UiState {
        theme: Theme::Ember,
        active_tab: Tab::Traces.index(),
        trace_view_mode: TraceViewMode::Detail,
        trace_focus: TraceFocus::TraceDetail,
        selected_trace_span: 4,
        ..UiState::default()
    };

    assert_screen_snapshot("trace_detail_120x40", &rendered_screen(120, 40, state));
}

#[test]
fn snapshot_llm_inspector_160x45() {
    let state = UiState {
        theme: Theme::Ember,
        active_tab: Tab::Llm.index(),
        llm_focus: LlmFocus::Detail,
        ..UiState::default()
    };

    assert_screen_snapshot("llm_inspector_160x45", &rendered_screen(160, 45, state));
}

#[test]
fn snapshot_logs_120x40() {
    let state = UiState {
        theme: Theme::Ember,
        active_tab: Tab::Logs.index(),
        selected_log: 1,
        ..UiState::default()
    };

    assert_screen_snapshot("logs_120x40", &rendered_screen(120, 40, state));
}

#[test]
fn snapshot_metrics_120x40() {
    let state = UiState {
        theme: Theme::Ember,
        active_tab: Tab::Metrics.index(),
        selected_metric: 2,
        ..UiState::default()
    };

    assert_screen_snapshot("metrics_120x40", &rendered_screen(120, 40, state));
}

#[test]
fn snapshot_help_overlay_80x24() {
    let state = UiState {
        theme: Theme::Ember,
        active_tab: Tab::Traces.index(),
        trace_view_mode: TraceViewMode::Detail,
        trace_focus: TraceFocus::TraceTree,
        show_help: true,
        ..UiState::default()
    };

    assert_screen_snapshot("help_overlay_80x24", &rendered_screen(80, 24, state));
}

#[test]
fn snapshot_command_palette_80x24() {
    let state = UiState {
        theme: Theme::Ember,
        show_command_palette: true,
        command_query: "trace".to_string(),
        ..UiState::default()
    };

    assert_screen_snapshot("command_palette_80x24", &rendered_screen(80, 24, state));
}

fn assert_screen_snapshot(name: &str, actual: &str) {
    let path = snapshot_path(name);
    if env::var_os("OTTYEL_UPDATE_SNAPSHOTS").is_some() {
        fs::create_dir_all(path.parent().expect("snapshot path should have a parent"))
            .expect("snapshot directory should be writable");
        fs::write(&path, actual).expect("snapshot should be writable");
        return;
    }

    let expected = fs::read_to_string(&path).unwrap_or_else(|error| {
        panic!(
            "failed to read snapshot {}: {error}. Re-run with OTTYEL_UPDATE_SNAPSHOTS=1",
            path.display()
        )
    });
    assert_eq!(
        expected,
        actual,
        "UI snapshot changed: {}. Re-run with OTTYEL_UPDATE_SNAPSHOTS=1 if intentional.",
        path.display()
    );
}

fn snapshot_path(name: &str) -> std::path::PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("src/ui/snapshots")
        .join(format!("{name}.snap"))
}

fn rendered_buffer(width: u16, height: u16, mut state: UiState) -> Buffer {
    let snapshot = testing::dashboard_snapshot();
    let mut cache = RenderCache::default();
    let root = Rect::new(0, 0, width, height);
    sync_trace_tree_scroll(root, &snapshot, &mut state);
    sync_render_cache(&snapshot, &state, &mut cache);
    sync_detail_scroll(root, &snapshot, &mut state, &cache);

    let backend = TestBackend::new(width, height);
    let mut terminal = Terminal::new(backend).expect("test terminal should initialize");
    terminal
        .draw(|frame| render(frame, &snapshot, &state, &cache))
        .expect("test terminal should render");

    terminal.backend().buffer().clone()
}

fn rendered_screen(width: u16, height: u16, state: UiState) -> String {
    buffer_text(&rendered_buffer(width, height, state))
}

/// Writes every snapshot screen as colored HTML to `target/ui-preview` for visual review:
/// `cargo test ui::snapshot_tests::write_color_previews -- --ignored`.
#[test]
#[ignore = "writes review artifacts; run explicitly"]
fn write_color_previews() {
    let screens = [
        ("overview", Tab::Overview, 120, 40, UiState::default()),
        ("traces", Tab::Traces, 120, 40, UiState::default()),
        (
            "trace_detail",
            Tab::Traces,
            120,
            40,
            UiState {
                trace_view_mode: TraceViewMode::Detail,
                trace_focus: TraceFocus::TraceTree,
                selected_trace_span: 1,
                ..UiState::default()
            },
        ),
        (
            "logs",
            Tab::Logs,
            120,
            40,
            UiState {
                selected_log: 1,
                ..UiState::default()
            },
        ),
        ("metrics", Tab::Metrics, 120, 40, UiState::default()),
        (
            "llm",
            Tab::Llm,
            160,
            45,
            UiState {
                llm_focus: LlmFocus::Detail,
                ..UiState::default()
            },
        ),
    ];
    let directory = Path::new(env!("CARGO_MANIFEST_DIR")).join("target/ui-preview");
    fs::create_dir_all(&directory).unwrap();
    let mut index =
        String::from("<html><body style='background:#111;color:#ccc;font-family:sans-serif'>");
    for (name, tab, width, height, state) in screens {
        let buffer = rendered_buffer(
            width,
            height,
            UiState {
                active_tab: tab.index(),
                theme: Theme::Ember,
                ..state
            },
        );
        let screen = buffer_html(&buffer);
        fs::write(
            directory.join(format!("{name}.html")),
            format!("<html><body style='margin:0;background:#100c0a'>{screen}</body></html>"),
        )
        .unwrap();
        index.push_str(&format!("<h3>{name}</h3>{screen}"));
    }
    index.push_str("</body></html>");
    fs::write(directory.join("index.html"), index).unwrap();
}

fn buffer_html(buffer: &Buffer) -> String {
    use ratatui::style::{Color, Modifier};
    let css = |color: Color, fallback: &str| match color {
        Color::Rgb(r, g, b) => format!("#{r:02x}{g:02x}{b:02x}"),
        _ => fallback.to_string(),
    };
    let mut html = String::from(
        "<pre style='font-family:Menlo,monospace;font-size:13px;line-height:1.25;display:inline-block;margin:0'>",
    );
    for row in buffer.content.chunks(buffer.area.width as usize) {
        for cell in row {
            let bold = if cell.modifier.contains(Modifier::BOLD) {
                "font-weight:bold;"
            } else {
                ""
            };
            let symbol = match cell.symbol() {
                "<" => "&lt;",
                ">" => "&gt;",
                "&" => "&amp;",
                other => other,
            };
            html.push_str(&format!(
                "<span style='color:{};background:{};{bold}'>{symbol}</span>",
                css(cell.fg, "#ddd"),
                css(cell.bg, "#100c0a")
            ));
        }
        html.push('\n');
    }
    html.push_str("</pre>");
    html
}

fn buffer_text(buffer: &Buffer) -> String {
    let mut text = buffer
        .content
        .chunks(buffer.area.width as usize)
        .map(|cells| cells.iter().map(|cell| cell.symbol()).collect::<String>())
        .collect::<Vec<_>>()
        .join("\n");
    text.push('\n');
    text
}
