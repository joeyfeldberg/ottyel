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

fn rendered_screen(width: u16, height: u16, mut state: UiState) -> String {
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

    buffer_text(terminal.backend().buffer())
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
