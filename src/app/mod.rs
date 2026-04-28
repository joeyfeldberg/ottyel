mod input;

use std::{cmp::Ordering, io, time::Duration};

use anyhow::{Context, Result};
use crossterm::{
    event::{DisableMouseCapture, EnableMouseCapture, Event, EventStream, KeyEventKind},
    execute,
    terminal::{EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode},
};
use futures::StreamExt;
use ratatui::{Terminal, backend::CrosstermBackend};
use tokio::{
    sync::{mpsc, watch},
    task,
    time::{MissedTickBehavior, interval},
};

use crate::{
    config::{Cli, Command, DoctorArgs, McpArgs, ServeArgs},
    domain::{DashboardSnapshot, LlmTimelineItem, SpanDetail, TraceSummary},
    preferences::UserPreferences,
    query::{QueryFilters, QueryService},
    store::Store,
    ui::{LlmSortMode, RenderCache, Tab, TraceViewMode, UiState},
};
use input::InputOutcome;

const RENDER_FRAME_MS: u64 = 16;
const MIN_SNAPSHOT_REFRESH_MS: u64 = 3_000;

#[derive(Debug, Default)]
struct TraceDetailCache {
    key: Option<TraceDetailCacheKey>,
    spans: Vec<SpanDetail>,
}

#[derive(Debug, Clone, PartialEq)]
struct TraceDetailCacheKey {
    trace: TraceSummary,
    filters: QueryFilters,
}

#[derive(Debug)]
struct SnapshotRefreshResult {
    request_id: u64,
    filters: QueryFilters,
    snapshot: Result<DashboardSnapshot>,
}

#[derive(Debug, Default)]
struct LlmTimelineCache {
    key: Option<LlmTimelineCacheKey>,
    items: Vec<LlmTimelineItem>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
struct LlmTimelineCacheKey {
    trace_id: String,
    span_id: String,
}

#[derive(Debug, Default)]
struct LlmTimelineRefreshState {
    in_flight: Option<LlmTimelineCacheKey>,
    desired: Option<LlmTimelineCacheKey>,
}

#[derive(Debug)]
struct LlmTimelineRefreshResult {
    trace_id: String,
    span_id: String,
    timeline: Result<Vec<LlmTimelineItem>>,
}

pub async fn run(cli: Cli) -> Result<()> {
    match cli.command.unwrap_or(Command::Serve(ServeArgs::default())) {
        Command::Serve(args) => serve(args).await,
        Command::Doctor(args) => doctor(args),
        Command::Mcp(args) => mcp(args),
    }
}

async fn serve(args: ServeArgs) -> Result<()> {
    let store = Store::open(&args.db_path, args.retention_hours, args.max_spans)?;
    let query = QueryService::new(store.clone(), args.page_size);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    let http_bind = args.http_bind.clone();
    let grpc_bind = args.grpc_bind.clone();
    let server = tokio::spawn(async move {
        crate::ingest::serve(&http_bind, &grpc_bind, store, shutdown_rx).await
    });

    let ui_result = run_terminal(&query, &args).await;
    let _ = shutdown_tx.send(true);
    let server_result = server.await.context("ingest task join failure")??;
    let _ = server_result;
    ui_result
}

fn doctor(args: DoctorArgs) -> Result<()> {
    let store = Store::open(&args.db_path, 24, 1000)?;
    let (traces, errors, logs, metrics, llm) = store.counts(None)?;
    println!("db: {}", args.db_path.display());
    println!("traces: {traces}");
    println!("error_spans: {errors}");
    println!("logs: {logs}");
    println!("metrics: {metrics}");
    println!("llm_spans: {llm}");
    Ok(())
}

fn mcp(args: McpArgs) -> Result<()> {
    let store = Store::open(&args.db_path, 24, 1000)?;
    let query = QueryService::new(store, args.page_size);
    crate::mcp::serve_stdio(query)
}

async fn run_terminal(query: &QueryService, args: &ServeArgs) -> Result<()> {
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen, EnableMouseCapture)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;
    terminal.clear()?;

    let terminal_result = terminal_loop(&mut terminal, query, args).await;

    disable_raw_mode()?;
    execute!(
        terminal.backend_mut(),
        DisableMouseCapture,
        LeaveAlternateScreen
    )?;
    terminal.show_cursor()?;
    terminal_result
}

async fn terminal_loop(
    terminal: &mut Terminal<CrosstermBackend<io::Stdout>>,
    query: &QueryService,
    args: &ServeArgs,
) -> Result<()> {
    let mut events = EventStream::new();
    let mut render_tick = interval(Duration::from_millis(RENDER_FRAME_MS));
    render_tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let mut snapshot_tick = interval(Duration::from_millis(snapshot_refresh_interval_ms(
        args.tick_rate_ms,
    )));
    snapshot_tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let mut state = UiState::default();
    if let Some(preferences) = UserPreferences::load() {
        preferences.apply_to_state(&mut state);
    }
    if let Some(theme) = args.theme {
        state.theme = theme;
    }
    let mut snapshot = query.snapshot(&input::filters(&state, &[]))?;
    let mut render_cache = RenderCache::default();
    let mut trace_detail_cache = TraceDetailCache::default();
    let mut llm_timeline_cache = LlmTimelineCache::default();
    let mut llm_timeline_refresh = LlmTimelineRefreshState::default();
    let mut saved_preferences = UserPreferences::from_state(&state);
    refresh_detail_state(
        query,
        &mut state,
        &mut snapshot,
        &mut trace_detail_cache,
        &llm_timeline_cache,
    )?;
    let (refresh_tx, mut refresh_rx) = mpsc::unbounded_channel();
    let (llm_refresh_tx, mut llm_refresh_rx) = mpsc::unbounded_channel();
    let mut refresh_in_flight = false;
    let mut next_refresh_request_id = 0_u64;
    let mut needs_redraw = true;
    let mut render_ready = true;
    loop {
        if needs_redraw && render_ready {
            input::sync_selection(&mut state, &snapshot);
            let size = terminal.size()?;
            crate::ui::sync_trace_tree_scroll(
                ratatui::layout::Rect::new(0, 0, size.width, size.height),
                &snapshot,
                &mut state,
            );
            crate::ui::sync_render_cache(&snapshot, &state, &mut render_cache);
            crate::ui::sync_detail_scroll(
                ratatui::layout::Rect::new(0, 0, size.width, size.height),
                &snapshot,
                &mut state,
                &render_cache,
            );
            drive_llm_timeline_refresh(
                query,
                &state,
                &mut snapshot,
                &mut llm_timeline_cache,
                &mut llm_timeline_refresh,
                &llm_refresh_tx,
            );
            terminal.draw(|frame| crate::ui::render(frame, &snapshot, &state, &render_cache))?;
            needs_redraw = false;
            render_ready = false;
        }

        tokio::select! {
            _ = render_tick.tick() => render_ready = true,
            _ = snapshot_tick.tick(), if !refresh_in_flight => {
                refresh_in_flight = true;
                next_refresh_request_id = next_refresh_request_id.saturating_add(1);
                spawn_snapshot_refresh(
                    query.clone(),
                    input::filters(&state, &snapshot.services),
                    next_refresh_request_id,
                    refresh_tx.clone(),
                );
            }
            Some(result) = refresh_rx.recv() => {
                if result.request_id == next_refresh_request_id {
                    refresh_in_flight = false;
                    let current_filters = input::filters(&state, &snapshot.services);
                    if current_filters == result.filters {
                        let selected_llm_span_id = snapshot
                            .llm
                            .get(state.selected_llm)
                            .map(|item| item.span_id.clone());
                        snapshot = result.snapshot?;
                        if let Some(span_id) = selected_llm_span_id {
                            if let Some(index) =
                                snapshot.llm.iter().position(|item| item.span_id == span_id)
                            {
                                state.selected_llm = index;
                            } else {
                                state.selected_llm =
                                    state.selected_llm.min(snapshot.llm.len().saturating_sub(1));
                            }
                        }
                        refresh_detail_state(
                            query,
                            &mut state,
                            &mut snapshot,
                            &mut trace_detail_cache,
                            &llm_timeline_cache,
                        )?;
                        needs_redraw = true;
                    }
                }
            }
            Some(result) = llm_refresh_rx.recv() => {
                apply_llm_timeline_refresh_result(
                    result,
                    &state,
                    &mut snapshot,
                    &mut llm_timeline_cache,
                    &mut llm_timeline_refresh,
                    query,
                    &llm_refresh_tx,
                );
                needs_redraw = true;
            }
            maybe_event = events.next() => {
                match maybe_event.transpose()? {
                        Some(Event::Key(key)) if key.kind == KeyEventKind::Press => {
                        let before = state.clone();
                        let outcome = input::handle_key(
                            key.code,
                            key.modifiers,
                            ratatui::layout::Rect::new(0, 0, terminal.size()?.width, terminal.size()?.height),
                            &mut state,
                            &snapshot,
                        );
                        if matches!(outcome, InputOutcome::Quit) {
                            break;
                        }
                        let changed = apply_input_outcome(
                            outcome,
                            query,
                            &mut state,
                            &mut snapshot,
                            &mut trace_detail_cache,
                            &llm_timeline_cache,
                        )?;
                        let state_changed = before != state;
                        needs_redraw |= changed || state_changed;
                        if changed || state_changed {
                            render_ready = true;
                            persist_preferences_if_changed(&state, &mut saved_preferences);
                        }
                    }
                    Some(event) => {
                        if handle_terminal_event(
                            event,
                            ratatui::layout::Rect::new(0, 0, terminal.size()?.width, terminal.size()?.height),
                            query,
                            &mut state,
                            &mut snapshot,
                            &mut trace_detail_cache,
                            &llm_timeline_cache,
                            &mut saved_preferences,
                            &mut needs_redraw,
                            &mut render_ready,
                        )? {
                            break;
                        }
                    }
                    None => break,
                }
            }
        }
    }

    Ok(())
}

fn handle_terminal_event(
    event: Event,
    root: ratatui::layout::Rect,
    query: &QueryService,
    state: &mut UiState,
    snapshot: &mut DashboardSnapshot,
    trace_detail_cache: &mut TraceDetailCache,
    llm_timeline_cache: &LlmTimelineCache,
    saved_preferences: &mut UserPreferences,
    needs_redraw: &mut bool,
    render_ready: &mut bool,
) -> Result<bool> {
    match event {
        Event::Key(key) if key.kind == KeyEventKind::Press => {
            let before = state.clone();
            let outcome = input::handle_key(key.code, key.modifiers, root, state, snapshot);
            if matches!(outcome, InputOutcome::Quit) {
                return Ok(true);
            }
            let changed = apply_input_outcome(
                outcome,
                query,
                state,
                snapshot,
                trace_detail_cache,
                llm_timeline_cache,
            )?;
            let state_changed = before != *state;
            *needs_redraw |= changed || state_changed;
            if changed || state_changed {
                *render_ready = true;
                persist_preferences_if_changed(state, saved_preferences);
            }
        }
        Event::Mouse(mouse) => {
            let is_wheel = matches!(
                mouse.kind,
                crossterm::event::MouseEventKind::ScrollDown
                    | crossterm::event::MouseEventKind::ScrollUp
            );
            let before = state.clone();
            let outcome = input::handle_mouse(mouse, root, state, snapshot);
            let state_changed = before != *state;
            if !matches!(outcome, InputOutcome::None) {
                let changed = apply_input_outcome(
                    outcome,
                    query,
                    state,
                    snapshot,
                    trace_detail_cache,
                    llm_timeline_cache,
                )?;
                *needs_redraw |= changed || state_changed;
                if (changed || state_changed) && !is_wheel {
                    *render_ready = true;
                }
                if changed || state_changed {
                    persist_preferences_if_changed(state, saved_preferences);
                }
            } else if state_changed {
                *needs_redraw = true;
                if !is_wheel {
                    *render_ready = true;
                }
                persist_preferences_if_changed(state, saved_preferences);
            }
        }
        Event::Resize(_, _) => {
            *needs_redraw = true;
            *render_ready = true;
        }
        _ => {}
    }

    Ok(false)
}

fn persist_preferences_if_changed(state: &UiState, saved_preferences: &mut UserPreferences) {
    let next = UserPreferences::from_state(state);
    if next != *saved_preferences && next.save().is_ok() {
        *saved_preferences = next;
    }
}

fn apply_input_outcome(
    outcome: InputOutcome,
    query: &QueryService,
    state: &mut UiState,
    snapshot: &mut DashboardSnapshot,
    trace_detail_cache: &mut TraceDetailCache,
    llm_timeline_cache: &LlmTimelineCache,
) -> Result<bool> {
    match outcome {
        InputOutcome::None => Ok(false),
        InputOutcome::RefreshDetails => {
            refresh_detail_state(
                query,
                state,
                snapshot,
                trace_detail_cache,
                llm_timeline_cache,
            )?;
            Ok(true)
        }
        InputOutcome::RefreshSnapshot => {
            let selected_llm_span_id = snapshot
                .llm
                .get(state.selected_llm)
                .map(|item| item.span_id.clone());
            *snapshot = query.snapshot(&input::filters(state, &snapshot.services))?;
            if let Some(span_id) = selected_llm_span_id {
                if let Some(index) = snapshot.llm.iter().position(|item| item.span_id == span_id) {
                    state.selected_llm = index;
                } else {
                    state.selected_llm =
                        state.selected_llm.min(snapshot.llm.len().saturating_sub(1));
                }
            }
            refresh_detail_state(
                query,
                state,
                snapshot,
                trace_detail_cache,
                llm_timeline_cache,
            )?;
            Ok(true)
        }
        InputOutcome::Quit => Ok(false),
    }
}
fn refresh_detail_state(
    query: &QueryService,
    state: &mut UiState,
    snapshot: &mut DashboardSnapshot,
    trace_detail_cache: &mut TraceDetailCache,
    llm_timeline_cache: &LlmTimelineCache,
) -> Result<()> {
    sort_llm_rows(state, snapshot);

    if Tab::ALL[state.active_tab] == Tab::Traces && state.trace_view_mode == TraceViewMode::Detail {
        let filters = input::filters(state, &snapshot.services);
        if let Some(trace) = snapshot.traces.get(state.selected_trace) {
            let next_key = TraceDetailCacheKey {
                trace: trace.clone(),
                filters,
            };
            if trace_detail_cache.key.as_ref() != Some(&next_key) {
                trace_detail_cache.spans = query.trace_detail(&trace.trace_id)?;
                trace_detail_cache.key = Some(next_key);
            }
            snapshot.selected_trace = trace_detail_cache.spans.clone();
        } else {
            snapshot.selected_trace.clear();
        }
    } else {
        snapshot.selected_trace.clear();
    }
    sync_llm_timeline_from_cache(state, snapshot, llm_timeline_cache);
    Ok(())
}

fn sort_llm_rows(state: &mut UiState, snapshot: &mut DashboardSnapshot) {
    if snapshot.llm.is_empty() {
        state.selected_llm = 0;
        state.llm_feed_scroll = 0;
        return;
    }

    let selected_span_id = snapshot
        .llm
        .get(state.selected_llm)
        .map(|item| item.span_id.clone());

    snapshot
        .llm
        .sort_by(|left, right| compare_llm_summary(left, right, state.llm_sort_mode));

    if let Some(selected_span_id) = selected_span_id {
        if let Some(index) = snapshot
            .llm
            .iter()
            .position(|item| item.span_id == selected_span_id)
        {
            state.selected_llm = index;
        } else {
            state.selected_llm = state.selected_llm.min(snapshot.llm.len().saturating_sub(1));
        }
    } else {
        state.selected_llm = state.selected_llm.min(snapshot.llm.len().saturating_sub(1));
    }
}

fn compare_llm_summary(
    left: &crate::domain::LlmSummary,
    right: &crate::domain::LlmSummary,
    mode: LlmSortMode,
) -> Ordering {
    let primary = match mode {
        LlmSortMode::Time => right.started_at_unix_nano.cmp(&left.started_at_unix_nano),
        LlmSortMode::Tokens => right
            .total_tokens
            .unwrap_or_default()
            .cmp(&left.total_tokens.unwrap_or_default()),
        LlmSortMode::Cost => right
            .cost
            .unwrap_or(-1.0)
            .partial_cmp(&left.cost.unwrap_or(-1.0))
            .unwrap_or(Ordering::Equal),
        LlmSortMode::Latency => right
            .latency_ms
            .unwrap_or(-1.0)
            .partial_cmp(&left.latency_ms.unwrap_or(-1.0))
            .unwrap_or(Ordering::Equal),
    };

    primary
        .then_with(|| right.started_at_unix_nano.cmp(&left.started_at_unix_nano))
        .then_with(|| right.span_id.cmp(&left.span_id))
}

fn sync_llm_timeline_from_cache(
    state: &UiState,
    snapshot: &mut DashboardSnapshot,
    llm_timeline_cache: &LlmTimelineCache,
) {
    if Tab::ALL[state.active_tab] != Tab::Llm {
        snapshot.selected_llm_timeline.clear();
        return;
    }

    let Some(llm) = snapshot.llm.get(state.selected_llm) else {
        snapshot.selected_llm_timeline.clear();
        return;
    };

    let key = LlmTimelineCacheKey {
        trace_id: llm.trace_id.clone(),
        span_id: llm.span_id.clone(),
    };
    if llm_timeline_cache.key.as_ref() == Some(&key) {
        snapshot.selected_llm_timeline = llm_timeline_cache.items.clone();
    } else {
        snapshot.selected_llm_timeline.clear();
    }
}

fn selected_llm_timeline_key(
    state: &UiState,
    snapshot: &DashboardSnapshot,
) -> Option<LlmTimelineCacheKey> {
    if Tab::ALL[state.active_tab] != Tab::Llm {
        return None;
    }

    snapshot
        .llm
        .get(state.selected_llm)
        .map(|llm| LlmTimelineCacheKey {
            trace_id: llm.trace_id.clone(),
            span_id: llm.span_id.clone(),
        })
}

fn drive_llm_timeline_refresh(
    query: &QueryService,
    state: &UiState,
    snapshot: &mut DashboardSnapshot,
    llm_timeline_cache: &mut LlmTimelineCache,
    llm_timeline_refresh: &mut LlmTimelineRefreshState,
    llm_refresh_tx: &mpsc::UnboundedSender<LlmTimelineRefreshResult>,
) {
    let desired = selected_llm_timeline_key(state, snapshot);
    llm_timeline_refresh.desired = desired.clone();
    sync_llm_timeline_from_cache(state, snapshot, llm_timeline_cache);

    let Some(desired) = desired else {
        return;
    };

    if llm_timeline_cache.key.as_ref() == Some(&desired)
        || llm_timeline_refresh.in_flight.as_ref() == Some(&desired)
    {
        return;
    }

    if llm_timeline_refresh.in_flight.is_some() {
        return;
    }

    llm_timeline_refresh.in_flight = Some(desired.clone());
    spawn_llm_timeline_refresh(query.clone(), desired, llm_refresh_tx.clone());
}

fn apply_llm_timeline_refresh_result(
    result: LlmTimelineRefreshResult,
    state: &UiState,
    snapshot: &mut DashboardSnapshot,
    llm_timeline_cache: &mut LlmTimelineCache,
    llm_timeline_refresh: &mut LlmTimelineRefreshState,
    query: &QueryService,
    llm_refresh_tx: &mpsc::UnboundedSender<LlmTimelineRefreshResult>,
) {
    let key = LlmTimelineCacheKey {
        trace_id: result.trace_id,
        span_id: result.span_id,
    };

    if llm_timeline_refresh.in_flight.as_ref() == Some(&key) {
        llm_timeline_refresh.in_flight = None;
    }

    if llm_timeline_refresh.desired.as_ref() == Some(&key)
        && let Ok(timeline) = result.timeline
    {
        llm_timeline_cache.key = Some(key.clone());
        llm_timeline_cache.items = timeline;
    }

    sync_llm_timeline_from_cache(state, snapshot, llm_timeline_cache);
    drive_llm_timeline_refresh(
        query,
        state,
        snapshot,
        llm_timeline_cache,
        llm_timeline_refresh,
        llm_refresh_tx,
    );
}

fn snapshot_refresh_interval_ms(tick_rate_ms: u64) -> u64 {
    (tick_rate_ms.saturating_mul(4)).max(MIN_SNAPSHOT_REFRESH_MS)
}

fn spawn_snapshot_refresh(
    query: QueryService,
    filters: QueryFilters,
    request_id: u64,
    refresh_tx: mpsc::UnboundedSender<SnapshotRefreshResult>,
) {
    task::spawn_blocking(move || {
        let snapshot = query.snapshot(&filters);
        let _ = refresh_tx.send(SnapshotRefreshResult {
            request_id,
            filters,
            snapshot,
        });
    });
}

fn spawn_llm_timeline_refresh(
    query: QueryService,
    key: LlmTimelineCacheKey,
    refresh_tx: mpsc::UnboundedSender<LlmTimelineRefreshResult>,
) {
    task::spawn_blocking(move || {
        let timeline = query.llm_timeline(&key.trace_id, &key.span_id);
        let _ = refresh_tx.send(LlmTimelineRefreshResult {
            trace_id: key.trace_id,
            span_id: key.span_id,
            timeline,
        });
    });
}

#[cfg(test)]
mod tests {
    use super::{
        MIN_SNAPSHOT_REFRESH_MS, TraceDetailCacheKey, snapshot_refresh_interval_ms, sort_llm_rows,
    };
    use crate::{
        domain::{DashboardSnapshot, LlmSummary, OverviewStats, TraceSummary},
        query::{LogFilters, QueryFilters, TimeWindow},
        ui::{LlmSortMode, UiState},
    };

    fn trace_summary(span_count: i64) -> TraceSummary {
        TraceSummary {
            trace_id: "trace-1".to_string(),
            service_name: "api".to_string(),
            root_name: "request".to_string(),
            span_count,
            error_count: 0,
            duration_ms: 10.0,
            started_at_unix_nano: 1,
        }
    }

    fn filters(search_query: Option<&str>) -> QueryFilters {
        QueryFilters {
            service: Some("api".to_string()),
            errors_only: false,
            time_window: TimeWindow::OneHour,
            search_query: search_query.map(str::to_string),
            log_filters: LogFilters::default(),
        }
    }

    #[test]
    fn trace_detail_cache_key_changes_when_filters_change() {
        let first = TraceDetailCacheKey {
            trace: trace_summary(2),
            filters: filters(None),
        };
        let second = TraceDetailCacheKey {
            trace: trace_summary(2),
            filters: filters(Some("timeout")),
        };

        assert_ne!(first, second);
    }

    #[test]
    fn trace_detail_cache_key_changes_when_trace_summary_changes() {
        let first = TraceDetailCacheKey {
            trace: trace_summary(2),
            filters: filters(None),
        };
        let second = TraceDetailCacheKey {
            trace: trace_summary(3),
            filters: filters(None),
        };

        assert_ne!(first, second);
    }

    #[test]
    fn snapshot_refresh_interval_is_slower_than_render_tick() {
        assert_eq!(snapshot_refresh_interval_ms(750), MIN_SNAPSHOT_REFRESH_MS);
        assert_eq!(snapshot_refresh_interval_ms(1_000), 4_000);
    }

    #[test]
    fn llm_sort_rows_orders_by_tokens_desc() {
        let mut state = UiState {
            llm_sort_mode: LlmSortMode::Tokens,
            ..UiState::default()
        };
        let mut snapshot = DashboardSnapshot {
            services: Vec::new(),
            overview: empty_overview(),
            traces: Vec::new(),
            selected_trace: Vec::new(),
            logs: Vec::new(),
            metrics: Vec::new(),
            llm: vec![
                llm_summary("span-a", 10, 1.0, 10),
                llm_summary("span-b", 200, 0.5, 12),
                llm_summary("span-c", 50, 2.0, 11),
            ],
            llm_rollups: Vec::new(),
            llm_sessions: Vec::new(),
            llm_model_comparisons: Vec::new(),
            llm_top_calls: Vec::new(),
            selected_llm_timeline: Vec::new(),
        };

        sort_llm_rows(&mut state, &mut snapshot);

        let ordered = snapshot
            .llm
            .iter()
            .map(|item| item.span_id.as_str())
            .collect::<Vec<_>>();
        assert_eq!(ordered, vec!["span-b", "span-c", "span-a"]);
    }

    #[test]
    fn llm_sort_rows_preserves_selected_span_across_reorder() {
        let mut state = UiState {
            llm_sort_mode: LlmSortMode::Latency,
            selected_llm: 2,
            ..UiState::default()
        };
        let mut snapshot = DashboardSnapshot {
            services: Vec::new(),
            overview: empty_overview(),
            traces: Vec::new(),
            selected_trace: Vec::new(),
            logs: Vec::new(),
            metrics: Vec::new(),
            llm: vec![
                llm_summary("span-a", 10, 100.0, 10),
                llm_summary("span-b", 20, 900.0, 11),
                llm_summary("span-c", 30, 300.0, 12),
            ],
            llm_rollups: Vec::new(),
            llm_sessions: Vec::new(),
            llm_model_comparisons: Vec::new(),
            llm_top_calls: Vec::new(),
            selected_llm_timeline: Vec::new(),
        };

        let selected_before = snapshot.llm[state.selected_llm].span_id.clone();
        sort_llm_rows(&mut state, &mut snapshot);

        assert_eq!(snapshot.llm[state.selected_llm].span_id, selected_before);
    }

    fn llm_summary(
        span_id: &str,
        tokens: u64,
        latency_ms: f64,
        started_at_unix_nano: i64,
    ) -> LlmSummary {
        LlmSummary {
            trace_id: "trace-1".to_string(),
            span_id: span_id.to_string(),
            started_at_unix_nano,
            service_name: "api".to_string(),
            span_name: format!("Prompt: {span_id}"),
            provider: "openai".to_string(),
            model: "gpt-4.1-mini".to_string(),
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
            total_tokens: Some(tokens),
            cost: None,
            latency_ms: Some(latency_ms),
            status: "STATUS_CODE_OK".to_string(),
            raw_json: serde_json::json!({}),
        }
    }

    fn empty_overview() -> OverviewStats {
        OverviewStats {
            service_count: 0,
            trace_count: 0,
            error_span_count: 0,
            log_count: 0,
            metric_count: 0,
            llm_count: 0,
        }
    }
}
