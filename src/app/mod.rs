mod input;

use std::{fs::OpenOptions, io, io::Write, time::Duration};

use anyhow::{Context, Result};
use crossterm::{
    event::{
        DisableMouseCapture, EnableMouseCapture, Event, EventStream, KeyEventKind, MouseEventKind,
    },
    execute,
    terminal::{EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode},
};
use futures::{FutureExt, StreamExt};
use ratatui::{Terminal, backend::CrosstermBackend};
use tokio::{
    sync::{mpsc, watch},
    task,
    time::interval,
};

use crate::{
    config::{Cli, Command, DoctorArgs, ServeArgs},
    domain::{DashboardSnapshot, SpanDetail, TraceSummary},
    query::{QueryFilters, QueryService},
    store::Store,
    ui::UiState,
};
use input::{InputOutcome, WheelTarget};

const WHEEL_DEBUG_LOG: &str = "/tmp/ottyel-wheel.log";
const WHEEL_DEBUG_EVENT_LIMIT: usize = 30;
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

#[derive(Debug, Clone, Copy)]
struct WheelPosition {
    primary: usize,
    offset: usize,
    detail_scroll: u16,
}

#[derive(Debug)]
struct SnapshotRefreshResult {
    request_id: u64,
    filters: QueryFilters,
    snapshot: Result<DashboardSnapshot>,
}

pub async fn run(cli: Cli) -> Result<()> {
    match cli.command.unwrap_or(Command::Serve(ServeArgs::default())) {
        Command::Serve(args) => serve(args).await,
        Command::Doctor(args) => doctor(args),
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
    let mut tick = interval(Duration::from_millis(args.tick_rate_ms));
    let mut snapshot_tick = interval(Duration::from_millis(snapshot_refresh_interval_ms(
        args.tick_rate_ms,
    )));
    let mut state = UiState {
        theme: args.theme,
        ..UiState::default()
    };
    let mut snapshot = query.snapshot(&input::filters(&state, &[]))?;
    let mut trace_detail_cache = TraceDetailCache::default();
    refresh_detail_state(query, &state, &mut snapshot, &mut trace_detail_cache)?;
    let (refresh_tx, mut refresh_rx) = mpsc::unbounded_channel();
    let mut refresh_in_flight = false;
    let mut next_refresh_request_id = 0_u64;
    let mut pending_event: Option<Event> = None;
    loop {
        input::sync_selection(&mut state, &snapshot);
        let size = terminal.size()?;
        crate::ui::sync_trace_tree_scroll(
            ratatui::layout::Rect::new(0, 0, size.width, size.height),
            &snapshot,
            &mut state,
        );
        crate::ui::sync_detail_scroll(
            ratatui::layout::Rect::new(0, 0, size.width, size.height),
            &snapshot,
            &mut state,
        );
        terminal.draw(|frame| crate::ui::render(frame, &snapshot, &state))?;

        if let Some(event) = pending_event.take() {
            if handle_terminal_event(
                event,
                ratatui::layout::Rect::new(0, 0, size.width, size.height),
                &mut events,
                &mut pending_event,
                query,
                &mut state,
                &mut snapshot,
                &mut trace_detail_cache,
            )? {
                break;
            }
            continue;
        }

        tokio::select! {
            _ = tick.tick() => {
            }
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
                        snapshot = result.snapshot?;
                        refresh_detail_state(query, &state, &mut snapshot, &mut trace_detail_cache)?;
                    }
                }
            }
            maybe_event = events.next() => {
                match maybe_event.transpose()? {
                        Some(Event::Key(key)) if key.kind == KeyEventKind::Press => {
                        let outcome = input::handle_key(
                            key.code,
                            key.modifiers,
                            ratatui::layout::Rect::new(0, 0, size.width, size.height),
                            &mut state,
                            &snapshot,
                        );
                        if matches!(outcome, InputOutcome::Quit) {
                            break;
                        }
                        apply_input_outcome(
                            outcome,
                            query,
                            &state,
                            &mut snapshot,
                            &mut trace_detail_cache,
                        )?;
                    }
                    Some(event) => {
                        if handle_terminal_event(
                            event,
                            ratatui::layout::Rect::new(0, 0, size.width, size.height),
                            &mut events,
                            &mut pending_event,
                            query,
                            &mut state,
                            &mut snapshot,
                            &mut trace_detail_cache,
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
    events: &mut EventStream,
    pending_event: &mut Option<Event>,
    query: &QueryService,
    state: &mut UiState,
    snapshot: &mut DashboardSnapshot,
    trace_detail_cache: &mut TraceDetailCache,
) -> Result<bool> {
    match event {
        Event::Key(key) if key.kind == KeyEventKind::Press => {
            let outcome = input::handle_key(key.code, key.modifiers, root, state, snapshot);
            if matches!(outcome, InputOutcome::Quit) {
                return Ok(true);
            }
            apply_input_outcome(outcome, query, state, snapshot, trace_detail_cache)?;
        }
        Event::Mouse(mouse) => {
            let wheel_target = input::wheel_target(mouse.column, mouse.row, root, state);
            let before_position = wheel_target.map(|target| wheel_position(target, state));
            let before = state.clone();
            let outcome = input::handle_mouse(mouse, root, state, snapshot);
            if !matches!(outcome, InputOutcome::None) {
                apply_input_outcome(outcome, query, state, snapshot, trace_detail_cache)?;
                record_wheel_debug(
                    state,
                    format!(
                        "{} target={} before={} after={} outcome={:?}",
                        wheel_direction_label(mouse.kind),
                        wheel_target_label(wheel_target),
                        wheel_position_label(before_position),
                        wheel_position_label(
                            wheel_target.map(|target| wheel_position(target, state))
                        ),
                        outcome,
                    ),
                );
            } else if before == *state
                && matches!(
                    mouse.kind,
                    MouseEventKind::ScrollDown | MouseEventKind::ScrollUp
                )
            {
                let drained = drain_stale_scroll_events(mouse.kind, events, pending_event)?;
                record_wheel_debug(
                    state,
                    format!(
                        "{} target={} before={} -> noop drained-same-dir={drained}",
                        wheel_direction_label(mouse.kind),
                        wheel_target_label(wheel_target),
                        wheel_position_label(before_position),
                    ),
                );
            } else if let Some(target) = wheel_target {
                record_wheel_debug(
                    state,
                    format!(
                        "{} target={} before={} after={}",
                        wheel_direction_label(mouse.kind),
                        wheel_target_label(Some(target)),
                        wheel_position_label(before_position),
                        wheel_position_label(Some(wheel_position(target, state))),
                    ),
                );
            }
        }
        Event::Resize(_, _) => {}
        _ => {}
    }

    Ok(false)
}

fn apply_input_outcome(
    outcome: InputOutcome,
    query: &QueryService,
    state: &UiState,
    snapshot: &mut DashboardSnapshot,
    trace_detail_cache: &mut TraceDetailCache,
) -> Result<()> {
    match outcome {
        InputOutcome::None => {}
        InputOutcome::RefreshDetails => {
            refresh_detail_state(query, state, snapshot, trace_detail_cache)?;
        }
        InputOutcome::RefreshSnapshot => {
            *snapshot = query.snapshot(&input::filters(state, &snapshot.services))?;
            refresh_detail_state(query, state, snapshot, trace_detail_cache)?;
        }
        InputOutcome::Quit => {}
    }

    Ok(())
}

fn drain_stale_scroll_events(
    direction: MouseEventKind,
    events: &mut EventStream,
    pending_event: &mut Option<Event>,
) -> Result<usize> {
    let mut drained = 0;
    loop {
        let Some(ready_event) = events.next().now_or_never() else {
            break;
        };
        let Some(next_event) = ready_event.transpose()? else {
            break;
        };

        match next_event {
            Event::Mouse(mouse) if mouse.kind == direction => drained += 1,
            other => {
                *pending_event = Some(other);
                break;
            }
        }
    }

    Ok(drained)
}

fn refresh_detail_state(
    query: &QueryService,
    state: &UiState,
    snapshot: &mut DashboardSnapshot,
    trace_detail_cache: &mut TraceDetailCache,
) -> Result<()> {
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
    if let Some(llm) = snapshot.llm.get(state.selected_llm) {
        snapshot.selected_llm_timeline = query.llm_timeline(&llm.trace_id, &llm.span_id)?;
    } else {
        snapshot.selected_llm_timeline.clear();
    }
    Ok(())
}

fn wheel_position(target: WheelTarget, state: &UiState) -> WheelPosition {
    match target {
        WheelTarget::TraceList => WheelPosition {
            primary: state.selected_trace,
            offset: state.trace_list_scroll,
            detail_scroll: 0,
        },
        WheelTarget::TraceTree | WheelTarget::TraceDetail => WheelPosition {
            primary: state.selected_trace_span,
            offset: state.trace_tree_scroll,
            detail_scroll: state.trace_detail_scroll,
        },
        WheelTarget::LogsFeed | WheelTarget::LogsDetail => WheelPosition {
            primary: state.selected_log,
            offset: state.log_feed_scroll,
            detail_scroll: state.log_detail_scroll,
        },
        WheelTarget::MetricsFeed | WheelTarget::MetricsDetail => WheelPosition {
            primary: state.selected_metric,
            offset: state.metric_feed_scroll,
            detail_scroll: state.metric_detail_scroll,
        },
        WheelTarget::LlmFeed | WheelTarget::LlmDetail => WheelPosition {
            primary: state.selected_llm,
            offset: state.llm_feed_scroll,
            detail_scroll: state.llm_detail_scroll,
        },
    }
}

fn wheel_direction_label(kind: MouseEventKind) -> &'static str {
    match kind {
        MouseEventKind::ScrollDown => "down",
        MouseEventKind::ScrollUp => "up",
        _ => "-",
    }
}

fn wheel_target_label(target: Option<WheelTarget>) -> &'static str {
    match target {
        Some(WheelTarget::TraceList) => "trace-list",
        Some(WheelTarget::TraceTree) => "trace-tree",
        Some(WheelTarget::TraceDetail) => "trace-detail",
        Some(WheelTarget::LogsFeed) => "logs-feed",
        Some(WheelTarget::LogsDetail) => "logs-detail",
        Some(WheelTarget::MetricsFeed) => "metrics-feed",
        Some(WheelTarget::MetricsDetail) => "metrics-detail",
        Some(WheelTarget::LlmFeed) => "llm-feed",
        Some(WheelTarget::LlmDetail) => "llm-detail",
        None => "-",
    }
}

fn wheel_position_label(position: Option<WheelPosition>) -> String {
    position
        .map(|position| {
            format!(
                "sel={} offset={} detail={}",
                position.primary, position.offset, position.detail_scroll
            )
        })
        .unwrap_or_else(|| "-".to_string())
}

fn record_wheel_debug(state: &mut UiState, line: String) {
    if !state.show_wheel_debug {
        return;
    }

    let timestamp = wheel_timestamp();
    let full_line = format!("{timestamp} {line}");
    state.wheel_debug_events.push_back(full_line.clone());
    while state.wheel_debug_events.len() > WHEEL_DEBUG_EVENT_LIMIT {
        state.wheel_debug_events.pop_front();
    }

    if let Ok(mut file) = OpenOptions::new()
        .create(true)
        .append(true)
        .open(WHEEL_DEBUG_LOG)
    {
        let _ = writeln!(file, "{full_line}");
    }
}

fn wheel_timestamp() -> String {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    format!("{:>10}.{:03}", now.as_secs(), now.subsec_millis())
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

#[cfg(test)]
mod tests {
    use super::{MIN_SNAPSHOT_REFRESH_MS, TraceDetailCacheKey, snapshot_refresh_interval_ms};
    use crate::{
        domain::TraceSummary,
        query::{LogFilters, QueryFilters, TimeWindow},
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
}
