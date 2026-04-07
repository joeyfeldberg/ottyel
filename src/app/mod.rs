mod input;

use std::{io, time::Duration};

use anyhow::{Context, Result};
use crossterm::{
    event::{DisableMouseCapture, EnableMouseCapture, Event, EventStream, KeyEventKind},
    execute,
    terminal::{EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode},
};
use futures::StreamExt;
use ratatui::{Terminal, backend::CrosstermBackend};
use tokio::{sync::watch, time::interval};

use crate::{
    config::{Cli, Command, DoctorArgs, ServeArgs},
    query::QueryService,
    store::Store,
    ui::UiState,
};

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
    let mut state = UiState {
        theme: args.theme,
        ..UiState::default()
    };
    let mut snapshot = query.snapshot(&input::filters(&state, &[]))?;
    refresh_detail_state(query, &state, &mut snapshot)?;

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

        tokio::select! {
            _ = tick.tick() => {
                snapshot = query.snapshot(&input::filters(&state, &snapshot.services))?;
                refresh_detail_state(query, &state, &mut snapshot)?;
            }
            maybe_event = events.next() => {
                match maybe_event.transpose()? {
                    Some(Event::Key(key)) if key.kind == KeyEventKind::Press => {
                        if input::handle_key(key.code, key.modifiers, &mut state, &snapshot) {
                            break;
                        }
                        snapshot = query.snapshot(&input::filters(&state, &snapshot.services))?;
                        refresh_detail_state(query, &state, &mut snapshot)?;
                    }
                    Some(Event::Mouse(mouse)) => {
                        input::handle_mouse(
                            mouse,
                            ratatui::layout::Rect::new(0, 0, size.width, size.height),
                            &mut state,
                            &snapshot,
                        );
                        snapshot = query.snapshot(&input::filters(&state, &snapshot.services))?;
                        refresh_detail_state(query, &state, &mut snapshot)?;
                    }
                    Some(Event::Resize(_, _)) => {}
                    Some(_) => {}
                    None => break,
                }
            }
        }
    }

    Ok(())
}

fn refresh_detail_state(
    query: &QueryService,
    state: &UiState,
    snapshot: &mut crate::domain::DashboardSnapshot,
) -> Result<()> {
    if let Some(trace) = snapshot.traces.get(state.selected_trace) {
        snapshot.selected_trace = query.trace_detail(&trace.trace_id)?;
    }
    if let Some(llm) = snapshot.llm.get(state.selected_llm) {
        snapshot.selected_llm_timeline = query.llm_timeline(&llm.trace_id, &llm.span_id)?;
    } else {
        snapshot.selected_llm_timeline.clear();
    }
    Ok(())
}
