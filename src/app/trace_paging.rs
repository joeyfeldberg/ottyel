use anyhow::Result;
use tokio::{sync::mpsc, task};

use crate::{
    domain::{DashboardSnapshot, TraceSummary},
    query::{Page, PageRequest, QueryFilters, QueryService, TraceCursor},
    ui::{Tab, TraceViewMode, UiState},
};

use super::input;

const PREFETCH_MARGIN_ROWS: usize = 50;

#[derive(Debug, Default)]
pub(super) struct TraceListPager {
    filters: Option<QueryFilters>,
    items: Vec<TraceSummary>,
    next_cursor: Option<TraceCursor>,
    in_flight: bool,
}

#[derive(Debug)]
pub(super) struct TracePageRefreshResult {
    filters: QueryFilters,
    page: Result<Page<TraceSummary, TraceCursor>>,
}

#[derive(Debug)]
pub(super) struct TraceListAnchor {
    selected_trace_id: Option<String>,
    top_trace_id: Option<String>,
}

impl TraceListAnchor {
    pub(super) fn capture(snapshot: &DashboardSnapshot, state: &UiState) -> Self {
        Self {
            selected_trace_id: snapshot
                .traces
                .get(state.selected_trace)
                .map(|trace| trace.trace_id.clone()),
            top_trace_id: (!state.trace_list_follow_selected)
                .then(|| {
                    snapshot
                        .traces
                        .get(state.trace_list_scroll)
                        .map(|trace| trace.trace_id.clone())
                })
                .flatten(),
        }
    }

    pub(super) fn restore(self, snapshot: &DashboardSnapshot, state: &mut UiState) {
        if let Some(trace_id) = self.selected_trace_id
            && let Some(index) = trace_index(snapshot, &trace_id)
        {
            state.selected_trace = index;
        } else {
            state.selected_trace = state
                .selected_trace
                .min(snapshot.traces.len().saturating_sub(1));
        }

        if let Some(trace_id) = self.top_trace_id
            && let Some(index) = trace_index(snapshot, &trace_id)
        {
            state.trace_list_scroll = index;
        } else {
            state.trace_list_scroll = state
                .trace_list_scroll
                .min(snapshot.traces.len().saturating_sub(1));
        }
    }
}

impl TraceListPager {
    fn set_first_page(&mut self, filters: QueryFilters, page: Page<TraceSummary, TraceCursor>) {
        if self.filters.as_ref() == Some(&filters) {
            self.merge_first_page(page);
        } else {
            self.filters = Some(filters);
            self.items = page.items;
            self.next_cursor = page.next_cursor;
        }
        self.in_flight = false;
    }

    fn merge_first_page(&mut self, page: Page<TraceSummary, TraceCursor>) {
        let next_cursor = page.next_cursor;
        if next_cursor.is_none() {
            self.items = page.items;
            self.next_cursor = None;
            return;
        }

        let mut merged = page.items;
        for item in self.items.drain(..) {
            if !merged
                .iter()
                .any(|candidate| candidate.trace_id == item.trace_id)
            {
                merged.push(item);
            }
        }
        self.items = merged;
        self.next_cursor = next_cursor;
    }

    fn apply_page_result(&mut self, result: TracePageRefreshResult) -> bool {
        if self.filters.as_ref() != Some(&result.filters) {
            return false;
        }
        self.in_flight = false;

        let Ok(page) = result.page else {
            return false;
        };

        let mut changed = false;
        for trace in page.items {
            if !self
                .items
                .iter()
                .any(|existing| existing.trace_id == trace.trace_id)
            {
                self.items.push(trace);
                changed = true;
            }
        }
        self.next_cursor = page.next_cursor;
        changed
    }

    fn should_prefetch(&self, state: &UiState, snapshot: &DashboardSnapshot) -> bool {
        if Tab::ALL[state.active_tab] != Tab::Traces || state.trace_view_mode != TraceViewMode::List
        {
            return false;
        }
        if self.in_flight || self.next_cursor.is_none() {
            return false;
        }
        if self.filters.as_ref() != Some(&input::filters(state, &snapshot.services)) {
            return false;
        }

        let furthest_position = state.selected_trace.max(state.trace_list_scroll);
        furthest_position.saturating_add(PREFETCH_MARGIN_ROWS) >= self.items.len()
    }

    fn sync_snapshot(&self, snapshot: &mut DashboardSnapshot) {
        snapshot.traces = self.items.clone();
    }
}

pub(super) fn sync_first_page(
    query: &QueryService,
    state: &mut UiState,
    snapshot: &mut DashboardSnapshot,
    pager: &mut TraceListPager,
    tx: &mpsc::UnboundedSender<TracePageRefreshResult>,
) -> Result<()> {
    let anchor = TraceListAnchor::capture(snapshot, state);
    let filters = input::filters(state, &snapshot.services);
    sync_first_page_for_filters(query, state, snapshot, pager, tx, filters, anchor)
}

pub(super) fn sync_first_page_for_filters(
    query: &QueryService,
    state: &mut UiState,
    snapshot: &mut DashboardSnapshot,
    pager: &mut TraceListPager,
    tx: &mpsc::UnboundedSender<TracePageRefreshResult>,
    filters: QueryFilters,
    anchor: TraceListAnchor,
) -> Result<()> {
    let page = query.traces_page(&filters, &PageRequest::first(query.page_size()))?;
    pager.set_first_page(filters, page);
    pager.sync_snapshot(snapshot);
    anchor.restore(snapshot, state);
    ensure_prefetch(query, state, snapshot, pager, tx);
    Ok(())
}

pub(super) fn apply_page_result(
    result: TracePageRefreshResult,
    state: &mut UiState,
    snapshot: &mut DashboardSnapshot,
    pager: &mut TraceListPager,
) -> bool {
    let anchor = TraceListAnchor::capture(snapshot, state);
    let changed = pager.apply_page_result(result);
    if changed {
        pager.sync_snapshot(snapshot);
        anchor.restore(snapshot, state);
    }
    changed
}

pub(super) fn ensure_prefetch(
    query: &QueryService,
    state: &UiState,
    snapshot: &DashboardSnapshot,
    pager: &mut TraceListPager,
    tx: &mpsc::UnboundedSender<TracePageRefreshResult>,
) {
    if !pager.should_prefetch(state, snapshot) {
        return;
    }

    let Some(filters) = pager.filters.clone() else {
        return;
    };
    let page = PageRequest {
        limit: query.page_size(),
        cursor: pager.next_cursor.clone(),
    };
    pager.in_flight = true;
    spawn_page_refresh(query.clone(), filters, page, tx.clone());
}

fn spawn_page_refresh(
    query: QueryService,
    filters: QueryFilters,
    page: PageRequest<TraceCursor>,
    tx: mpsc::UnboundedSender<TracePageRefreshResult>,
) {
    task::spawn_blocking(move || {
        let page = query.traces_page(&filters, &page);
        let _ = tx.send(TracePageRefreshResult { filters, page });
    });
}

fn trace_index(snapshot: &DashboardSnapshot, trace_id: &str) -> Option<usize> {
    snapshot
        .traces
        .iter()
        .position(|trace| trace.trace_id == trace_id)
}

#[cfg(test)]
mod tests {
    use super::{TraceListAnchor, TraceListPager};
    use crate::{
        domain::{DashboardSnapshot, OverviewStats, TraceSummary},
        query::{Page, QueryFilters, TraceCursor},
        ui::{Tab, TraceViewMode, UiState},
    };

    #[test]
    fn first_page_merge_keeps_loaded_older_pages() {
        let mut pager = TraceListPager::default();
        let filters = QueryFilters::default();
        pager.set_first_page(
            filters.clone(),
            page(vec![trace("b", 20), trace("a", 10)], Some(cursor("a", 10))),
        );

        pager.set_first_page(
            filters,
            page(vec![trace("c", 30), trace("b", 21)], Some(cursor("b", 21))),
        );

        let trace_ids = pager
            .items
            .iter()
            .map(|trace| trace.trace_id.as_str())
            .collect::<Vec<_>>();
        assert_eq!(trace_ids, vec!["c", "b", "a"]);
        assert_eq!(pager.items[1].started_at_unix_nano, 21);
    }

    #[test]
    fn first_page_merge_drops_stale_tail_when_complete() {
        let mut pager = TraceListPager::default();
        let filters = QueryFilters::default();
        pager.set_first_page(
            filters.clone(),
            page(vec![trace("b", 20), trace("a", 10)], Some(cursor("a", 10))),
        );

        pager.set_first_page(filters, page(vec![trace("c", 30), trace("b", 21)], None));

        let trace_ids = pager
            .items
            .iter()
            .map(|trace| trace.trace_id.as_str())
            .collect::<Vec<_>>();
        assert_eq!(trace_ids, vec!["c", "b"]);
        assert!(pager.next_cursor.is_none());
    }

    #[test]
    fn prefetch_uses_mouse_scroll_position() {
        let pager = TraceListPager {
            filters: Some(QueryFilters::default()),
            items: (0..100)
                .map(|index| trace(&format!("trace-{index}"), 100 - index))
                .collect(),
            next_cursor: Some(cursor("trace-99", 1)),
            in_flight: false,
        };
        let state = UiState {
            active_tab: Tab::Traces.index(),
            trace_view_mode: TraceViewMode::List,
            selected_trace: 0,
            trace_list_scroll: 60,
            trace_list_follow_selected: false,
            ..UiState::default()
        };

        assert!(pager.should_prefetch(&state, &snapshot(Vec::new())));
    }

    #[test]
    fn anchor_restores_mouse_scroll_top_trace() {
        let mut state = UiState {
            selected_trace: 1,
            trace_list_scroll: 2,
            trace_list_follow_selected: false,
            ..UiState::default()
        };
        let before = snapshot(vec![
            trace("new", 40),
            trace("selected", 30),
            trace("top", 20),
            trace("older", 10),
        ]);
        let after = snapshot(vec![
            trace("newer", 50),
            trace("new", 40),
            trace("selected", 30),
            trace("top", 20),
            trace("older", 10),
        ]);

        let anchor = TraceListAnchor::capture(&before, &state);
        anchor.restore(&after, &mut state);

        assert_eq!(state.selected_trace, 2);
        assert_eq!(state.trace_list_scroll, 3);
    }

    fn page(
        items: Vec<TraceSummary>,
        next_cursor: Option<TraceCursor>,
    ) -> Page<TraceSummary, TraceCursor> {
        Page { items, next_cursor }
    }

    fn snapshot(traces: Vec<TraceSummary>) -> DashboardSnapshot {
        DashboardSnapshot {
            services: Vec::new(),
            overview: OverviewStats {
                service_count: 0,
                trace_count: traces.len(),
                error_span_count: 0,
                log_count: 0,
                metric_count: 0,
                llm_count: 0,
            },
            traces,
            selected_trace: Vec::new(),
            logs: Vec::new(),
            metrics: Vec::new(),
            llm: Vec::new(),
            llm_rollups: Vec::new(),
            llm_sessions: Vec::new(),
            llm_model_comparisons: Vec::new(),
            llm_top_calls: Vec::new(),
            selected_llm_timeline: Vec::new(),
        }
    }

    fn trace(trace_id: &str, started_at_unix_nano: i64) -> TraceSummary {
        TraceSummary {
            trace_id: trace_id.to_string(),
            service_name: "api".to_string(),
            root_name: "request".to_string(),
            span_count: 1,
            error_count: 0,
            duration_ms: 1.0,
            started_at_unix_nano,
        }
    }

    fn cursor(trace_id: &str, started_at_unix_nano: i64) -> TraceCursor {
        TraceCursor {
            started_at_unix_nano,
            trace_id: trace_id.to_string(),
        }
    }
}
