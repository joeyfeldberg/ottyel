use ratatui::layout::{Constraint, Direction, Layout, Rect};

fn split_percent(percent: u16) -> [Constraint; 2] {
    let clamped = percent.clamp(20, 80);
    [
        Constraint::Percentage(clamped),
        Constraint::Percentage(100_u16.saturating_sub(clamped)),
    ]
}

/// The one-row header, the body, and the one-row footer.
pub(crate) fn root_sections(root: Rect) -> [Rect; 3] {
    let split = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(1),
            Constraint::Min(10),
            Constraint::Length(1),
        ])
        .split(root);
    [split[0], split[1], split[2]]
}

pub(crate) fn body_area(root: Rect) -> Rect {
    root_sections(root)[1]
}

/// The brand chip at the left of the header.
pub(crate) const BRAND: &str = " ◆ ottyel ";

/// The header column span of each tab, shared by rendering and mouse hit-testing.
pub(crate) fn header_tabs(header: Rect) -> Vec<(super::Tab, u16, u16)> {
    let mut x = header.x.saturating_add(BRAND.chars().count() as u16 + 1);
    let right = header.x.saturating_add(header.width);
    let mut tabs = Vec::new();
    for (index, tab) in super::Tab::ALL.into_iter().enumerate() {
        let width = tab_label(index, tab).chars().count() as u16;
        let end = x.saturating_add(width);
        if end > right {
            break;
        }
        tabs.push((tab, x, end));
        x = end;
    }
    tabs
}

pub(crate) fn tab_label(index: usize, tab: super::Tab) -> String {
    format!(" {} {} ", index + 1, tab.label())
}

pub(crate) fn contains(area: Rect, column: u16, row: u16) -> bool {
    column >= area.x
        && column < area.x.saturating_add(area.width)
        && row >= area.y
        && row < area.y.saturating_add(area.height)
}

pub(crate) fn trace_detail_sections(area: Rect, top_pct: u16) -> [Rect; 2] {
    let split = Layout::default()
        .direction(Direction::Vertical)
        .constraints(split_percent(top_pct))
        .split(area);
    [split[0], split[1]]
}

pub(crate) fn log_sections(area: Rect, left_pct: u16) -> [Rect; 2] {
    let split = Layout::default()
        .direction(Direction::Horizontal)
        .constraints(split_percent(left_pct))
        .split(area);
    [split[0], split[1]]
}

pub(crate) fn metric_sections(area: Rect, left_pct: u16) -> [Rect; 2] {
    let split = Layout::default()
        .direction(Direction::Horizontal)
        .constraints(split_percent(left_pct))
        .split(area);
    [split[0], split[1]]
}

pub(crate) fn metric_right_sections(area: Rect) -> [Rect; 2] {
    let split = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(9), Constraint::Min(10)])
        .split(area);
    [split[0], split[1]]
}

pub(crate) fn llm_sections(area: Rect, left_pct: u16) -> [Rect; 2] {
    let split = Layout::default()
        .direction(Direction::Horizontal)
        .constraints(split_percent(left_pct))
        .split(area);
    [split[0], split[1]]
}

pub(crate) fn llm_left_sections(area: Rect) -> [Rect; 3] {
    let split = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(8),
            Constraint::Length(7),
            Constraint::Min(10),
        ])
        .split(area);
    [split[0], split[1], split[2]]
}

pub(crate) fn trace_tree_area(body: Rect, top_pct: u16) -> Rect {
    trace_detail_sections(body, top_pct)[0]
}

pub(crate) fn trace_detail_area(body: Rect, top_pct: u16) -> Rect {
    trace_detail_sections(body, top_pct)[1]
}

pub(crate) fn log_detail_area(body: Rect, left_pct: u16) -> Rect {
    log_sections(body, left_pct)[1]
}

pub(crate) fn llm_detail_area(body: Rect, left_pct: u16) -> Rect {
    llm_sections(body, left_pct)[1]
}

pub(crate) fn llm_detail_sections(area: Rect) -> [Rect; 2] {
    let split = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Min(12), Constraint::Length(9)])
        .split(area);
    [split[0], split[1]]
}

pub(crate) fn trace_tree_viewport_height(area: Rect) -> usize {
    area.height.saturating_sub(2) as usize
}

pub(crate) fn detail_viewport_height(area: Rect) -> usize {
    area.height.saturating_sub(2) as usize
}

/// Inner text width: two border columns plus one column of padding on each side.
pub(crate) fn detail_viewport_width(area: Rect) -> usize {
    area.width.saturating_sub(4) as usize
}

pub(crate) fn table_viewport_height(area: Rect) -> usize {
    area.height.saturating_sub(3) as usize
}

pub(crate) fn scroll_window_offset(
    current_offset: usize,
    total_items: usize,
    viewport_height: usize,
    delta: isize,
) -> usize {
    if total_items == 0 || viewport_height == 0 || total_items <= viewport_height {
        return 0;
    }

    let max_offset = total_items.saturating_sub(viewport_height);
    (current_offset as isize + delta).clamp(0, max_offset as isize) as usize
}

pub(crate) fn clamp_window_offset(
    current_offset: usize,
    total_items: usize,
    viewport_height: usize,
) -> usize {
    scroll_window_offset(current_offset, total_items, viewport_height, 0)
}

pub(crate) fn follow_selected_offset(
    current_offset: usize,
    total_items: usize,
    selected_index: usize,
    viewport_height: usize,
) -> usize {
    if total_items == 0 || viewport_height == 0 || total_items <= viewport_height {
        return 0;
    }

    let max_offset = total_items.saturating_sub(viewport_height);
    let offset = current_offset.min(max_offset);

    if selected_index < offset {
        return selected_index;
    }

    let visible_end = offset.saturating_add(viewport_height);
    if selected_index >= visible_end {
        return (selected_index + 1)
            .saturating_sub(viewport_height)
            .min(max_offset);
    }

    offset
}

pub(crate) fn trace_tree_scroll_offset(
    current_offset: usize,
    total_lines: usize,
    selected_line: usize,
    viewport_height: usize,
) -> usize {
    follow_selected_offset(current_offset, total_lines, selected_line, viewport_height)
}

pub(crate) fn clamp_scroll(current: u16, line_count: usize, viewport_height: usize) -> u16 {
    if viewport_height == 0 || line_count <= viewport_height {
        return 0;
    }

    let max_scroll = line_count.saturating_sub(viewport_height);
    current.min(u16::try_from(max_scroll).unwrap_or(u16::MAX))
}

pub(crate) fn centered_rect(percent_x: u16, percent_y: u16, area: Rect) -> Rect {
    let vertical = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Percentage((100 - percent_y) / 2),
            Constraint::Percentage(percent_y),
            Constraint::Percentage((100 - percent_y) / 2),
        ])
        .split(area);

    Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage((100 - percent_x) / 2),
            Constraint::Percentage(percent_x),
            Constraint::Percentage((100 - percent_x) / 2),
        ])
        .split(vertical[1])[1]
}
