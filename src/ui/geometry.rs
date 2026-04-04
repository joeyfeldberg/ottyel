use ratatui::layout::{Constraint, Direction, Layout, Rect};

pub(crate) fn body_area(root: Rect) -> Rect {
    Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),
            Constraint::Length(1),
            Constraint::Min(10),
            Constraint::Length(1),
        ])
        .split(root)[2]
}

pub(crate) fn trace_detail_sections(area: Rect) -> [Rect; 2] {
    let split = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Percentage(62), Constraint::Percentage(38)])
        .split(area);
    [split[0], split[1]]
}

pub(crate) fn log_sections(area: Rect) -> [Rect; 2] {
    let split = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(58), Constraint::Percentage(42)])
        .split(area);
    [split[0], split[1]]
}

pub(crate) fn metric_sections(area: Rect) -> [Rect; 2] {
    let split = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(52), Constraint::Percentage(48)])
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

pub(crate) fn llm_sections(area: Rect) -> [Rect; 2] {
    let split = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(55), Constraint::Percentage(45)])
        .split(area);
    [split[0], split[1]]
}

pub(crate) fn trace_tree_area(body: Rect) -> Rect {
    trace_detail_sections(body)[0]
}

pub(crate) fn trace_detail_area(body: Rect) -> Rect {
    trace_detail_sections(body)[1]
}

pub(crate) fn log_detail_area(body: Rect) -> Rect {
    log_sections(body)[1]
}

pub(crate) fn metric_detail_area(body: Rect) -> Rect {
    metric_right_sections(metric_sections(body)[1])[1]
}

pub(crate) fn llm_detail_area(body: Rect) -> Rect {
    llm_sections(body)[1]
}

pub(crate) fn trace_tree_viewport_height(area: Rect) -> usize {
    area.height.saturating_sub(2) as usize
}

pub(crate) fn detail_viewport_height(area: Rect) -> usize {
    area.height.saturating_sub(2) as usize
}

pub(crate) fn trace_tree_scroll_offset(
    current_offset: usize,
    total_lines: usize,
    selected_line: usize,
    viewport_height: usize,
) -> usize {
    if total_lines == 0 || viewport_height == 0 || total_lines <= viewport_height {
        return 0;
    }

    let max_offset = total_lines.saturating_sub(viewport_height);
    let offset = current_offset.min(max_offset);

    if selected_line < offset {
        return selected_line;
    }

    let visible_end = offset.saturating_add(viewport_height);
    if selected_line >= visible_end {
        return (selected_line + 1)
            .saturating_sub(viewport_height)
            .min(max_offset);
    }

    offset
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
