use crate::{config::Theme, ui::Tab};

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum PaletteAction {
    SwitchTab(Tab),
    SetTheme(Theme),
    CycleTheme,
    ToggleHelp,
    CycleService,
    ClearService,
    CycleTimeWindow,
    ToggleTraceErrors,
    ReturnToTraceList,
    ToggleLogTail,
    ClearGlobalSearch,
    ClearLogSearch,
    Quit,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct PaletteCommand {
    pub title: &'static str,
    pub aliases: &'static [&'static str],
    pub action: PaletteAction,
}

const COMMANDS: &[PaletteCommand] = &[
    PaletteCommand {
        title: "Go to Overview",
        aliases: &["overview", "tab overview", "home"],
        action: PaletteAction::SwitchTab(Tab::Overview),
    },
    PaletteCommand {
        title: "Go to Trace Explorer",
        aliases: &["traces", "trace explorer", "tab traces"],
        action: PaletteAction::SwitchTab(Tab::Traces),
    },
    PaletteCommand {
        title: "Go to Logs",
        aliases: &["logs", "tab logs"],
        action: PaletteAction::SwitchTab(Tab::Logs),
    },
    PaletteCommand {
        title: "Go to Metrics",
        aliases: &["metrics", "tab metrics"],
        action: PaletteAction::SwitchTab(Tab::Metrics),
    },
    PaletteCommand {
        title: "Go to LLM Inspector",
        aliases: &["llm", "inspector", "tab llm"],
        action: PaletteAction::SwitchTab(Tab::Llm),
    },
    PaletteCommand {
        title: "Cycle Theme",
        aliases: &["theme", "next theme", "palette"],
        action: PaletteAction::CycleTheme,
    },
    PaletteCommand {
        title: "Theme: Ember",
        aliases: &["theme ember", "ember"],
        action: PaletteAction::SetTheme(Theme::Ember),
    },
    PaletteCommand {
        title: "Theme: Tidal",
        aliases: &["theme tidal", "tidal"],
        action: PaletteAction::SetTheme(Theme::Tidal),
    },
    PaletteCommand {
        title: "Theme: Grove",
        aliases: &["theme grove", "grove"],
        action: PaletteAction::SetTheme(Theme::Grove),
    },
    PaletteCommand {
        title: "Theme: Paper",
        aliases: &["theme paper", "paper", "light"],
        action: PaletteAction::SetTheme(Theme::Paper),
    },
    PaletteCommand {
        title: "Theme: Neon",
        aliases: &["theme neon", "neon"],
        action: PaletteAction::SetTheme(Theme::Neon),
    },
    PaletteCommand {
        title: "Open Help",
        aliases: &["help", "commands", "shortcuts"],
        action: PaletteAction::ToggleHelp,
    },
    PaletteCommand {
        title: "Cycle Service Filter",
        aliases: &["service", "filter service", "next service"],
        action: PaletteAction::CycleService,
    },
    PaletteCommand {
        title: "Clear Service Filter",
        aliases: &["service all", "reset service", "clear service"],
        action: PaletteAction::ClearService,
    },
    PaletteCommand {
        title: "Cycle Time Window",
        aliases: &["window", "time", "time window"],
        action: PaletteAction::CycleTimeWindow,
    },
    PaletteCommand {
        title: "Toggle Errors-Only Traces",
        aliases: &["errors", "trace errors", "error filter"],
        action: PaletteAction::ToggleTraceErrors,
    },
    PaletteCommand {
        title: "Return to Trace List",
        aliases: &["trace list", "close trace", "back traces"],
        action: PaletteAction::ReturnToTraceList,
    },
    PaletteCommand {
        title: "Toggle Log Tail",
        aliases: &["tail", "follow logs", "log tail"],
        action: PaletteAction::ToggleLogTail,
    },
    PaletteCommand {
        title: "Clear Global Search",
        aliases: &["clear search", "reset search", "global search"],
        action: PaletteAction::ClearGlobalSearch,
    },
    PaletteCommand {
        title: "Clear Log Search",
        aliases: &["clear log search", "reset log search", "log filter text"],
        action: PaletteAction::ClearLogSearch,
    },
    PaletteCommand {
        title: "Quit",
        aliases: &["exit", "close app"],
        action: PaletteAction::Quit,
    },
];

pub fn matching_commands(query: &str) -> Vec<PaletteCommand> {
    let query = query.trim().to_ascii_lowercase();
    if query.is_empty() {
        return COMMANDS.to_vec();
    }

    COMMANDS
        .iter()
        .copied()
        .filter(|command| command_matches(*command, &query))
        .collect()
}

fn command_matches(command: PaletteCommand, query: &str) -> bool {
    command.title.to_ascii_lowercase().contains(query)
        || command
            .aliases
            .iter()
            .any(|alias| alias.to_ascii_lowercase().contains(query))
}

#[cfg(test)]
mod tests {
    use super::{PaletteAction, matching_commands};
    use crate::{config::Theme, ui::Tab};

    #[test]
    fn matching_commands_filters_by_title_and_alias() {
        let logs = matching_commands("logs");
        assert!(
            logs.iter()
                .any(|command| command.action == PaletteAction::SwitchTab(Tab::Logs))
        );

        let tail = matching_commands("follow");
        assert!(
            tail.iter()
                .any(|command| command.action == PaletteAction::ToggleLogTail)
        );

        let paper = matching_commands("light");
        assert!(
            paper
                .iter()
                .any(|command| command.action == PaletteAction::SetTheme(Theme::Paper))
        );
    }
}
