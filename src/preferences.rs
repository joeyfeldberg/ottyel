use std::{
    fs,
    path::{Path, PathBuf},
};

use directories::ProjectDirs;
use serde::{Deserialize, Serialize};

use crate::{
    config::Theme,
    query::TimeWindow,
    ui::{LayoutPreset, Tab, UiState},
};

const PREFERENCES_VERSION: u8 = 1;
const MIN_LAYOUT_SPLIT_PCT: u16 = 30;
const MAX_LAYOUT_SPLIT_PCT: u16 = 70;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct UserPreferences {
    pub version: u8,
    pub theme: Theme,
    pub active_tab: Tab,
    pub layout_preset: LayoutPreset,
    pub trace_split_pct: u16,
    pub log_split_pct: u16,
    pub metric_split_pct: u16,
    pub llm_split_pct: u16,
    pub time_window: TimeWindow,
}

impl Default for UserPreferences {
    fn default() -> Self {
        let state = UiState::default();
        Self::from_state(&state)
    }
}

impl UserPreferences {
    pub fn from_state(state: &UiState) -> Self {
        Self {
            version: PREFERENCES_VERSION,
            theme: state.theme,
            active_tab: Tab::ALL
                .get(state.active_tab)
                .copied()
                .unwrap_or(Tab::Overview),
            layout_preset: state.layout_preset,
            trace_split_pct: state.trace_split_pct,
            log_split_pct: state.log_split_pct,
            metric_split_pct: state.metric_split_pct,
            llm_split_pct: state.llm_split_pct,
            time_window: state.time_window,
        }
    }

    pub fn apply_to_state(&self, state: &mut UiState) {
        state.theme = self.theme;
        state.active_tab = self.active_tab.index();
        state.layout_preset = self.layout_preset;
        state.trace_split_pct = clamp_split_pct(self.trace_split_pct);
        state.log_split_pct = clamp_split_pct(self.log_split_pct);
        state.metric_split_pct = clamp_split_pct(self.metric_split_pct);
        state.llm_split_pct = clamp_split_pct(self.llm_split_pct);
        state.time_window = self.time_window;
    }

    pub fn load() -> Option<Self> {
        Self::load_from_path(&preferences_path()).ok().flatten()
    }

    pub fn save(&self) -> std::io::Result<()> {
        Self::save_to_path(&preferences_path(), self)
    }

    fn load_from_path(path: &Path) -> std::io::Result<Option<Self>> {
        if !path.exists() {
            return Ok(None);
        }

        let contents = fs::read_to_string(path)?;
        let preferences = serde_json::from_str::<Self>(&contents).ok();
        Ok(preferences)
    }

    fn save_to_path(path: &Path, preferences: &Self) -> std::io::Result<()> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let encoded = serde_json::to_vec_pretty(preferences)?;
        fs::write(path, encoded)
    }
}

fn preferences_path() -> PathBuf {
    ProjectDirs::from("", "", "ottyel")
        .map(|dirs| dirs.config_local_dir().join("preferences.json"))
        .unwrap_or_else(|| PathBuf::from(".ottyel/preferences.json"))
}

fn clamp_split_pct(value: u16) -> u16 {
    value.clamp(MIN_LAYOUT_SPLIT_PCT, MAX_LAYOUT_SPLIT_PCT)
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::UserPreferences;
    use crate::{
        config::Theme,
        query::TimeWindow,
        ui::{LayoutPreset, Tab, UiState},
    };

    #[test]
    fn preferences_round_trip_through_disk() {
        let tempdir = tempdir().unwrap();
        let path = tempdir.path().join("preferences.json");
        let preferences = UserPreferences {
            theme: Theme::Neon,
            active_tab: Tab::Llm,
            layout_preset: LayoutPreset::PrimaryFocus,
            trace_split_pct: 68,
            log_split_pct: 61,
            metric_split_pct: 57,
            llm_split_pct: 54,
            time_window: TimeWindow::SixHours,
            ..UserPreferences::default()
        };

        UserPreferences::save_to_path(&path, &preferences).unwrap();
        let loaded = UserPreferences::load_from_path(&path).unwrap().unwrap();

        assert_eq!(loaded, preferences);
    }

    #[test]
    fn preferences_apply_clamps_split_sizes() {
        let mut state = UiState::default();
        let preferences = UserPreferences {
            trace_split_pct: 5,
            log_split_pct: 95,
            metric_split_pct: 5,
            llm_split_pct: 95,
            ..UserPreferences::default()
        };

        preferences.apply_to_state(&mut state);

        assert_eq!(state.trace_split_pct, 30);
        assert_eq!(state.log_split_pct, 70);
        assert_eq!(state.metric_split_pct, 30);
        assert_eq!(state.llm_split_pct, 70);
    }
}
