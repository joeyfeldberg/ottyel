use std::path::PathBuf;

use clap::{Args, Parser, Subcommand, ValueEnum};

#[derive(Debug, Clone, Parser)]
#[command(
    name = "ottyel",
    version,
    about = "A local OTEL + LLM telemetry workstation in the terminal"
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Option<Command>,
}

#[derive(Debug, Clone, Subcommand)]
pub enum Command {
    Serve(ServeArgs),
    Doctor(DoctorArgs),
}

#[derive(Debug, Clone, Args)]
pub struct ServeArgs {
    #[arg(
        long = "http-bind",
        visible_alias = "bind",
        default_value = "127.0.0.1:4318"
    )]
    pub http_bind: String,
    #[arg(long, default_value = "127.0.0.1:4317")]
    pub grpc_bind: String,
    #[arg(long, default_value = ".ottyel/ottyel.db")]
    pub db_path: PathBuf,
    #[arg(long, default_value_t = 24)]
    pub retention_hours: u64,
    #[arg(long, default_value_t = 100_000)]
    pub max_spans: usize,
    #[arg(long, default_value_t = 750)]
    pub tick_rate_ms: u64,
    #[arg(long, default_value_t = 500)]
    pub page_size: usize,
    #[arg(long, value_enum, default_value_t = Theme::Ember)]
    pub theme: Theme,
}

impl Default for ServeArgs {
    fn default() -> Self {
        Self {
            http_bind: "127.0.0.1:4318".to_string(),
            grpc_bind: "127.0.0.1:4317".to_string(),
            db_path: PathBuf::from(".ottyel/ottyel.db"),
            retention_hours: 24,
            max_spans: 100_000,
            tick_rate_ms: 750,
            page_size: 500,
            theme: Theme::Ember,
        }
    }
}

#[derive(Debug, Clone, Args)]
pub struct DoctorArgs {
    #[arg(long, default_value = ".ottyel/ottyel.db")]
    pub db_path: PathBuf,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, ValueEnum)]
pub enum Theme {
    Ember,
    Tidal,
    Grove,
    Paper,
    Neon,
}

impl Theme {
    pub const ALL: [Self; 5] = [
        Self::Ember,
        Self::Tidal,
        Self::Grove,
        Self::Paper,
        Self::Neon,
    ];

    pub fn label(self) -> &'static str {
        match self {
            Self::Ember => "ember",
            Self::Tidal => "tidal",
            Self::Grove => "grove",
            Self::Paper => "paper",
            Self::Neon => "neon",
        }
    }
}
