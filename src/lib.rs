pub mod app;
pub mod commands;
pub mod config;
pub mod domain;
pub mod ingest;
pub mod preferences;
pub mod query;
pub mod store;
pub mod ui;

use anyhow::Result;
use clap::Parser;

pub async fn run() -> Result<()> {
    let cli = config::Cli::parse();
    app::run(cli).await
}
