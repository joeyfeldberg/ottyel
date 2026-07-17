use std::{
    num::{NonZeroU64, NonZeroUsize},
    path::PathBuf,
};

use clap::{Args, Parser, Subcommand, ValueEnum};
use directories::ProjectDirs;
use serde::{Deserialize, Serialize};

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
    Mcp(McpArgs),
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
    #[arg(long, default_value_os_t = default_db_path())]
    pub db_path: PathBuf,
    #[arg(long, default_value_t = 24)]
    pub retention_hours: u64,
    #[arg(long, default_value_t = 100_000)]
    pub max_spans: usize,
    #[arg(long, default_value_t = 750)]
    pub tick_rate_ms: u64,
    #[arg(long, default_value_t = 500)]
    pub page_size: usize,
    #[arg(long, value_enum)]
    pub theme: Option<Theme>,
    /// Maximum OTLP requests admitted before decode and retained through SQLite outcome.
    #[arg(
        long,
        value_parser = parse_max_otlp_in_flight,
        default_value_t = NonZeroUsize::new(4).unwrap()
    )]
    pub max_otlp_in_flight: NonZeroUsize,
    /// Maximum OTLP transport bytes. HTTP applies this to compressed or identity body bytes;
    /// gRPC uses the smaller of this and the decompressed limit as Tonic's single decode cap.
    #[arg(long, default_value_t = NonZeroUsize::new(4 * 1024 * 1024).unwrap())]
    pub max_otlp_wire_bytes: NonZeroUsize,
    /// Maximum decompressed protobuf bytes. HTTP enforces this separately after gzip; gRPC uses
    /// the smaller of this and the transport limit as Tonic's single decode cap.
    #[arg(long, default_value_t = NonZeroUsize::new(4 * 1024 * 1024).unwrap())]
    pub max_otlp_decompressed_bytes: NonZeroUsize,
    /// Maximum milliseconds to await an OTLP request before returning a retryable timeout.
    /// Accepted storage work may finish afterward and retains capacity until its outcome.
    #[arg(long, default_value_t = NonZeroU64::new(30_000).unwrap())]
    pub otlp_request_timeout_ms: NonZeroU64,
    /// Post-decode maximum spans, log records, or metric data points per OTLP request.
    #[arg(long, default_value_t = NonZeroUsize::new(10_000).unwrap())]
    pub max_otlp_records: NonZeroUsize,
    /// Post-decode maximum KeyValue attributes across an OTLP request.
    #[arg(long, default_value_t = NonZeroUsize::new(100_000).unwrap())]
    pub max_otlp_attributes: NonZeroUsize,
    /// Post-decode maximum envelopes, nested values, and repeated structural items per request.
    #[arg(long, default_value_t = NonZeroUsize::new(250_000).unwrap())]
    pub max_otlp_structures: NonZeroUsize,
    /// Post-decode maximum nested AnyValue depth, including the root value.
    #[arg(long, default_value_t = NonZeroUsize::new(16).unwrap())]
    pub max_otlp_any_value_depth: NonZeroUsize,
    /// Post-decode maximum bytes in any individual dynamic protobuf string or bytes field.
    #[arg(long, default_value_t = NonZeroUsize::new(1024 * 1024).unwrap())]
    pub max_otlp_value_bytes: NonZeroUsize,
}

impl Default for ServeArgs {
    fn default() -> Self {
        Self {
            http_bind: "127.0.0.1:4318".to_string(),
            grpc_bind: "127.0.0.1:4317".to_string(),
            db_path: default_db_path(),
            retention_hours: 24,
            max_spans: 100_000,
            tick_rate_ms: 750,
            page_size: 500,
            theme: None,
            max_otlp_in_flight: NonZeroUsize::new(4).unwrap(),
            max_otlp_wire_bytes: NonZeroUsize::new(4 * 1024 * 1024).unwrap(),
            max_otlp_decompressed_bytes: NonZeroUsize::new(4 * 1024 * 1024).unwrap(),
            otlp_request_timeout_ms: NonZeroU64::new(30_000).unwrap(),
            max_otlp_records: NonZeroUsize::new(10_000).unwrap(),
            max_otlp_attributes: NonZeroUsize::new(100_000).unwrap(),
            max_otlp_structures: NonZeroUsize::new(250_000).unwrap(),
            max_otlp_any_value_depth: NonZeroUsize::new(16).unwrap(),
            max_otlp_value_bytes: NonZeroUsize::new(1024 * 1024).unwrap(),
        }
    }
}

fn parse_max_otlp_in_flight(value: &str) -> Result<NonZeroUsize, String> {
    let parsed = value
        .parse::<usize>()
        .map_err(|err| format!("invalid OTLP in-flight limit: {err}"))?;
    let parsed = NonZeroUsize::new(parsed)
        .ok_or_else(|| "OTLP in-flight limit must be greater than zero".to_string())?;
    if parsed.get() > tokio::sync::Semaphore::MAX_PERMITS {
        return Err(format!(
            "OTLP in-flight limit must be at most {}",
            tokio::sync::Semaphore::MAX_PERMITS
        ));
    }
    Ok(parsed)
}

#[derive(Debug, Clone, Args)]
pub struct DoctorArgs {
    #[arg(long, default_value_os_t = default_db_path())]
    pub db_path: PathBuf,
}

#[derive(Debug, Clone, Args)]
pub struct McpArgs {
    #[arg(long, default_value_os_t = default_db_path())]
    pub db_path: PathBuf,
    #[arg(long, default_value_t = 100)]
    pub page_size: usize,
}

fn default_db_path() -> PathBuf {
    ProjectDirs::from("", "", "ottyel")
        .map(|dirs| dirs.data_local_dir().join("ottyel.db"))
        .unwrap_or_else(|| PathBuf::from(".ottyel/ottyel.db"))
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, ValueEnum, Serialize, Deserialize)]
pub enum Theme {
    #[serde(rename = "ember")]
    Ember,
    #[serde(rename = "tidal")]
    Tidal,
    #[serde(rename = "grove")]
    Grove,
    #[serde(rename = "paper")]
    Paper,
    #[serde(rename = "neon")]
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

#[cfg(test)]
mod tests {
    use clap::Parser;
    use tokio::sync::Semaphore;

    use super::{Cli, Command, ServeArgs};

    #[test]
    fn otlp_limits_reject_zero_at_the_cli_boundary() {
        for flag in [
            "--max-otlp-in-flight",
            "--max-otlp-wire-bytes",
            "--max-otlp-decompressed-bytes",
            "--otlp-request-timeout-ms",
            "--max-otlp-records",
            "--max-otlp-attributes",
            "--max-otlp-structures",
            "--max-otlp-any-value-depth",
            "--max-otlp-value-bytes",
        ] {
            assert!(
                Cli::try_parse_from(["ottyel", "serve", flag, "0"]).is_err(),
                "{flag} accepted zero"
            );
        }
    }

    #[test]
    fn otlp_in_flight_limit_cannot_exceed_semaphore_capacity() {
        let too_large = (Semaphore::MAX_PERMITS + 1).to_string();
        let error = Cli::try_parse_from([
            "ottyel",
            "serve",
            "--max-otlp-in-flight",
            too_large.as_str(),
        ])
        .unwrap_err()
        .to_string();

        assert!(error.contains("OTLP in-flight limit must be at most"));
    }

    #[test]
    fn otlp_envelope_defaults_are_transport_aligned() {
        let Command::Serve(args) = Cli::try_parse_from(["ottyel", "serve"])
            .unwrap()
            .command
            .unwrap()
        else {
            panic!("expected serve command");
        };

        assert_eq!(args.max_otlp_in_flight.get(), 4);
        assert_eq!(args.max_otlp_wire_bytes, args.max_otlp_decompressed_bytes);
        assert_eq!(args.otlp_request_timeout_ms.get(), 30_000);
        assert_eq!(
            args.max_otlp_wire_bytes,
            ServeArgs::default().max_otlp_wire_bytes
        );
    }
}
