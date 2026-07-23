use std::path::PathBuf;

use anyhow::{Result, bail};
use clap::{Parser, ValueEnum};
use serde::Serialize;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, ValueEnum)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Profile {
    Smoke,
    Reference,
}

impl Profile {
    fn defaults(self) -> (usize, usize) {
        match self {
            Self::Smoke => (1, 3),
            Self::Reference => (3, 20),
        }
    }

    pub(crate) fn output_name(self) -> &'static str {
        match self {
            Self::Smoke => "ingest-field-work-smoke.json",
            Self::Reference => "ingest-field-work-reference.json",
        }
    }
}

#[derive(Debug, Parser)]
#[command(about = "Deterministic OTLP protobuf field-work benchmark")]
struct Args {
    #[arg(long = "bench", hide = true)]
    _bench: bool,

    #[arg(long, value_enum, default_value_t = Profile::Smoke)]
    profile: Profile,

    #[arg(long)]
    output: Option<PathBuf>,

    #[arg(long)]
    warmup: Option<usize>,

    #[arg(long)]
    samples: Option<usize>,

    #[arg(long)]
    machine_label: Option<String>,

    #[arg(long)]
    cpu: Option<String>,

    #[arg(long)]
    memory_gib: Option<u64>,
}

#[derive(Debug)]
pub(crate) struct RunConfig {
    pub profile: Profile,
    pub output: PathBuf,
    pub warmup: usize,
    pub samples: usize,
    pub machine_label: Option<String>,
    pub cpu: Option<String>,
    pub memory_gib: Option<u64>,
}

impl RunConfig {
    pub(crate) fn parse() -> Result<Self> {
        Self::from_args(Args::parse())
    }

    fn from_args(args: Args) -> Result<Self> {
        let (default_warmup, default_samples) = args.profile.defaults();
        let warmup = args.warmup.unwrap_or(default_warmup);
        let samples = args.samples.unwrap_or(default_samples);
        if warmup == 0 || samples == 0 {
            bail!("warmup and samples must both be greater than zero");
        }
        if args.profile == Profile::Reference
            && (args.machine_label.as_deref().is_none_or(str::is_empty)
                || args.cpu.as_deref().is_none_or(str::is_empty)
                || args.memory_gib.is_none_or(|memory| memory == 0))
        {
            bail!("reference profile requires --machine-label, --cpu, and a non-zero --memory-gib");
        }

        Ok(Self {
            profile: args.profile,
            output: args.output.unwrap_or_else(|| {
                PathBuf::from("target/performance").join(args.profile.output_name())
            }),
            warmup,
            samples,
            machine_label: args.machine_label,
            cpu: args.cpu,
            memory_gib: args.memory_gib,
        })
    }
}

#[cfg(test)]
mod tests {
    use clap::Parser;

    use super::{Args, Profile, RunConfig};

    #[test]
    fn smoke_defaults_are_stable() {
        let config =
            RunConfig::from_args(Args::try_parse_from(["ingest_field_work", "--bench"]).unwrap())
                .unwrap();

        assert_eq!(config.profile, Profile::Smoke);
        assert_eq!(config.warmup, 1);
        assert_eq!(config.samples, 3);
        assert_eq!(
            config.output,
            std::path::PathBuf::from("target/performance/ingest-field-work-smoke.json")
        );
    }

    #[test]
    fn zero_measurements_and_anonymous_reference_runs_are_rejected() {
        let zero =
            Args::try_parse_from(["ingest_field_work", "--warmup", "0", "--samples", "1"]).unwrap();
        assert!(RunConfig::from_args(zero).is_err());

        let anonymous =
            Args::try_parse_from(["ingest_field_work", "--profile", "reference"]).unwrap();
        assert!(RunConfig::from_args(anonymous).is_err());
    }

    #[test]
    fn reference_metadata_and_overrides_are_retained() {
        let config = RunConfig::from_args(
            Args::try_parse_from([
                "ingest_field_work",
                "--profile",
                "reference",
                "--output",
                "/tmp/field-work.json",
                "--warmup",
                "4",
                "--samples",
                "30",
                "--machine-label",
                "stable-host",
                "--cpu",
                "Example CPU",
                "--memory-gib",
                "64",
            ])
            .unwrap(),
        )
        .unwrap();

        assert_eq!(config.profile, Profile::Reference);
        assert_eq!(
            config.output,
            std::path::PathBuf::from("/tmp/field-work.json")
        );
        assert_eq!(config.warmup, 4);
        assert_eq!(config.samples, 30);
        assert_eq!(config.machine_label.as_deref(), Some("stable-host"));
        assert_eq!(config.cpu.as_deref(), Some("Example CPU"));
        assert_eq!(config.memory_gib, Some(64));

        let parse_function: fn() -> anyhow::Result<RunConfig> = RunConfig::parse;
        std::hint::black_box(parse_function);
    }
}
