use std::path::PathBuf;

use anyhow::{Result, bail};
use clap::{Parser, ValueEnum};
use serde::Serialize;

#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize, ValueEnum)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Profile {
    Smoke,
    Reference,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize)]
pub(crate) struct Scale {
    pub trace_spans: usize,
    pub logs: usize,
    pub metric_points: usize,
    pub large_trace_spans: usize,
    pub ai_operations: usize,
    pub acknowledgement_spans: usize,
    pub trace_batch_size: usize,
    pub log_batch_size: usize,
    pub metric_batch_size: usize,
    pub ai_batch_size: usize,
}

impl Profile {
    pub(crate) fn scale(self) -> Scale {
        match self {
            Self::Smoke => Scale {
                trace_spans: 2_000,
                logs: 5_000,
                metric_points: 5_000,
                large_trace_spans: 500,
                ai_operations: 1_000,
                acknowledgement_spans: 1_000,
                trace_batch_size: 1_000,
                log_batch_size: 2_500,
                metric_batch_size: 2_500,
                ai_batch_size: 1_000,
            },
            Self::Reference => Scale {
                trace_spans: 100_000,
                logs: 1_000_000,
                metric_points: 1_000_000,
                large_trace_spans: 10_000,
                ai_operations: 100_000,
                acknowledgement_spans: 1_000,
                trace_batch_size: 5_000,
                log_batch_size: 10_000,
                metric_batch_size: 20_000,
                ai_batch_size: 2_000,
            },
        }
    }

    fn defaults(self) -> (usize, usize) {
        match self {
            Self::Smoke => (2, 5),
            Self::Reference => (3, 20),
        }
    }
}

#[derive(Debug, Parser)]
#[command(about = "Deterministic Ottyel store performance baseline")]
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
        let scale = args.profile.scale();
        validate_scale(args.profile, scale)?;
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
                PathBuf::from(format!(
                    "target/performance/{}.json",
                    match args.profile {
                        Profile::Smoke => "smoke",
                        Profile::Reference => "reference",
                    }
                ))
            }),
            warmup,
            samples,
            machine_label: args.machine_label,
            cpu: args.cpu,
            memory_gib: args.memory_gib,
        })
    }
}

fn validate_scale(profile: Profile, scale: Scale) -> Result<()> {
    let values = [
        scale.trace_spans,
        scale.logs,
        scale.metric_points,
        scale.large_trace_spans,
        scale.ai_operations,
        scale.acknowledgement_spans,
        scale.trace_batch_size,
        scale.log_batch_size,
        scale.metric_batch_size,
        scale.ai_batch_size,
    ];
    if values.contains(&0) {
        bail!("all profile scale and batch values must be greater than zero");
    }
    if scale.trace_batch_size > scale.trace_spans
        || scale.log_batch_size > scale.logs
        || scale.metric_batch_size > scale.metric_points
        || scale.ai_batch_size > scale.ai_operations
    {
        bail!("profile batch size cannot exceed its dataset size");
    }
    if profile == Profile::Reference
        && (scale.trace_spans < 100_000
            || scale.logs < 1_000_000
            || scale.metric_points < 1_000_000
            || scale.large_trace_spans < 10_000
            || scale.ai_operations < 100_000
            || scale.acknowledgement_spans != 1_000)
    {
        bail!("reference profile is below the required baseline scale");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    #[test]
    fn reference_profile_encodes_required_scale() {
        let scale = super::Profile::Reference.scale();

        assert_eq!(scale.trace_spans, 100_000);
        assert_eq!(scale.logs, 1_000_000);
        assert_eq!(scale.metric_points, 1_000_000);
        assert_eq!(scale.large_trace_spans, 10_000);
        assert_eq!(scale.ai_operations, 100_000);
        assert_eq!(scale.acknowledgement_spans, 1_000);
        super::validate_scale(super::Profile::Reference, scale).unwrap();
    }

    #[test]
    fn arguments_apply_profile_defaults_and_overrides() {
        let default = super::RunConfig::from_args(
            <super::Args as clap::Parser>::try_parse_from([
                "store_baseline",
                "--profile",
                "smoke",
                "--bench",
            ])
            .unwrap(),
        )
        .unwrap();
        assert_eq!(default.profile, super::Profile::Smoke);
        assert_eq!(
            default.output,
            std::path::PathBuf::from("target/performance/smoke.json")
        );
        assert_eq!(default.warmup, 2);
        assert_eq!(default.samples, 5);
        assert_eq!(default.machine_label, None);
        assert_eq!(default.cpu, None);
        assert_eq!(default.memory_gib, None);

        let overridden = super::RunConfig::from_args(
            <super::Args as clap::Parser>::try_parse_from([
                "store_baseline",
                "--profile",
                "reference",
                "--output",
                "/tmp/reference.json",
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
        assert_eq!(overridden.profile, super::Profile::Reference);
        assert_eq!(
            overridden.output,
            std::path::PathBuf::from("/tmp/reference.json")
        );
        assert_eq!(overridden.warmup, 4);
        assert_eq!(overridden.samples, 30);
        assert_eq!(overridden.machine_label.as_deref(), Some("stable-host"));
        assert_eq!(overridden.cpu.as_deref(), Some("Example CPU"));
        assert_eq!(overridden.memory_gib, Some(64));

        let parse_function: fn() -> anyhow::Result<super::RunConfig> = super::RunConfig::parse;
        std::hint::black_box(parse_function);
    }

    #[test]
    fn zero_measurement_counts_are_rejected() {
        let args = <super::Args as clap::Parser>::try_parse_from([
            "store_baseline",
            "--warmup",
            "0",
            "--samples",
            "1",
        ])
        .unwrap();
        assert!(super::RunConfig::from_args(args).is_err());
    }

    #[test]
    fn anonymous_reference_run_is_rejected() {
        let args = <super::Args as clap::Parser>::try_parse_from([
            "store_baseline",
            "--profile",
            "reference",
        ])
        .unwrap();
        assert!(super::RunConfig::from_args(args).is_err());
    }
}
