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

#[derive(Debug, Clone, Copy, Eq, PartialEq, Serialize)]
pub(crate) struct Sampling {
    pub burst_warmup: usize,
    pub burst_samples: usize,
    pub low_rate_warmup: usize,
    pub low_rate_samples: usize,
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

    pub(crate) fn sampling(self) -> Sampling {
        match self {
            Self::Smoke => Sampling {
                burst_warmup: 2,
                burst_samples: 5,
                low_rate_warmup: 2,
                low_rate_samples: 20,
            },
            Self::Reference => Sampling {
                burst_warmup: 3,
                burst_samples: 20,
                low_rate_warmup: 3,
                low_rate_samples: 100,
            },
        }
    }
}

#[derive(Debug, Parser)]
#[command(about = "Deterministic Ottyel adjacent-writer benchmark")]
struct Args {
    #[arg(long = "bench", hide = true)]
    _bench: bool,

    #[arg(long, value_enum, default_value_t = Profile::Smoke)]
    profile: Profile,

    #[arg(long)]
    output: Option<PathBuf>,

    #[arg(long)]
    machine_label: Option<String>,

    #[arg(long)]
    cpu: Option<String>,

    #[arg(long)]
    memory_gib: Option<u64>,

    #[arg(long)]
    storage_label: Option<String>,
}

#[derive(Debug)]
pub(crate) struct RunConfig {
    pub profile: Profile,
    pub output: PathBuf,
    pub machine_label: Option<String>,
    pub cpu: Option<String>,
    pub memory_gib: Option<u64>,
    pub storage_label: Option<String>,
}

impl RunConfig {
    pub(crate) fn parse() -> Result<Self> {
        Self::from_args(Args::parse())
    }

    fn from_args(args: Args) -> Result<Self> {
        let machine_label = normalized_label(args.machine_label, "--machine-label")?;
        let cpu = normalized_label(args.cpu, "--cpu")?;
        let storage_label = normalized_label(args.storage_label, "--storage-label")?;
        if args.profile == Profile::Reference
            && (machine_label.is_none()
                || cpu.is_none()
                || args.memory_gib.is_none_or(|memory| memory == 0)
                || storage_label.is_none())
        {
            bail!(
                "reference profile requires --machine-label, --cpu, non-zero --memory-gib, and \
                 --storage-label"
            );
        }

        Ok(Self {
            profile: args.profile,
            output: args.output.unwrap_or_else(|| {
                PathBuf::from(format!(
                    "target/performance/writer-coalescing-{}.json",
                    match args.profile {
                        Profile::Smoke => "smoke",
                        Profile::Reference => "reference",
                    }
                ))
            }),
            machine_label,
            cpu,
            memory_gib: args.memory_gib,
            storage_label,
        })
    }
}

fn normalized_label(value: Option<String>, flag: &str) -> Result<Option<String>> {
    value
        .map(|value| {
            let value = value.trim();
            if value.is_empty() {
                bail!("{flag} cannot be empty or whitespace");
            }
            Ok(value.to_string())
        })
        .transpose()
}

#[cfg(test)]
mod tests {
    #[test]
    fn profiles_pin_the_predeclared_sampling_contract() {
        assert_eq!(
            super::Profile::Reference.sampling(),
            super::Sampling {
                burst_warmup: 3,
                burst_samples: 20,
                low_rate_warmup: 3,
                low_rate_samples: 100,
            }
        );
        assert_eq!(super::Profile::Reference.scale().trace_spans, 100_000);
        assert_eq!(super::Profile::Reference.scale().logs, 1_000_000);
        assert_eq!(super::Profile::Reference.scale().metric_points, 1_000_000);
    }

    #[test]
    fn reference_profile_requires_machine_identity() {
        let args = <super::Args as clap::Parser>::try_parse_from([
            "writer_coalescing",
            "--profile",
            "reference",
        ])
        .unwrap();
        assert!(super::RunConfig::from_args(args).is_err());
    }

    #[test]
    fn reference_profile_requires_a_nonempty_storage_label() {
        let args = <super::Args as clap::Parser>::try_parse_from([
            "writer_coalescing",
            "--profile",
            "reference",
            "--machine-label",
            "stable-host",
            "--cpu",
            "Example CPU",
            "--memory-gib",
            "32",
        ])
        .unwrap();
        assert!(super::RunConfig::from_args(args).is_err());

        let args = <super::Args as clap::Parser>::try_parse_from([
            "writer_coalescing",
            "--profile",
            "reference",
            "--machine-label",
            "stable-host",
            "--cpu",
            "Example CPU",
            "--memory-gib",
            "32",
            "--storage-label",
            "internal-nvme",
        ])
        .unwrap();
        assert_eq!(
            super::RunConfig::from_args(args)
                .unwrap()
                .storage_label
                .as_deref(),
            Some("internal-nvme")
        );
    }

    #[test]
    fn smoke_uses_the_dedicated_default_report_path() {
        let args = <super::Args as clap::Parser>::try_parse_from(["writer_coalescing"]).unwrap();
        let config = super::RunConfig::from_args(args).unwrap();
        assert_eq!(
            config.output,
            std::path::PathBuf::from("target/performance/writer-coalescing-smoke.json")
        );
    }

    #[test]
    fn identity_labels_are_trimmed() {
        let args = <super::Args as clap::Parser>::try_parse_from([
            "writer_coalescing",
            "--machine-label",
            " stable-host ",
            "--cpu",
            "\tExample CPU\n",
            "--storage-label",
            " internal-nvme ",
        ])
        .unwrap();
        let config = super::RunConfig::from_args(args).unwrap();
        assert_eq!(config.machine_label.as_deref(), Some("stable-host"));
        assert_eq!(config.cpu.as_deref(), Some("Example CPU"));
        assert_eq!(config.storage_label.as_deref(), Some("internal-nvme"));
    }

    #[test]
    fn whitespace_only_identity_labels_are_rejected() {
        for flag in ["--machine-label", "--cpu", "--storage-label"] {
            let args = <super::Args as clap::Parser>::try_parse_from([
                "writer_coalescing",
                flag,
                " \t\n ",
            ])
            .unwrap();
            assert!(super::RunConfig::from_args(args).is_err(), "{flag}");
        }
    }
}
