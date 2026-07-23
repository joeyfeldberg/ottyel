use std::{hint::black_box, time::Instant};

use anyhow::{Result, anyhow, ensure};
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use prost::Message;

use super::{
    IngestLimits,
    bench_config::RunConfig,
    fixtures::{DefaultPolicyOutcome, Fixture},
    policy::{PolicyError, ValidateOtlp},
    preflight::{PreflightError, PreflightOtlp},
    report::{Measurement, MeasurementOutcome, Phase},
    stats::Distribution,
};

pub(crate) struct RunResults {
    pub configured_default_work_unit_limit: usize,
    pub measurements: Vec<Measurement>,
}

pub(crate) fn run(fixtures: &[Fixture], config: &RunConfig) -> Result<RunResults> {
    let limits = IngestLimits::default();
    let mut measurements = Vec::with_capacity(fixtures.len() * Phase::ALL.len());
    for fixture in fixtures {
        let decoded = verify_pipeline(fixture, &limits)?;
        for phase in Phase::ALL {
            let latency = match phase {
                Phase::Preflight => measure(config, || {
                    execute_preflight(black_box(fixture.bytes.as_slice()), black_box(&limits))
                }),
                Phase::ProstDecode => measure(config, || {
                    ExportTraceServiceRequest::decode(black_box(fixture.bytes.as_slice()))
                        .expect("verified fixture must continue to decode")
                }),
                Phase::PostdecodeValidate => measure(config, || {
                    black_box(&decoded)
                        .validate(black_box(&limits))
                        .expect("verified decoded graph must continue to validate");
                }),
                Phase::FullPipeline => measure(config, || {
                    execute_full_pipeline(black_box(fixture.bytes.as_slice()), black_box(&limits))
                }),
            }?;
            let outcome = measurement_outcome(phase, fixture.metadata.expected_default_outcome);
            measurements.push(Measurement::new(&fixture.metadata, phase, outcome, latency));
        }
    }
    Ok(RunResults {
        configured_default_work_unit_limit: limits.max_work_units,
        measurements,
    })
}

fn measurement_outcome(phase: Phase, policy_outcome: DefaultPolicyOutcome) -> MeasurementOutcome {
    match phase {
        Phase::Preflight | Phase::FullPipeline => {
            MeasurementOutcome::for_policy_outcome(policy_outcome)
        }
        Phase::ProstDecode | Phase::PostdecodeValidate => MeasurementOutcome::Completed,
    }
}

fn verify_pipeline(fixture: &Fixture, limits: &IngestLimits) -> Result<ExportTraceServiceRequest> {
    if let Some(expected_work_units) = fixture.metadata.expected_work_units {
        let inferred_outcome = if expected_work_units > limits.max_work_units {
            DefaultPolicyOutcome::BudgetRejected
        } else {
            DefaultPolicyOutcome::Accepted
        };
        ensure!(
            inferred_outcome == fixture.metadata.expected_default_outcome,
            "{} declares {expected_work_units} work units and {:?}, inconsistent with the \
             configured default limit of {}",
            fixture.metadata.name,
            fixture.metadata.expected_default_outcome,
            limits.max_work_units
        );
    }

    let actual_outcome = classify_preflight_outcome(
        ExportTraceServiceRequest::preflight(&fixture.bytes, limits),
        limits.max_work_units,
    )
    .map_err(|error| {
        anyhow!(
            "{} produced an unexpected production preflight result during setup: {error}",
            fixture.metadata.name
        )
    })?;
    ensure!(
        actual_outcome == fixture.metadata.expected_default_outcome,
        "{} expected default preflight outcome {:?}, got {actual_outcome:?}",
        fixture.metadata.name,
        fixture.metadata.expected_default_outcome
    );

    let request = ExportTraceServiceRequest::decode(fixture.bytes.as_slice()).map_err(|error| {
        anyhow!(
            "{} failed Prost decode during setup: {error}",
            fixture.metadata.name
        )
    })?;
    request.validate(limits).map_err(|error| {
        anyhow!(
            "{} failed postdecode validation during setup: {error}",
            fixture.metadata.name
        )
    })?;
    Ok(request)
}

fn execute_preflight(bytes: &[u8], limits: &IngestLimits) -> DefaultPolicyOutcome {
    classify_preflight_outcome(
        ExportTraceServiceRequest::preflight(bytes, limits),
        limits.max_work_units,
    )
    .expect("verified preflight outcome must remain deterministic")
}

fn execute_full_pipeline(bytes: &[u8], limits: &IngestLimits) -> Option<ExportTraceServiceRequest> {
    match execute_preflight(bytes, limits) {
        DefaultPolicyOutcome::Accepted => {
            let request = ExportTraceServiceRequest::decode(bytes)
                .expect("verified fixture must continue to decode");
            request
                .validate(limits)
                .expect("verified decoded graph must continue to validate");
            Some(request)
        }
        DefaultPolicyOutcome::BudgetRejected => None,
    }
}

fn classify_preflight_outcome(
    result: std::result::Result<(), PreflightError>,
    expected_work_unit_limit: usize,
) -> Result<DefaultPolicyOutcome> {
    match result {
        Ok(()) => Ok(DefaultPolicyOutcome::Accepted),
        Err(PreflightError::Budget(PolicyError::Budget {
            budget: "protobuf work unit",
            limit,
        })) if limit == expected_work_unit_limit => Ok(DefaultPolicyOutcome::BudgetRejected),
        Err(error) => Err(anyhow!(
            "expected acceptance or protobuf work unit budget {expected_work_unit_limit}, got \
             {error:?}"
        )),
    }
}

fn measure<T>(config: &RunConfig, mut operation: impl FnMut() -> T) -> Result<Distribution> {
    for _ in 0..config.warmup {
        black_box(operation());
    }

    let mut samples = Vec::with_capacity(config.samples);
    for _ in 0..config.samples {
        let started = Instant::now();
        let result = operation();
        let elapsed = started.elapsed();
        black_box(result);
        samples.push(elapsed.as_nanos().min(u128::from(u64::MAX)) as u64);
    }
    Distribution::from_samples(samples)
}

#[cfg(test)]
#[allow(unused_imports)]
mod tests {
    use super::{
        DefaultPolicyOutcome, ExportTraceServiceRequest, IngestLimits, PolicyError, PreflightError,
        PreflightOtlp, classify_preflight_outcome, execute_full_pipeline, measurement_outcome,
    };
    use crate::fixtures::build_all;
    use crate::report::{MeasurementOutcome, Phase};
    use prost::Message;

    #[test]
    fn default_fixture_outcomes_match_production_preflight_exactly() {
        let limits = IngestLimits::default();
        let fixtures = build_all().unwrap();

        for fixture in fixtures {
            let outcome = classify_preflight_outcome(
                ExportTraceServiceRequest::preflight(&fixture.bytes, &limits),
                limits.max_work_units,
            )
            .unwrap();
            assert_eq!(
                outcome, fixture.metadata.expected_default_outcome,
                "{}",
                fixture.metadata.name
            );
        }
    }

    #[test]
    fn only_the_exact_default_work_budget_error_is_classified_as_rejection() {
        assert_eq!(
            classify_preflight_outcome(
                Err(PreflightError::Budget(PolicyError::Budget {
                    budget: "protobuf work unit",
                    limit: 2_000_000,
                })),
                2_000_000,
            )
            .unwrap(),
            DefaultPolicyOutcome::BudgetRejected
        );
        assert!(
            classify_preflight_outcome(
                Err(PreflightError::Budget(PolicyError::Budget {
                    budget: "protobuf work unit",
                    limit: 7,
                })),
                2_000_000,
            )
            .is_err()
        );
        assert!(
            classify_preflight_outcome(
                Err(PreflightError::Budget(PolicyError::Budget {
                    budget: "structure",
                    limit: 2_000_000,
                })),
                2_000_000,
            )
            .is_err()
        );
    }

    #[test]
    fn phase_outcome_only_reports_policy_rejection_for_policy_phases() {
        assert_eq!(
            measurement_outcome(Phase::Preflight, DefaultPolicyOutcome::Accepted),
            MeasurementOutcome::Completed
        );
        assert_eq!(
            measurement_outcome(Phase::FullPipeline, DefaultPolicyOutcome::BudgetRejected),
            MeasurementOutcome::BudgetRejected
        );
        assert_eq!(
            measurement_outcome(Phase::ProstDecode, DefaultPolicyOutcome::BudgetRejected),
            MeasurementOutcome::Completed
        );
        assert_eq!(
            measurement_outcome(
                Phase::PostdecodeValidate,
                DefaultPolicyOutcome::BudgetRejected
            ),
            MeasurementOutcome::Completed
        );
    }

    #[test]
    fn rejected_full_pipeline_stops_before_prost_decode() {
        let limits = IngestLimits::default();
        let mut bytes = Vec::with_capacity(limits.max_work_units * 2 + 3);
        for _ in 0..=limits.max_work_units {
            bytes.extend_from_slice(&[0x10, 0x00]);
        }
        bytes.push(0xff);

        assert!(ExportTraceServiceRequest::decode(bytes.as_slice()).is_err());
        assert!(execute_full_pipeline(&bytes, &limits).is_none());
    }
}
