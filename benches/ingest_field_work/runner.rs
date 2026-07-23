use std::{hint::black_box, time::Instant};

use anyhow::{Result, anyhow};
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use prost::Message;

use super::{
    IngestLimits,
    bench_config::RunConfig,
    fixtures::Fixture,
    policy::ValidateOtlp,
    preflight::PreflightOtlp,
    report::{Measurement, Phase},
    stats::Distribution,
};

pub(crate) fn run(fixtures: &[Fixture], config: &RunConfig) -> Result<Vec<Measurement>> {
    let limits = IngestLimits::default();
    let mut measurements = Vec::with_capacity(fixtures.len() * Phase::ALL.len());
    for fixture in fixtures {
        let decoded = verify_pipeline(fixture, &limits)?;
        for phase in Phase::ALL {
            let latency = match phase {
                Phase::Preflight => measure(config, || {
                    let result = ExportTraceServiceRequest::preflight(
                        black_box(fixture.bytes.as_slice()),
                        black_box(&limits),
                    );
                    black_box(result).expect("verified preflight must remain deterministic");
                })?,
                Phase::ProstDecode => measure(config, || {
                    ExportTraceServiceRequest::decode(black_box(fixture.bytes.as_slice()))
                        .expect("verified fixture must continue to decode")
                })?,
                Phase::PostdecodeValidate => measure(config, || {
                    black_box(&decoded)
                        .validate(black_box(&limits))
                        .expect("verified decoded graph must continue to validate");
                })?,
                Phase::FullPipeline => measure(config, || {
                    ExportTraceServiceRequest::preflight(
                        black_box(fixture.bytes.as_slice()),
                        black_box(&limits),
                    )
                    .expect("verified preflight must remain deterministic");
                    let request =
                        ExportTraceServiceRequest::decode(black_box(fixture.bytes.as_slice()))
                            .expect("verified fixture must continue to decode");
                    request
                        .validate(black_box(&limits))
                        .expect("verified decoded graph must continue to validate");
                    request
                })?,
            };
            measurements.push(Measurement::new(&fixture.metadata, phase, latency));
        }
    }
    Ok(measurements)
}

fn verify_pipeline(fixture: &Fixture, limits: &IngestLimits) -> Result<ExportTraceServiceRequest> {
    ExportTraceServiceRequest::preflight(&fixture.bytes, limits).map_err(|error| {
        anyhow!(
            "{} failed production preflight during setup: {error}",
            fixture.metadata.name
        )
    })?;
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
