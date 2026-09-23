use std::{array, hint::black_box, path::Path, time::Instant};

use anyhow::{Result, ensure};
use futures::future::join_all;
use ottyel::store::{
    Store,
    benchmark_support::{PreparedTraceExport, WriteReceipt},
};
use rusqlite::{Connection, OpenFlags};

use crate::{
    bench_config::RunConfig,
    counters::{snapshot_delta, validate_candidate, validate_persisted_row_count},
    data::{SeededStore, acknowledgement_request},
    measurement::{
        BurstMeasurement, CounterTotals, LowRateMeasurement, Measurements, RateDistribution,
    },
    stats::Distribution,
};

pub(crate) const EXPORTS_PER_BURST: usize = 4;
pub(crate) const RECORDS_PER_EXPORT: usize = 250;
pub(crate) const RECORDS_PER_BURST: usize = EXPORTS_PER_BURST * RECORDS_PER_EXPORT;
const BURST_BATCH_INDEX_BASE: usize = 1_000_000;
const LOW_RATE_BATCH_INDEX_BASE: usize = 2_000_000;

struct BurstSample {
    makespan_ns: u64,
    admission_ack_ns: Vec<u64>,
    release_ack_ns: Vec<u64>,
    records_per_second: f64,
    exports_per_second: f64,
    counters: CounterTotals,
}

struct LowRateSample {
    acknowledgement_ns: u64,
    counters: CounterTotals,
}

pub(crate) fn run(seeded: &SeededStore, config: &RunConfig) -> Result<Measurements> {
    let sampling = config.profile.sampling();
    let initial_span_rows = span_count(&seeded.database_path)?;
    let mut expected_span_rows = initial_span_rows;
    let mut next_burst_batch = BURST_BATCH_INDEX_BASE;

    for _ in 0..sampling.burst_warmup {
        expected_span_rows += RECORDS_PER_BURST as u64;
        run_burst(
            &seeded.store,
            &seeded.database_path,
            &mut next_burst_batch,
            expected_span_rows,
        )?;
    }

    let mut makespans = Vec::with_capacity(sampling.burst_samples);
    let mut admission_acknowledgements =
        Vec::with_capacity(sampling.burst_samples * EXPORTS_PER_BURST);
    let mut release_acknowledgements =
        Vec::with_capacity(sampling.burst_samples * EXPORTS_PER_BURST);
    let mut record_rates = Vec::with_capacity(sampling.burst_samples);
    let mut export_rates = Vec::with_capacity(sampling.burst_samples);
    let mut retention_per_burst = Vec::with_capacity(sampling.burst_samples);
    let mut burst_counters = CounterTotals::default();

    for _ in 0..sampling.burst_samples {
        expected_span_rows += RECORDS_PER_BURST as u64;
        let sample = run_burst(
            &seeded.store,
            &seeded.database_path,
            &mut next_burst_batch,
            expected_span_rows,
        )?;
        makespans.push(sample.makespan_ns);
        admission_acknowledgements.extend(sample.admission_ack_ns);
        release_acknowledgements.extend(sample.release_ack_ns);
        record_rates.push(sample.records_per_second);
        export_rates.push(sample.exports_per_second);
        retention_per_burst.push(sample.counters.retention_elapsed_ns);
        burst_counters.add(&sample.counters);
    }

    let mut next_low_rate_batch = LOW_RATE_BATCH_INDEX_BASE;
    for _ in 0..sampling.low_rate_warmup {
        expected_span_rows += 1;
        run_low_rate(
            &seeded.store,
            &seeded.database_path,
            &mut next_low_rate_batch,
            expected_span_rows,
        )?;
    }

    let mut low_rate_acknowledgements = Vec::with_capacity(sampling.low_rate_samples);
    let mut low_rate_retention = Vec::with_capacity(sampling.low_rate_samples);
    let mut low_rate_counters = CounterTotals::default();
    for _ in 0..sampling.low_rate_samples {
        expected_span_rows += 1;
        let sample = run_low_rate(
            &seeded.store,
            &seeded.database_path,
            &mut next_low_rate_batch,
            expected_span_rows,
        )?;
        low_rate_acknowledgements.push(sample.acknowledgement_ns);
        low_rate_retention.push(sample.counters.retention_elapsed_ns);
        low_rate_counters.add(&sample.counters);
    }

    let final_span_rows = span_count(&seeded.database_path)?;
    validate_persisted_row_count(final_span_rows, expected_span_rows, "final benchmark")?;

    Ok(Measurements {
        structural_assertions_passed: true,
        initial_span_rows,
        final_span_rows,
        burst: BurstMeasurement {
            makespan: Distribution::from_samples(makespans)?,
            submission_attempt_to_completion_ack: Distribution::from_samples(
                admission_acknowledgements,
            )?,
            release_to_completion_ack: Distribution::from_samples(release_acknowledgements)?,
            records_per_second: RateDistribution::from_samples(record_rates)?,
            exports_per_second: RateDistribution::from_samples(export_rates)?,
            retention_elapsed_per_burst: Distribution::from_samples(retention_per_burst)?,
            counters: burst_counters,
        },
        low_rate: LowRateMeasurement {
            submission_attempt_to_completion_ack: Distribution::from_samples(
                low_rate_acknowledgements,
            )?,
            retention_elapsed_per_export: Distribution::from_samples(low_rate_retention)?,
            counters: low_rate_counters,
        },
    })
}

fn run_burst(
    store: &Store,
    database_path: &Path,
    next_batch: &mut usize,
    expected_span_rows: u64,
) -> Result<BurstSample> {
    let prepared: [PreparedTraceExport; EXPORTS_PER_BURST] = array::from_fn(|_| {
        let batch = *next_batch;
        *next_batch += 1;
        Store::prepare_trace_export_for_benchmark(acknowledgement_request(
            RECORDS_PER_EXPORT,
            batch,
        ))
    });
    let parked = store.park_writer_for_benchmark()?;
    let before = store.writer_benchmark_snapshot()?;
    let mut admitted = Vec::with_capacity(EXPORTS_PER_BURST);
    for request in prepared {
        let started = Instant::now();
        admitted.push((started, store.try_ingest_traces_for_benchmark(request)?));
    }

    let released = Instant::now();
    parked.release()?;
    let completions = futures::executor::block_on(join_all(
        admitted
            .into_iter()
            .map(|(admitted_at, receipt)| wait_for_receipt(receipt, admitted_at, released)),
    ));
    let makespan = released.elapsed();

    let mut admission_ack_ns = Vec::with_capacity(EXPORTS_PER_BURST);
    let mut release_ack_ns = Vec::with_capacity(EXPORTS_PER_BURST);
    for completion in completions {
        let (accepted, admission_elapsed, release_elapsed) = completion?;
        ensure!(
            accepted == RECORDS_PER_EXPORT,
            "burst export acknowledged {accepted} records, expected {RECORDS_PER_EXPORT}"
        );
        black_box(accepted);
        admission_ack_ns.push(duration_nanos(admission_elapsed));
        release_ack_ns.push(duration_nanos(release_elapsed));
    }

    let after = store.writer_benchmark_snapshot()?;
    let counters = snapshot_delta(before, after)?;
    validate_candidate(&counters, EXPORTS_PER_BURST as u64)?;
    validate_persisted_row_count(span_count(database_path)?, expected_span_rows, "burst")?;
    let seconds = makespan.as_secs_f64().max(f64::MIN_POSITIVE);
    Ok(BurstSample {
        makespan_ns: duration_nanos(makespan),
        admission_ack_ns,
        release_ack_ns,
        records_per_second: RECORDS_PER_BURST as f64 / seconds,
        exports_per_second: EXPORTS_PER_BURST as f64 / seconds,
        counters,
    })
}

async fn wait_for_receipt(
    receipt: WriteReceipt<usize>,
    admitted_at: Instant,
    released: Instant,
) -> Result<(usize, std::time::Duration, std::time::Duration)> {
    let accepted = receipt.wait().await?;
    Ok((accepted, admitted_at.elapsed(), released.elapsed()))
}

fn run_low_rate(
    store: &Store,
    database_path: &Path,
    next_batch: &mut usize,
    expected_span_rows: u64,
) -> Result<LowRateSample> {
    let request =
        Store::prepare_trace_export_for_benchmark(acknowledgement_request(1, *next_batch));
    *next_batch += 1;
    let before = store.writer_benchmark_snapshot()?;
    let started = Instant::now();
    let receipt = store.try_ingest_traces_for_benchmark(request)?;
    let accepted = futures::executor::block_on(receipt.wait())?;
    let elapsed = started.elapsed();
    ensure!(
        accepted == 1,
        "low-rate export acknowledged {accepted} records"
    );
    black_box(accepted);

    let after = store.writer_benchmark_snapshot()?;
    let counters = snapshot_delta(before, after)?;
    validate_candidate(&counters, 1)?;
    validate_persisted_row_count(
        span_count(database_path)?,
        expected_span_rows,
        "low-rate export",
    )?;
    Ok(LowRateSample {
        acknowledgement_ns: duration_nanos(elapsed),
        counters,
    })
}

fn span_count(path: &Path) -> Result<u64> {
    let connection = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY)?;
    let count: i64 = connection.query_row("SELECT COUNT(*) FROM spans", [], |row| row.get(0))?;
    u64::try_from(count).map_err(Into::into)
}

fn duration_nanos(duration: std::time::Duration) -> u64 {
    duration.as_nanos().min(u128::from(u64::MAX)) as u64
}
