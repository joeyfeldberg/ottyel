use std::{hint::black_box, time::Instant};

use anyhow::{Result, ensure};
use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use ottyel::query::{PageRequest, QueryFilters};

use super::{
    config::RunConfig,
    data::{AI_SERVICE, SEARCH_MARKER, SERVICE, SeededStore, acknowledgement_request},
    report::Measurement,
    stats::Distribution,
};

pub(crate) fn run(seeded: &SeededStore, config: &RunConfig) -> Result<Vec<Measurement>> {
    let scale = config.profile.scale();
    let default_filters = QueryFilters::default();
    let service_filters = QueryFilters {
        service: Some(SERVICE.to_string()),
        ..QueryFilters::default()
    };
    let ai_filters = QueryFilters {
        service: Some(AI_SERVICE.to_string()),
        ..QueryFilters::default()
    };
    let search_filters = QueryFilters {
        service: Some(SERVICE.to_string()),
        search_query: Some(SEARCH_MARKER.to_string()),
        ..QueryFilters::default()
    };

    let mut acknowledgement_batch = 0;
    let measurements = vec![
        measure(
            config,
            Scenario {
                name: "ingest_acknowledgement_1000_spans",
                description: "Store::ingest_traces acknowledgement for a prebuilt, previously unseen 1,000-span batch, including commit and current retention enforcement",
                operations_per_sample: scale.acknowledgement_spans,
                expected_result_count: scale.acknowledgement_spans,
            },
            || {
                let request =
                    acknowledgement_request(scale.acknowledgement_spans, acknowledgement_batch);
                acknowledgement_batch += 1;
                request
            },
            |request| seeded.store.ingest_traces(request),
            |accepted| Ok(*accepted),
        )?,
        measure(
            config,
            Scenario {
                name: "dashboard_snapshot_all_tabs",
                description: "QueryService::snapshot with default filters, including counts, all four first pages, and current AI aggregate queries",
                operations_per_sample: 1,
                expected_result_count: 200,
            },
            || default_filters.clone(),
            |filters| seeded.query.snapshot(&filters),
            |snapshot| {
                ensure!(!snapshot.services.is_empty(), "snapshot services are empty");
                ensure!(
                    snapshot.overview.trace_count > 0,
                    "snapshot trace count is empty"
                );
                ensure!(
                    snapshot.overview.log_count > 0,
                    "snapshot log count is empty"
                );
                ensure!(
                    snapshot.overview.metric_count > 0,
                    "snapshot metric count is empty"
                );
                ensure!(
                    snapshot.overview.llm_count > 0,
                    "snapshot AI count is empty"
                );
                Ok(snapshot.traces.len()
                    + snapshot.logs.len()
                    + snapshot.metrics.len()
                    + snapshot.llm.len())
            },
        )?,
        measure(
            config,
            Scenario {
                name: "first_trace_page",
                description: "first 50 trace summaries for one service through QueryService::traces_page",
                operations_per_sample: 1,
                expected_result_count: 50,
            },
            || (service_filters.clone(), PageRequest::first(50)),
            |(filters, page)| seeded.query.traces_page(&filters, &page),
            |page| Ok(page.items.len()),
        )?,
        measure(
            config,
            Scenario {
                name: "first_log_page",
                description: "first 50 logs for one service through QueryService::logs_page",
                operations_per_sample: 1,
                expected_result_count: 50,
            },
            || (service_filters.clone(), PageRequest::first(50)),
            |(filters, page)| seeded.query.logs_page(&filters, &page),
            |page| Ok(page.items.len()),
        )?,
        measure(
            config,
            Scenario {
                name: "first_metric_page",
                description: "first 50 metric points for one service through QueryService::metrics_page",
                operations_per_sample: 1,
                expected_result_count: 50,
            },
            || (service_filters.clone(), PageRequest::first(50)),
            |(filters, page)| seeded.query.metrics_page(&filters, &page),
            |page| Ok(page.items.len()),
        )?,
        measure(
            config,
            Scenario {
                name: "first_ai_page",
                description: "first 50 normalized AI operations for one service through QueryService::llm_page",
                operations_per_sample: 1,
                expected_result_count: 50,
            },
            || (ai_filters.clone(), PageRequest::first(50)),
            |(filters, page)| seeded.query.llm_page(&filters, &page),
            |page| Ok(page.items.len()),
        )?,
        measure(
            config,
            Scenario {
                name: "large_trace_detail",
                description: "QueryService::trace_detail for the profile's single large trace, including event and link lookups",
                operations_per_sample: 1,
                expected_result_count: scale.large_trace_spans,
            },
            || seeded.large_trace_id.clone(),
            |trace_id| seeded.query.trace_detail(&trace_id),
            |spans| Ok(spans.len()),
        )?,
        measure(
            config,
            Scenario {
                name: "trace_text_search",
                description: "first trace page using the current case-insensitive LIKE search across IDs, names, and JSON attributes",
                operations_per_sample: 1,
                expected_result_count: 1,
            },
            || (search_filters.clone(), PageRequest::first(50)),
            |(filters, page)| seeded.query.traces_page(&filters, &page),
            |page| Ok(page.items.len()),
        )?,
        measure(
            config,
            Scenario {
                name: "empty_log_export_with_retention",
                description: "Store::ingest_logs acknowledgement for an empty request, including an empty transaction and all current retention scans",
                operations_per_sample: 1,
                expected_result_count: 0,
            },
            ExportLogsServiceRequest::default,
            |request| seeded.store.ingest_logs(request),
            |accepted| Ok(*accepted),
        )?,
    ];
    Ok(measurements)
}

#[derive(Clone, Copy)]
struct Scenario {
    name: &'static str,
    description: &'static str,
    operations_per_sample: usize,
    expected_result_count: usize,
}

fn measure<Input, Output>(
    config: &RunConfig,
    scenario: Scenario,
    mut prepare: impl FnMut() -> Input,
    mut execute: impl FnMut(Input) -> Result<Output>,
    result_count: impl Fn(&Output) -> Result<usize>,
) -> Result<Measurement> {
    for _ in 0..config.warmup {
        let input = prepare();
        let output = execute(input)?;
        let current_result_count = result_count(&output)?;
        ensure!(
            current_result_count == scenario.expected_result_count,
            "{} returned {current_result_count} results, expected {}",
            scenario.name,
            scenario.expected_result_count
        );
        black_box(output);
    }

    let mut samples = Vec::with_capacity(config.samples);
    let mut stable_result_count = None;
    for _ in 0..config.samples {
        let input = prepare();
        let started = Instant::now();
        let output = execute(input)?;
        let elapsed = started.elapsed();
        let current_result_count = result_count(&output)?;
        ensure!(
            current_result_count == scenario.expected_result_count,
            "{} returned {current_result_count} results, expected {}",
            scenario.name,
            scenario.expected_result_count
        );
        if let Some(expected) = stable_result_count {
            ensure!(
                current_result_count == expected,
                "{} returned an unstable result count: {current_result_count} != {expected}",
                scenario.name
            );
        } else {
            stable_result_count = Some(current_result_count);
        }
        black_box(output);
        samples.push(super::report::duration_nanos(elapsed));
    }

    let latency = Distribution::from_samples(samples)?;
    let p50_operations_per_second =
        scenario.operations_per_sample as f64 * 1_000_000_000.0 / latency.p50_ns.max(1) as f64;
    Ok(Measurement {
        name: scenario.name,
        description: scenario.description,
        operations_per_sample: scenario.operations_per_sample,
        result_count: stable_result_count.unwrap_or_default(),
        latency,
        p50_operations_per_second,
    })
}
