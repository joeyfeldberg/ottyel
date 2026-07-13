# Performance Baseline

Ottyel's store benchmark is a release-mode diagnostic harness, not a product claim.
It records latency distributions for the current public `Store` and `QueryService`
behavior and writes a stable JSON report for before/after comparisons.

The implementation is under `benches/store_baseline.rs` and `benches/support/`.
Run it with:

```sh
cargo bench --bench store_baseline -- --profile smoke \
  --output target/performance/smoke.json
```

The non-CI reference profile is intentionally much larger and must not run in normal
pull-request CI:

```sh
cargo bench --bench store_baseline -- --profile reference \
  --machine-label "replace-with-stable-machine-name" \
  --cpu "replace-with-exact-cpu-model" \
  --memory-gib 32 \
  --output target/performance/reference.json
```

Do not use a smoke result to claim that a provisional performance budget passes. Smoke
exists to catch broken generators, queries, output, and extreme regressions quickly.

## Profiles

Both profiles generate the same deterministic shape with fixed IDs, names, attributes,
and synthetic timestamps. Records are sent through the public OTLP projection methods,
not inserted with benchmark-only SQL. The fixed future timestamp keeps every record in
the active 24-hour view and makes retention deletion a no-op while retaining its scan
cost.

| Input | Smoke | Reference | Reference batch size |
| --- | ---: | ---: | ---: |
| Ordinary trace spans | 2,000 | 100,000 | 5,000 |
| Logs | 5,000 | 1,000,000 | 10,000 |
| Metric points | 5,000 | 1,000,000 | 20,000 |
| Spans in one large trace | 500 | 10,000 | 5,000 |
| AI operations represented as spans | 1,000 | 100,000 | 2,000 |
| New spans per acknowledgement sample | 1,000 | 1,000 | 1,000 |

The reference setup therefore contains 210,000 span rows before timed scenarios. Each
acknowledgement warmup and sample prepares a distinct 1,000-span batch outside the timed
interval, and the store capacity reserves space for every one of them so max-span
retention cannot trim during this scenario. With the default three warmups and 20
samples, the reference database ends with 233,000 spans. Seed batches are constructed
and dropped one at a time, so the generator does not allocate the full reference dataset
in memory. Setup time includes temporary database creation, schema initialization, every
seed export acknowledgement, and the retention work performed after each export.

Smoke defaults to two warmups and five measured samples per scenario. Reference defaults
to three warmups and 20 samples. Override them only when the changed sampling policy is
recorded with the result:

```sh
cargo bench --bench store_baseline -- --profile smoke --warmup 3 --samples 10
```

## Measured Scenarios

Every scenario prepares or clones its input before starting the timer, consumes its
result with `std::hint::black_box`, verifies that result counts are stable, and reports
`count`, `min_ns`, `p50_ns`, `p95_ns`, `p99_ns`, and `max_ns`. The JSON also includes
operations per sample and median operations per second, which is especially useful for
the 1,000-span ingest batch.

| JSON name | What the interval includes |
| --- | --- |
| `ingest_acknowledgement_1000_spans` | A prebuilt, previously unseen 1,000-span `Store::ingest_traces` batch, transaction commit, and current post-export retention scans. Protobuf decode and fixture construction are excluded. Every warmup and sample uses new IDs. |
| `dashboard_snapshot_all_tabs` | `QueryService::snapshot` with default filters: services/counts, all four first pages, AI rollups, sessions, comparisons, and top calls. This is the current all-tab snapshot, not a future bounded overview model. |
| `first_trace_page` | The first 50 trace summaries for `perf-service`, using the current service/time candidate query and complete-trace aggregation. |
| `first_log_page` | The first 50 logs for `perf-service`, ordered by event timestamp and row ID. |
| `first_metric_page` | The first 50 points from the current global metric feed for `perf-service`. This is not a metric-series query. |
| `first_ai_page` | The first 50 normalized AI operations for `perf-ai`, joined back to spans and ordered by start time. |
| `large_trace_detail` | All spans in the profile's single large trace plus the current event and link lookups. The smoke trace has 500 spans; only reference measures 10,000. |
| `trace_text_search` | The first trace page for a known marker using the current `LIKE` search over IDs, service, span name, and attribute JSON. This is not FTS. |
| `empty_log_export_with_retention` | An empty `Store::ingest_logs` transaction and acknowledgement followed by every current retention statement. The result count is zero by design. |

The harness explicitly reports two unsupported scenarios in JSON:

- `targeted_metric_series`: no public query currently identifies and downsamples one
  metric series. Timing the global metric feed would mislabel the operation.
- `concurrent_ingest_read`: the harness does not yet have a controlled overlap rendezvous
  or separate read/write latency contract for the current Store.

`targeted_metric_series` must remain unsupported until the corresponding query exists.
`concurrent_ingest_read` becomes supported when the harness defines and implements a
controlled contention workload and latency contract. A smaller page limit is not a
substitute for either capability.

## Report Schema

The pretty-printed JSON has a versioned, stable field layout. It records:

- `schema_version`, generation time, selected profile, exact scale, warmup count, and
  measured sample count;
- OS, architecture, logical CPU count, `rustc --version`, Git revision, and dirty state;
- optional reference-machine label, exact CPU model, and installed memory;
- total setup duration, final main database bytes, and live `-wal` bytes;
- ordered scenario measurements and explicit unsupported scenarios.

The database and WAL sizes are sampled after all measurements while the store connection
is live. The database is temporary and is deleted after the report is written.

## Reference Protocol

Reference results are comparable only when they come from the same stable machine and
storage class. Record a durable machine label, the exact CPU model, and installed memory
on every reference run. The harness automatically adds OS, architecture, logical CPU
count, compiler version, and Git state.

Before running reference:

1. Use an optimized `cargo bench` build and the same Rust toolchain as the comparison.
2. Connect AC power and use the same power/performance mode.
3. Stop unrelated CPU, memory, and disk-intensive work.
4. Keep the repository and temporary directory on the same filesystem/storage class.
5. Confirm enough free space for more than two million SQLite rows and WAL growth.
6. Preserve the complete JSON result; do not copy only one percentile into a document.

The first team reference machine has not been designated yet. Until one is named and a
clean reference run is checked in or attached to a release record, all budgets in the
review plan remain provisional. The reference profile is deliberately excluded from
normal CI because setup performs the product's current post-export retention scans after
every bounded batch and can take substantial time.

## Comparing Changes

Compare distributions, setup time, database/WAL size, scale, compiler, and Git state.
For a query or storage change, also capture the relevant `EXPLAIN QUERY PLAN` separately;
this first harness slice does not yet assert query plans. Report both before and after
JSON from the same machine. Treat large variance or a dirty worktree as a reason to rerun,
not as evidence that a budget passed or failed.
