# Performance Baselines

Ottyel's store and OTLP field-work benchmarks are release-mode diagnostic harnesses, not
product claims. They record latency distributions and write versioned JSON reports for
before/after comparisons.

The store implementation is under `benches/store_baseline.rs` and `benches/support/`.
It measures the current public `Store` and `QueryService` behavior. Run it with:

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
| `ingest_acknowledgement_1000_spans` | A prebuilt, previously unseen 1,000-span `Store::ingest_traces` batch, primary-record/canonical-byte measurement, immediate writer admission, owner-thread transaction commit, current post-export retention scans, and receipt acknowledgement. Protobuf decode and fixture construction are excluded. Every warmup and sample uses new IDs, and this sequential scenario does not measure saturation. |
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

The deterministic writer-owner tests use internal gates to prove pressure and lifecycle
behavior, but those gates are not a benchmark workload. The fixed 64-command storage
queue remains independent from OTLP writer admission, which defaults to 40,000 aggregate
primary records and 16 MiB of canonical protobuf bytes across queued and executing work.
Request decoding and the complete OTLP overload matrix remain outside this harness
scenario.

## Writer-Owner Smoke Check

The 2026-07-14 ownership change was checked with the documented smoke profile on the same
machine. The pre-change report used clean revision `debf7f4`; the post-change report used
the implementation worktree before commit.

| Diagnostic | Before | After |
| --- | ---: | ---: |
| Setup | 120.3 ms | 151.0 ms |
| 1,000-span acknowledgement p50 | 15.929 ms | 15.136 ms |
| 1,000-span acknowledgement p95 | 21.627 ms | 21.254 ms |
| All-tab snapshot p50 | 18.393 ms | 18.279 ms |
| All-tab snapshot p95 | 19.117 ms | 18.652 ms |
| Main database | 7,802,880 bytes | 7,802,880 bytes |
| Live WAL | 4,445,512 bytes | 4,445,512 bytes |

Five smoke samples can catch an extreme regression but cannot establish a throughput
gain. Setup variance increased in this pair, while measured operation percentiles were
flat to directionally lower. A controlled concurrent scenario and repeated reference
runs remain required before making a performance claim.

## Weighted Writer Admission Smoke Check

The 2026-07-23 weighted-admission change was checked with the same smoke profile on the
same machine. The before report used clean revision `f5d86de`; the after report used the
implementation worktree. The timed public ingest path includes exact primary-record and
canonical `encoded_len()` measurement.

| Diagnostic | Before | After |
| --- | ---: | ---: |
| Setup | 120.5 ms | 135.6 ms |
| 1,000-span acknowledgement p50 | 15.479 ms | 15.393 ms |
| 1,000-span acknowledgement p95 | 23.429 ms | 23.996 ms |
| All-tab snapshot p50 | 18.553 ms | 18.238 ms |
| All-tab snapshot p95 | 18.712 ms | 18.338 ms |
| Main database | 7,802,880 bytes | 7,802,880 bytes |
| Live WAL | 4,445,512 bytes | 4,445,512 bytes |

The five-sample acknowledgement p50 was flat and p95 increased by about 2.4%; setup
variance increased. This diagnostic found no broad regression, but it is not a
throughput claim or a substitute for the missing controlled-concurrency workload and
reference-machine runs.

## OTLP Protobuf Field-Work Benchmark

`benches/ingest_field_work.rs` measures preflight, Prost decode, decoded-graph
validation, and their full pipeline against deterministic trace-request wire fixtures at
the 4 MiB request boundary. The benchmark source-includes the private production policy
and preflight modules so measurements cannot silently drift from runtime behavior or
require a wider public API.

Run the smoke profile to verify fixture generation and report output:

```sh
cargo bench --bench ingest_field_work -- --profile smoke \
  --output target/performance/ingest-field-work-smoke.json
```

Run the reference profile only on a named, stable machine:

```sh
cargo bench --bench ingest_field_work -- --profile reference \
  --machine-label "replace-with-stable-machine-name" \
  --cpu "replace-with-exact-cpu-model" \
  --memory-gib 32 \
  --output target/performance/ingest-field-work-reference.json
```

The fixtures cover one low-dispatch unknown blob, millions of short unknown varints,
maximum-length unknown varints, repeated depth-100 unknown groups, duplicate known Span
scalars, and a near-limit canonical SpanLink wire shape. Setup verifies every encoded
size, decoded cardinality, declared work count, and expected default-policy outcome before
timing. `preflight` and `full_pipeline` apply the production default policy.
`prost_decode` and `postdecode_validate` deliberately run in isolation, even for a
fixture that production preflight rejects, so their results are diagnostic rather than
an admission outcome.

The original report schema v1 decision rule required two consecutive clean release runs
on the same designated machine. A work budget was warranted when any adversarial
full-pipeline p95 exceeded 100 ms, or exceeded both 25 ms and eight times the blob-control
p95. Both clean `da66f11` reference runs on `mac15-7-m3pro-12c` (Apple M3 Pro, 12 cores,
18 GiB, Rust 1.96.0) crossed the conditional threshold for depth-100 groups. The
schema-v2 report retains this as `historical_decision_gate`, marks it inapplicable to
post-mitigation measurements, and reports current results under
`mitigation_observations`.

| Full-pipeline fixture | Baseline v1 p95, runs 1 / 2 | Work-budget v2 p95, runs 1 / 2 | Default v2 outcome |
| --- | ---: | ---: | --- |
| Unknown length-delimited blob | 42 ns / 42 ns | 42 ns / 42 ns | completed |
| Unknown zero varints | 22.641 ms / 26.122 ms | 9.845 ms / 10.028 ms | budget rejected |
| Unknown maximum-length varints | 5.216 ms / 5.233 ms | 5.576 ms / 5.602 ms | completed |
| Unknown depth-100 groups | 33.613 ms / 35.543 ms | 6.543 ms / 6.463 ms | budget rejected |
| Duplicate known Span scalars | 18.521 ms / 19.314 ms | 9.815 ms / 9.912 ms | budget rejected |
| Near-limit canonical SpanLink wire shape | not in v1 | 45.698 ms / 44.903 ms | completed |

The accepted maximum-length-varint path became 6.9% and 7.1% slower in the paired runs,
which is the measured cost of per-key work accounting on this extreme input. The shorter
post-mitigation times for rejected fixtures measure earlier termination, not faster
complete decoding.

The post-mitigation runs used clean `35f48ca` on the same machine. The default
`--max-otlp-work-units 2000000` counter is global to one preflight scan and charges one
unit for every valid field key, one for each known nested-message or unknown-group entry,
and one for each packed primitive element. Varint value bytes are not charged separately
because the decompressed-byte limit already bounds byte-linear work. A work-budget
failure maps through the existing atomic oversized-request path before Prost.

The accepted SpanLink control is 4 MiB minus three bytes, contains 249,997 links at the
250,000-structure boundary, and consumes 1,688,853 work units. Its short IDs are accepted
by the current receiver but are not proof of OTLP record validity; it exists only to
demonstrate headroom for a canonical wire layout under the current preflight policy.
Rejected measurements report a null p50 throughput because early rejection is not
completed-input throughput. Preserve both complete JSON reports when changing the policy,
fixtures, pinned protobuf schema, compiler, or machine.

## Store Report Schema

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
