# Ottyel Implementation Review and Plan

Status: proposed implementation plan

Review date: 2026-07-10

Reviewed revision: `06bf94d` (`main`)

This review treats the implementation, tests, rendered snapshots, and commit history as
the source of truth. `README.md` and `ROADMAP.md` were used only to find claims that
needed verification. They are not evidence that a feature is complete.

## Executive Judgment

Ottyel has a real and useful product core: one local binary receives OTLP over HTTP and
gRPC, keeps the data in SQLite, presents a strong keyboard-first trace UI, and exposes
the same local data to agents over MCP. The trace tree, raw attribute inspection, log
pivot, and initial AI-aware views are more than a demo.

The current implementation is not yet a trustworthy observability workstation. It can
display plausible but incorrect trace summaries, merge unrelated metric streams, count
agent and tool spans as model calls, silently omit important OTLP fields, and make model
comparisons across incomparable work. The receiver also serializes every ingest and
query through one SQLite connection and runs retention after every export. UI caches
mask that design under small loads but do not remove it.

The right product is not a terminal clone of Grafana, Jaeger, or a hosted LLM dashboard.
Ottyel should be the fastest local answer to these questions:

1. Is my application actually sending valid and complete telemetry?
2. Which request or agent run failed, became slow, looped, or became expensive?
3. Which model call, tool call, retrieval, log, or metric exemplar explains it?
4. What changed between two comparable runs, prompt versions, or model cohorts?
5. Can a local coding agent investigate the same evidence without receiving secrets or
   an unbounded payload?

The implementation order must therefore be:

1. Make received data and query results correct.
2. Make ingest bounded, observable, and non-blocking.
3. Build AI operation semantics on top of correct traces, logs, events, and metrics.
4. Turn the TUI and MCP server into investigation workflows.
5. Add broader convenience features only after the first four are measured.

## Product Direction

### Primary Use Case

Ottyel is a local investigation workbench for developers instrumenting services, agent
workflows, evaluations, and prompt iterations. A typical session is minutes to a day of
telemetry from one workstation, not multi-tenant long-term monitoring.

### Product Wedge

The strongest wedge is trace-first AI application debugging:

- ordinary distributed spans establish causal execution context;
- logs explain failures and application decisions;
- metrics provide supporting health and exemplars;
- AI semantics identify model inference, agents, tools, retrieval, embeddings,
  guardrails, prompts, and evaluations;
- the TUI is for human inspection;
- MCP is for deterministic, local agent investigation.

### Non-Goals For The Next Milestones

- hosted or multi-user operation;
- replacing an OpenTelemetry Collector in production;
- a general long-term metrics TSDB;
- alerting and on-call incident management;
- automatic LLM-generated root cause analysis inside Ottyel;
- silent model-price estimation from a model name;
- more themes, layout options, or dashboard panels before data trust is fixed.

## What The Current Code Tried To Do

The commit history shows a feature-first workstation followed by successive attempts to
protect the UI from query cost:

- dual OTLP transports and SQLite persistence;
- separate trace, log, metric, and LLM tabs;
- trace tree navigation, waterfall bars, and a longest-path heuristic;
- prompt/output inspection, session grouping, model rollups, and a synthetic tool
  timeline;
- background snapshots, detail caches, bounded LLM scans, and trace pagination;
- MCP resources and tools over stdio;
- terminal snapshots and focused navigation tests.

Those later performance changes were directionally correct. They moved periodic work to
blocking tasks, cached selected trace detail, bounded one LLM session scan to 256 rows,
and paged traces. They optimized symptoms around the current store interface. They did
not change the single-connection store, per-request retention, global aggregate scans,
or monolithic all-tab snapshot.

## Current Strengths To Preserve

- Both default OTLP ports and both transports already exist.
- A successful export is acknowledged only after a SQLite transaction commits.
- Raw resource and record attributes are retained for the fields that are stored.
- Span events and links are queryable and visible.
- Trace detail is a two-step list-to-tree workflow with stable keyboard semantics.
- The TUI has unusually good navigation and snapshot coverage for this stage.
- Cursor paging exists for signal lists and MCP search tools.
- AI attributes are normalized without removing raw span attributes.
- MCP tool results include both text and `structuredContent`.
- The default listeners are loopback-only.

These are foundations. They should be migrated, not replaced with a separate product.

## Severity-Ranked Findings

| Priority | Finding | Evidence | User impact |
| --- | --- | --- | --- |
| P0 (resolved 2026-07-10) | Trace filters produced partial summaries | `src/store/queries.rs`, `src/store/tests.rs` | Candidate trace IDs are now selected before complete-trace aggregation; regressions cover error-only, service, text, time, and cursor paging |
| P0 | Metric streams are conflated and lossy | `src/store/ingest.rs`, `src/ui/details.rs` | Different attribute sets are charted together; histogram buckets, quantiles, temporality details, exemplars, unit, and description are lost |
| P0 | Every store operation shares one mutex-protected connection | `src/store/mod.rs` | OTLP/HTTP, OTLP/gRPC, UI reads, MCP reads, and retention block one another |
| P0 | Retention runs after every export | `src/store/ingest.rs` | Sustained ingest pays repeated table scans and delete transactions even when nothing expires |
| P0 | OTLP overload and failure behavior is incomplete | `src/ingest.rs` | No explicit bounded queue, partial success, consistent request limits, gzip setup, or retry-correct error mapping |
| P0 | SQLite identity is based on global `span_id` | `src/store/schema.rs` | The logical identity `(trace_id, span_id)` is not preserved; joins and upserts can corrupt colliding traces |
| P0 | There is no schema migration mechanism | `src/store/schema.rs` | Correcting the store cannot be shipped safely to an existing database |
| P0 | Sensitive AI content has no central policy | store, TUI, and MCP paths | Prompts, outputs, tool arguments, and raw attributes can be persisted and returned without masking or payload budgets |
| P1 | AI operations are misclassified as LLM calls | `src/domain.rs` | OpenInference agent, tool, retrieval, evaluator, and prompt spans inflate model-call counts and show as `unknown/unknown` |
| P1 | Current GenAI events and attributes are only partly understood | `src/domain.rs`, `src/store/ingest.rs` | Event-based inference details and evaluations are ignored; current structured messages, tool results, cache/reasoning tokens, agents, and TTFT are missing |
| P1 | The all-tab snapshot does work that is not rendered | `src/query.rs`, `src/app/mod.rs` | Every refresh reads every signal; rollups and top calls are queried after their UI panels were removed; the first trace page is queried twice |
| P1 (partially resolved 2026-07-11) | Log time and severity semantics were wrong and remain incomplete | `src/store/helpers.rs`, `src/store/ingest.rs`, `src/store/tests/log_semantics.rs` | Event-time fallback and empty-text numeric severity labels are corrected; the v1 schema still drops observed time, numeric severity, event name, and other fields |
| P1 | Retention can leave corrupt-looking investigations | `src/store/ingest.rs` | Span-count trimming deletes individual spans, leaves orphan span events, and can retain partial traces |
| P1 | Startup and runtime failures are not visible in the TUI | `src/app/mod.rs` | A failed listener bind is not reported until exit; a refresh error exits the terminal loop instead of showing stale data plus an error |
| P1 | MCP claims read-only behavior but opens a writable store | `src/app/mod.rs`, `src/store/mod.rs` | `ottyel mcp` can create a database and execute schema initialization and WAL pragmas |
| P1 | MCP responses can be unbounded | `src/mcp/resources.rs`, `src/mcp/tools.rs` | A large trace or prompt can consume excessive time and model context; `search_llm` always computes all aggregates |
| P2 | Large modules slow safe change | `src/app/input.rs`, `src/store/queries.rs`, `src/ui/details.rs`, `src/ui/traces.rs` | Several production modules exceed the repository's own 800-line stop threshold |
| P2 (partially resolved 2026-07-11) | Quality gates were incomplete | `.github/workflows/rust.yml` | Normal CI now enforces format, strict all-target Clippy, and tests; migration, conformance, and performance jobs remain open |
| P2 | Product documentation was materially stale | `ROADMAP.md`, `README.md` | The private roadmap was replaced on 2026-07-10; README claims and crossed screenshot filenames/captions still need evidence-based updates |

## Detailed Review

### 1. Store And Ingest Contention

`Store` wraps one `rusqlite::Connection` in `Arc<Mutex<_>>`. Synchronous database work
runs directly in async HTTP and gRPC handlers. WAL cannot provide concurrent reader
benefits when all access is serialized before SQLite sees it.

Each trace, log, or metric export:

1. takes the same mutex;
2. writes one transaction using repeated `execute` calls;
3. commits;
4. reacquires the mutex;
5. issues retention deletes across all signal tables;
6. counts every span and may trim individual rows.

The correct SQLite design is one dedicated writer plus a small read pool. SQLite still
has one writer, but UI and MCP reads can use WAL snapshots while a bounded writer queue
provides explicit pressure and commit acknowledgements.

### 2. Query Cost And Incorrect Read Models

`QueryService::snapshot` always loads services, five counts, four feeds, three LLM
rollup queries, sessions, model comparisons, and two top-call queries. The TUI no longer
renders `llm_rollups` or `llm_top_calls`, but still computes them. After a periodic
snapshot, trace paging fetches the first trace page again.

The monolithic `DashboardSnapshot` forces every consumer and test fixture to know about
every tab. It also makes active-view refresh impossible. Replace it with independent
read models such as `OverviewView`, `TraceListView`, `LogView`, `MetricView`, and
`AiRunView`, each with its own generation/watermark and refresh cadence.

At review time, trace list filters were also semantically wrong. For example,
`errors_only` placed a status predicate in `WHERE`, then computed `COUNT`, `MIN`, `MAX`,
and root name from only error spans. Service, search, and time predicates had the same
problem. This was corrected on 2026-07-10 by selecting candidate trace IDs before
aggregating all spans in each trace, with regressions for non-root matches and cursor
paging. A materialized trace-level projection is still required for the final performance
design.

### 3. Trace And Log Fidelity

The store drops span trace state, flags, status message, dropped counts, scope identity,
and schema URLs. It also does not validate zero or incorrectly sized trace/span IDs.
Duration is computed by converting epoch nanoseconds to `f64` before subtraction.

For logs, the v1 schema keeps only one derived timestamp. Since 2026-07-11, ingest uses
event time when present and observed time as the fallback, matching the OTel Logs Data
Model. When source severity text is empty, ingest now derives every defined OTLP numeric
short label, and focused tests cover the existing filter categories. The schema still
discards observed time, numeric severity, flags, scope, schema URL, dropped counts, and
event name. Preserving non-empty source severity text also means numeric filtering cannot
be correct for arbitrary source labels until both severity fields are stored. A non-empty
OTel log event name is how current structured events, including GenAI events, are
identified.

### 4. Metric Fidelity

The metric tab is not reliable enough for debugging today:

- a gauge and sum lose point start time, flags, and exemplars;
- a sum loses `is_monotonic` and stores temporality as a debug string;
- a histogram is reduced to `sum` plus a text summary;
- an exponential histogram loses scale, zero count, and bucket ranges;
- a summary loses quantile values;
- metric name, service, and instrument kind are treated as series identity;
- point attributes are not exposed in `MetricSummary` and are ignored when charting;
- the chart uses whatever matching rows happen to be in the global 500-row feed.

A metric stream is identified by resource, instrumentation scope, metric descriptor,
and point attributes. Rendering must depend on instrument type. Until that is true,
the UI should not describe a mixed sparkline as a metric trend.

### 5. AI And LLM Semantics

The current `LlmAttributes::is_present` model conflates AI operation detection with model
inference detection. `openinference.span.kind` is accepted as `operation`, which causes
`AGENT`, `TOOL`, `RETRIEVER`, `RERANKER`, `EMBEDDING`, `GUARDRAIL`, `EVALUATOR`, and
`PROMPT` spans to enter `llm_spans`. GenAI `invoke_agent`, `execute_tool`, embeddings,
and retrieval operations have the same risk.

The normalizer also uses a short list of exact keys. It does not parse current
OpenInference flattened `llm.input_messages.<index>...` and
`llm.output_messages.<index>...` structures. Current OTel GenAI content can arrive on
the `gen_ai.client.inference.operation.details` event, and evaluations arrive as
`gen_ai.evaluation.result`; the log ingest path drops event name and no AI projection
reads log events.

Other missing first-class fields include:

- requested model versus response model;
- response ID and finish reasons;
- agent ID, name, and version;
- prompt name, version, and variables;
- tool call ID, type, arguments, and result;
- reasoning tokens and cache creation/read tokens;
- streaming flag and time to first chunk;
- error type and GenAI exception events;
- evaluation name, score, label, explanation, and target;
- retrieval and embedding operation identity;
- semantic source, source version, and normalization warnings.

The current timeline is inferred: prompt at offset zero, output at span end, one tool
fallback at the midpoint, and descendant spans as steps. It is useful as a sketch but
must be labeled inferred unless backed by real events, tool-call IDs, and timestamps.

The session panel silently scans only the newest 256 AI rows and then truncates to five
sessions. Totals and durations can therefore be incomplete. Model comparison averages
also mix different prompts and workloads, so they cannot support a performance or
quality decision without cohorts and sample counts.

### 6. OTLP Protocol Behavior

The binary protobuf success path is small and works, but receiver behavior needs a
deliberate contract:

- configure gzip acceptance for HTTP and gRPC;
- validate HTTP content type and use matching response encoding;
- return protobuf `google.rpc.Status` bodies for HTTP failures;
- distinguish bad data from transient storage failure and overload;
- use retryable HTTP 429/503 and gRPC `ResourceExhausted`/`Unavailable` when appropriate;
- return per-signal partial success when only some records are rejected;
- enforce wire-size, decompressed-size, record-count, and queue-byte limits;
- expose accepted, rejected, duplicate, and dropped counts by signal and reason;
- optionally add OTLP/JSON after the binary and compressed paths are conformant.

HTTP 500 and gRPC `Internal` for every storage error can cause exporters to treat a
transient problem as non-retryable. Backpressure must be explicit rather than emerging
as a blocked Tokio worker on the SQLite mutex.

### 7. Retention And Lifecycle

Retention must be maintenance work, not request work. It should operate on whole traces,
delete all related rows through composite foreign keys/cascades, and cap logs, metric
points, AI events, database bytes, and WAL size in addition to spans. Maintenance should
run on a timer or accepted-record threshold and delete bounded chunks.

Startup should pre-bind both listeners or wait for a readiness result before entering
the TUI. The header should show HTTP/gRPC health, last accepted time, recent rates,
queue depth, database size, rejected records, and the last ingest/query error. A query
failure should leave the last good view visible.

Terminal setup needs an RAII cleanup guard so an error during initialization or drawing
cannot leave the shell in raw mode.

### 8. Privacy And Local Security

SQLite files created under the current process umask can be world-readable. The
directional audit database was created with mode `0644`. AI payloads commonly contain
PII, credentials, proprietary code, and tool results.

Add one `ContentPolicy` used by ingest projection, TUI detail, export, and MCP. It should
support:

- raw local storage and display;
- metadata-only storage;
- key-pattern and value-pattern redaction;
- per-field byte/character limits with explicit truncation metadata;
- separate MCP defaults, with raw content requiring an explicit flag;
- irreversible ingest-time masking when configured;
- raw attributes after policy application, never an unredacted hidden copy;
- Unix database mode `0600` and private parent directories;
- a warning or refusal when binding non-loopback without an explicit insecure override.

Encryption at rest is not required for the next milestone. File permissions, content
policy, payload budgets, and clear non-loopback behavior provide more immediate value.

### 9. MCP Usefulness And Compliance

MCP is a strong product differentiator, but the useful interface is an investigation
model, not a dump of database rows. Current problems include:

- initialization echoes any requested protocol version rather than selecting a server
  version;
- malformed JSON terminates the stdio server instead of returning a parse error;
- resources and tools can return an unbounded trace or content field;
- trace detail and AI detail have no verbosity/content policy;
- `search_llm` recomputes and returns every aggregate section on each call;
- tools publish no output schemas;
- MCP opens the database through the writable initialization path;
- aggregate completeness and normalization provenance are not returned.

Keep MCP deterministic. Do not add an embedded model. Give external agents small,
composable, typed tools that return evidence, coverage, and caveats.

## Directional Performance Evidence

These are single warm-run observations on the review machine, not benchmark results.
They exist to validate priorities. Phase 0 must replace them with repeatable benches.

Synthetic database:

- 100,000 spans in 10,000 traces;
- 200,000 logs;
- 200,000 metric points;
- 10,000 normalized AI rows;
- 127 MiB SQLite file;
- generated outside the repository at `/tmp/ottyel-audit-20260710.db`.

Observed behavior:

| Operation | Directional result |
| --- | --- |
| No-op retention sequence matching one export | about 0.24 seconds even though no row expired |
| Unfiltered trace first page | about 0.10 seconds |
| Global metric first page | about 0.18 seconds |
| LLM session source scan | about 0.08 seconds before Rust JSON grouping |
| MCP overview | consumed about 0.97 seconds of user CPU because it calls the full dashboard snapshot and discards the feeds |

`EXPLAIN QUERY PLAN` showed:

- trace listing scans `spans` via `idx_spans_trace` and uses a temporary B-tree for
  ordering;
- metric listing scans the full `metrics` table and uses a temporary B-tree;
- LLM time-window listing scans `spans`, probes `llm_spans`, and uses a temporary
  B-tree;
- leading-wildcard JSON/text search scans candidate rows.

At the default three-second snapshot cadence, this work competes with both OTLP
transports for the same mutex. The dominant gains will come from data projections,
indexes, active-view queries, and connection ownership, not micro-optimizing Ratatui.

## Target Architecture

### Runtime Data Flow

```text
OTLP/HTTP + OTLP/gRPC
        |
        v
decode -> validate -> content policy -> project records
        |
        v
bounded queue measured in records and bytes
        |
        v
dedicated SQLite writer thread
  - prepared statements
  - coalesced transactions
  - touched-trace projection updates
  - commit acknowledgement / partial-success report
  - scheduled retention and WAL maintenance

TUI read jobs + MCP read jobs
        |
        v
small read-only SQLite connection pool over WAL snapshots
        |
        v
typed, independently refreshed view models
```

Do not move to an async database library merely to make the types async. `rusqlite` on a
dedicated writer thread and bounded blocking read jobs fits SQLite's actual concurrency
model and keeps the implementation understandable.

### Logical Store Model

The exact DDL belongs in a reviewed migration, but the logical identities are decided:

- `resources`: canonical raw attributes, schema URL, dropped count, fingerprint;
- `scopes`: name, version, attributes, schema URL, fingerprint;
- `traces`: one materialized summary per trace, including root/entry service, observed
  and root duration, counts, last ingest time, and completeness flags;
- `trace_services`: distinct service membership for correct service filtering;
- `spans`: composite primary key `(trace_id, span_id)`, resource/scope references,
  trace state, flags, status message, dropped counts, raw attributes;
- `span_events` and `span_links`: composite parent references and stable ordinal;
- `logs`: stable local row ID, event and observed timestamps, event name, severity number
  and text, flags, typed body, resource/scope, trace context, raw attributes;
- `metric_streams`: resource, scope, descriptor, point-attribute fingerprint, kind,
  temporality, monotonicity, unit, and description;
- `metric_points`: complete type-specific point data, flags, start/time, exemplars, and
  structured histogram/summary fields;
- `ai_operations`: one projection per relevant span, classified operation kind and
  normalized low-cardinality dimensions with source/provenance;
- `ai_events`: normalized inference detail, exception, and evaluation events linked to
  their log record and optional trace/span;
- `signal_stats_minute`: bounded per-signal health/count buckets for overview queries;
- `schema_migrations`: ordered schema version and application record.

Raw payload fidelity does not require an entity-attribute-value table. Keep canonical
JSON for raw attributes and typed columns for dimensions Ottyel actually filters or
aggregates. Add FTS only for explicitly searchable text after content policy is applied.

### AI Operation Model

Replace `LlmAttributes` as the detection primitive with a typed classifier:

```text
ModelInference
Agent
Tool
Retrieval
Rerank
Embedding
Guardrail
Evaluation
Prompt
ChainOrWorkflow
OtherAi
```

Use independent adapters for:

1. current OpenTelemetry GenAI spans and log events;
2. current OpenInference flattened span attributes;
3. narrowly documented legacy OpenLLMetry aliases.

Each adapter returns a normalized operation plus source, version/dialect, field-level
provenance, and warnings. Precedence is per field, not "whichever convention appears
first." Model rollups include only `ModelInference`. Agent/tool/retrieval/evaluation
operations remain first-class and visible in an AI run.

## Implementation Plan

### Phase 0: Freeze A Trustworthy Baseline

Goal: make correctness and performance changes measurable before changing the store.

- [ ] Add checked-in OTLP fixtures for traces, logs, metrics, GenAI events/spans,
  OpenInference spans, mixed conventions, malformed records, and duplicate delivery.
- [ ] Generate fixtures through real SDK/exporter shapes where practical; do not use UI
  seed objects as ingest evidence.
- [x] Add a deterministic release-mode harness foundation for unique-batch ingest,
  current snapshot/feed queries, trace detail, `LIKE` search, retention overhead,
  distributions, throughput, and database/WAL growth.
- [ ] Extend the harness with targeted metric series, controlled concurrent ingest/read
  and UI-stall measurement, MCP response budgets, and query-plan assertions.
- [x] Cover at least 100k spans, 1m logs, 1m metric points, a 10k-span trace, and 100k AI
  operations in the non-CI benchmark profile.
- [ ] Record `EXPLAIN QUERY PLAN` assertions for critical list/detail queries.
- [x] Add complete-trace summary regressions for error-only, service, text, time-window,
  and cursor-paged trace filters, then correct candidate-trace query semantics.
- [x] Add schema-neutral log regressions and correct event-time fallback plus empty-text
  numeric severity derivation.
- [ ] Add correctness tests demonstrating the metric-series, AI-classification,
  retention-orphan, composite-identity, and stale-projection bugs before fixing them.
- [x] Add `cargo fmt --check` and Clippy to CI; clean the current Clippy baseline rather
  than adding broad allows.
- [x] Define the repeatable smoke/reference profiles and machine protocol in
  `docs/performance.md`.
- [ ] Designate a stable reference machine and preserve the first clean reference result.

Acceptance:

- each remaining P0 correctness bug has a failing regression test;
- benchmark commands and dataset generators are documented and deterministic;
- CI enforces format, test, and a clean Clippy run;
- product behavior changes land only with a focused regression and passing full suite.

### Phase 1: Add Migrations And Safe Store Ownership

Goal: create the seam required for every subsequent data fix.

- [ ] Add ordered, transactional schema migrations using `PRAGMA user_version` or a
  migration table; test fresh install and every supported upgrade path.
- [ ] Back up or copy the v1 database before a non-trivial migration and run integrity
  checks before and after migration.
- [ ] Add a read-only/query-only open mode for MCP and non-repair doctor operations.
- [ ] Set private database and directory permissions on Unix.
- [ ] Replace `Arc<Mutex<Connection>>` with a dedicated writer owner and a small bounded
  read connection pool.
- [ ] Route every database call through blocking workers; no SQLite call may run on the
  terminal event loop or an async network worker.
- [ ] Use prepared/cached statements and batch one signal export per transaction.
- [ ] Add a typed `StoreError` classification: invalid data, busy/overloaded, unavailable,
  corruption, migration required, and internal defect.

Acceptance:

- MCP can open an existing database without creating or modifying it;
- UI reads proceed during a representative writer transaction under WAL;
- all async handlers remain responsive while the database is deliberately slowed;
- migration interruption tests recover without silent data loss;
- existing v1 data remains inspectable after upgrade.

### Phase 2: Make OTLP Ingest Bounded And Protocol-Correct

Goal: behave predictably under malformed, compressed, concurrent, and excessive input.

- [ ] Introduce `IngestBatch` and `IngestReport { accepted, rejected, warnings }` per
  signal.
- [ ] Decode and validate IDs, timestamps, record counts, and required invariants before
  projection; reject bad records without losing valid siblings when possible.
- [ ] Add a bounded queue with record and byte budgets and await commit acknowledgement.
- [ ] Coalesce adjacent batches within a small time/size budget without delaying low-rate
  local development traffic.
- [ ] Configure gzip for both transports and consistent wire/decompressed size limits.
- [ ] Validate HTTP content types and encode OTLP success, partial success, and
  `google.rpc.Status` failure bodies correctly.
- [ ] Map invalid data, overload, and transient storage failure to retry-correct HTTP and
  gRPC statuses.
- [ ] Add HTTP and gRPC integration tests for all three signals, gzip, empty envelopes,
  malformed subsets, limits, overload, and graceful shutdown.
- [ ] Add optional OTLP/JSON only after binary protobuf conformance is covered.
- [ ] Expose in-memory accepted/rejected/rate/queue/latency statistics by signal.

Acceptance:

- memory is bounded by configured request and queue budgets;
- overload returns an explicit retryable response instead of blocking indefinitely;
- valid records in a mixed request commit and rejected counts are reported;
- the receiver accepts required gzip traffic on both transports;
- shutdown stops intake, drains or reports the bounded queue, commits, and exits within a
  configured timeout.

### Phase 3: Correct The Core Telemetry Store And Queries

Goal: make every displayed trace, log, and metric result semantically defensible.

- [ ] Migrate spans and related tables to composite `(trace_id, span_id)` identity and
  cascading relations.
- [ ] Preserve resource, instrumentation scope, schema URL, trace state, flags, status
  message, and all dropped counts.
- [ ] Compute duration with checked integer subtraction and retain invalid-time warnings.
- [ ] Materialize one `traces` row per touched trace after each batch; do not globally
  regroup spans for list refresh.
- [ ] Store trace service membership and completeness flags for missing parent, no root,
  multiple roots, invalid IDs, clock skew, dropped data, and recently changing trace.
- [ ] Filter candidate trace IDs, then return the complete trace summary.
- [ ] Preserve both log timestamps and select event time with observed time as fallback.
- [ ] Preserve event name, severity number/text, flags, typed body, scope, and schema URL.
- [ ] Implement faithful metric descriptors, series identity, point types, temporality,
  monotonicity, histograms, summaries, exponential histograms, and exemplars.
- [ ] Add targeted metric-series queries with time bucketing/downsampling and exemplar
  pivots; never derive a series from the global feed page.
- [ ] Replace interpolated SQL values with bound parameters.
- [ ] Add indexes aligned to global time order, service/time order, trace relations,
  series/time, and AI dimensions.
- [ ] Add content-policy-aware FTS for span names, log body/event name, prompt name, and
  selected AI text; keep exact ID filters on normal indexes.
- [ ] Replace overview full counts with bounded time buckets or incremental counters.

Acceptance:

- applying any trace filter changes membership/order but not the summary of a given
  trace;
- two identical span IDs in different traces remain independent through ingest, detail,
  LLM projection, links, and retention;
- metric points with different attributes never share a series;
- histogram and summary fixtures round-trip all queryable structure;
- log ordering follows OTel event-time fallback rules;
- all critical query plans use their intended indexes without a full-table temporary
  sort at the reference scale.

### Phase 4: Remove Snapshot Work And Finish Runtime Reliability

Goal: refresh only the view the user is using and never freeze or exit on a transient
read failure.

- [ ] Replace `DashboardSnapshot` with per-view models and independent refresh state.
- [ ] Query overview at a slower cadence; query only the active tab plus shared header
  health; pause hidden detail refreshes.
- [ ] Remove the duplicate first trace page and all unused TUI aggregate queries.
- [ ] Give logs, metrics, and AI runs the same stable keyset paging as traces.
- [ ] Keep selections by stable identity rather than mutable row index.
- [ ] Make filter changes, trace open, metric series, and AI detail asynchronous with
  generation IDs and stale-result rejection.
- [ ] Bound loaded pages and replace O(n^2) trace merge/deduplication with indexed sets or
  a windowed list.
- [ ] Pre-bind listeners and send readiness/health into the UI before declaring ingest
  active.
- [ ] Keep the last good view on query failure and show a compact health/error banner.
- [ ] Add a terminal lifecycle guard and fault-injection tests for terminal and server
  startup failures.

Acceptance:

- no database operation occurs on the input/render task;
- switching filters and opening a 10k-span trace does not block input handling;
- hidden tabs cause no feed or aggregate queries;
- a failed refresh leaves navigation usable and exposes the failure;
- listener bind failure is visible before the main UI claims the endpoint is ready.

### Phase 5: Build Correct AI Run Observability

Goal: answer why an AI run failed, slowed down, looped, or cost more using comparable
evidence.

- [ ] Implement the typed AI operation classifier and convention adapters described
  above.
- [ ] Normalize current OTel GenAI spans and log events, including inference detail,
  exceptions, evaluations, agents, tools, retrieval, embeddings, streaming, cache, and
  reasoning usage.
- [ ] Parse current OpenInference flattened messages, multimodal parts, tool calls,
  retrieval documents, evaluations, and prompt metadata.
- [ ] Keep legacy aliases isolated and tested; record which adapter populated each field.
- [ ] Store requested and response model separately.
- [ ] Store reported cost with source and currency. Mark estimated cost separately if a
  future versioned price catalog is added; never silently combine them.
- [ ] Build complete run/session projections without a hidden newest-256-row scan.
- [ ] Add completeness and content-availability indicators to every AI aggregate.
- [ ] Construct timelines from real spans/events and tool-call IDs. Mark inferred prompt,
  tool, or output positions explicitly.
- [ ] Detect useful run anomalies deterministically: model/tool errors, repeated tool
  calls, retry/fallback sequences, excessive agent steps, high token usage, context
  compaction, and missing output.
- [ ] Compare models only within an explicit cohort such as prompt name/version, agent
  version, operation, service, or evaluation dataset. Show sample count, p50/p95 latency,
  error rate, token distribution, TTFT, cache usage, cost coverage, and evaluation score.

Acceptance:

- agent/tool/retrieval/evaluation spans never inflate model inference call counts;
- official current GenAI and OpenInference fixtures normalize with field provenance;
- event-based GenAI content and evaluations are visible and correlated;
- session/run totals are exact for the selected window or explicitly marked partial;
- a model comparison cannot be shown without its cohort and data-coverage caveats.

### Phase 6: Turn The TUI Into An Investigation Workflow

Goal: reduce time from "telemetry arrived" to a supported explanation.

- [ ] Make Overview an investigation inbox: receiver health, last data, rejected records,
  data-quality warnings, recent errors, slow traces/runs, and high-token/cost runs.
- [ ] Rename LLM Inspector to AI Runs or Agent Runs once the operation model exists.
- [ ] Make the primary AI list run/session-oriented, with nested agent, model, tool,
  retrieval, guardrail, and evaluation operations.
- [ ] Add bidirectional pivots: trace/span to AI run, AI operation to full trace, any
  operation to correlated logs, exemplar to trace, and linked span to linked trace.
- [ ] Add filters for operation kind, provider, requested/response model, agent, prompt
  name/version, conversation, error type, duration, tokens, TTFT, cost coverage, and eval.
- [ ] Render gauges, sums/rates, histograms, summaries, and exponential histograms using
  their actual semantics.
- [ ] Show trace completeness and call the current tree calculation "longest nested path"
  unless its causal critical-path assumptions are proven for the trace.
- [ ] Add copy/export for IDs and a bounded investigation bundle with content policy.
- [ ] Add empty, partial, loading, stale, overload, migration, and error snapshots.

Acceptance:

- a user can start from a rejected-ingest warning, slow run, tool failure, log, or exemplar
  and reach the relevant trace evidence without retyping an ID;
- UI labels distinguish reported, derived, inferred, partial, and unavailable values;
- generic traces remain first-class and do not require AI telemetry;
- all navigation changes update contextual help and footer snapshots.

### Phase 7: Make MCP A Safe Investigation Interface

Goal: give coding agents bounded, semantically correct evidence instead of database dumps.

- [ ] Correct protocol negotiation, JSON-RPC parse/error handling, lifecycle behavior,
  cancellation where useful, and current MCP conformance tests.
- [ ] Add output schemas and explicit read-only annotations/descriptions where supported.
- [ ] Apply central content policy, row limits, byte budgets, truncation metadata, and
  stable opaque cursors to every response.
- [ ] Split expensive aggregates from search. `search_ai_operations` must not compute
  model, session, and top-call aggregates unless requested.
- [ ] Replace or supplement current tools with:
  - `get_ingest_health`
  - `search_traces`
  - `get_trace_summary`
  - `get_trace_page`
  - `search_ai_runs`
  - `get_ai_run`
  - `get_ai_operation`
  - `get_session`
  - `compare_ai_cohort`
  - `search_logs`
  - `get_metric_series`
  - `export_investigation`
- [ ] Return data quality, completeness, normalization provenance, content policy, and
  next cursor with every relevant result.
- [ ] Make large trace and run detail progressive: summary first, then bounded span/event
  pages or selected subtrees.
- [ ] Keep all tools deterministic and read-only.

Acceptance:

- no single default tool call can return an unbounded trace or prompt body;
- default MCP output is redacted/truncated according to its declared policy;
- a local agent can answer "why was this run slow?" using bounded calls with cited trace,
  span, event, and log IDs;
- malformed input yields a protocol response and does not terminate the server;
- MCP opening and querying leaves database bytes and schema unchanged.

### Phase 8: Maintenance, Code Locality, And Release Quality

Goal: keep the corrected system easy to extend without central files growing again.

- [ ] Split store queries by owned read model: traces, logs/events, metrics, AI, and
  health. Keep shared SQL construction parameterized and small.
- [ ] Split AI domain types and convention adapters out of `domain.rs`.
- [ ] Split per-tab input handlers and mouse hit-testing out of `app/input.rs` while
  preserving one navigation contract.
- [ ] Split trace, log, metric, and AI detail builders out of `ui/details.rs`.
- [ ] Introduce a runtime state object so event handlers do not take 8-12 mutable
  arguments.
- [ ] Keep production modules near the repository's 500-line target and stop at 800.
- [ ] Add migration, protocol, sustained-load, and performance regression jobs at
  appropriate CI cadences.
- [ ] Add cross-platform smoke builds, dependency policy, release profile, and packaging
  only after the core data model stabilizes.
- [x] Replace the stale private roadmap with a milestone plan derived from this review.

Acceptance:

- central production modules satisfy the repository size discipline;
- each signal read model and AI adapter is testable through its public interface;
- normal pull requests run format, Clippy, unit/integration tests, and small regression
  fixtures;
- scheduled jobs run large benchmarks and sustained-ingest tests with trend output.

## Provisional Performance Budgets

Phase 0 must record hardware and baseline distributions. These are initial gates, not
marketing claims:

| Scenario | Budget |
| --- | --- |
| Overview health/count view at reference scale | p95 <= 50 ms |
| First page of an indexed active feed | p95 <= 50 ms |
| Trace detail with 10,000 spans plus events/links | p95 <= 100 ms |
| Targeted metric series over one day, downsampled to terminal width | p95 <= 75 ms |
| Indexed exact ID lookup | p95 <= 10 ms |
| FTS investigation search at reference scale | p95 <= 250 ms |
| Commit acknowledgement for a 1,000-span local batch without overload | p95 <= 100 ms |
| Sustained 5,000 mixed records/second | no data loss below configured capacity and UI input stalls < 50 ms |
| Retention | no maintenance scans on every export; bounded chunks and < 5% steady writer time |
| Memory | bounded request, queue, loaded pages, and response bytes with no growth over a 30-minute steady-load run |

Every performance change must report before/after throughput and latency distributions,
database size, WAL size, and query plan. A smaller query limit is not a performance fix
when it makes totals silently incomplete.

## Test Matrix Required Before Calling 0.2 Trustworthy

### OTLP

- HTTP and gRPC for traces, logs, and every metric type;
- gzip and no compression;
- valid, empty, malformed, mixed-validity, oversized, overloaded, and duplicate requests;
- retryable versus non-retryable failures and partial success;
- graceful shutdown with queued and in-flight requests.

### Storage

- fresh schema, v1 migration, interrupted migration, read-only open, corrupt database;
- composite ID collision, out-of-order spans, retransmitted changed spans, stale AI
  projection removal, events/links cascade;
- whole-trace and per-signal retention, size caps, WAL checkpoint, no orphans;
- resource/scope/schema preservation and raw attributes after content policy.

### Queries

- filters preserve complete trace summaries;
- stable keyset paging under concurrent inserts;
- time windows at exact boundaries and clock-skew cases;
- large trace detail, linked traces, FTS, and explain-plan checks;
- stable selection identities across refresh.

### Logs And Events

- event-time fallback, numeric severity mapping/filtering, structured bodies, event name,
  trace flags, uncorrelated events, and GenAI event correlation.

### Metrics

- gauge, delta/cumulative monotonic sum and reset, histogram, exponential histogram,
  summary, exemplars, multiple resources/scopes, and multiple point-attribute series;
- instrument-aware rendering and downsampling.

### AI

- current OTel GenAI inference, agents, tools, retrieval, embeddings, exceptions,
  evaluations, streaming, cache/reasoning usage, and MCP spans;
- current OpenInference LLM, agent, chain, tool, retriever, reranker, embedding,
  guardrail, evaluator, and prompt spans;
- mixed conventions, conflicting fields, malformed structured content, legacy aliases,
  redaction, truncation, provenance, inferred versus observed timeline steps;
- cohort comparison and exact/partial run totals.

### TUI And MCP

- loading, empty, stale, partial, error, overload, migration, and redacted snapshots;
- protocol initialization, malformed JSON, tool validation, bounded pages, cursors,
  output schemas, cancellation, read-only database behavior, and content budgets.

## Recommended First Ten Pull Requests

Keep each pull request a vertical, reversible step with tests and measurements.

1. [ ] Add correctness regressions and the repeatable performance harness.
2. [x] Make format and Clippy clean and enforce them in CI.
3. [ ] Add migrations, integrity checks, and read-only store open mode.
4. [ ] Add the writer owner/read pool without changing the v1 logical schema.
5. [ ] Add the bounded ingest queue, request budgets, gzip, typed errors, and ingest health.
6. [ ] Ship the v2 composite trace/log schema, materialized trace summaries, and whole-trace
   retention.
7. [ ] Ship faithful metric streams/points and targeted metric series queries.
8. [ ] Replace the monolithic snapshot with active-view asynchronous read models.
9. [ ] Add the typed OTel GenAI/OpenInference operation projection and exact run/session
   aggregates.
10. [ ] Rework AI Runs and MCP investigation flows on the new read models.

Do not combine pull requests 4 through 9 into one migration. Each step should retain a
working TUI, expose compatibility behavior, and include a rollback or recovery story.

## Documentation And Roadmap Cleanup

Current documentation work:

- [x] Replace the stale private roadmap with the execution view in `ROADMAP.md`.
- [ ] Commit this review as the shared planning baseline while keeping `ROADMAP.md`
  private.
- [ ] Map roadmap execution state to tracked issues or pull requests after Phase 0 issues
  are created.
- [ ] Update README claims only when acceptance tests prove them.
- [ ] Fix the crossed screenshot filenames/captions.
- [ ] Document supported convention versions and the normalization provenance visible in
  the product.
- [ ] Publish benchmark methodology and last-known results, not adjectives such as
  "fast."

## Validation Performed For This Review

- `cargo fmt -- --check`: passed.
- `cargo test`: passed, 112 product tests plus 6 performance-harness support tests.
- `cargo clippy --all-targets -- -D warnings`: passed after resolving all 13 baseline
  diagnostics without lint suppressions.
- `cargo test ui::snapshot_tests`: passed, 8 snapshots.
- `cargo bench --bench store_baseline -- --profile smoke`: passed with 9 measured
  scenarios, exact cardinality checks, and 2 explicitly unsupported scenarios. The smoke
  run is diagnostic only and does not prove the reference budgets.
- UI snapshots and both checked-in screenshots were inspected.
- Store queries and retention were exercised against the directional synthetic database
  described above.
- Current primary OTel, GenAI, OpenInference, and MCP specifications were checked.

## Primary Specification References

- OTLP 1.10.0: <https://opentelemetry.io/docs/specs/otlp/>
- OTel trace API and SpanContext: <https://opentelemetry.io/docs/specs/otel/trace/api/>
- OTel log/event data model: <https://opentelemetry.io/docs/specs/otel/logs/data-model/>
- OTel metrics data model: <https://opentelemetry.io/docs/specs/otel/metrics/data-model/>
- OTel GenAI semantic conventions repository:
  <https://github.com/open-telemetry/semantic-conventions-genai>
- OTel GenAI events:
  <https://github.com/open-telemetry/semantic-conventions-genai/blob/main/docs/gen-ai/gen-ai-events.md>
- OTel GenAI model spans:
  <https://github.com/open-telemetry/semantic-conventions-genai/blob/main/docs/gen-ai/gen-ai-spans.md>
- OTel GenAI agent/tool spans:
  <https://github.com/open-telemetry/semantic-conventions-genai/blob/main/docs/gen-ai/gen-ai-agent-spans.md>
- OTel GenAI metrics:
  <https://github.com/open-telemetry/semantic-conventions-genai/blob/main/docs/gen-ai/gen-ai-metrics.md>
- OTel MCP semantic conventions:
  <https://github.com/open-telemetry/semantic-conventions-genai/blob/main/docs/gen-ai/mcp.md>
- OpenInference specification: <https://arize-ai.github.io/openinference/spec/>
- MCP 2025-11-25 schema:
  <https://modelcontextprotocol.io/specification/2025-11-25/schema>
