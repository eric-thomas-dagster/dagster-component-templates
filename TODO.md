# TODO

Open work tracked across the registry. Closed items get deleted, not crossed
out — git log is the history.

## LLM-facing discovery — Claude Skill vs. hosted MCP server vs. keep improving the AI docs

Today we ship the CLI's `AI-tool config templates` (CLAUDE.md /
`.cursorrules` / `.aider.conf.yml`) that customers copy into their
projects. That gives a coding LLM the vocabulary + `dagster-component
search/add/schema/info` commands + a task→component cheatsheet. It
works but has three limits:

1. **Cold cache — the LLM starts from zero on every session** unless
   the user installs the templates + the LLM actually reads them.
2. **Search is CLI-mediated** — the LLM has to shell out for every
   `search` / `schema` call, and the JSON responses aren't optimized
   for token budget.
3. **The 990-component surface is too big for even the whole manifest
   to fit in-context** — we already ranked-search internally (see
   `search_qa` regression tests), but that ranker lives inside the
   CLI, not exposed to the LLM as a tool.

Three delivery vectors to evaluate — not mutually exclusive:

### A. Claude Skill (`dagster-community-components`)

Ships as a subdirectory of skills in Claude Code / Claude.ai. Analogous
to the existing `dagster-expert` + `dagster-integrations` skills we
already use. Would provide:

- **A curated task-router** (like `dagster-expert`'s router table) that
  maps "I need X" → `dagster-component add <id>` recommendations.
- **Reference docs on-demand** — one skill file per category (ai,
  transformation, integration, etc.) so the LLM only pays token cost
  for the areas relevant to the current task.
- **Cheat-sheet** for common composition patterns (workspace + pipeline
  + Model Registry, ingestion → transform → sink, agent+MCP+HITL).
- **Runs inside the coding IDE** — no network round-trip.

Downside: users have to *install* the skill (or Claude has to bundle
it). Discovery gap: how does a customer even know to reach for it?

### B. Hosted MCP server on Vercel (`mcp.dcc.dagster.cloud`)

Model Context Protocol server that any LLM (Claude, ChatGPT with MCP,
Cursor with MCP) can attach as a tool source. Tools would include:

- `search_components(query, category?, vendor?, validation_level?)`
  → returns top-K component IDs + one-line descriptions + install
  command. Reuses the ranker from `search_qa`.
- `get_component_schema(id)` → returns the schema.json contents. Lets
  the LLM write a valid `defs.yaml` on first try.
- `get_walkthrough(slug)` → returns the .md contents from
  examples/. Lets the LLM crib the composition shape.
- `suggest_composition(intent_free_text)` → LLM-driven ("we want to
  ingest from Salesforce, transform, land in Snowflake, plus a
  freshness alert") → returns the 4-5 components + a wiring diagram.
  Would use our own LLM (Anthropic API) under the hood; user's client
  LLM does the YAML authoring against those recommendations.

Upside: **discoverable via `Manage MCP servers → add
mcp.dcc.dagster.cloud`** — a real deploy story on Vercel that anyone
can point their agent at. Zero-install for the user. Same tools work
from Claude Code, Cursor, ChatGPT (as MCP support rolls out),
Continue.dev, etc.

Downside: infra to run (Vercel Functions + probably a small Redis for
the ranker cache); needs API-key management if we do
`suggest_composition` via our own LLM.

### C. Double-down on the AI-tool config templates

Keep shipping CLAUDE.md / cursor / aider configs, but:

- Make them *deep-linkable* from every component detail page in the
  Vercel UI ("Copy this component's schema to your Claude/Cursor
  clipboard").
- Push the ranked-search results directly into the templates as a
  "recent additions" section so the LLM has fresh context every
  time the templates are re-fetched.
- Wire in an `mcp:dagster-plus` tool config example so Claude gets
  live catalog access alongside the templates.

Upside: cheapest, no new infra.
Downside: still cold-cache; still requires the user to install +
configure something in their editor.

### Recommendation

**Ship A + B in parallel.** They serve different audiences:
- **Skill (A)** — the Claude Code power user with a project already
  scaffolded. Skill instructs "reach for `dagster-component search`
  first, here's what the categories mean, here are the canonical
  composition patterns."
- **MCP server (B)** — the coder with any MCP-aware agent (Cursor,
  ChatGPT, Continue) who wants live catalog lookups without any
  local install. Also: internal Dagster engineers exploring what
  the community registry has.

**A is smaller** — one directory of markdown files + a routing table.
Can prototype today.
**B is bigger** — Vercel deploy + endpoint design + tool schemas +
observability + rate limits + potentially our own LLM budget. But
higher discoverability ceiling.

Both leverage the same underlying assets (manifest.json + schema.json
per component + examples/*.md). Neither replaces C (templates stay
useful for cold-start / offline flows).

Order of ops if we do both:
1. **A first** — sketch the skill (1-2 days), dogfood against ~5
   common prompts ("build me a snowflake+dbt+dagster+ mlflow
   pipeline", "watch an S3 bucket for new files → RAG index").
2. **B second** — reuse the skill's category prose + ranker as
   the MCP server's tool implementations.

## More example walkthroughs needed

The manifest tracks `validation: { level: code|infra|live, ... }` per
component (see VALIDATION.md). Roughly 500 components in the registry have
no live walkthrough yet. Each example covers ~5–40 components, so we need
10–20 more example demos to hit broad live-coverage. Priority targets:

- **ingestion (49 components)** — kafka_to_db, sqs_to_db, kinesis, eventhubs,
  pubsub, sftp, csv_file, etc. Many share infra patterns and could be
  validated together with a single localstack/redpanda/etc. setup.
- **sensor (40)** — file watchers, polling sensors, webhook receivers.
- **io_manager (15)** — most are validated via `setup_local_io_demo.sh`,
  but cloud-backed (s3, gcs, adls) IO managers need separate validation.
- **external (21)** — declare-only assets; mostly need a "you can see them
  in the UI" smoke test rather than full materialization. Now also support
  the canonical partition shape (including `dynamic`) so a multi-tenant
  external-table walkthrough is a natural follow-up.
- **check (7)** — Great Expectations / Soda / etc.
- **resource (55)** — most are connection-handle wrappers; validation =
  resource initializes without error against the real backend.
- **partition-shape demo** — a small walkthrough showing the new shape
  end-to-end: dynamic partitions on a `external_snowflake_table` plus a
  `PerPartitionBackfillJob` driving multi-tenant rebuilds. Closes the loop
  on the original consumer feedback.

The web UI's "Trust & feedback" surface reads from `manifest.json`'s
`validation.level` field — every new walkthrough should bump that for
its components.

## Op-job category — mostly shipped

The initial ideas here have landed. Current op-job families in `jobs/`:

- **Lineage catalog exports** (7): `openlineage`, `openmetadata`, `alation`,
  `collibra`, `datahub`, `purview`, `data360`, `webhook_lineage`. Each is
  an op-shaped sibling of the corresponding `lineage_to_*` asset sink.
- **Event log exports** (5): `event_log_to_{s3,bigquery,snowflake,datadog,splunk}`.
- **Run / audit warehouse exports** (2): `run_history_to_warehouse_job`,
  `dagster_audit_to_warehouse_job` (Dagster+ audit log).
- **Cleanup / operational hygiene** (6): `dagster_run_prune_job`,
  `dagster_asset_materialization_prune_job`, `dagster_check_results_prune_job`,
  `dagster_compute_logs_archive_job`, `dagster_stale_partition_cleanup_job`,
  `stuck_run_terminator_job`.

Remaining candidates (not urgent):

- **Cost telemetry export** — push run-cost metrics (compute time × tier)
  to a billing system.
- **Compliance snapshot export** — periodic snapshot of which assets ran
  with what data classification, for audit trails.

## Dagster+ CLI: custom selections + custom metrics (via GraphQL)

We already ship a CLI for publishing **alert policies** to a Dagster+
deployment. We need matching CLIs for the other two things ops teams
maintain out-of-band:

- **`publish-custom-selections` CLI** — takes a YAML/JSON manifest of
  custom asset selections and PUTs them to the deployment via the
  Dagster+ GraphQL API. Idempotent (upsert by name).
- **`publish-custom-metrics` CLI** — same shape for custom metrics
  (Insights). Manifest describes name / definition / units / etc.,
  posts via GraphQL.
- **Unified `dcc publish` CLI** — takes a directory or single manifest,
  detects which of the three (alert policies / selections / metrics)
  each file is, and dispatches to the right subcommand. One entrypoint
  for GitOps-shaped "deploy my Dagster+ config" flows.

All three should reuse the existing GraphQL client + auth pattern from
the alert-policy CLI. Land under `tools/` or a new `cli/` dir.

## Pipeline components: opt-in multi-asset / step-visibility mode

Every `*_pipeline` component (snowpark_pipeline, polars_pipeline,
pyspark_pipeline, warehouse_pipeline, ml_pipeline, agentic_pipeline)
today produces exactly one Dagster asset per pipeline. Users can't
see individual step progress in the graph — they see one long-running
asset materialize.

Add an opt-in flag (`expose_steps: true` or `mode: multi_asset`) that
turns the pipeline into a `@dg.multi_asset` where each `steps[]` entry
becomes its own asset in the graph. Not subsettable (we still build one
compiled instruction per run — Snowpark builds one query plan, polars
one lazy frame, etc.), but at least users get:
- Per-step status color in the graph
- Per-step logs attributed correctly
- Per-step metadata (row counts, timings)
- Clear picture of what the pipeline actually does

Default stays `expose_steps: false` (one asset per pipeline) to avoid
breaking existing users. Non-subsettability + one-shot-execution
should be called out clearly in the schema description so users
understand the constraint.

## snowpark_pipeline: `ml` op follow-ups

The initial `ml` op (kmeans/xgboost/etc. via snowflake-ml-python) shipped
in commit `b8315af9`. Snowflake Model Registry integration landed in
snowpark_pipeline v1.2.0 — fit-mode ops persist to the Registry when
`model_name` is set, predict/transform ops load a versioned model by
`model_name` + `model_version` (default `latest`). Remaining follow-ups:

- **Training metrics as MaterializeMetadata** — after `fit_predict`,
  emit `snowpark/ml/silhouette` (KMeans), `snowpark/ml/r2` (regression),
  `snowpark/ml/accuracy` (classification), etc. Currently no evaluation
  metrics surface on the asset.
- **Train/test split helper** — add a `split` op (or extend `sample`)
  so users can fit on train + predict on test in one pipeline
  without needing a separate op.
- **Model metadata in the asset** — surface `snowpark/ml/algorithm`,
  `snowpark/ml/hyperparameters`, `snowpark/ml/input_columns` as
  MaterializeMetadata on every ml-containing pipeline for
  discoverability.

## Model-persistence story for the other `*_pipeline` components

The snowpark_pipeline `ml` op now has a full train-once/predict-often
story via the Snowflake Model Registry (v1.2.0). Each other pipeline
component should get an equivalent MLOps story wired against the
native registry for its runtime — same `model_name` / `model_version`
/ `mode: fit|predict|transform` shape, so users can move between
pipelines without relearning the fields.

Per-pipeline target backend:

- ~~**ml_pipeline** (scikit-learn / xgboost / lightgbm) — MLflow Model
  Registry~~ — shipped in ml_pipeline v1.2.0. `register_model` /
  `load_model` ops with `backend: mlflow | snowflake`. MLflow requires
  `tracking_uri` (or `tracking_uri_env_var`) — hard-fails rather than
  silently writing to `file:./mlruns/` (which vanishes on ephemeral
  compute). Snowflake backend uses `snowflake-ml-python`'s Registry
  with a `connection:` dict matching snowpark_pipeline.
- **pyspark_pipeline** — MLflow again (`mlflow.spark.log_model`), or
  Databricks Model Registry when the runtime is Databricks.
  `pyspark.ml.PipelineModel.save(...) + .load(...)` for the
  no-MLflow fallback. Same op names + `backend:` selector as
  ml_pipeline (add `databricks-managed-mlflow` as a third backend).
- **agentic_pipeline** — the analog isn't "model weights" but
  **planner state**: the compiled plan (tool sequence, prompts,
  temperatures). Already partly done via PlannedCatalogAgent's
  StateBackedComponent. Formalize as `plan_name` / `plan_version`
  with a JSON blob store (Snowflake stage, S3, or Dagster state
  backend).
- **polars_pipeline** — no ML today. If we ever add an `ml` op there
  (via `polars-ml` or scikit-learn round-trip through arrow), same
  shape.
- **warehouse_pipeline** — dialect-specific. BigQuery ML has its own
  `CREATE MODEL` + `ML.PREDICT`; Snowflake covered by snowpark; other
  warehouses (Redshift ML, Postgres via MADlib) are more manual. Punt
  until there's real demand.

Uniform op shape across pipelines:

```yaml
- op: ml
  mode: fit | predict | fit_predict | transform | fit_transform
  algorithm: <family-specific>
  input_columns: [...]
  # persistence
  model_name: <required to persist/load>
  model_version: auto | latest | <literal>
  # backend-specific overrides
  registry_uri: <optional; MLflow tracking URI, etc.>
```

Ship order (updated): ~~ml_pipeline~~ done. Next: **pyspark_pipeline**,
then agentic_pipeline planner persistence formalization.

## Partition shape rework — Phase 1 item 5 (strict validation)

Items 1–4 of the partition rework landed. Item 5 — Pydantic
`model_validator(mode="after")` enforcing the rules below — is the only
piece outstanding. Additive change, doesn't require touching every
component again.

- `partition_type=dynamic` requires `dynamic_partition_name`.
- `partition_type=multi` (legacy shape) requires `partition_values`.
- Time-based types (`daily`/`weekly`/`monthly`/`hourly`) require
  `partition_start`. Currently silently default to `2024-01-01`.
- `partition_dimensions` and the flat fields are mutually informative:
  setting both should raise a clear error rather than silently choosing
  one.

## Demo runtime issues — secondary

`setup_analytics_demo.sh`:
- `pip_output` (point_in_polygon) — works against a public Natural Earth
  states geojson URL, but that's an external dependency. Could ship a
  small bundled geojson with the demo for hermetic tests.

`setup_transformations_demo.sh`:
- `orders_in_duckdb` is a custom asset (not a component) that
  occasionally fails under the multiprocess executor due to duckdb file
  lock contention with parallel tasks. Race condition, retry usually
  passes. Could serialize against a duckdb-touching tag.
