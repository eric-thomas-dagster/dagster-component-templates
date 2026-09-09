# TODO

Open work tracked across the registry. Closed items get deleted, not crossed
out — git log is the history.

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
