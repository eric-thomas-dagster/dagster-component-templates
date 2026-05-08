# TODO

Open work tracked across the registry. Closed items get deleted, not crossed
out — git log is the history.

## subscription_metrics + ad_spend_std need ins= wiring

Both components use `deps=` + manual `context.load_asset_value()` instead of
`ins={"role": AssetIn(...)}`. When all assets launch together, the manual
loader hits the storage path before the upstream has been written, and the
component raises. Same bug pattern as priority_scorer had — apply the same
fix (declare AssetIn, take upstream as kwarg, let Dagster's IO manager
handle the chaining).

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
  in the UI" smoke test rather than full materialization.
- **check (7)** — Great Expectations / Soda / etc.
- **resource (55)** — most are connection-handle wrappers; validation =
  resource initializes without error against the real backend.

The web UI's "Trust & feedback" surface reads from `manifest.json`'s
`validation.level` field — every new walkthrough should bump that for
its components.

## Op-job category: more "export to X" jobs

`jobs/openlineage_export_job` (just landed) is one example. Other
candidates that fit the same op-job pattern (run-shaped, not asset-shaped):

- **SIEM audit log export** — emit Dagster run events to Splunk / Datadog /
  Sumo on a schedule, separate from `dagster_plus_to_siem_job` which is
  cloud-specific.
- **Cost telemetry export** — push run-cost metrics (compute time × tier)
  to a billing system.
- **Compliance export** — periodic snapshot of which assets ran with what
  data classification, for audit trails.

These should land in `jobs/` alongside the existing cleanup / trigger jobs.

## Partition shape rework (Phase 1)

The registry's partition handling has known gaps from external consumer
feedback. See conversation history for the full diagnosis. Five concrete
items, in priority order:

1. **Dynamic partitions support across all 324 partition-aware templates.**
   Add `type: dynamic` to the partition-type enum + a `dynamic_partition_name`
   field. Required for multi-tenant SaaS use cases where partitions grow at
   runtime. Highest value of all the partition fixes.
2. **Generalize MultiPartitionsDefinition.** Currently hardcoded as
   `{"date": Daily, _dim: Static}`. Replace with a list of dimension specs
   so users can express `(tenant, date)`, `(static, static)`, `(dynamic, date)`.
3. **Add partitions_def support to all 21 `external_*` templates.** They
   currently declare-only with no partition surface; the implementation is
   `dg.AssetSpec(partitions_def=...)`.
4. **PerPartitionBackfillJobComponent** — add `dynamic_partitions_all` and
   `dynamic_partitions_subset(filter)` strategies. Replace
   `context.repository_def.assets_defs_by_key` (internal API) with
   `define_asset_job(selection=...)`. Add `concurrency_key_template: str`
   so per-partition runs get partition-derived concurrency keys.
5. **Strict validation.** Pydantic `model_validator(mode="after")` that
   fails on `partition_type=multi` without explicit dim, and on missing
   `partition_start` when type is time-based. No more silent `"segment"`
   default or `"2020-01-01"` start.

Each item is a self-contained PR. Items 1–3 will touch the most files
(~324 components); a codemod is the right tool, since the registry's
distribution model is one self-contained `component.py` per template
(no shared helper module).

## Demo runtime issues — secondary

These don't block validation of our pending changes, but are real bugs.

`setup_analytics_demo.sh`:
- `pip_output` (point_in_polygon) — needs a real geojson source; demo
  currently points at a public Natural Earth states URL, which works
  but is a fragile dependency.
- `gradient_boosting`, `nn`, `stepwise` — sklearn data-shape errors
  (continuous label vs classifier, n_features mismatch). Demo synthetic
  data may need richer columns to satisfy these models in their default
  configs.
