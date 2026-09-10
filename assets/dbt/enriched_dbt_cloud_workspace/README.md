# Enriched dbt Cloud Workspace Component

A drop-in enrichment of `dagster_dbt.DbtCloudComponent`. Every opt-in field
defaults to off — set no flags and behavior is identical to the base
component. Turn on flags to layer in mid-run monitoring, job selection,
and the same manifest-based enrichments as
[`EnrichedDbtProjectComponent`](../enriched_dbt_project/README.md).

## What's in this vs the base `DbtCloudComponent`

| Feature | Base | Enriched |
|---|---|---|
| Mid-run per-model events (parse debug logs during execution) | no | **yes** — `monitor_runs: true` |
| Filter which Cloud jobs get mirrored (dbt-style selection DSL) | no | **yes** — `job_selection_include/exclude` |
| Freshness policies from `sources.freshness` + dbt 1.9+ `build_after` | no | **yes** — `derive_freshness_policies` |
| Emit exposures as observable AssetSpecs (with deps) | no | **yes** — `emit_exposures_as_assets` |
| Per-column AssetCheckSpec for enforced contracts | no | **yes** — `emit_contract_checks` |
| dbt mesh: emit stubs for `external_packages` | no | **yes** — `external_packages` |
| Metadata surfacing (`dbt_docs_url` + `include_*` flags) | no | **yes** |

## Fields

### Cloud-specific

| Name | Required | Default | Description |
|---|---|---|---|
| `workspace` | yes | — | `DbtCloudWorkspace` (`account_id`, `project_id`, `environment_id`, `token`) |
| `monitor_runs` | | `false` | Parse dbt Cloud debug logs during execution to yield per-model Output events as models complete |
| `fail_fast` | | `false` | With `monitor_runs=true`: cancel the dbt Cloud run on first failure |
| `poll_interval` | | `5.0` | Seconds between debug-log polls |
| `job_selection_include` | | | Selection string — jobs matching are mirrored. Selectors: `type:deploy`, `*_prod`, `id:12345`, or bare glob. Space-separated union. Empty = all. |
| `job_selection_exclude` | | | Jobs matching are dropped AFTER include |

### Enrichment (same as `EnrichedDbtProjectComponent`)

| Name | Default | Description |
|---|---|---|
| `dbt_docs_url` | | Base URL of hosted dbt docs / Cloud Explorer |
| `include_exposures` | `false` | Attach exposures list as metadata |
| `include_metrics` | `false` | Attach metrics as metadata |
| `include_semantic_models` | `false` | Attach semantic models as metadata |
| `include_contracts` | `false` | Attach contract config as metadata |
| `include_meta` | `false` | Attach full `node.meta` dict as metadata |
| `include_source_freshness` | `false` | Attach source freshness thresholds as metadata |
| `include_doc_blocks` | `false` | Resolve `{{ doc() }}` refs, embed contents |
| `emit_exposures_as_assets` | `false` | Real: emit exposures as observable AssetSpecs |
| `derive_freshness_policies` | `false` | Real: FreshnessPolicy on sources + build_after models |
| `emit_contract_checks` | `false` | Real: per-column AssetCheckSpec for enforced contracts |
| `external_packages` | | Real: dbt mesh stubs for imported package models |
| `asset_overrides` | | Per-asset override deps |

## Selection DSL examples

```yaml
job_selection_include: "type:deploy"                    # only deploy jobs
job_selection_include: "*_prod"                          # any job named *_prod
job_selection_include: "type:deploy type:merge"          # deploy OR merge (union)
job_selection_exclude: "type:ci"                         # everything except CI
job_selection_include: "*"
job_selection_exclude: "*_experimental"                  # everything except experimental
```

Selector forms:

- `type:<value>` — matches `job.job_type` exactly (`ci`, `deploy`, `merge`, `scheduled`, `other`)
- `name:<glob>` — fnmatch glob against `job.name`
- `id:<int>` — exact `job.id` match
- `<glob>` — bare token = shorthand for `name:<glob>`
- `*` (or empty) — matches every job

## Mid-run monitor

When `monitor_runs: true`, the component wraps each mirrored AssetsDefinition with
a `DbtCloudRunMonitor` that:

1. Triggers the dbt Cloud run via `workspace.cli(["build"], context=context)`
2. Polls the run's debug logs every `poll_interval` seconds
3. Parses per-model results (`N of M OK created ... SCHEMA.model_name`)
4. Yields Dagster `Output` events **as each model completes** — Dagster
   processes them (triggers alerts, updates the UI) then resumes the generator
5. On completion, yields remaining events from `run_results.json` for
   anything not already streamed (tests, missed models)

With `fail_fast: true`, the dbt Cloud run is cancelled on the first
failure and the Dagster run fails immediately. With `fail_fast: false`
(default), failures are logged in real time but the run continues so
all failures are captured in a single run.

If the Dagster run itself is cancelled mid-execution, the monitor also
cancels the dbt Cloud run so it doesn't keep consuming compute.

## Roadmap

**Phase 3+ (queued):**

- Semantic layer as observable AssetSpecs (`emit_semantic_layer_as_assets`)
- Polling sensor emits `AssetCheckEvaluation` events for dbt test results
  (ports `et/dbt-cloud-sensor-check-evaluations` PR)
- Mesh-aware polling sensor filters external_packages events
  (ports `et/dbt-cloud-sensor-mesh-aware` PR)
- `code_version_strategy: hash | sqlglot | disabled`
- Skip-reason metadata surfaced on materialization
- `dbt state explain` output as per-model metadata
- Configurable `lag_tolerance` on freshness derivation

## Provenance

Vendored from user PR branches — swap to upstream imports when merged:

- `_run_monitor.py` ← `et/dbt-cloud-monitor-runs` (via
  [`dbt-cloud-mesh-demo`](https://github.com/eric-thomas-dagster/dbt-cloud-mesh-demo)
  which mirrors the same shape)
- `_job_selection.py` ← `et/dbt-cloud-mirror-jobs-selection`
- Enrichment helpers (freshness / contract / exposure / external-package)
  ← `et/dbt-source-freshness-policies` + `et/dbt-model-freshness-automation-condition`
  + `et/dbt-contract-asset-checks` + `et/dbt-exposures-as-assets` + `et/dbt-mesh-external-packages`

## Companion

- **[`EnrichedDbtProjectComponent`](../enriched_dbt_project/README.md)** — same
  enrichment vocabulary for dbt Core (`project:` instead of `workspace:`).
