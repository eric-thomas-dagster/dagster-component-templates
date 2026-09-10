# Enriched dbt Project Component

A drop-in enrichment of `dagster_dbt.DbtProjectComponent`. Every opt-in field
defaults to off — install and set no flags, behavior is identical to the base
component. Turn on flags to layer in metadata, real freshness policies,
exposure lineage, contract checks, and dbt mesh support.

**Renamed from `DbtDocsEnrichedProjectComponent`** — the old class name is
kept as a backward-compat alias so existing YAML doesn't break. The rename
reflects that this component now does far more than surface docs metadata.

## Two modes (planned)

- **dbt Core** (via `project:`) — wired today. All Phase 1 enrichments live.
- **dbt Cloud** (via `dbt_cloud_workspace:`) — scaffolded but raises
  `NotImplementedError`. Phase 2 wires it in via composition with
  `DbtCloudComponent` and ports the mid-run monitor + selection DSL from
  [`dbt-cloud-mesh-demo`](https://github.com/eric-thomas-dagster/dbt-cloud-mesh-demo).

## Enrichments (Phase 1 — shipped)

### Metadata-only surfacing

| Flag | What it adds |
|---|---|
| `dbt_docs_url` | Clickable link to hosted dbt docs per node — `{url}/#!/{resource_type}/{unique_id}` |
| `include_exposures` | Downstream exposures list per model, as JSON metadata |
| `include_metrics` | Metric definitions per referenced model, as JSON metadata |
| `include_semantic_models` | Semantic model definitions per referenced model, as JSON |
| `include_contracts` | `contract_enforced` + column constraints, as metadata |
| `include_meta` | Full `node.meta` dict (minus the `dagster` subkey), as JSON |
| `include_source_freshness` | Source freshness thresholds + `loaded_at_field` + loader, as metadata |
| `include_doc_blocks` | Resolves `{{ doc() }}` references, embeds contents as metadata |

### Real behavior

| Flag | What it does |
|---|---|
| `emit_exposures_as_assets` | Emits each dbt exposure as an observable `AssetSpec` with real `AssetDep`s on its upstream models. Adds downstream lineage — "if this model breaks, which dashboards are affected?" Kind is derived from `exposure.type` (`dashboard` / `notebook` / `analysis` / `ml` / `application`) so the UI renders a distinct icon. |
| `derive_freshness_policies` | Attaches a real `FreshnessPolicy` to sources (from `sources.freshness.{warn_after, error_after}`) and to dbt 1.9+ models (from `config.freshness.build_after`). Also honors explicit `meta.dagster.freshness_policy` config in either `time_window` or `cron` shape — user config wins over derivation. |
| `emit_contract_checks` | For every model with `config.contract.enforced: true`, emits one `AssetCheckSpec` per column constraint (`not_null`, `unique`, `primary_key`, `foreign_key`, `check`). Contract violations surface as failing checks. |
| `external_packages` | dbt mesh: emit observable stub `AssetSpec`s for models whose `package_name` matches. Pair with `exclude: 'package:X'` on the base component so this project's dbt run doesn't try to rebuild them. Downstream lineage still renders even though the upstream is owned by a different Dagster code location. |

### Per-model config (read from dbt YAML `meta.dagster.*`)

```yaml
# In your dbt schema.yml — no Dagster YAML change needed
- name: fct_fuel_margin_daily
  config:
    meta:
      dagster:
        partitions_def:
          type: daily
          start_date: "2025-08-01"
        automation_condition:
          preset: eager
        freshness_policy:
          type: time_window
          fail_window_seconds: 3600
          warn_window_seconds: 1800
```

Partition types supported: `daily`, `hourly`, `weekly`, `monthly`, `static`, `dynamic`.
Automation presets: `eager`, `on_missing`, `any_downstream_conditions`,
`on_deploy_if_code_changed`, or a raw `cron: "0 9 * * *"`.
Freshness shapes: `time_window` (fail_window_seconds, warn_window_seconds?) or
`cron` (deadline_cron, lower_bound_delta_seconds, timezone?).

## Fields

| Name | Required | Default | Description |
|---|---|---|---|
| `project` | yes | — | Path to dbt project (containing `dbt_project.yml`) |
| `cli_args` | | `["build"]` | dbt CLI args |
| `select` | | | dbt selection string |
| `exclude` | | | dbt exclusion string |
| `translation` | | | Per-node translation config |
| `dbt_docs_url` | | | Base URL of hosted dbt docs |
| `include_exposures` | | `false` | Metadata: attach exposures list |
| `include_metrics` | | `false` | Metadata: attach metrics |
| `include_semantic_models` | | `false` | Metadata: attach semantic models |
| `include_contracts` | | `false` | Metadata: attach contract config |
| `include_meta` | | `false` | Metadata: attach full `node.meta` |
| `include_source_freshness` | | `false` | Metadata: attach source freshness |
| `include_doc_blocks` | | `false` | Metadata: resolve + embed doc blocks |
| `manifest_path` | | `{project}/target/manifest.json` | Override manifest path |
| `asset_overrides` | | | Per-asset deps injection |
| `emit_exposures_as_assets` | | `false` | Real: emit exposures as AssetSpecs |
| `derive_freshness_policies` | | `false` | Real: attach FreshnessPolicy to sources + build_after models |
| `emit_contract_checks` | | `false` | Real: per-column AssetCheckSpec for enforced contracts |
| `external_packages` | | | Real: dbt mesh — emit stubs for imported package models |
| `dbt_cloud_workspace` | | | **Not yet wired** — Phase 2 |
| `job_filter` | | | Cloud mode — Phase 2 |
| `monitor_runs` | | `false` | Cloud mode mid-run monitor — Phase 2 |
| `fail_fast` | | `false` | Cloud mode — cancel on first failure — Phase 2 |
| `poll_interval` | | `5.0` | Cloud mode poll seconds — Phase 2 |

## Roadmap

**Phase 2 — dbt Cloud mode**

- Compose with `DbtCloudComponent` when `dbt_cloud_workspace` is set
- Port `DbtCloudRunMonitor` from `dbt-cloud-mesh-demo` — parses debug logs
  mid-run to yield per-model Output events as each model completes
- Selection DSL for filtering which Cloud jobs to mirror
- Mesh-aware exclusion (Cloud sensors filter external_packages events)

**Phase 3+ — additional enrichments**

- `emit_semantic_layer_as_assets` — semantic_models + metrics as AssetSpecs
- `code_version_strategy: hash | sqlglot | disabled` — sqlglot canonicalizes
  SQL before hashing, so whitespace / comment changes don't bump the version
- Materialization kinds (`table` / `view` / `incremental` chips in the UI)
- Explorer URL + opt-out SQL-in-description polish
- Slim-CI helpers (`defer_config`, `state_manifest_path` + dbt/state tags)
- Skip-reason metadata (surface `node.status == 'skipped'` reason on materialization)
- `dbt state explain` output as per-model metadata
- Configurable `lag_tolerance` on freshness policy derivation

## Related components

- **`DbtStateReusePatch`** — monkey-patches dagster-dbt to treat `no-op`
  (state-reuse) and `partial success` (microbatch) statuses as materialization
  events. Bridge component until [dagster#34010](https://github.com/dagster-io/dagster/pull/34010) merges + releases.
- **`DbtCloudJobSensor`**, **`DbtRunJobComponent`**, **`DbtCloudTriggerJobComponent`**
  — event-driven and job-shaped triggers for dbt runs.

## Provenance

Portable enrichment functions are vendored from PR branches by the maintainer.
When the upstream PRs merge and release, the vendored sections in
`component.py` can be deleted and replaced with imports from
`dagster_dbt.asset_utils` / `dagster_dbt.asset_specs`. Source branches:

- `et/dbt-exposures-as-assets`
- `et/dbt-source-freshness-policies`
- `et/dbt-model-freshness-automation-condition`
- `et/dbt-contract-asset-checks`
- `et/dbt-contract-metadata-json`
- `et/dbt-mesh-external-packages`
