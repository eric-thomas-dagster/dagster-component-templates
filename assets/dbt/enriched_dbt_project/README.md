# Enriched dbt Project Component

Drop-in replacement for `dagster_dbt.DbtProjectComponent` that reads the
dbt manifest and layers a set of opt-in enrichments on top: real
`FreshnessPolicy` derivation, exposures / semantic layer / mesh stubs as
observable `AssetSpec`s, contract asset checks, per-model config via
`meta.dagster.*`, and rich metadata surfacing.

Every field is opt-in. Set none of them and the component behaves
identically to the base `DbtProjectComponent`.

## Configuration surface

### dbt project

| Field | Default | What |
|---|---|---|
| `project` | *required* | Path to dbt project (containing `dbt_project.yml`) |
| `cli_args` | `["build"]` | dbt CLI args |
| `select` | | dbt selection string |
| `exclude` | | dbt exclusion string |
| `translation` | | Per-node translation config |
| `manifest_path` | `{project}/target/manifest.json` | Override manifest path |

### Metadata surfacing (attach as JSON metadata on each asset)

| Field | What |
|---|---|
| `dbt_docs_url` | Base URL of your hosted dbt docs. Each asset gets a clickable `{url}/#!/{resource_type}/{unique_id}` link |
| `include_exposures` | Attach exposures list |
| `include_metrics` | Attach metric definitions |
| `include_semantic_models` | Attach semantic model definitions |
| `include_contracts` | Attach contract config (enforced flag + column constraints) |
| `include_meta` | Attach full `node.meta` dict (minus the `dagster` subkey) |
| `include_source_freshness` | Attach source freshness thresholds + loader |
| `include_doc_blocks` | Resolve `{{ doc() }}` refs and embed contents |

### Real behavior (change what Dagster emits or how it evaluates assets)

| Field | What |
|---|---|
| `emit_exposures_as_assets` | Emit dbt exposures as observable `AssetSpec`s with real deps on upstream models. Kind is `dashboard` / `notebook` / `analysis` / `ml` / `application` per `exposure.type` for distinct UI icons. |
| `emit_semantic_layer_as_assets` | Emit dbt `semantic_models` + `metrics` as observable `AssetSpec`s (kinds `semantic_model` / `metric`). semantic_models dep on their upstream model; metrics dep on the semantic models they aggregate. |
| `emit_contract_checks` | For every model with `config.contract.enforced: true`, emit one `AssetCheckSpec` per column constraint (`not_null`, `unique`, `primary_key`, `foreign_key`, `check`). |
| `external_packages` | dbt mesh: emit observable stub `AssetSpec`s for models whose `package_name` matches. Pair with `exclude: 'package:X'` so this project's dbt run doesn't try to rebuild them — downstream lineage still renders because the upstream Dagster code location merges in. |
| `enable_materialization_kinds` | Add each model's `config.materialized` value (`table` / `view` / `incremental` / `materialized_view` / `ephemeral` / `seed` / `snapshot`) as a Dagster kind for distinct UI icons. |
| `derive_freshness_policies` | Attach a real `FreshnessPolicy` to sources (from `sources.freshness.warn_after/error_after`) and to models (from dbt 1.9+ `config.freshness.build_after` and/or dbt State `config.state.lag_tolerance`; `fail_window = max(build_after, lag_tolerance)`). Honors explicit `meta.dagster.freshness_policy` overrides. |
| `auto_trigger_on_freshness_failure` | With `derive_freshness_policies`: also attach `AutomationCondition.freshness_failed()` so Dagster triggers the rebuild when the derived policy fails. |
| `derive_lag_tolerance_automation` | For models with `config.state.lag_tolerance` (and no user-supplied `automation_condition`): attach `.newly_updated().since(cron_tick_passed(cron))` where the cron is snapped from lag_tolerance (`30m → */30 * * * *`, `4h → 0 */4 * * *`, `1d → 0 0 * * *`). Dagster drives the trigger; dbt still enforces the exact gate on its own build step. |
| `code_version_strategy` | `disabled` (default) / `hash` / `sqlglot`. `hash` uses dbt's manifest `checksum.checksum` (bumps on any file edit including whitespace). `sqlglot` parses `compiled_code`, strips comments + normalizes whitespace, then hashes — semantic-only versioning that pairs with `AutomationCondition.code_version_changed()`. Falls back to `hash` if `sqlglot` isn't installed. |
| `asset_overrides` | Per-asset overrides keyed by asset key. Today supports `{depends_on: [...]}` to inject Dagster asset dependencies (e.g. external assets not managed by dbt). |

### Per-model config (read from dbt YAML `meta.dagster.*`)

Keep partition + automation + freshness config alongside the dbt project
instead of in Dagster YAML:

```yaml
# In your dbt schema.yml
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

- **Partitions:** `daily`, `hourly`, `weekly`, `monthly`, `static`, `dynamic`
- **Automation presets:** `eager`, `on_missing`, `any_downstream_conditions`, `on_deploy_if_code_changed`, or raw `cron: "0 9 * * *"`
- **Freshness shapes:** `time_window` (`fail_window_seconds`, `warn_window_seconds?`) or `cron` (`deadline_cron`, `lower_bound_delta_seconds`, `timezone?`)

User-supplied `meta.dagster.automation_condition` always wins over any
`auto_trigger_on_freshness_failure` / `derive_lag_tolerance_automation`
derivation.

## Full example

```yaml
type: dagster_community_components.EnrichedDbtProjectComponent
attributes:
  project: "{{ project_root }}/dbt_project"
  cli_args: [build]

  dbt_docs_url: "https://dbt-docs.internal.mycompany.com"

  # Metadata surfacing
  include_exposures: true
  include_contracts: true
  include_source_freshness: true

  # Real emission + policy attachment
  emit_exposures_as_assets: true
  emit_semantic_layer_as_assets: true
  emit_contract_checks: true
  enable_materialization_kinds: true

  # Freshness + automation
  derive_freshness_policies: true
  auto_trigger_on_freshness_failure: true      # freshness fails → Dagster rebuilds
  derive_lag_tolerance_automation: false        # (mutually exclusive with above)

  # Code version — sqlglot canonicalizes SQL so whitespace/comment edits
  # don't bump the version. Pairs with AutomationCondition.code_version_changed().
  code_version_strategy: sqlglot

  # dbt mesh: models imported from another dbt project
  external_packages: [shared_core]
  exclude: "package:shared_core"

  # External asset dep injection
  asset_overrides:
    fct_daily_pnl:
      depends_on: [fx_rates]
```

## Related

- **[`EnrichedDbtCloudWorkspaceComponent`](../enriched_dbt_cloud_workspace/README.md)** — same enrichment vocabulary for dbt Cloud
- **`DbtStateReusePatch`** — bridge component that monkey-patches dagster-dbt to treat `no-op` (state-reuse) and `partial success` (microbatch) as materialization events
- **`DbtCloudJobSensor`**, **`DbtRunJobComponent`**, **`DbtCloudTriggerJobComponent`** — job-shaped triggers for dbt runs
