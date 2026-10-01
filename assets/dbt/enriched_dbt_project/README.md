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
| `emit_source_assets` | Emit each dbt source as an observable external `AssetSpec` (kinds `dbt`, `source`). Sources become first-class Dagster nodes (freshness policies apply, downstream selectors work, `+<key>` returns the source). Merges with any upstream Fivetran / Sling / manual observable-source declaration at the same key. |
| `emit_semantic_layer_as_assets` | Emit dbt `semantic_models` + `metrics` as observable `AssetSpec`s (kinds `semantic_model` / `metric`). semantic_models dep on their upstream model; metrics dep on the semantic models they aggregate. |
| `emit_contract_checks` | For every model with `config.contract.enforced: true`, emit one `AssetCheckSpec` per column constraint (`not_null`, `unique`, `primary_key`, `foreign_key`, `check`). |
| `external_packages` | dbt mesh: emit observable stub `AssetSpec`s for models whose `package_name` matches. Pair with `exclude: 'package:X'` so this project's dbt run doesn't try to rebuild them — downstream lineage still renders because the upstream Dagster code location merges in. The stub's key is computed via this project's own configured translator (`translation` / `translation_settings`), not a bare model-name guess, so it matches the upstream project's real published key by default whenever both sides use an equivalent translation scheme — falls back to `meta.dagster.asset_key` / a bare name only if the translator call fails. |
| `enable_materialization_kinds` | Add each model's `config.materialized` value (`table` / `view` / `incremental` / `materialized_view` / `ephemeral` / `seed` / `snapshot`) as a Dagster kind for distinct UI icons. |
| `derive_freshness_policies` | Attach a real `FreshnessPolicy` to sources (from `sources.freshness.warn_after/error_after`) and to models (from dbt 1.9+ `config.freshness.build_after` and/or dbt State `config.state.lag_tolerance`; `fail_window = max(build_after, lag_tolerance)`). Honors explicit `meta.dagster.freshness_policy` overrides. |
| `auto_trigger_on_freshness_failure` | With `derive_freshness_policies`: also attach `AutomationCondition.freshness_failed()` so Dagster triggers the rebuild when the derived policy fails. |
| `derive_lag_tolerance_automation` | For models with `config.state.lag_tolerance` (and no user-supplied `automation_condition`): attach `.newly_updated().since(cron_tick_passed(cron))` where the cron is snapped from lag_tolerance (`30m → */30 * * * *`, `4h → 0 */4 * * *`, `1d → 0 0 * * *`). Dagster drives the trigger; dbt still enforces the exact gate on its own build step. |
| `default_automation_condition` | Fallback `AutomationCondition` applied when NOTHING else set one — lowest precedence, after per-model `meta.dagster.automation_condition`, `auto_trigger_on_freshness_failure`, and `derive_lag_tolerance_automation`. Same shape as `meta.dagster.automation_condition` (below). Lets a team declare its own default automation policy once, via YAML, instead of annotating every model or patching this component. |
| `code_version_strategy` | `disabled` (default) / `hash` / `sqlglot`. `hash` uses dbt's manifest `checksum.checksum` (bumps on any file edit including whitespace). `sqlglot` parses `compiled_code`, strips comments + normalizes whitespace, then hashes — semantic-only versioning that pairs with `AutomationCondition.code_version_changed()`. Falls back to `hash` if `sqlglot` isn't installed. |
| `state_manifest_path` | Path to a dbt state `manifest.json` (or a directory containing one). Enables the checksum-comparison enrichments below. |
| `include_state_explain` | Requires `state_manifest_path`. Attaches `dbt_state/state` (`new` / `unchanged` / `modified`) and `dbt_state/explanation` (human-readable reason) metadata per model. Build-time equivalent of the state-reuse case of `dbt state explain`. |
| `derive_state_tags` | Requires `state_manifest_path`. Adds a `dbt/state` tag with the same value so `tag:dbt/state=modified` selections work. |
| `asset_overrides` | Per-asset overrides keyed by either the serialized AssetKey string (e.g. `fct_daily_pnl`) or the dbt `unique_id` (e.g. `model.my_project.fct_daily_pnl`) — the latter is easier to get right since it doesn't depend on the translation scheme. Today supports `{depends_on: [...]}` to inject Dagster asset dependencies (e.g. external assets not managed by dbt, or a mesh stub whose computed key you want to double-check before relying on it). |
| `defer_config` | dbt slim CI config: `{state_path, defer, favor_state}`. Appends `--state <path> [--defer] [--favor-state]` to the dbt invocation so the run only builds changed models, deferring `ref()` resolution to the state's tables for unchanged upstream models. Pairs naturally with `state_manifest_path` (typically both point at the same state artifact). |

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

Automation condition precedence, highest to lowest:

1. per-model `meta.dagster.automation_condition` (above) — always wins
2. `auto_trigger_on_freshness_failure` → `freshness_failed()`
3. `derive_lag_tolerance_automation` → lag-tolerance-derived condition
4. `default_automation_condition` (component-level, same shape as #1) —
   applied only when nothing above set one

## `post_processing:` vs this component's fields

Dagster's own `post_processing:` block is available on **every** component's
`defs.yaml` — including the plain `dagster_dbt.DbtProjectComponent` directly,
no enrichment needed:

```yaml
type: dagster_dbt.DbtProjectComponent     # works without this enriched wrapper
attributes:
  project: "{{ project_root }}/dbt_project"
post_processing:
  assets:
    - target: "tag:team=finance"          # full asset-selection DSL: key, tag, kind, wildcard
      attributes:
        automation_condition: "{{ finance_default_automation() }}"   # a template_vars_module function
        owners: ["team:finance"]
```

**Use `post_processing:`** when you want to set a policy across a selection
of assets, from the Dagster-repo side, by tag/kind/key. It's the more
general, idiomatic mechanism and doesn't require this enriched component at
all. One thing to know: for anything other than `metadata`/`tags`, it
**replaces** the attribute unconditionally for every asset the selector
matches — there's no "only if unset", and `attributes.deps` replaces the
whole dependency list rather than appending to it.

**Use this component's fields instead** when:
- You want a default that backs off for any asset that already has its own
  `automation_condition` — `default_automation_condition` only fills the
  gap, it never clobbers a per-model override. `post_processing` can't
  express that without a hand-maintained excluding selector.
- The person declaring per-model policy owns the **dbt model**, not the
  Dagster repo — `meta.dagster.automation_condition` lives in dbt YAML,
  authored without touching Dagster code or needing to already know the
  computed Dagster asset key.
- You want to **add** one dependency without re-declaring a model's entire
  existing dep list — `asset_overrides.depends_on` appends; `post_processing`'s
  `attributes.deps` would require listing every real dbt-derived dep too.

Neither mechanism can fix a **wrong** AssetKey — that's why `external_packages`
above computes the mesh stub's key via the real configured translator rather
than relying on an attribute override to paper over a mismatch.

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
  default_automation_condition:                 # fallback for everything else
    preset: eager

  # Code version — sqlglot canonicalizes SQL so whitespace/comment edits
  # don't bump the version. Pairs with AutomationCondition.code_version_changed().
  code_version_strategy: sqlglot

  # dbt mesh: models imported from another dbt project
  external_packages: [shared_core]
  exclude: "package:shared_core"

  # External asset dep injection -- keyed by AssetKey string or unique_id
  asset_overrides:
    fct_daily_pnl:
      depends_on: [fx_rates]
    model.shared_core.customer_summary:          # equivalent, by unique_id
      depends_on: [fx_rates]
```

## dbt v2 / Fusion compatibility

dbt v2 (formerly "Fusion", GA September 2026; the Apache-2.0 subset is now
called dbt OSS) produces a manifest that's wire-compatible with dbt v1 —
same schema version, only additive new fields — with one exception: it
doesn't emit a top-level `child_map` key at all. This component builds that
mapping itself from each resource's `depends_on.nodes` whenever the
manifest doesn't supply one (same workaround `dagster-dbt` itself ships),
so `include_exposures` / `include_metrics` / `include_semantic_models` keep
working under either engine. Everything else here — `meta.dagster.*`,
`external_packages`, freshness/automation derivation — reads plain
manifest fields that exist under both engines unchanged.

## Related

- **[`EnrichedDbtCloudWorkspaceComponent`](../enriched_dbt_cloud_workspace/README.md)** — same enrichment vocabulary for dbt Cloud
- **`DbtStateReusePatch`** — bridge component that monkey-patches dagster-dbt to treat `no-op` (state-reuse) and `partial success` (microbatch) as materialization events
- **`DbtCloudJobSensor`**, **`DbtRunJobComponent`**, **`DbtCloudTriggerJobComponent`** — job-shaped triggers for dbt runs

[//]: # (FIELDS:START - auto-generated by tools/regen_readme_fields.py)

## Fields

### Other

| Field | Type | Default | Description |
|---|---|---|---|
| `dbt_cloud_workspace` | `Any` | — | — |

[//]: # (FIELDS:END)
