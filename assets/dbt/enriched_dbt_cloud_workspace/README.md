# Enriched dbt Cloud Workspace Component

Drop-in replacement for `dagster_dbt.DbtCloudComponent` that adds a
mid-run per-model monitor, dbt-style job-selection DSL, `mirror_jobs`
modes for surfacing Cloud jobs as Dagster assets or `@job`s (or both),
and the same manifest-based enrichments as
[`EnrichedDbtProjectComponent`](../enriched_dbt_project/README.md) —
`FreshnessPolicy` derivation, exposures / semantic layer / mesh stubs,
contract asset checks, rich metadata.

Every field is opt-in. Set none of them and the component behaves
identically to the base `DbtCloudComponent`.

## Configuration surface

### dbt Cloud workspace

| Field | Default | What |
|---|---|---|
| `workspace` | *required* | `DbtCloudWorkspace` (`account_id`, `project_id`, `environment_id`, `token`) |
| `select` | | dbt selection string |
| `exclude` | | dbt exclusion string |
| `translation` | | Per-node translation config |
| `manifest_path` | (fetched via workspace) | Override the manifest.json path used for enrichment |

### Mid-run per-model monitor

| Field | Default | What |
|---|---|---|
| `monitor_runs` | `false` | Parse dbt Cloud debug logs during execution and yield per-model `Output` events as each model completes — instead of waiting for the entire job to finish. Enables mid-run alerting via Dagster+. |
| `fail_fast` | `false` | With `monitor_runs=true`: cancel the dbt Cloud run on the first model failure and fail the Dagster run immediately. |
| `poll_interval` | `5.0` | Seconds between debug-log polls. Lower catches failures faster but makes more API calls. |

When the Dagster run is cancelled mid-execution, the monitor also
cancels the dbt Cloud run so it doesn't keep consuming compute.

### Mirror Cloud jobs

| Field | Default | What |
|---|---|---|
| `mirror_jobs` | `off` | `off` / `asset` / `job` / `both`. See table below. |
| `job_trigger_defaults` | | Trigger overrides sent by every mirrored `@job` (applies with `mirror_jobs` = `job` or `both`). Fields: `cause`, `steps_override`, `git_sha`, `git_branch`, `schema_override`, `threads_override`. Any unset field falls back to the Cloud job's configured value. |
| `job_selection_include` | | Selection string; jobs matching are mirrored. Selectors: `type:deploy`, `*_prod`, `id:12345`, or bare glob (name-glob shorthand). Space-separated union. Empty = mirror all. |
| `job_selection_exclude` | | Jobs matching are dropped AFTER include. |

| `mirror_jobs` mode | What Dagster emits per Cloud job |
|---|---|
| `off` | Nothing (backward-compatible default) |
| `asset` | One observable `AssetSpec` (kind: `dbt_cloud_job`). Downstream AutomationConditions react when the job runs. Materialization events flow through the polling sensor. |
| `job` | One Dagster `@job` that triggers + waits for the Cloud run. Schedulable, launchable from the UI, wireable to `@run_status_sensor` downstream. |
| `both` | Both AssetSpec + launchable `@job` |

Dagster's internal `DAGSTER_ADHOC_JOB__*` pool is filtered out
automatically.

**Selection DSL examples:**

```yaml
job_selection_include: "type:deploy"                    # only deploy jobs
job_selection_include: "*_prod"                          # any job named *_prod
job_selection_include: "type:deploy type:merge"          # deploy OR merge (union)
job_selection_exclude: "type:ci"                         # everything except CI
```

### Metadata surfacing (attach as JSON metadata on each asset)

| Field | What |
|---|---|
| `dbt_docs_url` | Base URL of your hosted dbt docs or dbt Cloud Explorer URL. Each asset gets a clickable `{url}/#!/{resource_type}/{unique_id}` link |
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
| `emit_exposures_as_assets` | Emit dbt exposures as observable `AssetSpec`s with real deps on upstream models. Kind is `dashboard` / `notebook` / `analysis` / `ml` / `application`. |
| `emit_source_assets` | Emit each dbt source as an observable external `AssetSpec` (kinds `dbt`, `source`). Merges with upstream Fivetran / Sling declarations at the same key. |
| `emit_semantic_layer_as_assets` | Emit dbt `semantic_models` + `metrics` as observable `AssetSpec`s (kinds `semantic_model` / `metric`). |
| `emit_contract_checks` | For every model with `config.contract.enforced: true`, emit one `AssetCheckSpec` per column constraint. |
| `external_packages` | dbt mesh: emit observable stub `AssetSpec`s for models whose `package_name` matches. Pair with `exclude: 'package:X'`. The stub's key is computed via this component's own configured translator, not a bare model-name guess, so it matches the upstream project's real key when both sides share a translation scheme. |
| `external_package_workspaces` | When the two projects' manifests resolve the same mesh model's key differently (see below): the fully correct fix, when silver and gold share one dbt Cloud account/token. Keyed by package name, each entry `{project_id, environment_id}`. |
| `external_package_schemas` | Same problem, package-level fallback: a default `config.schema` to assume for every model in a package when the real workspace isn't configured. Keyed by package name. |
| `enable_materialization_kinds` | Add each model's `config.materialized` value (`table` / `view` / `incremental` / `materialized_view` / `ephemeral` / `seed` / `snapshot`) as a Dagster kind. |
| `derive_freshness_policies` | Attach a real `FreshnessPolicy` to sources (from `sources.freshness.warn_after/error_after`) and to models (from dbt 1.9+ `config.freshness.build_after` and/or dbt State `config.state.lag_tolerance`). Honors explicit `meta.dagster.freshness_policy` overrides. |
| `auto_trigger_on_freshness_failure` | With `derive_freshness_policies`: also attach `AutomationCondition.freshness_failed()` so Dagster triggers the rebuild when the derived policy fails. |
| `derive_lag_tolerance_automation` | For models with `config.state.lag_tolerance`: attach `.newly_updated().since(cron_tick_passed(cron))` where the cron is snapped from lag_tolerance. |
| `default_automation_condition` | Fallback `AutomationCondition` applied when nothing else set one — lowest precedence, after per-model `meta.dagster.automation_condition` (new: see below), `auto_trigger_on_freshness_failure`, and `derive_lag_tolerance_automation`. Same shape as `meta.dagster.automation_condition`. Lets a team declare its own default policy via YAML instead of patching this component. |
| `code_version_strategy` | `disabled` / `hash` / `sqlglot`. `sqlglot` parses `compiled_code`, strips comments + normalizes whitespace, then hashes — whitespace/comment edits don't bump. Pairs with `AutomationCondition.code_version_changed()`. |
| `state_manifest_path` | Path to a dbt state `manifest.json` (or a directory containing one). Enables the checksum-comparison enrichments below. |
| `include_state_explain` | Requires `state_manifest_path`. Attaches `dbt_state/state` + `dbt_state/explanation` metadata per model — build-time equivalent of the state-reuse case of `dbt state explain`. |
| `derive_state_tags` | Requires `state_manifest_path`. Adds a `dbt/state` tag with `new` / `unchanged` / `modified` so `tag:dbt/state=modified` selections work. |
| `asset_overrides` | Per-asset overrides keyed by either the asset key or the dbt `unique_id`. Supports `{depends_on: [...]}` to inject Dagster asset dependencies, and `{asset_key: '...'}` to force an `external_packages` mesh stub's own key to a specific value — see the mesh-key-mismatch note below. |

### Per-model automation condition (new — read from `meta.dagster.automation_condition`)

Previously, this component had NO way to set a per-model automation
condition from dbt YAML at all — only the component-level
`auto_trigger_on_freshness_failure` / `derive_lag_tolerance_automation`
flags, which apply uniformly. Now it supports the same `meta.dagster.*`
escape hatch as `EnrichedDbtProjectComponent`:

```yaml
# In your dbt model's config
config:
  meta:
    dagster:
      automation_condition:
        preset: eager
```

Supported: `{preset: eager | on_missing | any_downstream_conditions |
on_deploy_if_code_changed}` or `{cron: "0 9 * * *"}`. Always wins over
every component-level automation field above.

## `post_processing:` vs this component's fields

Dagster's own `post_processing:` block is available on **every** component's
`defs.yaml` — including the plain `dagster_dbt.DbtCloudComponent` directly,
no enrichment needed:

```yaml
type: dagster_dbt.DbtCloudComponent       # works without this enriched wrapper
attributes:
  workspace: ...
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

`post_processing` can't fix a **wrong** AssetKey at all — that's why
`external_packages` above computes the mesh stub's key via the real
configured translator rather than relying on an attribute override to paper
over a mismatch. The three mechanisms below *can*, for the one class of
mismatch the translator fix can't close on its own.

### When a mesh stub's key still doesn't line up

The translator-based fix makes `external_packages` compute a stub's key the
same way `dagster-dbt` computes any key: `[config.schema, name]` when the
model configures a schema, else just `[name]`. That's "same logic" — but
the two projects' manifests are two separate `dbt parse` runs, and nothing
guarantees they resolve `config.schema` for the *same* model identically.
Schema-name generation (`generate_schema_name`, custom macros, vars,
target/profile) can legitimately differ between "parsed as the real
project" and "parsed as an installed package dependency inside someone
else's project" — most commonly, the downstream project's manifest simply
never carries the upstream model's schema config override at all, so the
stub falls back to a bare name while the upstream project's own code
location publishes a schema-nested key. Dagster then sees two unrelated
assets instead of one, and lineage across the mesh boundary silently stops.

Check this by comparing the stub's key (grep the downstream project's
`manifest.json` for the model's `unique_id`, look at its `config.schema`)
against the real asset's key (same `unique_id`, upstream project's own
`manifest.json`). Four fixes, cheapest/most-automatic first:

**0. `translation` — dagster-dbt's own key-template field, if you're
comfortable changing key computation project-wide.** This isn't something
this component adds — it's the base `DbtCloudComponent`'s standard
`translation` field, and this component passes it through unmodified. It
takes a Jinja template per `AssetSpec` attribute, with the raw dbt manifest
node available as `node`. The mismatch above exists because
`config.schema` is only set when a model explicitly configures a custom
schema, while dbt's fully-resolved physical schema (the one SQL actually
compiles against) is always on the node as a top-level field. Falling back
to it closes the gap at the source, for every model, with no per-package
config:

```yaml
translation:
  key: >-
    {% if node.resource_type == 'source' %}{{ node.source_name }}/{{ node.name }}
    {%- else %}{{ node.config.schema or node.schema }}/{{ node.name }}{% endif %}
```

Two things to weigh before reaching for this first: it changes the key of
**every** asset in the workspace, not just mesh stubs — review the diff
against your existing selectors/`asset_overrides`/downstream references
before shipping it. And it only closes the mismatch automatically if the
*upstream* project's own code location applies an equivalent template to
its real assets (or already nests by schema today) — a stub computed this
way still needs a real asset published at the same key to line up with.
When both sides can agree on one `translation` template, this is the
cleanest fix of the four: no package list to maintain, no new field to
learn, nothing specific to mesh at all.

**1. `external_package_workspaces` — the fully correct fix, when the two
projects share one dbt Cloud account/token (the common setup).** Point at
the upstream package's own `project_id`/`environment_id` — not a file path,
no separate credentials, it reuses `workspace`'s own account/token:

```yaml
external_package_workspaces:
  shared_core:
    project_id: "{{ env.SILVER_DBT_CLOUD_PROJECT_ID }}"
    environment_id: "{{ env.SILVER_DBT_CLOUD_ENVIRONMENT_ID }}"
```

This fetches silver's REAL manifest live (a second `DbtCloudWorkspace`
triggers an ad hoc `dbt parse` job in silver's project, the same mechanism
this component already uses for its own manifest) and runs the real
translator against it directly — exact parity by construction, for every
model in the package, no per-model or per-package guessing. Falls back to
mechanism 2, then 3, if the fetch fails (different account, no Cloud
access, a transient API error) — logged as a warning, never a hard failure.

**2. `external_package_schemas` — a package-level default, when the real
workspace isn't configured or reachable.** If the whole package shares one
schema (the common case), set it once instead of one `asset_overrides`
entry per model:

```yaml
external_package_schemas:
  shared_core: silver_cursorapi
```

This only fills in a schema that's genuinely *missing* from the compiled
node — it never clobbers a schema the manifest already has, and it doesn't
help a model whose real key doesn't fit the package's default.

**3. `asset_overrides.asset_key` — a manual, per-model override for the
rare exception**, keyed by the model's dbt `unique_id` (stable across both
manifests, unlike the computed key). Always wins over both mechanisms
above:

```yaml
asset_overrides:
  model.shared_core.filtered_usage_events:
    asset_key: "silver_cursorapi/filtered_usage_events"   # the upstream project's real published key
```

Renaming assets in Dagster-side post-processing can't help with any of
this, since `post_processing` operates on already-built specs by selector,
after the mismatch has already prevented the two assets from being
recognized as one.

### Enhanced polling sensor

When any of the flags below is set, the base OOTB polling sensor is replaced
with an enhanced version that walks recent dbt Cloud runs and emits richer
events.

| Field | What |
|---|---|
| `emit_test_check_evaluations` | Emit `AssetCheckEvaluation` (pass/fail/warn) for every model result in each finished Cloud run. Externally-triggered runs (Cloud schedule, Cloud UI) surface test outcomes as check tiles in the Dagster UI instead of being dropped. |
| `emit_skip_reason_observations` | For every skipped model in `run_results.json`, emit an `AssetObservation` carrying `skip_reason` (from dbt's `message` field) as metadata. Engineers see WHY a model was skipped (`parent failed`, `state-reuse`, `microbatch batch skipped`, selector exclusion, etc.) without opening dbt Cloud. |
| `filter_external_packages_from_sensor` | Skip sensor events for models whose `package_name` is in `external_packages` — the upstream Dagster code location owns them. Prevents duplicate materializations when both projects' sensors observe the same run. |

## Full example

```yaml
type: dagster_community_components.EnrichedDbtCloudWorkspaceComponent
attributes:
  workspace:
    account_id: "{{ env.DBT_CLOUD_ACCOUNT_ID }}"
    project_id: "{{ env.DBT_CLOUD_PROJECT_ID }}"
    environment_id: "{{ env.DBT_CLOUD_ENVIRONMENT_ID }}"
    token: "{{ env.DBT_CLOUD_TOKEN }}"

  # Mid-run monitor
  monitor_runs: true
  fail_fast: false
  poll_interval: 5.0

  # Mirror Cloud jobs
  mirror_jobs: both
  job_trigger_defaults:
    cause: "Triggered by Dagster"
  job_selection_include: "type:deploy type:merge"
  job_selection_exclude: "*_experimental"

  # Metadata surfacing
  dbt_docs_url: "https://cloud.getdbt.com/accounts/12345/develop/12345/docs"
  include_exposures: true
  include_contracts: true

  # Real emission + policy attachment
  emit_exposures_as_assets: true
  emit_semantic_layer_as_assets: true
  emit_contract_checks: true
  enable_materialization_kinds: true

  # Freshness + automation
  derive_freshness_policies: true
  auto_trigger_on_freshness_failure: true
  code_version_strategy: sqlglot
  default_automation_condition:        # fallback for everything else
    preset: eager

  # dbt mesh
  external_packages: [shared_core]
  exclude: "package:shared_core"

  # Mesh stub key mismatch: cheapest/most-automatic fix first (see "When a
  # mesh stub's key still doesn't line up" above) -- only set ONE of these
  # for a given package, whichever fits your setup.
  external_package_workspaces:                    # the fully correct fix, same dbt Cloud account
    shared_core:
      project_id: "{{ env.SILVER_DBT_CLOUD_PROJECT_ID }}"
      environment_id: "{{ env.SILVER_DBT_CLOUD_ENVIRONMENT_ID }}"
  external_package_schemas:                       # package-level fallback otherwise
    shared_core: silver_cursorapi

  # External asset dep injection -- keyed by AssetKey string or unique_id
  asset_overrides:
    model.shared_core.customer_summary:
      depends_on: [fx_rates]
    model.shared_core.filtered_usage_events:     # manual override for the rare exception
      asset_key: "silver_cursorapi/filtered_usage_events"
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

## dbt State vs `state_manifest_path` — these are two different things

Two unrelated mechanisms share the word "state" here:

- **`state_manifest_path` / `include_state_explain` / `derive_state_tags`**
  are built on the mechanism dbt has always shipped for free: two
  `manifest.json` files diffed by checksum. No paid dependency — works
  identically on dbt OSS.
- **`config.state.lag_tolerance`** (read by `derive_freshness_policies` /
  `derive_lag_tolerance_automation`) is a config key belonging to **dbt
  State**, dbt Labs' separate, paid, usage-based product (billed per Daily
  Active Target Table), where a backend service decides whether to skip a
  rebuild. This component only *reads* that config value as a plain number
  from the manifest — it never calls dbt State's service, so setting
  `lag_tolerance` works as a Dagster-side hint whether or not you're
  actually paying for the add-on.

**If Dagster is already orchestrating the upstream load, you likely don't
need `lag_tolerance` or dbt State at all.** `lag_tolerance` exists to solve
a problem specific to dbt not knowing when upstream data actually finished
loading — it has to wait/poll and guess. Dagster doesn't have that
problem: if the upstream load is itself a Dagster asset, wire a real
`deps:` edge to it and let a plain `AutomationCondition.eager()` (or your
own condition) fire the moment that asset actually materializes — exact
timing, no heuristic, no paid add-on required. Reach for
`derive_lag_tolerance_automation` only when the upstream load is genuinely
outside Dagster's visibility (an external system with no corresponding
Dagster asset) and dbt's own `lag_tolerance` config is the only signal
available at all.

## Related

- **[`EnrichedDbtProjectComponent`](../enriched_dbt_project/README.md)** — same enrichment vocabulary for dbt Core
- **`DbtStateReusePatch`** — bridge patch for no-op / partial-success statuses
- **`DbtCloudJobSensor`**, **`DbtCloudTriggerJobComponent`** — event-driven and job-shaped triggers

[//]: # (FIELDS:START - auto-generated by tools/regen_readme_fields.py)

## Fields

### Source / target

| Field | Type | Default | Description |
|---|---|---|---|
| `select` | `str` | — | — |
| `filter_external_packages_from_sensor` | `bool` | `false` | — |

### Other

| Field | Type | Default | Description |
|---|---|---|---|
| `workspace` | `Any` | — | — |
| `translation` | `Any` | — | — |
| `exclude` | `str` | — | — |
| `monitor_runs` | `bool` | `false` | — |
| `fail_fast` | `bool` | `false` | — |
| `poll_interval` | `float` | `5.0` | — |
| `mirror_jobs` | `Literal['off', 'asset', 'job', 'both']` | `"off"` | — |
| `job_trigger_defaults` | `Dict[str, Any]` | — | — |
| `job_selection_include` | `str` | — | — |
| `job_selection_exclude` | `str` | — | — |
| `dbt_docs_url` | `str` | — | — |
| `include_exposures` | `bool` | `false` | — |
| `include_metrics` | `bool` | `false` | — |
| `include_semantic_models` | `bool` | `false` | — |
| `include_contracts` | `bool` | `false` | — |
| `include_meta` | `bool` | `false` | — |
| `include_source_freshness` | `bool` | `false` | — |
| `include_doc_blocks` | `bool` | `false` | — |
| `manifest_path` | `str` | — | — |
| `asset_overrides` | `Dict[str, AssetOverride]` | — | — |
| `emit_exposures_as_assets` | `bool` | `false` | — |
| `emit_source_assets` | `bool` | `false` | — |
| `derive_freshness_policies` | `bool` | `false` | — |
| `emit_contract_checks` | `bool` | `false` | — |
| `external_packages` | `List[str]` | — | — |
| `external_package_schemas` | `Dict[str, str]` | — | — |
| `external_package_workspaces` | `Dict[str, Dict[str, int]]` | — | — |
| `emit_semantic_layer_as_assets` | `bool` | `false` | — |
| `enable_materialization_kinds` | `bool` | `false` | — |
| `auto_trigger_on_freshness_failure` | `bool` | `false` | — |
| `derive_lag_tolerance_automation` | `bool` | `false` | — |
| `default_automation_condition` | `Dict[str, Any]` | — | — |
| `code_version_strategy` | `Literal['disabled', 'hash', 'sqlglot']` | `"disabled"` | — |
| `state_manifest_path` | `str` | — | — |
| `include_state_explain` | `bool` | `false` | — |
| `derive_state_tags` | `bool` | `false` | — |
| `emit_test_check_evaluations` | `bool` | `false` | — |
| `emit_skip_reason_observations` | `bool` | `false` | — |

[//]: # (FIELDS:END)
