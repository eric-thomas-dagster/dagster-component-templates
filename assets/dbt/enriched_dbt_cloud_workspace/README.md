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
| `emit_semantic_layer_as_assets` | Emit dbt `semantic_models` + `metrics` as observable `AssetSpec`s (kinds `semantic_model` / `metric`). |
| `emit_contract_checks` | For every model with `config.contract.enforced: true`, emit one `AssetCheckSpec` per column constraint. |
| `external_packages` | dbt mesh: emit observable stub `AssetSpec`s for models whose `package_name` matches. Pair with `exclude: 'package:X'`. |
| `enable_materialization_kinds` | Add each model's `config.materialized` value (`table` / `view` / `incremental` / `materialized_view` / `ephemeral` / `seed` / `snapshot`) as a Dagster kind. |
| `derive_freshness_policies` | Attach a real `FreshnessPolicy` to sources (from `sources.freshness.warn_after/error_after`) and to models (from dbt 1.9+ `config.freshness.build_after` and/or dbt State `config.state.lag_tolerance`). Honors explicit `meta.dagster.freshness_policy` overrides. |
| `auto_trigger_on_freshness_failure` | With `derive_freshness_policies`: also attach `AutomationCondition.freshness_failed()` so Dagster triggers the rebuild when the derived policy fails. |
| `derive_lag_tolerance_automation` | For models with `config.state.lag_tolerance`: attach `.newly_updated().since(cron_tick_passed(cron))` where the cron is snapped from lag_tolerance. |
| `code_version_strategy` | `disabled` / `hash` / `sqlglot`. `sqlglot` parses `compiled_code`, strips comments + normalizes whitespace, then hashes — whitespace/comment edits don't bump. Pairs with `AutomationCondition.code_version_changed()`. |
| `asset_overrides` | Per-asset overrides keyed by asset key. Today supports `{depends_on: [...]}` to inject Dagster asset dependencies. |

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

  # dbt mesh
  external_packages: [shared_core]
  exclude: "package:shared_core"
```

## Related

- **[`EnrichedDbtProjectComponent`](../enriched_dbt_project/README.md)** — same enrichment vocabulary for dbt Core
- **`DbtStateReusePatch`** — bridge patch for no-op / partial-success statuses
- **`DbtCloudJobSensor`**, **`DbtCloudTriggerJobComponent`** — event-driven and job-shaped triggers
