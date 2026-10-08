# Asset To Job Trigger Sensor

Monitor an asset selection; fire a `RunRequest` at a named job when any matched asset gets a new materialization.

This exists for a real gap: Dagster's declarative reactivity (`deps:` + `AutomationCondition` via `post_processing`, or `define_asset_job`'s own `automation_condition`) only applies to **asset-backed** things. A job built from a bare `@dg.op` — like a component that mirrors an external system's own pre-existing job (triggering it by ID, preserving its own configuration, rather than letting Dagster decide what to run) — isn't asset-backed, and can't be made reactive either way. Reacting to an upstream event for a job-shaped target needs an explicit sensor bridging "this asset updated" → `RunRequest(job_name=...)`. This component is that sensor, built once and configured per case instead of hand-written custom code each time it comes up.

```yaml
type: dagster_component_templates.AssetToJobTriggerSensorComponent
attributes:
  sensor_name: electricera_pipe_to_dbt_trigger_sensor
  monitored_selection: "group:electricera_snowpipes"
  job_name: fuel_and_trading_electricera_prod_job
  minimum_interval_seconds: 30
```

## `monitored_selection` syntax

Same selection language this repo's other bulk/selection-driven components already use (`enhanced_data_quality_checks`, `automation_condition_applicator`, `bulk_freshness_policies`), resolved against sibling assets in the same defs folder:

| Form | Example | Notes |
|---|---|---|
| Explicit list | `["sap/bseg", "sap/konv"]` | No discovery needed — exact keys. |
| Everything | `"*"` | All discovered sibling assets. |
| Tag | `"tag:cadence=hourly"` | |
| Group | `"group:electricera_snowpipes"` | Hierarchical groups need quotes: `'group:"marketing/*"'`. |
| Kind | `"kind:snowflake"` | |
| Boolean composition | `"group:sap_landing and tag:cadence=hourly"` | |
| Bare glob (fallback) | `"sap/*"` | Only tried if the string isn't valid selection syntax. |

An empty match raises a clear error naming the sensor and how many sibling assets were found, rather than silently creating a sensor that never fires.

## Fields

- `sensor_name` (required) — unique sensor name.
- `monitored_selection` (required) — see table above.
- `job_name` (required) — the target job's name. Resolved by name at the whole-project `Definitions` level, same as any other cross-component `RunRequest(job_name=...)` reference — this component doesn't need to "know about" the target job's own component.
- `minimum_interval_seconds` (default `30`).
- `default_status` (default `"stopped"`) — flip to `"running"` once the upstream system this sensor watches has real, working credentials.

## When to use this vs. direct `post_processing`/`AutomationCondition`

| | `post_processing` + `AutomationCondition` | `asset_to_job_trigger_sensor` |
|---|---|---|
| Target is | A real asset | A job built from a bare op (not asset-backed) |
| Mechanism | Declarative, zero custom code | A real sensor, explicit `RunRequest` |
| Good for | Any normal asset dependency | Mirroring an external system's own pre-existing job by name/ID, where you want to preserve that job's own configuration rather than letting Dagster decide what to run |

If the thing you're triggering is (or could reasonably be) an asset, prefer `post_processing`'s `deps:`/`automation_condition:` instead — it's simpler and needs no sensor at all. Reach for this component specifically when the target is job-shaped and can't be made asset-backed without losing something real (e.g. `enriched_dbt_cloud_workspace`'s `mirror_jobs: job` — asset-backing it would collapse per-model visibility down to one coarse "job ran" node).

## Sister components

- `enhanced_data_quality_checks` / `automation_condition_applicator` / `bulk_freshness_policies` — this repo's other selection-DSL-powered components; same resolution mechanism.
