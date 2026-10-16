# Aggregate Freshness Sensor

Alert when an **entire selection** of assets has gone silent — one check result, not hundreds of per-asset alerts.

This answers a different question than a per-asset freshness policy. `post_processing`'s `attributes.freshness_policy` (via `dg.FreshnessPolicy.time_window()`/`.cron()`) answers "is *this one* asset stale" — the right tool when each asset has its own real cadence. This component answers "did the *whole feed* stop producing anything at all" — the failure mode of an upstream system going down entirely, distinct from one table's individual schedule slipping. Applying a tight aggregate window per-asset across a large, mixed-cadence group would misfire constantly on its slower members; this component instead polls the group once per tick and asks a single question.

```yaml
type: dagster_community_components.AggregateFreshnessSensorComponent
attributes:
  sensor_name: sap_hourly_total_stoppage_sensor
  monitored_selection: "group:sap_hourly"
  rollup_asset_key: sap_ecc/hourly_feed_health
  max_silence_seconds: 600
  minimum_interval_seconds: 60
```

## Mechanism

A plain `@dg.sensor` polls `instance.get_latest_materialization_events(...)` for every resolved asset key in **one batched call**, takes the single most recent event across the whole selection, and compares its age against `max_silence_seconds`. `alert_mode` picks what happens with that result — two genuinely different Dagster+ alert types, with different scoping, not just a style preference:

**The failure condition is all-or-nothing, not a percentage.** Taking the *freshest* event across the group means the check only fails when every single monitored asset is stale — if even one is still updating, the whole group reads as healthy. There's no "X% of assets are silent" threshold. This is why `monitored_selection` should be scoped to one cadence-homogeneous tier (e.g. `group:sap_hourly`, not every table regardless of cadence) rather than a large mixed-cadence group — within one real tier, "all stale" is a fast, meaningful signal for "the upstream system that feeds this tier went down," not something that takes forever to trip. Partial failure *within* a tier (a handful of tables going silent while others keep flowing) is a different failure mode, already covered by per-asset `post_processing.attributes.freshness_policy` instead — see "Why not just set a tight `post_processing.attributes.freshness_policy`..." below for how the two mechanisms split by failure mode.

| | `alert_mode: check` (default) | `alert_mode: fail_tick` |
|---|---|---|
| What happens | Reports **one** `AssetCheckEvaluation` per tick against a declared "rollup" asset (`rollup_asset_key`). The sensor tick itself always succeeds. | No asset is created at all. The sensor **raises** when the selection is stale, failing its own tick. |
| Alert via | Dagster+'s **Asset** alert type, scoped to `rollup_asset_key`. Confirmed that alert type's targeting is by asset key/selection/group, *not* by a specific check name — this only isolates cleanly because the rollup asset carries exactly one check. | Dagster+'s **Automation** alert type, which *can* target one specific named sensor directly (confirmed: `schedules_or_sensors: [{location_name, repo_name, name}]`). |
| Real cost | A synthetic asset shows up in the catalog that was never "really" materialized. | A genuine bug in this sensor's own code and "the selection is just stale" look **identical** as a failed tick — `check` mode keeps those two meanings separate on purpose; this trades that away for sensor-level alert targeting and zero synthetic assets. |

Confirmed directly that `SensorResult(asset_events=[...])` accepts `AssetCheckEvaluation` objects and persists them queryably with no pre-declared `AssetCheckSpec` required (`check` mode), and that raising inside a `@dg.sensor` function gets caught and recorded as a tick failure by Dagster's sensor daemon, the same way any other sensor bug would be (`fail_tick` mode).

## `monitored_selection` syntax

Same selection language this repo's other selection-driven components use (`asset_to_job_trigger_sensor`, `enhanced_data_quality_checks`, `automation_condition_applicator`), resolved against sibling assets in the same defs folder:

| Form | Example | Notes |
|---|---|---|
| Explicit list | `["sap/bseg", "sap/konv"]` | No discovery needed — exact keys. |
| Everything | `"*"` | All discovered sibling assets. |
| Tag | `"tag:cadence=hourly"` | |
| Group | `"group:sap_hourly"` | Hierarchical groups need quotes: `'group:"marketing/*"'`. |
| Kind | `"kind:snowflake"` | |
| Boolean composition | `"group:sap_landing and tag:cadence=hourly"` | |
| Bare glob (fallback) | `"sap/*"` | Only tried if the string isn't valid selection syntax. |

An empty match raises a clear error naming the sensor and how many sibling assets were found, rather than silently creating a sensor that never fires.

## `monitored_folder` — monitoring a different folder than your own

By default, `monitored_selection` resolves against this component's own **parent** folder — the normal case, where this component lives in its own dedicated subfolder next to the assets it monitors. Set `monitored_folder` (a path relative to this component's own folder, e.g. `"../other_scenario"`) to point it at a different folder instead — confirmed safe as long as that folder doesn't contain the currently-resolving document itself.

One thing this can't do: point it at `"."` to search its own folder when this component is co-located as a second `---`-separated YAML document inside the *same* `defs.yaml` as a `dagster.DefsFolderComponent` (yes, one `defs.yaml` can hold multiple documents — confirmed directly). That specific case is a real `RecursionError`, not a config gap: `context.build_defs()` on the exact node currently being resolved recurses into itself. Any folder at or above where a co-located component's own file lives recurses back through that same file, so there's no `monitored_folder` value that fixes it — this component has to stay in its own dedicated subfolder.

[//]: # (FIELDS:START - auto-generated by tools/regen_readme_fields.py)

## Fields

### Required

| Field | Type | Description |
|---|---|---|
| `sensor_name` | `str` | Unique sensor name. |
| `monitored_selection` | `Union[str, List[str]]` | Asset selection to monitor in aggregate: explicit key list, the Dagster selection DSL (tag:/group:/kind:/boolean composition), '*' for everything, or a bare fnmatch glob. Resolved against discovered assets in monitored_f… _(full docs in schema.json + component README)_ |
| `max_silence_seconds` | `int` | Fail the aggregate check if the most recent materialization across the WHOLE monitored selection is older than this many seconds -- or if nothing in the selection has ever materialized at all. |

### Catalog metadata

| Field | Type | Default | Description |
|---|---|---|---|
| `group_name` | `str` | — | Group name for the declared rollup asset. |
| `description` | `str` | — | Description for the declared rollup asset. |

### Sensor configuration

| Field | Type | Default | Description |
|---|---|---|---|
| `minimum_interval_seconds` | `int` | `60` | Minimum seconds between sensor evaluations. |
| `default_status` | `str` | `"stopped"` | 'running' or 'stopped' -- initial status of the sensor. |

### Other

| Field | Type | Default | Description |
|---|---|---|---|
| `monitored_folder` | `str` | — | Override which folder monitored_selection resolves against, as a path relative to this component's own folder (e.g. '../other_scenario'). Defaults to this component's immediate parent folder -- the normal case, sibling d… _(full docs in schema.json + component README)_ |
| `alert_mode` | `Literal['check', 'fail_tick']` | `"check"` | 'check': declare rollup_asset_key as a visible, declare-only asset and report pass/fail via AssetCheckEvaluation against it every tick -- the sensor tick itself always succeeds. Alert via a Dagster+ 'Asset' alert policy… _(full docs in schema.json + component README)_ |
| `rollup_asset_key` | `str` | — | Required when alert_mode='check': asset key for the single declared 'rollup' asset the aggregate check is reported against (e.g. 'sap_ecc/hourly_feed_health'). Declare-only -- this component never materializes it, it onl… _(full docs in schema.json + component README)_ |
| `check_name` | `str` | `"total_stoppage"` | Name of the asset check reported against rollup_asset_key. |
| `include_observations` | `bool` | `false` | Also consider AssetObservation events (not just materializations) when finding the most recent event per monitored asset. Costs one extra query per monitored asset per tick, so it's opt-in -- leave off for large selectio… _(full docs in schema.json + component README)_ |

[//]: # (FIELDS:END)

## Why not just set a tight `post_processing.attributes.freshness_policy` on the whole group?

Because that attaches the *same* window to *every* asset in the selection individually — each one must independently satisfy it. For a mixed-cadence group (some hourly, some daily, some rare) that's wrong twice over: too tight for the slow tiers (constant false alarms) and not actually answering "did the feed stop" (one table updating on schedule while 249 others go silent would still show every individual check passing or failing on its own terms, never surfacing the aggregate picture). This component is the complement, not a replacement — the two mechanisms split by failure mode, not by preference:

| Failure mode | Caught by |
|---|---|
| One or a few assets individually miss their own cadence | Per-asset `post_processing.attributes.freshness_policy` — fires per asset |
| The *entire* selection goes dark at once (upstream system down) | This component — fires once, not once per asset |

Use per-asset `freshness_policy` for each table's own SLA, and this for the feed-level heartbeat, scoped to one cadence-homogeneous tier at a time (see "The failure condition is all-or-nothing" above for why scoping matters here).

## Sister components

- `asset_to_job_trigger_sensor` / `enhanced_data_quality_checks` / `automation_condition_applicator` — this repo's other selection-DSL-powered components; same resolution mechanism.
- `post_processing`'s `attributes.freshness_policy` (via `dg.FreshnessPolicy.time_window()`/`.cron()`) — the per-asset complement to this component's aggregate check.
