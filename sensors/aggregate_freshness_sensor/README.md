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

A plain `@dg.sensor` polls `instance.get_latest_materialization_events(...)` for every resolved asset key in **one batched call**, takes the single most recent event across the whole selection, and compares its age against `max_silence_seconds`. It reports the result as **one** `AssetCheckEvaluation` against a single declared "rollup" asset (`rollup_asset_key`) — confirmed directly that `SensorResult(asset_events=[...])` accepts `AssetCheckEvaluation` objects and persists them queryably with no pre-declared `AssetCheckSpec` required.

The sensor itself never raises or fails when the selection is stale — it succeeds every tick and reports a passed/failed check. That's deliberate: a sensor *tick* failing should mean "this monitoring broke" (a real bug), not "the thing it's monitoring is stale" (a business condition). Wire a Dagster+ **"asset check failed"** alert policy to `rollup_asset_key`'s `check_name` — that's the actual alerting mechanism, no bespoke code needed.

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

## Fields

- `sensor_name` (required) — unique sensor name.
- `monitored_selection` (required) — see table above.
- `monitored_folder` (optional) — see section above.
- `rollup_asset_key` (required) — the single declared asset the check is reported against. Declare-only: this component never materializes it, it only exists to carry the check so it's visible in the asset catalog and alertable in Dagster+.
- `max_silence_seconds` (required) — fail the check if the freshest event across the *whole* selection is older than this, or if nothing in the selection has ever produced an event at all.
- `check_name` (default `"total_stoppage"`).
- `include_observations` (default `false`) — also consider `AssetObservation` events, not just materializations, when finding each asset's most recent event. Costs one extra query per monitored asset per tick (observations aren't batchable the way materializations are), so it's opt-in — leave off unless some monitored assets are observation-only.
- `minimum_interval_seconds` (default `60`).
- `default_status` (default `"stopped"`).
- `group_name` / `description` — optional metadata for the declared rollup asset.

## Why not just set a tight `post_processing.attributes.freshness_policy` on the whole group?

Because that attaches the *same* window to *every* asset in the selection individually — each one must independently satisfy it. For a mixed-cadence group (some hourly, some daily, some rare) that's wrong twice over: too tight for the slow tiers (constant false alarms) and not actually answering "did the feed stop" (one table updating on schedule while 249 others go silent would still show every individual check passing or failing on its own terms, never surfacing the aggregate picture). This component is the complement, not a replacement — use per-asset `freshness_policy` for each table's own SLA, and this for the feed-level heartbeat.

## Sister components

- `asset_to_job_trigger_sensor` / `enhanced_data_quality_checks` / `automation_condition_applicator` — this repo's other selection-DSL-powered components; same resolution mechanism.
- `post_processing`'s `attributes.freshness_policy` (via `dg.FreshnessPolicy.time_window()`/`.cron()`) — the per-asset complement to this component's aggregate check.
