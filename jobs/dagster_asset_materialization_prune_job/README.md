# DagsterAssetMaterializationPruneJobComponent

Op-shaped job that wipes asset materialization history for configured
assets, preserving the last N via **synthetic re-emission** tagged
`_re_emitted_by_prune_job=true`. Useful for high-frequency assets that
would otherwise accumulate huge histories.

## Behavior

1. For each `asset_keys[]` entry:
   - Fetches the N most-recent materialization events (before wipe).
   - Calls `instance.wipe_assets([key])` — nukes ALL materialization history for that key.
   - Re-emits the preserved N events as synthetic `AssetMaterialization`s tagged `_re_emitted_by_prune_job=true` + `_original_timestamp` so history is visibly labeled as archival.
2. `dry_run=True` by default — first run just logs what would happen.

## YAML example

```yaml
type: dagster_component_templates.DagsterAssetMaterializationPruneJobComponent
attributes:
  job_name: prune_high_frequency_asset_history
  schedule: "0 5 * * 0"                # weekly Sunday at 5am
  default_status: STOPPED               # keep off until confident
  asset_keys:
    - analytics/orders
    - analytics/user_sessions
  keep_last_n_per_asset: 100
  dry_run: true                         # confirm first
```

## Options

- `asset_keys` — user-strings of assets to prune. Required.
- `keep_last_n_per_asset` — synthetic re-emit count after wipe. Set 0 to wipe everything.
- `dry_run` — default True; flip to False when ready.

## Caveats

- **Destructive**. Wipe is applied to the ENTIRE asset history before re-emission. If re-emission fails partway, some events are lost. Default `dry_run=True` is deliberate.
- Re-emitted events have a NEW timestamp (now), not the original. Original timestamp is preserved in `_original_timestamp` metadata for reference.
- The `_re_emitted_by_prune_job` tag lets downstream freshness / anomaly detection ignore archival re-emissions.
