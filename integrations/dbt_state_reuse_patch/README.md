# DbtStateReusePatchComponent

Bridge patch that makes `dagster-dbt` treat two success-equivalent dbt statuses as materialization events instead of dropping them (or failing the whole Dagster run):

- **`no-op`** — dbt Cloud state-reuse: the model was skipped because state said it's already up-to-date. Warehouse is current; Dagster should reflect that.
- **`partial success`** — incremental microbatch: some batches succeeded and are in the warehouse. Same — Dagster should mark the asset materialized.

Under the hood, `dagster-dbt` today compares status against `NodeStatus.Success` with `==`. Anything else (including `no-op`) falls through — the OSS `@dbt_assets` op raises on missing required outputs, or the Cloud v2 sensor silently drops the event. Either way the run appears failed even though dbt itself succeeded.

## Why "bridge"

The permanent fix is in flight upstream: [dagster-io/dagster#34010](https://github.com/dagster-io/dagster/pull/34010). Once that merges and ships in a released `dagster-dbt`, remove this component from your `defs.yaml`.

This component is **idempotent** and **auto-detects** when the upstream fix is present. If your installed `dagster-dbt` already has `_SUCCESS_EQUIVALENT_NODE_STATUSES` in its source, `build_defs` is a no-op — safe to leave in place across upgrades.

## Example

```yaml
type: dagster_community_components.DbtStateReusePatchComponent
attributes: {}
```

No configuration. Drop this into any `defs.yaml` in the same code-location as your dbt component (or in its own subdirectory). Patches apply globally to `dagster-dbt` at code-location boot; effect persists for the life of the process.

## What it patches

| Entry point | What we do |
|---|---|
| `dagster_dbt.core.dbt_cli_event.DbtCliEventMessage._get_node_status` | Wrap to return `"success"` when raw status is `no-op` or `partial success`. Downstream `==` comparisons then pass. |
| `dagster_dbt.cloud_v2.run_handler.DbtCloudJobRunResults.to_default_asset_events` | Wrap to rewrite `status` in the run_results dict from `no-op` / `partial success` → `success` before the original method sees it. Restores the original dict when done so nothing else is affected. |

Both patches log to `dagster_community_components.dbt_state_reuse_patch` at code-location boot:

```
dbt_state_reuse_patch [oss]: patched: DbtCliEventMessage._get_node_status
dbt_state_reuse_patch [cloud_v2]: patched: DbtCloudJobRunResults.to_default_asset_events
```

Or, if the upstream fix is already installed:

```
dbt_state_reuse_patch [oss]: skipped: upstream fix present
dbt_state_reuse_patch [cloud_v2]: skipped: upstream fix present
```

## When to remove

After upgrading `dagster-dbt` past the release that includes [#34010](https://github.com/dagster-io/dagster/pull/34010). The component will `skipped: upstream fix present` on its own from that point — deleting the `defs.yaml` entry is just cleanup.

## Fields

None. This component intentionally has no configuration.
