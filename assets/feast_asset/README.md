# FeastAssetComponent

Materialize features from a source table into a [Feast](https://feast.dev/) feature store as a Dagster asset.

## Use case

Feature stores need to be kept fresh relative to upstream source tables. `FeastAssetComponent` lets you:

- **Gate feature materialization on upstream table assets** via `deps`, so features only refresh after source data is ready.
- **Choose between incremental and full materialization** — use `incremental: true` (default) to pick up only new data since the last Feast materialization, or `incremental: false` for a full date-range run.
- **Observe materialization results** — feature view names, end date, and mode are surfaced as Dagster asset metadata.

## Prerequisites

Install Feast in the Dagster run environment:

```bash
pip install feast
```

A `feature_store.yaml` must exist at `feature_store_repo_path` (defaults to `.`).

## Quick start

```yaml
type: dagster_component_templates.FeastAssetComponent
attributes:
  asset_name: customer_features
  feature_store_repo_path: ./feature_repo
  feature_views:
    - customer_stats
    - transaction_features
  incremental: true
  group_name: feature_store
  deps:
    - marts/customers
    - marts/transactions
```

## Incremental vs. full materialization

| Mode | Feast call | When to use |
|---|---|---|
| `incremental: true` (default) | `store.materialize_incremental(end_date)` | Daily/hourly refreshes; Feast tracks the watermark |
| `incremental: false` | `store.materialize(start_date, end_date)` | Backfills or first-time loads |

For non-incremental runs, set `start_date_env_var` to the name of an environment variable containing an ISO-8601 date string (e.g. `2024-01-01` or `2024-01-01T00:00:00`).

## Controlling end date

`end_date_offset_days` shifts the end date backward from UTC now. For example, `end_date_offset_days: 1` sets end date to yesterday midnight UTC, which is useful when source data has a one-day lag.

## Reference

| Field | Type | Default | Description |
|---|---|---|---|
| `asset_name` | `str` | `"feast_features"` | Dagster asset key |
| `feature_store_repo_path` | `str` | `"."` | Path to directory containing `feature_store.yaml` |
| `feature_views` | `list[str]` | `None` | Feature views to materialize (None = all) |
| `incremental` | `bool` | `true` | Use incremental materialization |
| `start_date_env_var` | `str` | `None` | Env var with ISO start date (non-incremental only) |
| `end_date_offset_days` | `int` | `0` | Days before now for end_date |
| `group_name` | `str` | `"feature_store"` | Dagster asset group |
| `deps` | `list[str]` | `None` | Upstream asset keys |
