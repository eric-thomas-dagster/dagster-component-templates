# TectonAssetComponent

Trigger [Tecton](https://www.tecton.ai/) feature view materialization jobs as a Dagster asset.

## Use case

Tecton manages feature pipelines for ML models. `TectonAssetComponent` lets you:

- **Trigger materialization as part of a Dagster pipeline** — gate feature freshness on upstream table assets via `deps`.
- **Target online, offline, or both stores** — control with the `online` and `offline` flags.
- **Materialize specific feature views or all views** — list them explicitly or leave `feature_views` unset to trigger the entire workspace.
- **Observe job IDs and counts** as Dagster asset metadata for auditability.

## Prerequisites

Install the Tecton client library:

```bash
pip install tecton-client
```

Set the API key environment variable (default: `TECTON_API_KEY`):

```bash
export TECTON_API_KEY=your-api-key-here
```

## Quick start

```yaml
type: dagster_component_templates.TectonAssetComponent
attributes:
  asset_name: tecton_features
  workspace: production
  tecton_api_key_env_var: TECTON_API_KEY
  feature_views:
    - user_features
    - item_features
  online: true
  offline: true
  group_name: feature_store
  deps:
    - marts/users
    - marts/items
```

## Authentication

The component reads credentials from environment variables at runtime:

| Variable | Purpose | Default env var name |
|---|---|---|
| API key | Authenticate with Tecton | `TECTON_API_KEY` |
| Cluster URL | Custom Tecton URL | set via `tecton_url_env_var` |

If `tecton_url_env_var` is not set, the component defaults to `https://app.tecton.ai`.

## Selecting feature views

When `feature_views` is omitted the component calls `ws.list_feature_views()` and triggers materialization for every view in the workspace. This is convenient for development but may trigger unexpected jobs in production — prefer an explicit list for production deployments.

## Store targets

Both `online` and `offline` default to `true`. Set either to `false` to skip that store:

```yaml
online: true
offline: false   # only refresh the online store
```

## Reference

| Field | Type | Default | Description |
|---|---|---|---|
| `workspace` | `str` | required | Tecton workspace name |
| `tecton_api_key_env_var` | `str` | `"TECTON_API_KEY"` | Env var holding the API key |
| `tecton_url_env_var` | `str` | `None` | Env var holding the Tecton cluster URL |
| `feature_views` | `list[str]` | `None` | Feature views to trigger (None = all) |
| `online` | `bool` | `true` | Materialize to online store |
| `offline` | `bool` | `true` | Materialize to offline store |
| `asset_name` | `str` | `"tecton_features"` | Dagster asset key |
| `group_name` | `str` | `"feature_store"` | Dagster asset group |
| `deps` | `list[str]` | `None` | Upstream asset keys |
