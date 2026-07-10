# QlikComposeWorkspaceComponent

Auto-emit one Dagster asset per **Qlik Compose Workflow / Data Mart** across every Project (Data Warehouse). `StateBackedComponent` — discovery cached to disk, refreshed on explicit trigger. Materializing a Workflow asset runs the workflow to terminal state; Data Mart assets are observational.

The workspace-shape peer of the low-level `qlik_compose_*` components.

## Related components

- `qlik_compose_resource` — shared auth
- `qlik_compose_workflow_trigger_job` — imperative workflow control
- `qlik_compose_workflow_status_sensor` — event-drive on state
- `qlik_compose_workflow_metrics_ingestion` — metrics DataFrame

## When to use which

| Goal | Use |
|---|---|
| Wrap ALL Compose workflows + data marts as Dagster assets with one YAML | `qlik_compose_workspace` (this) |
| Run a specific workflow with bespoke config from a Dagster job | `qlik_compose_workflow_trigger_job` |
| Kick a downstream job when a workflow completes | `qlik_compose_workflow_status_sensor` |
| Feed DW-build metrics into observability dashboards | `qlik_compose_workflow_metrics_ingestion` |

## Selector shape

Same as Fivetran's `connector_selector`:

```yaml
workflow_selector:
  by_name: [FullBuildAndPopulate]
  by_pattern: ["Incremental_*"]
  exclude_by_name: [test_workflow]
  exclude_by_pattern: ["*_deprecated"]
```

Same shape for `data_mart_selector`. Omit → include everything.

## Example

```yaml
type: dagster_community_components.QlikComposeWorkspaceComponent
attributes:
  base_url_env_var: QLIK_COMPOSE_URL
  api_token_env_var: QLIK_COMPOSE_TOKEN
  projects: [FinanceDW, SalesDW]
  workflow_selector:
    by_pattern: ["FullBuild*", "Incremental*"]
  group_name: qlik_compose
  defs_state:
    management_type: LOCAL_FILESYSTEM
    refresh_if_dev: true
```

## Config reference

| Field | Type | Default | Description |
|---|---|---|---|
| `base_url_env_var` | str | — | Env var with Compose base URL. |
| `api_token_env_var` | str | — | Env var with API token. |
| `username_env_var` / `password_env_var` | str | — | Session-based auth (dev). |
| `verify_ssl` | bool | `true` | TLS verification. |
| `projects` | list[str] | all | Whitelist of Compose projects. |
| `workflow_selector` | object | — | Filter workflows. Fivetran-shape. |
| `data_mart_selector` | object | — | Filter data marts. Fivetran-shape. |
| `group_name` | str | — | Dagster asset group. |
| `asset_key_prefix` | list[str] | `["qlik_compose"]` | Prefix; final key = `[...prefix, project, workflow|data_mart, name]`. |
| `compute_kind` | str | `qlik_compose` | UI compute-kind label. |
| `wait_for_completion` | bool | `true` | For Workflow assets: wait to terminal state. |
| `poll_interval_seconds` | int | `30` | Seconds between status polls. |
| `timeout_seconds` | int | `3600` | Deadline for wait_for_completion. |
| `defs_state` | object | — | `LOCAL_FILESYSTEM` (dev) or `CLOUD_OBJECT_STORE` (Dagster+). |

[Schema](schema.json) · [Example](example.yaml)
