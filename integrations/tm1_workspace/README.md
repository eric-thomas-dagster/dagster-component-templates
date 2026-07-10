# TM1WorkspaceComponent

Auto-emit one Dagster asset per **TM1 Cube / Process / Chore** by enumerating the workspace via the TM1 REST API. `StateBackedComponent` — discovery cached to disk, refreshed on explicit trigger, not on every project load.

- **Cube assets** — observational; materialize records presence. Use `tm1_cube_data_ingestion` for actual data extraction.
- **Process / Chore assets** — executable; materialize invokes `ExecuteProcess` / `Execute` on TM1 and polls to terminal status.

The workspace-shape peer of:
- `tm1_resource` — shared auth
- `tm1_process_trigger_job` — imperative single-process control
- `tm1_process_status_sensor` — event-drive on status
- `tm1_cube_data_ingestion` — cube data as DataFrame
- **`tm1_workspace` (this)** — auto-emit assets per object, zero per-object YAML

## When to use which

| Goal | Use |
|---|---|
| Wrap ALL TM1 processes + chores as Dagster assets with one YAML | `tm1_workspace` |
| Execute a specific process with parameter overrides from a specific Dagster job | `tm1_process_trigger_job` |
| React to a specific process's status transitions | `tm1_process_status_sensor` |
| Pull a cube slice into a DataFrame | `tm1_cube_data_ingestion` |

Most customers should start with `tm1_workspace` — one YAML, everything discovered.

## Selector shape

Each object kind has its own selector — mirrors Fivetran's `connector_selector`:

```yaml
cube_selector:
  by_name: [Sales, Finance]                # exact names to include
  by_pattern: [Actual_*]                    # globs to include
  exclude_by_name: [test_cube]              # exact names to exclude
  exclude_by_pattern: [*_deprecated]        # globs to exclude
```

Same shape for `process_selector` and `chore_selector`. Omit a selector → include everything for that kind. `exclude_by_*` always wins.

## Example

```yaml
type: dagster_community_components.TM1WorkspaceComponent
attributes:
  base_url_env_var: TM1_URL
  username_env_var: TM1_USER
  password_env_var: TM1_PASSWORD
  cube_selector:
    by_name: [Sales, Finance]
  process_selector:
    by_pattern: [Load_*]
  group_name: tm1_planning
  defs_state:
    management_type: LOCAL_FILESYSTEM
    refresh_if_dev: true
```

## Config reference

| Field | Type | Default | Description |
|---|---|---|---|
| `base_url_env_var` | str | — | Env var with TM1 base URL (include port). |
| `username_env_var` / `password_env_var` | str | — | Native auth. |
| `cam_namespace_env_var` | str | — | CAM SSO namespace. |
| `verify_ssl` | bool | `true` | TLS verification. |
| `cube_selector` | object | — | Filter cubes. |
| `process_selector` | object | — | Filter TI processes. |
| `chore_selector` | object | — | Filter chores. |
| `group_name` | str | — | Dagster asset group. |
| `asset_key_prefix` | list[str] | `["tm1"]` | Prefix segments; final key = `[...prefix, cube|process|chore, name]`. |
| `compute_kind` | str | `tm1` | UI compute-kind label. |
| `wait_for_completion` | bool | `true` | For Process/Chore assets: wait for TM1 to finish. |
| `timeout_seconds` | int | `1800` | HTTP timeout for process/chore execution. |
| `defs_state` | object | — | `LOCAL_FILESYSTEM` (dev) or `CLOUD_OBJECT_STORE` (Dagster+). |

## Fivetran parity

If you use `FivetranWorkspace` / `AirbyteCloudWorkspace` from official Dagster integrations, this is the same idea for TM1.
