# TM1ResourceComponent

Shared connection resource for **IBM Planning Analytics (TM1) REST API v11+**. Other TM1 components — `tm1_process_trigger_job`, `tm1_process_status_sensor`, `tm1_cube_data_ingestion`, `tm1_workspace` — reference this resource, so you configure credentials in one place.

## When to use

- You're running IBM Planning Analytics (TM1) for planning / forecasting / consolidation and want Dagster to orchestrate: schedule TI processes / chores, react to process state changes, ingest cube slices as DataFrames, auto-emit assets for the cube + process catalog.
- You have Cognos-backed CAM security or native TM1 auth and want to talk to TM1 via its REST API from Dagster.

## Auth modes

| Mode | Config | Best for |
|---|---|---|
| **Basic auth** | `username_env_var` + `password_env_var` | Native TM1 users (`IntegratedSecurityMode: 1`), dev / self-hosted |
| **CAM SSO** | Basic + `cam_namespace_env_var` | Cognos-backed enterprise TM1 (`IntegratedSecurityMode: 4/5`) |

## Config

```yaml
type: dagster_community_components.TM1ResourceComponent
attributes:
  resource_key: tm1_resource
  base_url_env_var: TM1_URL             # e.g. https://tm1.acme.com:5495
  username_env_var: TM1_USER
  password_env_var: TM1_PASSWORD
```

| Field | Type | Default | Description |
|---|---|---|---|
| `resource_key` | str | `tm1_resource` | Resource registration key. |
| `base_url_env_var` | str | — | Env var with TM1 base URL (include port; no `/api` path). |
| `username_env_var` | str | — | Env var with TM1 username. |
| `password_env_var` | str | — | Env var with TM1 password. |
| `cam_namespace_env_var` | str | — | Env var with CAM namespace. Omit for native auth. |
| `verify_ssl` | bool | `true` | TLS cert verification. |

## Related components

- **`tm1_process_trigger_job`** — execute a TI process or chore from Dagster (imperative)
- **`tm1_process_status_sensor`** — trigger a downstream Dagster job when a TI process finishes / errors (event-driven)
- **`tm1_cube_data_ingestion`** — materialize a cube slice via MDX as a DataFrame asset
- **`tm1_workspace`** — auto-emit one Dagster asset per (Cube, Process, Chore) via a `StateBackedComponent` — same shape as `FivetranWorkspace`

## Distinct from

- **`dagster-community-components` doesn't cover Cognos Analytics BI** (dashboards/reports) — that's a separate `cognos_resource`. TM1 is the *planning cube* side of the IBM analytics stack.
