# QlikReplicateWorkspaceComponent

Auto-emit one Dagster asset per Qlik Replicate task by enumerating the workspace via the **Qlik Enterprise Manager REST API**. `StateBackedComponent` — discovery is cached to disk and refreshed on explicit trigger, not on every project load. Materializing an asset triggers the underlying task (run / reload / stop) and polls to completion.

This is the **workspace-shape peer** of the low-level Qlik Replicate components:

- [`qlik_replicate_resource`](../../resources/qlik_replicate_resource/) — shared auth
- [`qlik_replicate_task_trigger_job`](../../jobs/qlik_replicate_task_trigger_job/) — imperative single-task control
- [`qlik_replicate_task_status_sensor`](../../sensors/qlik_replicate_task_status_sensor/) — event-drive on state transitions
- [`qlik_replicate_task_metrics_ingestion`](../../assets/ingestion/qlik_replicate_task_metrics_ingestion/) — per-task metrics DataFrame
- **`qlik_replicate_workspace` (this component)** — auto-emit one asset per task, zero per-task YAML

## When to use which

| You want to… | Use |
|---|---|
| Wrap ALL your Replicate tasks as Dagster assets with one YAML | `qlik_replicate_workspace` (this) |
| Control a specific task from a specific Dagster job | `qlik_replicate_task_trigger_job` |
| Kick a downstream Dagster job when a task hits a state | `qlik_replicate_task_status_sensor` |
| Feed CDC latency / throughput into observability dashboards | `qlik_replicate_task_metrics_ingestion` |

Most customers should start with `qlik_replicate_workspace` — it's the shortest path from Enterprise Manager to Dagster assets.

## How it works

**Discovery time** (`write_state_to_path`) — happens once, on explicit trigger:

1. Login to Enterprise Manager (session or API-token auth)
2. `GET /servers` → list of registered Replicate servers
3. For each server: `GET /servers/<s>/tasks` → task list
4. For each task: `GET /servers/<s>/tasks/<t>` → current state + stage
5. Apply `servers` filter + `include_task_patterns` / `exclude_task_patterns` globs
6. Write the snapshot to the cached state file

**Load time** (`build_defs_from_state`) — happens on every `dg check`, `dg dev`, cold start:

1. Read the cached snapshot (no API calls)
2. Emit one `@dg.asset` per (server, task)
3. Each asset's key = `[...asset_key_prefix, server, task]`
4. Each asset's `compute` = trigger task + poll to completion

**Refresh discovery** by running the Dagster+ auto-refresh, `dg utils refresh-defs-state`, or setting `defs_state.refresh_if_dev: true` for `dg dev`.

## Example

```yaml
type: dagster_community_components.QlikReplicateWorkspaceComponent
attributes:
  base_url_env_var: QLIK_EM_URL
  api_token_env_var: QLIK_EM_API_TOKEN
  servers: [prod-replicate-01]
  group_name: qlik_replicate
  action: run
  wait_for_completion: true
  defs_state:
    management_type: LOCAL_FILESYSTEM
    refresh_if_dev: true
```

## Config reference

| Field | Type | Default | Description |
|---|---|---|---|
| `base_url_env_var` | str | — | Env var with EM base URL (no trailing slash, no API path). |
| `api_token_env_var` | str | — | Env var with EM API token. Preferred for production. |
| `username_env_var` / `password_env_var` | str | — | Session-based auth (dev). Required unless api_token is set. |
| `verify_ssl` | bool | `true` | TLS verification. Set false for self-signed dev servers. |
| `servers` | list[str] | all | Whitelist of Replicate servers to include. |
| `task_selector` | object | — | Task filter. Mirrors Fivetran's `connector_selector`. See below. |
| `group_name` | str | — | Dagster asset group name. |
| `asset_key_prefix` | list[str] | `["qlik_replicate"]` | Prefix segments for emitted asset keys. Final key = `[...prefix, server, task]`. |
| `compute_kind` | str | `qlik_replicate` | Asset compute_kind for UI display. |
| `action` | str | `run` | What happens on materialize: `run` / `reload` / `stop`. |
| `run_option` | str | `RESUME_PROCESSING` | For `action: run`: `RESUME_PROCESSING` / `RELOAD_TARGET` / `RECOVER` / `RESUME_PROCESSING_FROM_TIMESTAMP` |
| `wait_for_completion` | bool | `true` | Poll until task hits terminal state. Set false for continuous CDC. |
| `poll_interval_seconds` | int | `15` | Seconds between status polls. |
| `timeout_seconds` | int | `3600` | Deadline for wait_for_completion. |
| `defs_state` | dict | — | StateBackedComponent config. `management_type: LOCAL_FILESYSTEM` (dev) or `CLOUD_OBJECT_STORE` (Dagster+). |

## `task_selector` — Fivetran-style filtering

Mirrors the shape of `FivetranWorkspace.connector_selector`. All four fields optional:

```yaml
task_selector:
  by_name: [orders_cdc, customers_cdc]      # exact names to include
  by_pattern: [orders_*]                     # fnmatch globs to include
  exclude_by_name: [test_task]               # exact names to exclude
  exclude_by_pattern: [*_deprecated, *_test] # fnmatch globs to exclude
```

**Semantics:**
- If neither `by_name` nor `by_pattern` is set, ALL tasks are included (subject to exclusions).
- If either `by_name` or `by_pattern` is set, only matching tasks are included.
- `exclude_by_*` always wins over `by_*` — matching an exclusion drops the task even if it matches an include.

## The Fivetran / Airbyte parity

If you're used to `FivetranWorkspace` / `AirbyteCloudWorkspace` from the official Dagster integrations, this is the same idea for Qlik. One YAML, auto-emit assets, refresh discovery on demand. Nothing to hand-wire per-task.
