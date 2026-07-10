# QlikReplicateTaskTriggerJobComponent

Trigger a **Qlik Replicate task** — start, stop, or full reload — via the Qlik Enterprise Manager REST API. Wraps a Dagster job around a single control-plane call.

Pairs with [`qlik_replicate_resource`](../../resources/qlik_replicate_resource/) for centralized auth.

## When to use

| Scenario | Config |
|---|---|
| Nightly full reload of a slowly-changing target | `action: reload`, `wait_for_completion: true`, `schedule: "0 3 * * *"` |
| One-shot database migration (start → wait for done → downstream) | `action: run`, `wait_for_completion: true`, chain via job dependencies |
| Enable continuous CDC after a maintenance window | `action: run`, `wait_for_completion: false` |
| Stop CDC before a schema change | `action: stop`, `wait_for_completion: false` |

## Actions

| Value | REST call | What it does |
|---|---|---|
| `run` (default) | `POST /tasks/{task}?action=run&option={run_option}` | Start or resume the task. Combine with `run_option`. |
| `stop` | `POST /tasks/{task}?action=stop` | Stop the task cleanly. |
| `reload` | `POST /tasks/{task}?action=reload` | Full reload: drops target, recreates, initial load. |

`run_option` values (only used when `action: run`):

- `RESUME_PROCESSING` (default) — resume from last position
- `RELOAD_TARGET` — reload target tables then continue CDC
- `RECOVER` — recover from a crash checkpoint
- `RESUME_PROCESSING_FROM_TIMESTAMP` — resume from a specific point (requires task-level config)

## Wait-for-completion behavior

When `wait_for_completion: true`, the op polls the task's status endpoint every `poll_interval_seconds` until either:
- A **terminal state** (`STOPPED` or `ERROR`) is reached, or
- The `timeout_seconds` deadline expires (raises `Exception`)

For **continuous CDC tasks that never complete**, leave `wait_for_completion: false` — the op returns as soon as the action POST succeeds.

## Example — nightly full reload

```yaml
type: dagster_community_components.QlikReplicateTaskTriggerJobComponent
attributes:
  job_name: reload_orders_cdc
  server: prod-replicate-01
  task: orders_sqlserver_to_snowflake
  action: reload
  wait_for_completion: true
  poll_interval_seconds: 30
  timeout_seconds: 3600           # 1h ceiling for a large reload
  resource_key: qlik_replicate_resource
  schedule: "0 3 * * *"
  default_status: RUNNING
```

## Example — one-shot migration inside a Dagster job chain

```yaml
type: dagster_community_components.QlikReplicateTaskTriggerJobComponent
attributes:
  job_name: kickoff_customer_migration
  server: migration-replicate
  task: customer_db2_to_snowflake
  action: run
  run_option: RELOAD_TARGET
  wait_for_completion: true
  timeout_seconds: 14400          # 4h ceiling for the initial load
  resource_key: qlik_replicate_resource
```

## Related

- [`qlik_replicate_resource`](../../resources/qlik_replicate_resource/) — shared auth
- [`qlik_replicate_task_status_sensor`](../../sensors/qlik_replicate_task_status_sensor/) — trigger downstream jobs on task completion (event-driven pattern)
- [`qlik_replicate_task_metrics_ingestion`](../../assets/ingestion/qlik_replicate_task_metrics_ingestion/) — pull per-task metrics into a DataFrame asset for dashboarding
