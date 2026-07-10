# QlikReplicateTaskStatusSensorComponent

Poll a Qlik Replicate task's state via the Enterprise Manager REST API; trigger a Dagster job when the task transitions to a target state.

Pairs with [`qlik_replicate_resource`](../../resources/qlik_replicate_resource/) for auth.

## Common patterns

| Pattern | Config |
|---|---|
| **Task-completed → downstream work** — watch a one-shot migration; kick off validation / transform when it stops | `target_states: [STOPPED]` |
| **Task-errored → alert / remediation** — watch a critical CDC task; page on-call when it errors | `target_states: [ERROR]` |
| **Task-running → start dependents** — kick off CDC-dependent jobs once a task confirms running | `target_states: [RUNNING]` |
| **Either terminal state** — trigger post-processing on stop OR error | `target_states: [STOPPED, ERROR]` |

## How it works

- Every `minimum_interval_seconds`, the sensor GETs the task's status endpoint.
- If `task.state` (uppercased) is in `target_states`, the sensor builds a `fingerprint = "{state}|{last_state_change_time}"` and compares to its cursor.
- If the fingerprint is new, it emits a `RunRequest` with `run_key=fingerprint` (so Dagster deduplicates on top of the sensor cursor) and passes `qlik_server` / `qlik_task` / `state` / `transition_time` as op config.
- If the fingerprint matches its cursor, it skips (state hasn't changed since the last emission).

## Example

```yaml
type: dagster_community_components.QlikReplicateTaskStatusSensorComponent
attributes:
  sensor_name: orders_migration_done
  server: prod-replicate-01
  task: orders_sqlserver_to_snowflake
  target_states: [STOPPED]
  job_name: validate_orders_target
  resource_key: qlik_replicate_resource
  minimum_interval_seconds: 60
  default_status: running
```

## Related

- [`qlik_replicate_resource`](../../resources/qlik_replicate_resource/) — shared auth
- [`qlik_replicate_task_trigger_job`](../../jobs/qlik_replicate_task_trigger_job/) — the imperative counterpart (start/stop/reload)
- [`qlik_replicate_task_metrics_ingestion`](../../assets/ingestion/qlik_replicate_task_metrics_ingestion/) — pull per-task metrics for dashboards
