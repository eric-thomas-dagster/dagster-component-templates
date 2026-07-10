# QlikComposeWorkflowStatusSensorComponent

Poll a **Qlik Compose workflow** for its state; trigger a Dagster job on transitions into a target state. Cursor-deduped by `(state, last_completion_time)` so each completion fires exactly once.

Pairs with `qlik_compose_resource` for auth.

## Example

```yaml
type: dagster_community_components.QlikComposeWorkflowStatusSensorComponent
attributes:
  sensor_name: finance_dw_done
  project: FinanceDW
  workflow: FullBuildAndPopulate
  target_states: [COMPLETED]
  job_name: refresh_downstream_dbt_models
  resource_key: qlik_compose_resource
  minimum_interval_seconds: 60
```

## States

Compose exposes: `RUNNING`, `COMPLETED`, `STOPPED`, `ERROR`, `FAILED`, `WAITING`, `PENDING`.

## Related

- `qlik_compose_resource`
- `qlik_compose_workflow_trigger_job` — imperative counterpart
- `qlik_compose_workspace` — auto-emit workspace assets
