# TM1ProcessStatusSensorComponent

Poll a **TM1 TI process** for its execution status; trigger a Dagster job on transitions into a target status set. Cursor-deduped by `(status, execution_id)` so each execution fires downstream exactly once.

Pairs with [`tm1_resource`](../../resources/tm1_resource/) for auth.

## Common patterns

| Pattern | Config |
|---|---|
| Trigger dbt refresh when TI process completes | `target_statuses: [CompletedSuccessfully]` |
| Alert on-call on TI process failure | `target_statuses: [Aborted, HasMinorErrors, QuitCalled]` |
| Both success + failure to a triage job | `target_statuses: [CompletedSuccessfully, HasMinorErrors, Aborted]` |

## Statuses TM1 emits

- `CompletedSuccessfully`
- `CompletedWithMessages`
- `HasMinorErrors`
- `Aborted`
- `QuitCalled`

Match is exact-case.

## Example

```yaml
type: dagster_community_components.TM1ProcessStatusSensorComponent
attributes:
  sensor_name: gl_load_done
  process_name: LoadActualsFromGL
  target_statuses: [CompletedSuccessfully]
  job_name: refresh_downstream_dbt_models
  resource_key: tm1_resource
  minimum_interval_seconds: 60
```

## Related

- [`tm1_resource`](../../resources/tm1_resource/) — shared auth
- [`tm1_process_trigger_job`](../../jobs/tm1_process_trigger_job/) — the imperative counterpart
- [`tm1_workspace`](../../integrations/tm1_workspace/) — auto-emit per-Process asset that fires on materialize
