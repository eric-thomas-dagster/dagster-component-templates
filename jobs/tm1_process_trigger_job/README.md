# TM1ProcessTriggerJobComponent

Trigger a **TM1 TI process or chore** via the TM1 REST API. Wraps a Dagster job around a single `ExecuteProcess` (or `ExecuteChore`) call, with optional parameter overrides for TI processes.

Pairs with [`tm1_resource`](../../resources/tm1_resource/) for centralized auth.

## When to use

| Scenario | Config |
|---|---|
| Nightly GL data load with per-run parameters (year/month) | `target_type: process`, `parameters:`, `schedule: "0 4 * * *"` |
| Kick off a chore (bundled processes) | `target_type: chore`, `target_name: DailyRebuild` |
| Chain a process after an upstream ETL asset | `wait_for_completion: true`, tie to Dagster asset lineage |

## Target types

- **`process`** — a single TI (Turbo Integrator) process. Accepts `parameters` (dict of TI parameter overrides).
- **`chore`** — a group of processes with per-chore scheduling metadata. No parameters.

## Example

```yaml
type: dagster_community_components.TM1ProcessTriggerJobComponent
attributes:
  job_name: nightly_gl_load
  target_type: process
  target_name: LoadActualsFromGL
  parameters:
    pYear: "2026"
    pMonth: "07"
  resource_key: tm1_resource
  wait_for_completion: true
  timeout_seconds: 1800
  schedule: "0 4 * * *"
  default_status: RUNNING
```

## Related

- [`tm1_resource`](../../resources/tm1_resource/) — shared auth
- [`tm1_process_status_sensor`](../../sensors/tm1_process_status_sensor/) — trigger downstream Dagster jobs on process state
- [`tm1_cube_data_ingestion`](../../assets/ingestion/tm1_cube_data_ingestion/) — read a cube slice via MDX
- [`tm1_workspace`](../../integrations/tm1_workspace/) — auto-emit assets per Cube / Process / Chore (workspace shape)
