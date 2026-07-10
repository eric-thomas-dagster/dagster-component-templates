# JDEOrchestrationStatusSensorComponent

Poll a **JDE orchestration**'s last-run status; trigger a Dagster job on transitions into a target state. Cursor-deduped by `(state, jobId)` so downstream fires once per run.

Pairs with `jde_orchestrator_resource`.

## Example

```yaml
type: dagster_community_components.JDEOrchestrationStatusSensorComponent
attributes:
  sensor_name: ar_recon_done
  orchestration: JDE_AR_Recon
  target_states: [SUCCESS, COMPLETED]
  job_name: refresh_finance_dashboards
  resource_key: jde_orchestrator_resource
```

## Related

- `jde_orchestrator_resource`
- `jde_orchestration_trigger_job` — imperative counterpart
- `jde_orchestrator_workspace` — auto-emit assets per orchestration
