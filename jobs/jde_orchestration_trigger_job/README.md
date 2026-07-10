# JDEOrchestrationTriggerJobComponent

Trigger a **JDE Orchestrator orchestration** via REST. Wraps a Dagster job around the orchestration invocation with optional wait-for-completion polling.

Pairs with `jde_orchestrator_resource`.

## Sync vs async

- **Sync (`async_mode: false`, default)** — POST returns when orchestration completes. Right for orchestrations that finish in seconds.
- **Async (`async_mode: true`)** — POST returns immediately with a `jobId`; component polls `/status/{jobId}` to terminal state. Right for orchestrations that take minutes to hours.

## Example

```yaml
type: dagster_community_components.JDEOrchestrationTriggerJobComponent
attributes:
  job_name: nightly_ar_reconciliation
  orchestration: JDE_AR_Recon
  inputs:
    AsOfDate: "2026-07-10"
    Currency: USD
  resource_key: jde_orchestrator_resource
  async_mode: true
  wait_for_completion: true
  timeout_seconds: 3600
  schedule: "0 3 * * *"
```

## Related

- `jde_orchestrator_resource` — shared auth
- `jde_orchestration_status_sensor` — event-drive on completion
- `jde_orchestration_output_ingestion` — capture orchestration output as DataFrame
- `jde_orchestrator_workspace` — auto-emit assets per orchestration
