# QlikComposeWorkflowTriggerJobComponent

Trigger a **Qlik Compose workflow** — full build, incremental update, data-mart refresh — via the Compose REST API. Wraps a Dagster job around a single control-plane call, with optional wait-for-completion polling.

Pairs with `qlik_compose_resource` for auth.

## Example

```yaml
type: dagster_community_components.QlikComposeWorkflowTriggerJobComponent
attributes:
  job_name: build_finance_dw
  project: FinanceDW
  workflow: FullBuildAndPopulate
  action: run
  wait_for_completion: true
  poll_interval_seconds: 30
  timeout_seconds: 3600
  resource_key: qlik_compose_resource
  schedule: "0 2 * * *"
  default_status: RUNNING
```

## Related

- `qlik_compose_resource` — auth
- `qlik_compose_workflow_status_sensor` — event-drive
- `qlik_compose_workflow_metrics_ingestion` — metrics DataFrame
- `qlik_compose_workspace` — workspace-shape (auto-emit assets per Project × Workflow × Data Mart)
