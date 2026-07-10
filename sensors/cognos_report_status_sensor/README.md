# CognosReportStatusSensorComponent

Poll a **Cognos report** for its last-run status; trigger a Dagster job on transitions into target statuses. Cursor-deduped by `(status, run_id)` so downstream fires once per run.

Pairs with `cognos_resource`.

## Example

```yaml
type: dagster_community_components.CognosReportStatusSensorComponent
attributes:
  sensor_name: finance_report_done
  report_id: i8B1A56A56789ABCDEF01234567890AB
  target_statuses: [COMPLETED, SUCCEEDED]
  job_name: notify_finance_team
  resource_key: cognos_resource
```

## Related

- `cognos_resource`
- `cognos_report_run_job`
- `cognos_workspace`
