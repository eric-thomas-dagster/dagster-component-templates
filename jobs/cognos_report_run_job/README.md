# CognosReportRunJobComponent

Trigger a **Cognos Analytics report** run via REST. Supports parameter overrides + multiple output formats.

Pairs with `cognos_resource`.

## Example

```yaml
type: dagster_community_components.CognosReportRunJobComponent
attributes:
  job_name: daily_finance_report
  report_id: i8B1A56A56789ABCDEF01234567890AB
  output_format: PDF
  parameters:
    AsOfDate: "2026-07-10"
  resource_key: cognos_resource
  wait_for_completion: true
  schedule: "0 6 * * *"
```

Report IDs come from the report's URL in Cognos or the Store ID in Content Manager.

## Related

- `cognos_resource`
- `cognos_report_status_sensor` — event-drive on report completion
- `cognos_report_data_ingestion` — bring report output as a DataFrame
- `cognos_workspace` — auto-emit assets per report
