# QlikComposeWorkflowMetricsIngestionComponent

Emit a DataFrame with per-workflow metrics from **Qlik Compose**. One row per workflow per materialization. Foundation for DW-build observability dashboards.

Pairs with `qlik_compose_resource`.

## Emitted schema

| Column | Type | Notes |
|---|---|---|
| `project` | string | Compose project (DW) name |
| `workflow` | string | Workflow name |
| `state` | string | RUNNING / COMPLETED / STOPPED / ERROR / FAILED |
| `last_run_duration_seconds` | float | Wall-clock of the last run |
| `rows_loaded` | int | Total rows written by the workflow |
| `error_count` | int | Non-null even at zero |
| `last_completion_time` | string | ISO-8601 |
| `polled_at` | float | Unix epoch seconds |

## Example

```yaml
type: dagster_community_components.QlikComposeWorkflowMetricsIngestionComponent
attributes:
  asset_key: qlik_compose_metrics
  projects: [FinanceDW, SalesDW]
  group_name: qlik_observability
  resource_key: qlik_compose_resource
```

## Chaining

- `dataframe_to_snowflake` → BI dashboards
- `dataframe_to_prometheus` → alert on error rate
- `filter` (`error_count > 0`) → `dataframe_to_pagerduty` → alert on any errors

## Related

- `qlik_compose_resource`
- `qlik_compose_workflow_trigger_job`
- `qlik_compose_workspace`
