# CognosReportDataIngestionComponent

Run a **Cognos report** and materialize its output as a Dagster DataFrame. Right for landing report data in a warehouse or chaining Dagster transforms.

Pairs with `cognos_resource`.

## Output formats

Only CSV and JSON are parseable to DataFrames. Use `cognos_report_run_job` for PDF / HTML / XLSX exports (renders to file, not DataFrame).

## Example

```yaml
type: dagster_community_components.CognosReportDataIngestionComponent
attributes:
  asset_key: monthly_pnl_report
  report_id: i8B1A56A56789ABCDEF01234567890AB
  output_format: CSV
  parameters:
    Month: "2026-07"
  resource_key: cognos_resource
```

## Chaining

- `dataframe_to_snowflake` — land Cognos data in a warehouse
- `filter` + `summarize` — Cognos → analytics
- `dataframe_to_csv` — export for spreadsheet users

## Related

- `cognos_resource`
- `cognos_report_run_job`
- `cognos_workspace`
