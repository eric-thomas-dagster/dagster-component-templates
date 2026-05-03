# AssetJob

Creates a job over the configured asset selection so it shows up as a discrete unit in the Dagster UI's Jobs tab. Useful when you want a one-button 'run this subset' experience without committing to a schedule.

## Example

```yaml
type: dagster_component_templates.AssetJobComponent
attributes:
  job_name: backfill_revenue
  asset_keys: [orders_raw, orders_clean, daily_revenue, monthly_revenue]
  description: One-click revenue refresh from raw orders all the way through monthly rollup.
  tags:
    purpose: backfill
```
