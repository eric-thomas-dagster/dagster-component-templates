# CronSchedule

Defines a `ScheduleDefinition` + an underlying asset job that materializes the configured asset keys whenever the cron expression fires. Replaces the ad-hoc `dg launch --assets '*'` pattern with a real Dagster schedule that shows up in the UI.

## Example

```yaml
type: dagster_component_templates.CronScheduleComponent
attributes:
  schedule_name: nightly_etl
  cron_expression: "0 2 * * *"
  asset_keys: [orders_clean, customer_segments, daily_revenue]
  execution_timezone: America/Los_Angeles
  default_status: STOPPED
  tags:
    team: data-platform
```
