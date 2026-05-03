# IntervalSchedule

Friendlier than cron when you just want 'every 30 minutes'. Generates a cron expression under the hood, plus the same job + ScheduleDefinition shape as `cron_schedule`.

## Example

```yaml
type: dagster_component_templates.IntervalScheduleComponent
attributes:
  schedule_name: poll_inventory
  every: 30m
  asset_keys: [inventory_snapshot, stockout_alerts]
  default_status: STOPPED
```
