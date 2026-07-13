# PagerDutyWorkspaceComponent

Auto-emit one Dagster asset per **PagerDuty service**. `StateBackedComponent` — discovery cached to disk. Materializing an asset fetches recent incidents for the service and emits a DataFrame.

## Example

```yaml
type: dagster_community_components.PagerDutyWorkspaceComponent
attributes:
  api_token_env_var: PAGERDUTY_API_TOKEN
  service_selector:
    by_pattern: ["prod-*", "data-*"]
  incidents_since_days: 30
  incidents_limit: 100
  statuses: [triggered, acknowledged, resolved]
```

## Ingestion vs alerting

- **This workspace** = ingest incident history into Dagster (for SLA dashboards, MTTR analytics, on-call reports).
- **`pagerduty_incident_sensor` (existing)** = trigger a Dagster job when a new incident fires.
- **`pagerduty_heartbeat_job` (existing)** = keep a monitored service alive.

## Related

- `pagerduty_resource`
- `pagerduty_incident_sensor`
- `pagerduty_heartbeat_job`
