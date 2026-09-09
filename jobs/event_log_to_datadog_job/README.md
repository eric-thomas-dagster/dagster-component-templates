# EventLogToDatadogJobComponent

Op-shaped job that ships new Dagster event log entries to Datadog as
log entries via the HTTP intake API. Useful for teams whose obs stack
is Datadog — get Dagster reliability alongside your app + infra logs.

## Behavior

- Cursor via run tag; first run pulls `initial_lookback_hours` of history.
- Splits into ≤1000-entry batches (Datadog intake limit).
- Each event becomes a Datadog log with:
  - `service` and `env` tags (configurable)
  - `ddtags`: `job_name:X,event_type:Y`
  - Structured `attributes.*` fields for storage_id, run_id, step_key, asset_key

## YAML example

```yaml
type: dagster_component_templates.EventLogToDatadogJobComponent
attributes:
  job_name: dagster_events_to_datadog
  schedule: "*/5 * * * *"
  default_status: RUNNING
  api_key_env: DATADOG_API_KEY
  intake_url: https://http-intake.logs.datadoghq.com/api/v2/logs    # EU: .eu
  service: dagster
  env: prod
```

## Required env vars

```bash
DATADOG_API_KEY=...
```

## Datadog site

- US: `https://http-intake.logs.datadoghq.com/api/v2/logs` (default)
- EU: `https://http-intake.logs.datadoghq.eu/api/v2/logs`
- US3: `https://http-intake.logs.us3.datadoghq.com/api/v2/logs`
- Gov: `https://http-intake.logs.ddog-gov.com/api/v2/logs`
