# EventLogToSplunkJobComponent

Op-shaped job that ships new Dagster event log entries to Splunk via
the HTTP Event Collector (HEC).

## How this differs from `dagster_plus_to_siem_job`

| | `dagster_plus_to_siem_job` | `event_log_to_splunk_job` (this) |
|---|---|---|
| Data source | Dagster+ GraphQL (audit / runs / asset events) | Dagster OSS `context.instance` event log |
| Works on Dagster OSS? | ❌ | ✅ |
| Destinations | Splunk / Sentinel / Datadog / Sumo / S3 | Splunk HEC only |
| Normalization | OCSF / ECS transforms | Raw event dict |
| Shape | Compound: pull → normalize → ship | Simple: read → POST |

Reach for `dagster_plus_to_siem_job` when you're on Dagster+ and want
centralized security telemetry across multiple destinations. Reach for
this one when you're on Dagster OSS and want raw event log data into
your existing Splunk instance.

## Behavior

- Cursor via run tag; first run pulls `initial_lookback_hours` of history.
- Each event becomes a Splunk HEC event with `sourcetype`, optional `index`, `source=dagster`.
- Payload sent as newline-delimited JSON in a single POST.

## YAML example

```yaml
type: dagster_component_templates.EventLogToSplunkJobComponent
attributes:
  job_name: dagster_events_to_splunk
  schedule: "*/5 * * * *"
  default_status: RUNNING
  hec_url: https://splunk.acme.com:8088/services/collector/event
  hec_token_env: SPLUNK_HEC_TOKEN
  sourcetype: dagster:event
  index: main
  verify_ssl: true
```

## Required env vars

```bash
SPLUNK_HEC_TOKEN=<hec-token>    # from Splunk: Settings → Data Inputs → HTTP Event Collector
```
