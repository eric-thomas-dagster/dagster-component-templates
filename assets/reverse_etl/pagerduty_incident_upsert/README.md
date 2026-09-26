# `PagerDutyIncidentUpsertComponent`

Reverse-ETL sink: mirror an upstream DataFrame into **PagerDuty incidents** against a target service. Each row's `key_column` value becomes the incident's `incident_key` — PagerDuty's server-side dedup mechanism. Submitting the same key twice returns the existing open incident on the second call, so upsert is idempotent by design.

Optional `status_column` drives per-row transitions to `acknowledged`/`resolved`.

## When to use

- Auto-trigger PagerDuty incidents from a data pipeline's own health checks or downstream alerting logic, keeping them in sync (ack/resolve) as conditions change.

## Pairs with

- **`pagerduty_resource`** — connection (required).

## Configuration

| Field | Required | Description |
|---|---|---|
| `asset_name` | required | Output Dagster asset name. |
| `upstream_asset_key` | one of upstream_asset_key/source | Upstream Dagster asset providing the DataFrame. |
| `source` | one of upstream_asset_key/source | Inline source config (sql/csv/inline). |
| `service_id` | required | Target PagerDuty service ID (starts with 'P'). |
| `resource_key` | optional (default `pd`) | Resource key registered by PagerDutyResourceComponent. |
| `key_column` | required | Upstream column holding a stable unique key (-> incident_key). |
| `title_column` | required | Column holding the incident title. |
| `details_column` | optional | Column holding the incident body/details. |
| `urgency_column` | optional | Column holding urgency ('high'/'low'). |
| `status_column` | optional | Column holding target status per row. |
| `default_urgency` | optional (default `high`) | Urgency fallback when urgency_column unset. |
| `key_prefix` | optional (default `dagster-`) | Prepended to each key_column value to form the incident_key. |
| `batch_size` | optional (default `100`) | Max upstream rows to process per run. |

## Example
```yaml
type: dagster_component_templates.PagerDutyIncidentUpsertComponent
attributes:
  asset_name: pagerduty_incidents_mirror
  upstream_asset_key: incidents_seed
  service_id: PFF0H74
  resource_key: pd
  key_column: incident_id
  title_column: name
  details_column: description
  urgency_column: urgency
  status_column: status
```
