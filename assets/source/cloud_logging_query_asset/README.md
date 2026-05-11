# Cloud Logging Query Asset

Pull GCP log entries via [Cloud Logging filter language](https://cloud.google.com/logging/docs/view/logging-query-language) into a pandas DataFrame. One row per entry.

```yaml
type: dagster_component_templates.CloudLoggingQueryAssetComponent
attributes:
  asset_name: recent_errors
  filter: 'severity>=ERROR'
  lookback_minutes: 60
  max_entries: 500
```

## Columns produced

| Column | Type |
|---|---|
| `timestamp` | datetime |
| `severity` | string (DEBUG / INFO / WARNING / ERROR / CRITICAL / ALERT / EMERGENCY) |
| `log_name` | string |
| `resource_type` | string (`cloud_run_revision`, `k8s_container`, etc.) |
| `resource_labels` | dict |
| `text_payload` | string \| null |
| `json_payload` | dict \| null |
| `labels` | dict |
| `trace` | string |
| `insert_id` | string |

## Common filter patterns

| Use case | Filter |
|---|---|
| All errors from Cloud Run | `resource.type="cloud_run_revision" AND severity>=ERROR` |
| Audit deletes from GCS | `protoPayload.methodName="storage.objects.delete"` |
| Specific log stream | `logName="projects/<proj>/logs/my-app"` |
| Cloud Function errors | `resource.type="cloud_function" AND severity>=ERROR` |
| BigQuery query failures | `protoPayload.serviceName="bigquery.googleapis.com" AND severity=ERROR` |

## Auth

Service account needs `roles/logging.viewer` (or `roles/logging.privateLogViewer` for data-access audit logs).

## Why this vs. Cloud Logging native sinks?

Cloud Logging sinks (BigQuery / Pub/Sub / GCS) are great for high-volume retention but require pre-provisioning. This component is for **on-demand reads** in a Dagster pipeline — e.g. daily error-rate roll-ups, alerting on patterns, audit-event ingestion into a warehouse — without standing up a sink.
