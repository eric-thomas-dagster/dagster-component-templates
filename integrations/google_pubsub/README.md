# Google Cloud Pub/Sub

Imports Google Cloud Pub/Sub topics and subscriptions as Dagster assets for observing message queue status and throughput. Each topic and subscription becomes an observable asset; materialization records metadata about the queue configuration.

## Required packages

```
google-cloud-pubsub>=2.18.0
google-auth>=2.17.0
```

## Fields

| Field | Required | Default | Description |
|---|---|---|---|
| `project_id` | Yes | — | GCP project ID |
| `credentials_path` | No | `null` | Path to service account JSON (omit to use Application Default Credentials) |
| `import_topics` | No | `true` | Import Pub/Sub topics as observable assets |
| `import_subscriptions` | No | `true` | Import Pub/Sub subscriptions as observable assets |
| `filter_by_name_pattern` | No | `null` | Regex to filter entities by name |
| `exclude_name_pattern` | No | `null` | Regex to exclude entities by name |
| `generate_sensor` | No | `true` | Generate an observation sensor |
| `poll_interval_seconds` | No | `60` | Sensor poll interval |
| `group_name` | No | `google_pubsub` | Dagster asset group name |

## Example YAML

```yaml
type: dagster_component_templates.GooglePubSubComponent
attributes:
  project_id: my-gcp-project
  credentials_path: "{{ env('GOOGLE_APPLICATION_CREDENTIALS') }}"
  import_topics: true
  import_subscriptions: true
  filter_by_name_pattern: "^prod-"
  group_name: google_pubsub
```
