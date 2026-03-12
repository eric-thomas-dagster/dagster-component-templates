# Google Cloud Datastream

Imports Google Cloud Datastream CDC streams and (optionally) connection profiles as Dagster assets. Streams monitor change-data-capture replication from source databases (MySQL, PostgreSQL, Oracle) to BigQuery or Cloud Storage. An optional observation sensor emits `AssetMaterialization` events for running, paused, or failed streams.

## Required packages

```
google-cloud-datastream>=0.5.0
google-auth>=2.17.0
```

## Fields

| Field | Required | Default | Description |
|---|---|---|---|
| `project_id` | Yes | — | GCP project ID |
| `location` | No | `us-central1` | GCP location/region |
| `credentials_path` | No | `null` | Path to service account JSON (omit to use Application Default Credentials) |
| `import_streams` | No | `true` | Import Datastream streams as observable assets |
| `import_connection_profiles` | No | `false` | Import connection profiles as observable assets |
| `filter_by_name_pattern` | No | `null` | Regex to filter entities by name |
| `exclude_name_pattern` | No | `null` | Regex to exclude entities by name |
| `generate_sensor` | No | `true` | Generate an observation sensor |
| `poll_interval_seconds` | No | `60` | Sensor poll interval |
| `group_name` | No | `google_datastream` | Dagster asset group name |

## Example YAML

```yaml
type: dagster_component_templates.GoogleDatastreamComponent
attributes:
  project_id: my-gcp-project
  location: us-central1
  credentials_path: "{{ env('GOOGLE_APPLICATION_CREDENTIALS') }}"
  import_streams: true
  import_connection_profiles: false
  group_name: google_datastream
```
