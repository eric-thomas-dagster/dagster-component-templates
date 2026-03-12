# Google Cloud Functions

Imports Google Cloud Functions (2nd gen) as Dagster assets. Each function becomes a materializable asset; materialization invokes the function via the Cloud Functions API.

## Required packages

```
google-cloud-functions>=1.13.0
google-auth>=2.17.0
```

## Fields

| Field | Required | Default | Description |
|---|---|---|---|
| `project_id` | Yes | — | GCP project ID |
| `location` | No | `us-central1` | GCP location/region |
| `credentials_path` | No | `null` | Path to service account JSON (omit to use Application Default Credentials) |
| `import_functions` | No | `true` | Import Cloud Functions as assets |
| `filter_by_name_pattern` | No | `null` | Regex to filter functions by name |
| `exclude_name_pattern` | No | `null` | Regex to exclude functions by name |
| `group_name` | No | `google_cloud_functions` | Dagster asset group name |

## Example YAML

```yaml
type: dagster_component_templates.GoogleCloudFunctionsComponent
attributes:
  project_id: my-gcp-project
  location: us-central1
  credentials_path: "{{ env('GOOGLE_APPLICATION_CREDENTIALS') }}"
  import_functions: true
  filter_by_name_pattern: "^etl-"
  group_name: google_cloud_functions
```
