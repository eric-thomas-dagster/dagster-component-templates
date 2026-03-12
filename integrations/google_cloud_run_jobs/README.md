# Google Cloud Run Jobs

Imports Google Cloud Run Jobs as Dagster assets. Each Cloud Run Job becomes a materializable asset; materialization triggers a new job execution via the Cloud Run Jobs API.

## Required packages

```
google-cloud-run>=0.10.0
google-auth>=2.17.0
```

## Fields

| Field | Required | Default | Description |
|---|---|---|---|
| `project_id` | Yes | — | GCP project ID |
| `location` | No | `us-central1` | GCP location/region |
| `credentials_path` | No | `null` | Path to service account JSON (omit to use Application Default Credentials) |
| `import_jobs` | No | `true` | Import Cloud Run Jobs as assets |
| `filter_by_name_pattern` | No | `null` | Regex to filter jobs by name |
| `exclude_name_pattern` | No | `null` | Regex to exclude jobs by name |
| `group_name` | No | `google_cloud_run_jobs` | Dagster asset group name |

## Example YAML

```yaml
type: dagster_component_templates.GoogleCloudRunJobsComponent
attributes:
  project_id: my-gcp-project
  location: us-central1
  credentials_path: "{{ env('GOOGLE_APPLICATION_CREDENTIALS') }}"
  import_jobs: true
  group_name: google_cloud_run_jobs
```

## Asset Dependencies & Lineage

This component supports a `deps` field for declaring upstream Dagster asset dependencies:

```yaml
attributes:
  # ... other fields ...
  deps:
    - raw_orders              # simple asset key
    - raw/schema/orders       # asset key with path prefix
```

`deps` draws lineage edges in the Dagster asset graph without loading data at runtime. Use it to express that this asset depends on upstream tables or assets produced by other components.

Dependencies can also be wired externally via `map_resolved_asset_specs()` in `definitions.py` — the same approach used by [Dagster Designer](https://github.com/eric-thomas-dagster/dagster_designer).
