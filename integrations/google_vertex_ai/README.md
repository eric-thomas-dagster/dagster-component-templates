# Google Vertex AI

Imports Google Vertex AI training jobs, batch prediction jobs, custom jobs, and pipelines as Dagster assets. Discovers recently completed jobs as asset templates. An optional observation sensor emits `AssetMaterialization` events when training jobs succeed.

## Required packages

```
google-cloud-aiplatform>=1.38.0
google-auth>=2.17.0
```

## Fields

| Field | Required | Default | Description |
|---|---|---|---|
| `project_id` | Yes | — | GCP project ID |
| `location` | No | `us-central1` | GCP location/region |
| `credentials_path` | No | `null` | Path to service account JSON (omit to use Application Default Credentials) |
| `import_training_jobs` | No | `true` | Import training job definitions |
| `import_batch_prediction_jobs` | No | `true` | Import batch prediction job definitions |
| `import_custom_jobs` | No | `true` | Import custom job definitions |
| `import_pipelines` | No | `true` | Import pipeline definitions |
| `filter_by_name_pattern` | No | `null` | Regex to filter entities by name |
| `exclude_name_pattern` | No | `null` | Regex to exclude entities by name |
| `filter_by_labels` | No | `null` | Comma-separated label keys to filter by |
| `generate_sensor` | No | `true` | Generate an observation sensor |
| `poll_interval_seconds` | No | `60` | Sensor poll interval |
| `group_name` | No | `google_vertex_ai` | Dagster asset group name |

## Example YAML

```yaml
type: dagster_component_templates.GoogleVertexAIComponent
attributes:
  project_id: my-gcp-project
  location: us-central1
  credentials_path: "{{ env('GOOGLE_APPLICATION_CREDENTIALS') }}"
  import_training_jobs: true
  import_batch_prediction_jobs: true
  import_pipelines: true
  filter_by_labels: "env,team"
  group_name: google_vertex_ai
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
