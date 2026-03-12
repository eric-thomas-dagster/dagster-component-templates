# Google Cloud Dataflow

Imports Google Cloud Dataflow batch and streaming jobs as Dagster assets. Batch jobs are represented as materializable assets; streaming jobs are represented as observable assets (they run continuously). An optional observation sensor emits `AssetMaterialization` events for running, completed, or failed jobs.

## Required packages

```
google-cloud-dataflow-client>=0.8.0
google-auth>=2.17.0
```

## Fields

| Field | Required | Default | Description |
|---|---|---|---|
| `project_id` | Yes | — | GCP project ID |
| `location` | No | `us-central1` | GCP location/region |
| `credentials_path` | No | `null` | Path to service account JSON (omit to use Application Default Credentials) |
| `import_batch_jobs` | No | `true` | Import batch job templates as assets |
| `import_streaming_jobs` | No | `true` | Import streaming jobs as observable assets |
| `filter_by_name_pattern` | No | `null` | Regex to filter jobs by name |
| `exclude_name_pattern` | No | `null` | Regex to exclude jobs by name |
| `generate_sensor` | No | `true` | Generate an observation sensor |
| `poll_interval_seconds` | No | `60` | Sensor poll interval |
| `group_name` | No | `google_dataflow` | Dagster asset group name |

## Example YAML

```yaml
type: dagster_component_templates.GoogleDataflowComponent
attributes:
  project_id: my-gcp-project
  location: us-central1
  credentials_path: "{{ env('GOOGLE_APPLICATION_CREDENTIALS') }}"
  import_batch_jobs: true
  import_streaming_jobs: true
  group_name: google_dataflow
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
