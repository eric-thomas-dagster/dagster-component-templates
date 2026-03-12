# AWS SageMaker

Imports AWS SageMaker training jobs, batch transform jobs, processing jobs, and pipelines as Dagster assets. Discovers recently completed jobs (last 30 days) as templates for new runs. An optional observation sensor emits `AssetMaterialization` events when training jobs complete.

## Required packages

```
boto3>=1.26.0
botocore>=1.29.0
```

## Fields

| Field | Required | Default | Description |
|---|---|---|---|
| `aws_region` | Yes | — | AWS region |
| `aws_access_key_id` | No | `null` | AWS access key ID (omit to use IAM role) |
| `aws_secret_access_key` | No | `null` | AWS secret access key |
| `aws_session_token` | No | `null` | AWS session token |
| `import_training_jobs` | No | `true` | Import training job definitions |
| `import_transform_jobs` | No | `true` | Import transform job definitions |
| `import_processing_jobs` | No | `true` | Import processing job definitions |
| `import_pipelines` | No | `true` | Import pipeline definitions |
| `filter_by_name_pattern` | No | `null` | Regex to filter entities by name |
| `exclude_name_pattern` | No | `null` | Regex to exclude entities by name |
| `filter_by_tags` | No | `null` | Comma-separated tag keys to filter by |
| `generate_sensor` | No | `true` | Generate an observation sensor |
| `poll_interval_seconds` | No | `60` | Sensor poll interval |
| `group_name` | No | `aws_sagemaker` | Dagster asset group name |

## Example YAML

```yaml
type: dagster_component_templates.AWSSageMakerComponent
attributes:
  aws_region: us-east-1
  aws_access_key_id: "{{ env('AWS_ACCESS_KEY_ID') }}"
  aws_secret_access_key: "{{ env('AWS_SECRET_ACCESS_KEY') }}"
  import_training_jobs: true
  import_pipelines: true
  filter_by_name_pattern: "^prod-"
  group_name: aws_sagemaker
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
