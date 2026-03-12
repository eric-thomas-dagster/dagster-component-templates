# Coalesce Project

Introspects a Coalesce environment at component load time and creates one Dagster asset per Coalesce node (SQL model), with upstream dependencies that mirror the Coalesce DAG. Materialization triggers the Coalesce Scheduler API to run only the selected subset of nodes.

## Required packages

```
requests>=2.28.0
```

## Fields

| Field | Required | Default | Description |
|---|---|---|---|
| `environment_id` | Yes | — | Coalesce environment ID |
| `job_id` | Yes | — | Coalesce job ID used to execute node runs |
| `api_token_env_var` | Yes | — | Env var containing the Coalesce API token |
| `asset_key_prefix` | No | `coalesce` | Prefix for all generated asset keys |
| `group_name` | No | `coalesce` | Dagster asset group name |
| `poll_interval_seconds` | No | `10.0` | Seconds between run status polls |
| `timeout_seconds` | No | `3600` | Maximum seconds to wait for a run |
| `base_url` | No | `https://app.coalescesoftware.io` | Coalesce base URL |

## Example YAML

```yaml
type: dagster_component_templates.CoalesceProjectComponent
attributes:
  environment_id: "prod-environment-abc123"
  job_id: "daily-transform-job-xyz"
  api_token_env_var: COALESCE_API_TOKEN
  asset_key_prefix: coalesce
  group_name: coalesce_transforms
  timeout_seconds: 3600
```
