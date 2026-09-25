# LaunchDarkly Ingestion

Ingest LaunchDarkly feature flag and project data using dlt's generic REST API source.

Uses dlt's generic REST API source (`dlt.sources.rest_api.rest_api_source`).

By default, runs an in-memory DuckDB pipeline and returns a pandas DataFrame. Set `destination` to persist directly to any dlt-supported destination. See `../DESTINATIONS.md`.


> **Partition honesty note:** if you set `partition_type`, it controls Dagster's materialization/backfill schedule only. This connector's compute function does not use the partition key to filter the API request, so every partition run re-fetches the same full resource set rather than a bounded window.

## Configuration

| Field | Required | Description |
|---|---|---|
| `asset_name` | required | Name of the asset that will hold the data |
| `api_token` | required | LaunchDarkly API access token, sent raw in the Authorization header (no 'Bearer ' prefix). |
| `project_key` | optional | Project key. Required for environments/flags/segments resources; not needed for projects. |
| `resources` | optional | Comma-separated list of resources to extract: projects (account-level); environments, flags, segments (require project_key). Default: `projects` |

## Example
```yaml
type: dagster_component_templates.LaunchDarklyIngestionComponent
attributes:
  asset_name: launchdarkly_ingestion
  api_token: "${LAUNCHDARKLY_API_TOKEN}"
  project_key: "my-project"
  resources: "projects"
```
