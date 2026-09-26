# CircleCI Ingestion

Ingest CircleCI pipeline and workflow data using dlt's generic REST API source.

Uses dlt's generic REST API source (`dlt.sources.rest_api.rest_api_source`).

By default, runs an in-memory DuckDB pipeline and returns a pandas DataFrame. Set `destination` to persist directly to any dlt-supported destination. See `../DESTINATIONS.md`.


> **Partition honesty note:** if you set `partition_type`, it controls Dagster's materialization/backfill schedule only. This connector's compute function does not use the partition key to filter the API request, so every partition run re-fetches the same full resource set rather than a bounded window.

## Configuration

| Field | Required | Description |
|---|---|---|
| `asset_name` | required | Name of the asset that will hold the data |
| `api_token` | required | CircleCI personal API token. |
| `project_slug` | optional | Project slug (e.g. 'gh/my-org/my-repo'). Required for the project_pipelines resource. |
| `resources` | optional | Comma-separated list of resources to extract: pipelines (account-level); project_pipelines (requires project_slug). Default: `pipelines` |

## Example
```yaml
type: dagster_component_templates.CircleCIIngestionComponent
attributes:
  asset_name: circleci_ingestion
  api_token: "${CIRCLECI_API_TOKEN}"
  resources: "pipelines"
```
