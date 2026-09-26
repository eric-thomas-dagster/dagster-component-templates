# Vercel Ingestion

Ingest Vercel deployment, project, and domain data using dlt's generic REST API source.

Uses dlt's generic REST API source (`dlt.sources.rest_api.rest_api_source`).

By default, runs an in-memory DuckDB pipeline and returns a pandas DataFrame. Set `destination` to persist directly to any dlt-supported destination. See `../DESTINATIONS.md`.

> **Note:** Vercel's exact date-filter param names for deployments (since/until, milliseconds) were not independently confirmed against live docs this session -- verify before relying on partition binding in production. projects and domains are snapshot-style and are not bound.


> **Partition honesty note:** the `deployments` resource is genuinely bound to the partition window via `since`/`until` params. `projects` and `domains` are snapshot-style and are **not** bound -- every run re-fetches them in full regardless of partition.

## Configuration

| Field | Required | Description |
|---|---|---|
| `asset_name` | required | Name of the asset that will hold the data |
| `access_token` | required | Vercel personal or team access token. |
| `team_id` | optional | Vercel team ID. Required if the token has access to multiple teams. |
| `resources` | optional | Comma-separated list of resources to extract: deployments, projects, domains. Default: `deployments,projects` |

## Example
```yaml
type: dagster_component_templates.VercelIngestionComponent
attributes:
  asset_name: vercel_ingestion
  access_token: "${VERCEL_ACCESS_TOKEN}"
  resources: "deployments,projects"
```
