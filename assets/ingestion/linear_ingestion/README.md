# Linear Ingestion

Ingest Linear issue-tracker data via Linear's GraphQL API.

> **Note:** Linear's API is GraphQL-only -- there is no REST surface. This connector does **not** use dlt's generic `rest_api_source` (that machinery assumes REST paths/pagination and would be a poor, misleading fit). Instead it defines plain `@dlt.resource` generators that page through Linear's GraphQL cursor pagination (`pageInfo.hasNextPage` / `endCursor`) directly via `requests`, then hands those resources to a normal dlt pipeline -- so destination flexibility (snowflake/bigquery/postgres/filesystem/duckdb/etc, same as every other ingestion component in this repo) is preserved even though the fetch mechanism differs.

By default, runs an in-memory DuckDB pipeline and returns a pandas DataFrame. Set `destination` to persist directly to any dlt-supported destination. See `../DESTINATIONS.md`.

## Configuration

| Field | Required | Description |
|---|---|---|
| `asset_name` | required | Name of the asset that will hold the data |
| `api_key` | required | Linear personal API key or OAuth2 access token. Sent raw in the Authorization header (no 'Bearer ' prefix for a personal API key). |
| `resources` | optional | Comma-separated list of resources to extract: issues, projects, teams, cycles. Default: `issues,projects,teams` |

## Partitions

The `issues` resource is genuinely bound to the partition window: when `partition_type` is set, the GraphQL query includes a `filter: { createdAt: { gte: ..., lt: ... } }` clause scoped to the partition's time range. `projects`, `teams`, and `cycles` are snapshot-style and are **not** bound -- every run re-fetches them in full regardless of partition.

## Example
```yaml
type: dagster_component_templates.LinearIngestionComponent
attributes:
  asset_name: linear_ingestion
  api_key: "${LINEAR_API_KEY}"
  resources: "issues,projects,teams"
```
