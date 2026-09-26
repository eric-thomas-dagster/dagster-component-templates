# Docebo Ingestion

Ingest Docebo LMS user, course, and enrollment data using dlt's generic REST API source.

Uses dlt's generic REST API source (`dlt.sources.rest_api.rest_api_source`).

By default, runs an in-memory DuckDB pipeline and returns a pandas DataFrame. Set `destination` to persist directly to any dlt-supported destination. See `../DESTINATIONS.md`.

> **Note:** The data_selector (data.items) follows Docebo's typical response envelope but was not independently confirmed against live docs this session -- verify against your instance before production use. Pagination is offset-based (page/page_size, cap 200).


> **Partition honesty note:** if you set `partition_type`, it controls Dagster's materialization/backfill schedule only. This connector's compute function does not use the partition key to filter the API request, so every partition run re-fetches the same full resource set rather than a bounded window.

## Configuration

| Field | Required | Description |
|---|---|---|
| `asset_name` | required | Name of the asset that will hold the data |
| `subdomain` | required | Docebo subdomain (for mycompany.docebosaas.com). |
| `client_id` | required | Docebo OAuth2 client ID. |
| `client_secret` | required | Docebo OAuth2 client secret. |
| `resources` | optional | Comma-separated list of resources to extract: users, courses, enrollments. Default: `users,courses` |

## Example
```yaml
type: dagster_component_templates.DoceboIngestionComponent
attributes:
  asset_name: docebo_ingestion
  subdomain: "mycompany"
  client_id: "${DOCEBO_CLIENT_ID}"
  client_secret: "${DOCEBO_CLIENT_SECRET}"
  resources: "users,courses"
```
