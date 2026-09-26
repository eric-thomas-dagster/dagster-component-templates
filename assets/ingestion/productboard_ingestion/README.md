# Productboard Ingestion

Ingest Productboard notes, features, components, and objectives using dlt's generic REST API source.

Uses dlt's generic REST API source (`dlt.sources.rest_api.rest_api_source`).

By default, runs an in-memory DuckDB pipeline and returns a pandas DataFrame. Set `destination` to persist directly to any dlt-supported destination. See `../DESTINATIONS.md`.

> **Note:** The notes resource's date-filter param names (dateFrom/dateTo) were reconstructed from general knowledge, not independently confirmed against live docs this session -- verify before relying on partition binding in production. features/components/objectives are snapshot-style and are not bound.


> **Partition honesty note:** the `notes` resource is genuinely bound to the partition window via `dateFrom`/`dateTo` params. `features`, `components`, and `objectives` are snapshot-style and are **not** bound -- every run re-fetches them in full regardless of partition.

## Configuration

| Field | Required | Description |
|---|---|---|
| `asset_name` | required | Name of the asset that will hold the data |
| `access_token` | required | Productboard API access token. |
| `resources` | optional | Comma-separated list of resources to extract: notes, features, components, objectives. Default: `notes,features` |

## Example
```yaml
type: dagster_component_templates.ProductboardIngestionComponent
attributes:
  asset_name: productboard_ingestion
  access_token: "${PRODUCTBOARD_ACCESS_TOKEN}"
  resources: "notes,features"
```
