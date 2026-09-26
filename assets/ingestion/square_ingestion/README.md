# Square Ingestion

Ingest Square customer, catalog, and payment data using dlt's generic REST API source.

Uses dlt's generic REST API source (`dlt.sources.rest_api.rest_api_source`).

By default, runs an in-memory DuckDB pipeline and returns a pandas DataFrame. Set `destination` to persist directly to any dlt-supported destination. See `../DESTINATIONS.md`.

> **Note:** Square's Orders resource requires POST /v2/orders/search with a required location_ids body param, not a plain GET list, so it is not offered here as a simple list resource.


> **Partition honesty note:** if you set `partition_type`, it controls Dagster's materialization/backfill schedule only. This connector's compute function does not use the partition key to filter the API request, so every partition run re-fetches the same full resource set rather than a bounded window.

## Configuration

| Field | Required | Description |
|---|---|---|
| `asset_name` | required | Name of the asset that will hold the data |
| `access_token` | required | Square access token (Personal Access Token or OAuth2 token). |
| `resources` | optional | Comma-separated list of resources to extract: customers, catalog, payments. Default: `customers,payments` |

## Example
```yaml
type: dagster_component_templates.SquareIngestionComponent
attributes:
  asset_name: square_ingestion
  access_token: "${SQUARE_ACCESS_TOKEN}"
  resources: "customers,payments"
```
