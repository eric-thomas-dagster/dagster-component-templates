# Brex Ingestion

Ingest Brex user, location, vendor, and transfer data using dlt's generic REST API source.

Uses dlt's generic REST API source (`dlt.sources.rest_api.rest_api_source`).

By default, runs an in-memory DuckDB pipeline and returns a pandas DataFrame. Set `destination` to persist directly to any dlt-supported destination. See `../DESTINATIONS.md`.

> **Note:** Brex's API has also been documented under a platform.brexapi.com host in some places -- this connector uses api.brex.com; verify which host is current for your account type if requests fail. Pagination (cursor/next_cursor/has_more) param names are medium confidence.


> **Partition honesty note:** if you set `partition_type`, it controls Dagster's materialization/backfill schedule only. This connector's compute function does not use the partition key to filter the API request, so every partition run re-fetches the same full resource set rather than a bounded window.

## Configuration

| Field | Required | Description |
|---|---|---|
| `asset_name` | required | Name of the asset that will hold the data |
| `api_key` | required | Brex API key (generated in the Brex dashboard), used as a bearer token. |
| `resources` | optional | Comma-separated list of resources to extract: users, locations, vendors, transfers. Default: `users,transfers` |

## Example
```yaml
type: dagster_component_templates.BrexIngestionComponent
attributes:
  asset_name: brex_ingestion
  api_key: "${BREX_API_KEY}"
  resources: "users,transfers"
```
