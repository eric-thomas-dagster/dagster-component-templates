# ConvertKit (Kit) Ingestion

Ingest ConvertKit (rebranded 'Kit') subscriber, broadcast, and tag data using dlt's generic REST API source.

Uses dlt's generic REST API source (`dlt.sources.rest_api.rest_api_source`).

By default, runs an in-memory DuckDB pipeline and returns a pandas DataFrame. Set `destination` to persist directly to any dlt-supported destination. See `../DESTINATIONS.md`.

> **Note:** ConvertKit rebranded to 'Kit' -- this connector targets the current v4 API (api.kit.com). Pagination param names (likely after/before cursor style) are medium confidence.


> **Partition honesty note:** if you set `partition_type`, it controls Dagster's materialization/backfill schedule only. This connector's compute function does not use the partition key to filter the API request, so every partition run re-fetches the same full resource set rather than a bounded window.

## Configuration

| Field | Required | Description |
|---|---|---|
| `asset_name` | required | Name of the asset that will hold the data |
| `api_key` | required | ConvertKit (Kit) v4 API key, sent as the X-Kit-Api-Key header. |
| `resources` | optional | Comma-separated list of resources to extract: subscribers, broadcasts, tags, forms. Default: `subscribers,tags` |

## Example
```yaml
type: dagster_component_templates.ConvertKitIngestionComponent
attributes:
  asset_name: convertkit_ingestion
  api_key: "${CONVERTKIT_API_KEY}"
  resources: "subscribers,tags"
```
