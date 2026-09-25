# Segment Ingestion

Ingest Segment CDP workspace configuration data (sources, destinations, catalog) using dlt's generic REST API source.

Uses dlt's generic REST API source (`dlt.sources.rest_api.rest_api_source`).

By default, runs an in-memory DuckDB pipeline and returns a pandas DataFrame. Set `destination` to persist directly to any dlt-supported destination. See `../DESTINATIONS.md`.

> **Note:** This is Segment's Config API (workspace/source/destination configuration), not customer event data -- Segment's event pipeline has no general 'list all customer events' REST endpoint. Pagination param names for the Config API were not independently confirmed.


> **Partition honesty note:** if you set `partition_type`, it controls Dagster's materialization/backfill schedule only. This connector's compute function does not use the partition key to filter the API request, so every partition run re-fetches the same full resource set rather than a bounded window.

## Configuration

| Field | Required | Description |
|---|---|---|
| `asset_name` | required | Name of the asset that will hold the data |
| `access_token` | required | Segment workspace access token (requires Workspace Owner scope) for the Config API. |
| `resources` | optional | Comma-separated list of resources to extract: sources, destinations, catalog_sources. Default: `sources,destinations` |

## Example
```yaml
type: dagster_component_templates.SegmentIngestionComponent
attributes:
  asset_name: segment_ingestion
  access_token: "${SEGMENT_ACCESS_TOKEN}"
  resources: "sources,destinations"
```
