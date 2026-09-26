# Metabase Ingestion

Ingest Metabase database, card (question), and collection metadata using dlt's generic REST API source.

Uses dlt's generic REST API source (`dlt.sources.rest_api.rest_api_source`).

By default, runs an in-memory DuckDB pipeline and returns a pandas DataFrame. Set `destination` to persist directly to any dlt-supported destination. See `../DESTINATIONS.md`.

> **Note:** Metabase's auth header name (x-api-key) is used by newer versions; older self-hosted instances may instead require a session token from POST /api/session -- if authentication fails with a 401, your instance likely needs the session-token flow instead of a static API key. The /database endpoint wraps its list under a 'data' key; /card and /collection return bare top-level arrays -- confirmed medium-high confidence, not independently live-tested.


> **Partition honesty note:** if you set `partition_type`, it controls Dagster's materialization/backfill schedule only. This connector's compute function does not use the partition key to filter the API request, so every partition run re-fetches the same full resource set rather than a bounded window.

## Configuration

| Field | Required | Description |
|---|---|---|
| `asset_name` | required | Name of the asset that will hold the data |
| `instance_url` | required | Base URL of your Metabase instance (self-hosted or Cloud). |
| `api_key` | required | Metabase API key (newer versions; Settings > Admin > API Keys). |
| `resources` | optional | Comma-separated list of resources to extract: databases, cards, collections. Default: `databases,cards,collections` |

## Example
```yaml
type: dagster_component_templates.MetabaseIngestionComponent
attributes:
  asset_name: metabase_ingestion
  instance_url: "https://metabase.mycompany.com"
  api_key: "${METABASE_API_KEY}"
  resources: "databases,cards,collections"
```
