# Webex Ingestion

Ingest Cisco Webex room, meeting, and people data using dlt's generic REST API source.

Uses dlt's generic REST API source (`dlt.sources.rest_api.rest_api_source`).

By default, runs an in-memory DuckDB pipeline and returns a pandas DataFrame. Set `destination` to persist directly to any dlt-supported destination. See `../DESTINATIONS.md`.

> **Note:** Webex has three auth variants with different tradeoffs: a personal access token (static but expires ~12h, testing only), an OAuth2 integration (non-interactive refresh possible but needs the refresh flow implemented separately), or a bot token (static, long-lived -- used here as the best fit for unattended ingestion). meetings may require a hostEmail param for admin-scope listing beyond the bot's own meetings; not implemented here. Pagination via the HTTP Link header, not independently confirmed to be auto-handled by dlt's default paginator.


> **Partition honesty note:** if you set `partition_type`, it controls Dagster's materialization/backfill schedule only. This connector's compute function does not use the partition key to filter the API request, so every partition run re-fetches the same full resource set rather than a bounded window.

## Configuration

| Field | Required | Description |
|---|---|---|
| `asset_name` | required | Name of the asset that will hold the data |
| `bot_token` | required | Webex bot access token (static, long-lived) -- the recommended non-interactive auth path. A personal access token also works but expires after ~12 hours. |
| `resources` | optional | Comma-separated list of resources to extract: rooms, meetings, people. Default: `rooms,people` |

## Example
```yaml
type: dagster_component_templates.WebexIngestionComponent
attributes:
  asset_name: webex_ingestion
  bot_token: "${WEBEX_BOT_TOKEN}"
  resources: "rooms,people"
```
