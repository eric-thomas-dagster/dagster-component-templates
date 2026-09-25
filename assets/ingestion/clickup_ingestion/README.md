# ClickUp Ingestion

Ingest ClickUp project management data using dlt's generic REST API source.

Uses dlt's generic REST API source (`dlt.sources.rest_api.rest_api_source`) -- ClickUp has no official dlt-maintained verified-source package, so this follows the same config-driven REST connector pattern as this repo's ad-platform ingestions.

By default, runs an in-memory DuckDB pipeline and returns a pandas DataFrame. Set `destination` to persist directly to any dlt-supported destination. See `../DESTINATIONS.md`.


> **Partition honesty note:** if you set `partition_type`, it controls Dagster's materialization/backfill schedule only. This connector's compute function does not use the partition key to filter the API request, so every partition run re-fetches the same full resource set rather than a bounded window. See `calendly_ingestion` / `pagerduty_events_ingestion` for connectors where the fetch is genuinely bound to the partition.

## Configuration

| Field | Required | Description |
|---|---|---|
| `asset_name` | required | Name of the asset that will hold the data |
| `team_id` | required | ClickUp team (workspace) ID. |
| `api_token` | required | ClickUp personal API token or OAuth2 token. Use ${CLICKUP_API_TOKEN} for env vars. |
| `resources` | optional | Comma-separated list of resources to extract: spaces, folders, lists, tasks. Default: `spaces,folders,lists` |

## Example

```yaml
type: dagster_component_templates.ClickUpIngestionComponent
attributes:
  asset_name: clickup_ingestion
  team_id: "${CLICKUP_TEAM_ID}"
  api_token: "${CLICKUP_API_TOKEN}"
  resources: "spaces,folders,lists"
```

Sourced from dltHub's public REST API connector reference (`https://dlthub.com/context/source/clickup`). Not yet run against a live account -- validate auth/pagination details against the vendor's current API docs before production use.
