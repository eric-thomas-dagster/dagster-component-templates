# Freshservice Ingestion

Ingest Freshservice ITSM ticket, requester, agent, and asset data using dlt's generic REST API source. Distinct product from Freshdesk.

Uses dlt's generic REST API source (`dlt.sources.rest_api.rest_api_source`).

By default, runs an in-memory DuckDB pipeline and returns a pandas DataFrame. Set `destination` to persist directly to any dlt-supported destination. See `../DESTINATIONS.md`.


> **Partition honesty note:** if you set `partition_type`, it controls Dagster's materialization/backfill schedule only. This connector's compute function does not use the partition key to filter the API request, so every partition run re-fetches the same full resource set rather than a bounded window.

## Configuration

| Field | Required | Description |
|---|---|---|
| `asset_name` | required | Name of the asset that will hold the data |
| `domain` | required | Freshservice domain. |
| `api_key` | required | Freshservice API key, used as the HTTP Basic auth username (password is the literal string 'X'). |
| `resources` | optional | Comma-separated list of resources to extract: tickets, requesters, agents, assets. Default: `tickets,requesters` |

## Example
```yaml
type: dagster_component_templates.FreshserviceIngestionComponent
attributes:
  asset_name: freshservice_ingestion
  domain: "mycompany.freshservice.com"
  api_key: "${FRESHSERVICE_API_KEY}"
  resources: "tickets,requesters"
```
