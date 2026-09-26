# Zoho Desk Ingestion

Ingest Zoho Desk ticket, contact, and agent data using dlt's generic REST API source.

Uses dlt's generic REST API source (`dlt.sources.rest_api.rest_api_source`).

By default, runs an in-memory DuckDB pipeline and returns a pandas DataFrame. Set `destination` to persist directly to any dlt-supported destination. See `../DESTINATIONS.md`.

> **Note:** Docs fetch failed this session; the tickets/contacts/agents/departments paths and 'data' data_selector come from general knowledge of Zoho Desk's API, not a freshly-confirmed live fetch. Pagination is offset-based via from/limit (max ~100) -- not independently confirmed. This connector uses a real OAuth2 refresh-token exchange (unlike the older zoho_crm_ingestion in this repo, which takes a static already-minted access_token that expires in ~1 hour and requires manual reissuing).


> **Partition honesty note:** if you set `partition_type`, it controls Dagster's materialization/backfill schedule only. This connector's compute function does not use the partition key to filter the API request, so every partition run re-fetches the same full resource set rather than a bounded window.

## Configuration

| Field | Required | Description |
|---|---|---|
| `asset_name` | required | Name of the asset that will hold the data |
| `client_id` | required | Zoho app client ID (used for the OAuth2 refresh-token exchange). |
| `client_secret` | required | Zoho app client secret. |
| `refresh_token` | required | OAuth2 refresh token obtained once via Zoho's OAuth consent flow. |
| `org_id` | required | Zoho Desk organization ID (required on most API calls). |
| `datacenter` | optional | Zoho datacenter TLD suffix: 'com' (default), 'eu', 'in', 'com.au', or 'jp'. |
| `resources` | optional | Comma-separated list of resources to extract: tickets, contacts, agents, departments. Default: `tickets,contacts` |

## Example
```yaml
type: dagster_component_templates.ZohoDeskIngestionComponent
attributes:
  asset_name: zoho_desk_ingestion
  client_id: "${ZOHO_CLIENT_ID}"
  client_secret: "${ZOHO_CLIENT_SECRET}"
  refresh_token: "${ZOHO_DESK_REFRESH_TOKEN}"
  org_id: "${ZOHO_DESK_ORG_ID}"
  resources: "tickets,contacts"
```
