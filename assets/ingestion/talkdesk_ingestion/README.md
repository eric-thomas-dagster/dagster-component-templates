# Talkdesk Ingestion

Ingest Talkdesk contact center data (contacts, campaigns, users) using dlt's generic REST API source.

Uses dlt's generic REST API source (`dlt.sources.rest_api.rest_api_source`).

By default, runs an in-memory DuckDB pipeline and returns a pandas DataFrame. Set `destination` to persist directly to any dlt-supported destination. See `../DESTINATIONS.md`.

> **Note:** Talkdesk's OAuth2 client_credentials flow and API host split (token from the account subdomain, data from api.talkdeskapp.com) is medium confidence -- not independently confirmed against live docs this session. Pagination style is unconfirmed.


> **Partition honesty note:** if you set `partition_type`, it controls Dagster's materialization/backfill schedule only. This connector's compute function does not use the partition key to filter the API request, so every partition run re-fetches the same full resource set rather than a bounded window.

## Configuration

| Field | Required | Description |
|---|---|---|
| `asset_name` | required | Name of the asset that will hold the data |
| `account_subdomain` | required | Talkdesk account subdomain (for mycompany.talkdesk.com), used to obtain the OAuth2 token. |
| `client_id` | required | Talkdesk OAuth2 client_credentials client ID. |
| `client_secret` | required | Talkdesk OAuth2 client_credentials client secret. |
| `resources` | optional | Comma-separated list of resources to extract: contacts, campaigns, users. Default: `contacts,campaigns` |

## Example
```yaml
type: dagster_component_templates.TalkdeskIngestionComponent
attributes:
  asset_name: talkdesk_ingestion
  account_subdomain: "mycompany"
  client_id: "${TALKDESK_CLIENT_ID}"
  client_secret: "${TALKDESK_CLIENT_SECRET}"
  resources: "contacts,campaigns"
```
