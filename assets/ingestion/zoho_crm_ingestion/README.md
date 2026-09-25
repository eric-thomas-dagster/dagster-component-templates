# Zoho CRM Ingestion

Ingest Zoho CRM records using dlt's generic REST API source.

Uses dlt's generic REST API source (`dlt.sources.rest_api.rest_api_source`) -- Zoho CRM has no official dlt-maintained verified-source package, so this follows the same config-driven REST connector pattern as this repo's ad-platform ingestions.

By default, runs an in-memory DuckDB pipeline and returns a pandas DataFrame. Set `destination` to persist directly to any dlt-supported destination. See `../DESTINATIONS.md`.


> **Partition honesty note:** if you set `partition_type`, it controls Dagster's materialization/backfill schedule only. This connector's compute function does not use the partition key to filter the API request, so every partition run re-fetches the same full resource set rather than a bounded window. See `calendly_ingestion` / `pagerduty_events_ingestion` for connectors where the fetch is genuinely bound to the partition.

## Configuration

| Field | Required | Description |
|---|---|---|
| `asset_name` | required | Name of the asset that will hold the data |
| `access_token` | required | Zoho OAuth2 access token. Use ${ZOHO_ACCESS_TOKEN} for env vars. |
| `resources` | optional | Comma-separated list of resources to extract: Zoho CRM module API names, e.g. Leads, Contacts, Accounts, Deals. Default: `Leads,Contacts,Accounts,Deals` |

## Example

```yaml
type: dagster_component_templates.ZohoCrmIngestionComponent
attributes:
  asset_name: zoho_crm_ingestion
  access_token: "${ZOHO_ACCESS_TOKEN}"
  resources: "Leads,Contacts,Accounts,Deals"
```

Sourced from dltHub's public REST API connector reference (`https://dlthub.com/context/source/zoho_crm`). Not yet run against a live account -- validate auth/pagination details against the vendor's current API docs before production use.
