# Ramp Ingestion

Ingest Ramp corporate card/spend data using dlt's generic REST API source.

Uses dlt's generic REST API source (`dlt.sources.rest_api.rest_api_source`).

By default, runs an in-memory DuckDB pipeline and returns a pandas DataFrame. Set `destination` to persist directly to any dlt-supported destination. See `../DESTINATIONS.md`.

> **Note:** Not found on dltHub's reference catalog (404) -- base_url, auth, and resource paths here are from general knowledge of Ramp's public developer API, not a verified dltHub source page. Verify against https://docs.ramp.com before production use.


> **Partition honesty note:** if you set `partition_type`, it controls Dagster's materialization/backfill schedule only. This connector's compute function does not use the partition key to filter the API request, so every partition run re-fetches the same full resource set rather than a bounded window. See `calendly_ingestion` / `pagerduty_events_ingestion` for connectors where the fetch is genuinely bound to the partition.

## Configuration

| Field | Required | Description |
|---|---|---|
| `asset_name` | required | Name of the asset that will hold the data |
| `access_token` | required | Ramp OAuth2 access token (client-credentials grant). Use ${RAMP_ACCESS_TOKEN} for env vars. |
| `resources` | optional | Comma-separated list of resources to extract: transactions, cards, users, reimbursements. Default: `transactions,cards` |

## Example
```yaml
type: dagster_component_templates.RampIngestionComponent
attributes:
  asset_name: ramp_ingestion
  access_token: "${RAMP_ACCESS_TOKEN}"
  resources: "transactions,cards"
```
