# Navan Ingestion

Ingest Navan (travel & expense) data using dlt's generic REST API source.

Uses dlt's generic REST API source (`dlt.sources.rest_api.rest_api_source`).

By default, runs an in-memory DuckDB pipeline and returns a pandas DataFrame. Set `destination` to persist directly to any dlt-supported destination. See `../DESTINATIONS.md`.

> **Note:** Not found on dltHub's reference catalog (404) -- Navan's public developer API documentation is limited/partner-gated, so base_url, auth, and resource paths here are a best-effort placeholder shape, not verified against real docs or a dltHub reference page. Treat this one as the least-confident component in this batch; confirm the actual base_url and auth flow with your Navan account team before use.


> **Partition honesty note:** if you set `partition_type`, it controls Dagster's materialization/backfill schedule only. This connector's compute function does not use the partition key to filter the API request, so every partition run re-fetches the same full resource set rather than a bounded window. See `calendly_ingestion` / `pagerduty_events_ingestion` for connectors where the fetch is genuinely bound to the partition.

## Configuration

| Field | Required | Description |
|---|---|---|
| `asset_name` | required | Name of the asset that will hold the data |
| `access_token` | required | Navan OAuth2 access token (client-credentials grant). Use ${NAVAN_ACCESS_TOKEN} for env vars. |
| `resources` | optional | Comma-separated list of resources to extract: trips, expenses, users. Default: `trips,expenses` |

## Example
```yaml
type: dagster_component_templates.NavanIngestionComponent
attributes:
  asset_name: navan_ingestion
  access_token: "${NAVAN_ACCESS_TOKEN}"
  resources: "trips,expenses"
```
