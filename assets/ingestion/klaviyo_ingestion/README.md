# Klaviyo Ingestion

Ingest Klaviyo marketing data using dlt's generic REST API source.

Uses dlt's generic REST API source (`dlt.sources.rest_api.rest_api_source`) -- Klaviyo has no official dlt-maintained verified-source package, so this follows the same config-driven REST connector pattern as this repo's ad-platform ingestions.

By default, runs an in-memory DuckDB pipeline and returns a pandas DataFrame. Set `destination` to persist directly to any dlt-supported destination. See `../DESTINATIONS.md`.


> **Partition honesty note:** if you set `partition_type`, it controls Dagster's materialization/backfill schedule only. This connector's compute function does not use the partition key to filter the API request, so every partition run re-fetches the same full resource set rather than a bounded window. See `calendly_ingestion` / `pagerduty_events_ingestion` for connectors where the fetch is genuinely bound to the partition.

## Configuration

| Field | Required | Description |
|---|---|---|
| `asset_name` | required | Name of the asset that will hold the data |
| `api_key` | required | Klaviyo private API key. Use ${KLAVIYO_API_KEY} for env vars. |
| `resources` | optional | Comma-separated list of resources to extract: lists, campaigns, flows, profiles, metrics. Default: `lists,campaigns,flows,profiles,metrics` |

## Example

```yaml
type: dagster_component_templates.KlaviyoIngestionComponent
attributes:
  asset_name: klaviyo_ingestion
  api_key: "${KLAVIYO_API_KEY}"
  resources: "lists,campaigns,flows,profiles,metrics"
```

Sourced from dltHub's public REST API connector reference (`https://dlthub.com/context/source/klaviyo`). Not yet run against a live account -- validate auth/pagination details against the vendor's current API docs before production use.
