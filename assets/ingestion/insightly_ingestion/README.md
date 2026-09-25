# Insightly Ingestion

Ingest Insightly CRM data using dlt's generic REST API source.

Uses dlt's generic REST API source (`dlt.sources.rest_api.rest_api_source`) -- Insightly has no official dlt-maintained verified-source package, so this follows the same config-driven REST connector pattern as this repo's ad-platform ingestions.

By default, runs an in-memory DuckDB pipeline and returns a pandas DataFrame. Set `destination` to persist directly to any dlt-supported destination. See `../DESTINATIONS.md`.


> **Partition honesty note:** if you set `partition_type`, it controls Dagster's materialization/backfill schedule only. This connector's compute function does not use the partition key to filter the API request, so every partition run re-fetches the same full resource set rather than a bounded window. See `calendly_ingestion` / `pagerduty_events_ingestion` for connectors where the fetch is genuinely bound to the partition.

## Configuration

| Field | Required | Description |
|---|---|---|
| `asset_name` | required | Name of the asset that will hold the data |
| `pod` | required | Insightly pod identifier (from your Insightly URL, e.g. 'na1'). |
| `api_key` | required | Insightly API key (HTTP Basic username, blank password). Use ${INSIGHTLY_API_KEY} for env vars. |
| `resources` | optional | Comma-separated list of resources to extract: contacts, leads, organisations, opportunities, projects. Default: `contacts,leads,organisations,opportunities,projects` |

## Example

```yaml
type: dagster_component_templates.InsightlyIngestionComponent
attributes:
  asset_name: insightly_ingestion
  pod: "na1"
  api_key: "${INSIGHTLY_API_KEY}"
  resources: "contacts,leads,organisations,opportunities,projects"
```

Sourced from dltHub's public REST API connector reference (`https://dlthub.com/context/source/insightly`). Not yet run against a live account -- validate auth/pagination details against the vendor's current API docs before production use.
