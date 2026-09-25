# Typeform Ingestion

Ingest Typeform forms and responses using dlt's generic REST API source.

Uses dlt's generic REST API source (`dlt.sources.rest_api.rest_api_source`) -- Typeform has no official dlt-maintained verified-source package, so this follows the same config-driven REST connector pattern as this repo's ad-platform ingestions.

By default, runs an in-memory DuckDB pipeline and returns a pandas DataFrame. Set `destination` to persist directly to any dlt-supported destination. See `../DESTINATIONS.md`.


> **Partition honesty note:** if you set `partition_type`, it controls Dagster's materialization/backfill schedule only. This connector's compute function does not use the partition key to filter the API request, so every partition run re-fetches the same full resource set rather than a bounded window. See `calendly_ingestion` / `pagerduty_events_ingestion` for connectors where the fetch is genuinely bound to the partition.

## Configuration

| Field | Required | Description |
|---|---|---|
| `asset_name` | required | Name of the asset that will hold the data |
| `access_token` | required | Typeform personal access token. Use ${TYPEFORM_ACCESS_TOKEN} for env vars. |
| `form_id` | optional | Specific form ID to pull responses for (required if 'responses' is in resources). |
| `resources` | optional | Comma-separated list of resources to extract: forms, responses (responses requires form_id). Default: `forms` |

## Example

```yaml
type: dagster_component_templates.TypeformIngestionComponent
attributes:
  asset_name: typeform_ingestion
  access_token: "${TYPEFORM_ACCESS_TOKEN}"
  resources: "forms"
```

Sourced from dltHub's public REST API connector reference (`https://dlthub.com/context/source/typeform`). Not yet run against a live account -- validate auth/pagination details against the vendor's current API docs before production use.
