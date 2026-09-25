# Close CRM Ingestion

Ingest Close CRM data using dlt's generic REST API source.

Uses dlt's generic REST API source (`dlt.sources.rest_api.rest_api_source`) -- Close CRM has no official dlt-maintained verified-source package, so this follows the same config-driven REST connector pattern as this repo's ad-platform ingestions.

By default, runs an in-memory DuckDB pipeline and returns a pandas DataFrame. Set `destination` to persist directly to any dlt-supported destination. See `../DESTINATIONS.md`.

## Configuration

| Field | Required | Description |
|---|---|---|
| `asset_name` | required | Name of the asset that will hold the data |
| `api_key` | required | Close API key (used as HTTP Basic username, blank password). Use ${CLOSE_API_KEY} for env vars. |
| `resources` | optional | Comma-separated list of resources to extract: leads, contacts, activities, opportunities. Default: `leads,contacts,activities,opportunities` |

## Example

```yaml
type: dagster_component_templates.CloseCrmIngestionComponent
attributes:
  asset_name: close_crm_ingestion
  api_key: "${CLOSE_API_KEY}"
  resources: "leads,contacts,activities,opportunities"
```

Sourced from dltHub's public REST API connector reference (`https://dlthub.com/context/source/close_crm`). Not yet run against a live account -- validate auth/pagination details against the vendor's current API docs before production use.
