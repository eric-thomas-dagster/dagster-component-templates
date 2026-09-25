# Calendly Ingestion

Ingest Calendly scheduling data using dlt's generic REST API source.

Uses dlt's generic REST API source (`dlt.sources.rest_api.rest_api_source`) -- Calendly has no official dlt-maintained verified-source package, so this follows the same config-driven REST connector pattern as this repo's ad-platform ingestions.

By default, runs an in-memory DuckDB pipeline and returns a pandas DataFrame. Set `destination` to persist directly to any dlt-supported destination. See `../DESTINATIONS.md`.

## Configuration

| Field | Required | Description |
|---|---|---|
| `asset_name` | required | Name of the asset that will hold the data |
| `access_token` | required | Calendly personal access token. Use ${CALENDLY_ACCESS_TOKEN} for env vars. |
| `organization_uri` | optional | Organization URI to scope events/memberships to (optional). |
| `resources` | optional | Comma-separated list of resources to extract: scheduled_events, event_types, organization_memberships. Default: `scheduled_events,event_types` |

## Example

```yaml
type: dagster_component_templates.CalendlyIngestionComponent
attributes:
  asset_name: calendly_ingestion
  access_token: "${CALENDLY_ACCESS_TOKEN}"
  organization_uri: "https://api.calendly.com/organizations/AAAA"
  resources: "scheduled_events,event_types"
```

Sourced from dltHub's public REST API connector reference (`https://dlthub.com/context/source/calendly`). Not yet run against a live account -- validate auth/pagination details against the vendor's current API docs before production use.
