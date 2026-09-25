# Webflow Ingestion

Ingest Webflow CMS data using dlt's generic REST API source.

Uses dlt's generic REST API source (`dlt.sources.rest_api.rest_api_source`) -- Webflow has no official dlt-maintained verified-source package, so this follows the same config-driven REST connector pattern as this repo's ad-platform ingestions.

By default, runs an in-memory DuckDB pipeline and returns a pandas DataFrame. Set `destination` to persist directly to any dlt-supported destination. See `../DESTINATIONS.md`.

## Configuration

| Field | Required | Description |
|---|---|---|
| `asset_name` | required | Name of the asset that will hold the data |
| `access_token` | required | Webflow API access token. Use ${WEBFLOW_ACCESS_TOKEN} for env vars. |
| `site_id` | optional | Site ID to scope collections to (required if 'collections' is in resources). |
| `collection_id` | optional | Collection ID to pull items for (required if 'items' is in resources). |
| `resources` | optional | Comma-separated list of resources to extract: sites, collections, items (items requires collection_id). Default: `sites,collections` |

## Example

```yaml
type: dagster_component_templates.WebflowIngestionComponent
attributes:
  asset_name: webflow_ingestion
  access_token: "${WEBFLOW_ACCESS_TOKEN}"
  resources: "sites,collections"
```

Sourced from dltHub's public REST API connector reference (`https://dlthub.com/context/source/webflow`). Not yet run against a live account -- validate auth/pagination details against the vendor's current API docs before production use.
