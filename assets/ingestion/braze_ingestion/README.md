# Braze Ingestion

Ingest Braze marketing automation data using dlt's generic REST API source.

Uses dlt's generic REST API source (`dlt.sources.rest_api.rest_api_source`) -- Braze has no official dlt-maintained verified-source package, so this follows the same config-driven REST connector pattern as this repo's ad-platform ingestions.

By default, runs an in-memory DuckDB pipeline and returns a pandas DataFrame. Set `destination` to persist directly to any dlt-supported destination. See `../DESTINATIONS.md`.

## Configuration

| Field | Required | Description |
|---|---|---|
| `asset_name` | required | Name of the asset that will hold the data |
| `rest_endpoint` | required | Account-specific Braze REST endpoint (from the Braze dashboard). |
| `api_key` | required | Braze REST API key. Use ${BRAZE_API_KEY} for env vars. |
| `resources` | optional | Comma-separated list of resources to extract: segments, campaigns, canvases, content_blocks. Default: `segments,campaigns,canvases,content_blocks` |

## Example

```yaml
type: dagster_component_templates.BrazeIngestionComponent
attributes:
  asset_name: braze_ingestion
  rest_endpoint: "https://rest.iad-01.braze.com"
  api_key: "${BRAZE_API_KEY}"
  resources: "segments,campaigns,canvases,content_blocks"
```

Sourced from dltHub's public REST API connector reference (`https://dlthub.com/context/source/braze`). Not yet run against a live account -- validate auth/pagination details against the vendor's current API docs before production use.
