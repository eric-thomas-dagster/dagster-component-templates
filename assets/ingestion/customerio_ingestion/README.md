# Customer.io Ingestion

Ingest Customer.io App API data (campaigns, segments, newsletters) using dlt's generic REST API source.

Uses dlt's generic REST API source (`dlt.sources.rest_api.rest_api_source`).

By default, runs an in-memory DuckDB pipeline and returns a pandas DataFrame. Set `destination` to persist directly to any dlt-supported destination. See `../DESTINATIONS.md`.

> **Note:** Customer.io actually splits its API across three separate hosts (Track, App, Pipelines/CDP) with different auth schemes; this connector only covers the App API (Bearer token: campaigns/segments/newsletters). It does not expose a general customer list/CRUD resource -- Customer.io is event/segment-oriented, not a customer database browser. Verify exact endpoint paths against docs.customer.io/api before production use.


> **Partition honesty note:** if you set `partition_type`, it controls Dagster's materialization/backfill schedule only. This connector's compute function does not use the partition key to filter the API request, so every partition run re-fetches the same full resource set rather than a bounded window.

## Configuration

| Field | Required | Description |
|---|---|---|
| `asset_name` | required | Name of the asset that will hold the data |
| `app_api_key` | required | Customer.io App API bearer token. |
| `resources` | optional | Comma-separated list of resources to extract: campaigns, segments, newsletters. Default: `campaigns,segments` |

## Example
```yaml
type: dagster_component_templates.CustomerIOIngestionComponent
attributes:
  asset_name: customerio_ingestion
  app_api_key: "${CUSTOMERIO_APP_API_KEY}"
  resources: "campaigns,segments"
```
