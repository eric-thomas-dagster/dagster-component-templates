# Persona Ingestion

Ingest Persona identity-verification inquiry, account, and case data using dlt's generic REST API source.

Uses dlt's generic REST API source (`dlt.sources.rest_api.rest_api_source`).

By default, runs an in-memory DuckDB pipeline and returns a pandas DataFrame. Set `destination` to persist directly to any dlt-supported destination. See `../DESTINATIONS.md`.

> **Note:** Persona's API follows a JSON:API style (cursor pagination via page[after]/page[before]) -- exact param names were not independently confirmed against live docs this session (docs pages 404'd during research). Verify before relying on complete pulls beyond the first page.


> **Partition honesty note:** if you set `partition_type`, it controls Dagster's materialization/backfill schedule only. This connector's compute function does not use the partition key to filter the API request, so every partition run re-fetches the same full resource set rather than a bounded window.

## Configuration

| Field | Required | Description |
|---|---|---|
| `asset_name` | required | Name of the asset that will hold the data |
| `api_key` | required | Persona API key. |
| `resources` | optional | Comma-separated list of resources to extract: inquiries, accounts, cases. Default: `inquiries,accounts` |

## Example
```yaml
type: dagster_component_templates.PersonaIngestionComponent
attributes:
  asset_name: persona_ingestion
  api_key: "${PERSONA_API_KEY}"
  resources: "inquiries,accounts"
```
