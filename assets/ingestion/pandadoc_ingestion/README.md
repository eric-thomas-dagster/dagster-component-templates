# PandaDoc Ingestion

Ingest PandaDoc document, template, and folder data using dlt's generic REST API source.

Uses dlt's generic REST API source (`dlt.sources.rest_api.rest_api_source`).

By default, runs an in-memory DuckDB pipeline and returns a pandas DataFrame. Set `destination` to persist directly to any dlt-supported destination. See `../DESTINATIONS.md`.

> **Note:** The folders endpoint path (documents/folders) is medium confidence -- verify against live docs before production use.


> **Partition honesty note:** if you set `partition_type`, it controls Dagster's materialization/backfill schedule only. This connector's compute function does not use the partition key to filter the API request, so every partition run re-fetches the same full resource set rather than a bounded window.

## Configuration

| Field | Required | Description |
|---|---|---|
| `asset_name` | required | Name of the asset that will hold the data |
| `api_key` | required | PandaDoc API key. |
| `resources` | optional | Comma-separated list of resources to extract: documents, templates, folders. Default: `documents,templates` |

## Example
```yaml
type: dagster_component_templates.PandaDocIngestionComponent
attributes:
  asset_name: pandadoc_ingestion
  api_key: "${PANDADOC_API_KEY}"
  resources: "documents,templates"
```
