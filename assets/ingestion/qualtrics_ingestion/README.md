# Qualtrics Ingestion

Ingest Qualtrics survey and mailing list data using dlt's generic REST API source.

Uses dlt's generic REST API source (`dlt.sources.rest_api.rest_api_source`).

By default, runs an in-memory DuckDB pipeline and returns a pandas DataFrame. Set `destination` to persist directly to any dlt-supported destination. See `../DESTINATIONS.md`.


> **Partition honesty note:** if you set `partition_type`, it controls Dagster's materialization/backfill schedule only. This connector's compute function does not use the partition key to filter the API request, so every partition run re-fetches the same full resource set rather than a bounded window.

## Configuration

| Field | Required | Description |
|---|---|---|
| `asset_name` | required | Name of the asset that will hold the data |
| `datacenter_id` | required | Qualtrics datacenter ID (found in your account settings or the Qualtrics URL). |
| `api_token` | required | Qualtrics API token. |
| `directory_id` | optional | Directory (Contact List) ID. Required for the mailing_lists resource. |
| `resources` | optional | Comma-separated list of resources to extract: surveys (account-level); mailing_lists (requires directory_id). Default: `surveys` |

## Example
```yaml
type: dagster_component_templates.QualtricsIngestionComponent
attributes:
  asset_name: qualtrics_ingestion
  datacenter_id: "iad1"
  api_token: "${QUALTRICS_API_TOKEN}"
  resources: "surveys"
```
