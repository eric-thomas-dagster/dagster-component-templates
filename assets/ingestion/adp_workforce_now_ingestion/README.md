# ADP Workforce Now Ingestion

Ingest ADP Workforce Now HR data using dlt's generic REST API source.

Uses dlt's generic REST API source (`dlt.sources.rest_api.rest_api_source`).

By default, runs an in-memory DuckDB pipeline and returns a pandas DataFrame. Set `destination` to persist directly to any dlt-supported destination. See `../DESTINATIONS.md`.

> **Note:** ADP requires mutual-TLS (client certificate + key issued via ADP's API Central portal) to obtain an OAuth2 access token -- that certificate exchange happens outside this component (e.g. a scheduled token-refresh job); this component only consumes the resulting bearer access_token. Verify current endpoint paths against your ADP product edition before production use.


> **Partition honesty note:** if you set `partition_type`, it controls Dagster's materialization/backfill schedule only. This connector's compute function does not use the partition key to filter the API request, so every partition run re-fetches the same full resource set rather than a bounded window. See `calendly_ingestion` / `pagerduty_events_ingestion` for connectors where the fetch is genuinely bound to the partition.

## Configuration

| Field | Required | Description |
|---|---|---|
| `asset_name` | required | Name of the asset that will hold the data |
| `access_token` | required | ADP OAuth2 access token (obtained via mutual-TLS client credentials flow -- see README). Use ${ADP_ACCESS_TOKEN} for env vars. |
| `resources` | optional | Comma-separated list of resources to extract: workers, work_assignments, associate_contacts, job_requisitions. Default: `workers,work_assignments` |

## Example
```yaml
type: dagster_component_templates.AdpWorkforceNowIngestionComponent
attributes:
  asset_name: adp_workforce_now_ingestion
  access_token: "${ADP_ACCESS_TOKEN}"
  resources: "workers,work_assignments"
```
