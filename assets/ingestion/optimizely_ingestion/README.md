# Optimizely (Feature Experimentation) Ingestion

Ingest Optimizely Feature/Web Experimentation data (projects, experiments, audiences) using dlt's generic REST API source.

Uses dlt's generic REST API source (`dlt.sources.rest_api.rest_api_source`).

By default, runs an in-memory DuckDB pipeline and returns a pandas DataFrame. Set `destination` to persist directly to any dlt-supported destination. See `../DESTINATIONS.md`.

> **Note:** **Product-conflation risk**: Optimizely sells multiple, API-incompatible products -- this connector targets Feature/Web Experimentation (api.optimizely.com/v2), NOT Optimizely Content Cloud/CMS (a completely different API at cg.optimizely.com) and NOT the legacy 'Classic' Web Experimentation API (a third, different API at optimizelyapis.com/experiment/v1). Confirm which Optimizely product your organization actually uses before configuring this -- using the wrong one will simply 404 or authenticate against the wrong system entirely.


> **Partition honesty note:** if you set `partition_type`, it controls Dagster's materialization/backfill schedule only. This connector's compute function does not use the partition key to filter the API request, so every partition run re-fetches the same full resource set rather than a bounded window.

## Configuration

| Field | Required | Description |
|---|---|---|
| `asset_name` | required | Name of the asset that will hold the data |
| `access_token` | required | Optimizely Personal Access Token. |
| `project_id` | optional | Project ID. Required for experiments/audiences resources; not needed for projects. |
| `resources` | optional | Comma-separated list of resources to extract: projects (account-level); experiments, audiences (require project_id). Default: `projects` |

## Example
```yaml
type: dagster_component_templates.OptimizelyIngestionComponent
attributes:
  asset_name: optimizely_ingestion
  access_token: "${OPTIMIZELY_ACCESS_TOKEN}"
  resources: "projects"
```
