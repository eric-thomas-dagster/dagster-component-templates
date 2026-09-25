# Statsig Ingestion

Ingest Statsig feature gate and experiment configuration data using dlt's generic REST API source.

Uses dlt's generic REST API source (`dlt.sources.rest_api.rest_api_source`).

By default, runs an in-memory DuckDB pipeline and returns a pandas DataFrame. Set `destination` to persist directly to any dlt-supported destination. See `../DESTINATIONS.md`.

> **Note:** Statsig's Console API base path (statsigapi.net/console/v1) and the project_id query-param scoping were reconstructed from general knowledge, not a directly-confirmed live fetch. Verify the exact base_url and parameter names against a live Statsig account before production use.


> **Partition honesty note:** if you set `partition_type`, it controls Dagster's materialization/backfill schedule only. This connector's compute function does not use the partition key to filter the API request, so every partition run re-fetches the same full resource set rather than a bounded window.

## Configuration

| Field | Required | Description |
|---|---|---|
| `asset_name` | required | Name of the asset that will hold the data |
| `console_api_key` | required | Statsig Console API key. |
| `project_id` | optional | Project ID. Required for feature_gates/dynamic_configs/experiments resources; not needed for projects. |
| `resources` | optional | Comma-separated list of resources to extract: projects (org-level); feature_gates, dynamic_configs, experiments (require project_id). Default: `projects` |

## Example
```yaml
type: dagster_component_templates.StatsigIngestionComponent
attributes:
  asset_name: statsig_ingestion
  console_api_key: "${STATSIG_CONSOLE_API_KEY}"
  resources: "projects"
```
