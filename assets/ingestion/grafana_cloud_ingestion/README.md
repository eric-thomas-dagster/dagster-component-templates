# Grafana Cloud Ingestion

Ingest Grafana Cloud dashboard and datasource metadata using dlt's generic REST API source.

Uses dlt's generic REST API source (`dlt.sources.rest_api.rest_api_source`).

By default, runs an in-memory DuckDB pipeline and returns a pandas DataFrame. Set `destination` to persist directly to any dlt-supported destination. See `../DESTINATIONS.md`.

> **Note:** **Lower confidence.** Grafana is mid-migration from its legacy `/api` REST surface to a newer k8s-style `/apis` surface, and exact per-stack hostname/API-version details were not confirmed live this session -- verify against your actual stack before production use. This connector uses the legacy `/api` paths.


> **Partition honesty note:** if you set `partition_type`, it controls Dagster's materialization/backfill schedule only. This connector's compute function does not use the partition key to filter the API request, so every partition run re-fetches the same full resource set rather than a bounded window.

## Configuration

| Field | Required | Description |
|---|---|---|
| `asset_name` | required | Name of the asset that will hold the data |
| `stack_url` | required | Your Grafana Cloud stack URL (stack-specific -- no single fixed domain). |
| `service_account_token` | required | Grafana Cloud service account token. |
| `resources` | optional | Comma-separated list of resources to extract: dashboards, datasources. Default: `dashboards,datasources` |

## Example
```yaml
type: dagster_component_templates.GrafanaCloudIngestionComponent
attributes:
  asset_name: grafana_cloud_ingestion
  stack_url: "https://mystack.grafana.net"
  service_account_token: "${GRAFANA_SERVICE_ACCOUNT_TOKEN}"
  resources: "dashboards,datasources"
```
