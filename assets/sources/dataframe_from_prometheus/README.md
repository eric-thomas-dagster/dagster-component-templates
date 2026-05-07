# Prometheus PromQL Query

Run a PromQL query against any Prometheus-compatible server's HTTP API and return results as a DataFrame. Works against open-source Prometheus, Cortex, Thanos, Mimir, VictoriaMetrics, Azure Managed Prometheus, Grafana Cloud.

## Backends

Works against any Prometheus-compatible HTTP API:

- Open-source Prometheus
- Cortex / Thanos / Mimir (set `extra_headers: {X-Scope-OrgID: tenant1}` for multi-tenant)
- VictoriaMetrics
- **Azure Managed Prometheus** — bearer token from `az account get-access-token --resource https://prometheus.monitor.azure.com`
- Grafana Cloud — bearer token from Grafana Cloud API key
- AWS Managed Prometheus — needs sigv4 sidecar/proxy

## Validated

Local Prometheus + pushgateway: 4 samples pushed via `dataframe_to_prometheus`, scraped, queried back with correct labels.
