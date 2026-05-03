# PrometheusResource

Wraps `dagster-prometheus`'s `PrometheusResource` so any asset / op can push counters and gauges to a Prometheus pushgateway. Useful for emitting per-run metrics that flow into Grafana dashboards.

Wraps the official `dagster-prometheus` package.

## Example

```yaml
type: dagster_component_templates.PrometheusResourceComponent
attributes:
  gateway: <fill in>
  timeout: <fill in>
  resource_key: <fill in>
```

## Requirements

```
dagster
dagster-prometheus
prometheus_client
```
