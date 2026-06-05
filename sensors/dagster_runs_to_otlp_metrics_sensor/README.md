# Dagster Runs → OTLP Metrics

Push Dagster run lifecycle events as **OpenTelemetry metrics** to any OTLP/HTTP endpoint.

**Distinct from `dagster_runs_to_otlp_sensor`** which emits OTel **logs**. Pick this one if your downstream wants metrics (Datadog OTel ingest, Grafana OTel, Honeycomb metrics endpoint, Splunk OTel Collector metrics receiver, etc.).

## Metrics emitted per run

| Metric | OTel type | Value | Attributes |
|---|---|---|---|
| `dagster.runs.total` | Sum (cumulative, monotonic) | `1` per event | `dagster.status`, `dagster.job` |
| `dagster.runs.duration_seconds` | Gauge | `duration_s` | `dagster.status`, `dagster.job` |
| `dagster.runs.failure_step` | Sum | `1` per failed run | `dagster.job`, `dagster.step` |

Resource attributes:

| Attribute | Value |
|---|---|
| `service.name` | from `service_name` config (default `dagster`) |
| `service.instance.id` | from `location_label` config |

## Targets it works with

- **OpenTelemetry Collector** (`otel/opentelemetry-collector-contrib`) — point `otlp_endpoint_env_var` at `http://collector:4318` and configure your downstream from the collector side.
- **Datadog OTel ingest** — `OTLP_METRICS_ENDPOINT=https://api.datadoghq.com/api/v2/otlp` + headers `DD-API-KEY=...`.
- **Grafana Cloud OTel** — `https://otlp-gateway-…grafana.net/otlp/v1/metrics`.
- **Honeycomb metrics endpoint** — `https://api.honeycomb.io` + headers `x-honeycomb-team=…,x-honeycomb-dataset=…`.

## Example

```yaml
type: dagster_component_templates.DagsterRunsToOtlpMetricsSensorComponent
attributes:
  sensor_name: dagster_runs_to_otel_metrics
  otlp_endpoint_env_var: OTLP_METRICS_ENDPOINT
  otlp_headers_env_var: OTLP_METRICS_HEADERS    # k=v,k=v
  service_name: dagster
  location_label: prod-east
  default_status: RUNNING
```

## When to use which sensor

| Need | Sensor |
|---|---|
| OTel logs (Splunk OTel Collector, Honeycomb logs, Grafana Loki via OTel) | `dagster_runs_to_otlp_sensor` |
| **OTel metrics** (Datadog OTel ingest, Grafana OTel, Honeycomb metrics) | **`dagster_runs_to_otlp_metrics_sensor` ← this** |
| StatsD / DogStatsD (Datadog Agent, graphite-statsd, etcd-statsd) | `dagster_runs_to_statsd_sensor` |
| Prometheus push gateway / VictoriaMetrics | `dagster_runs_to_prometheus_sensor` |
| InfluxDB line protocol | `dagster_runs_to_influxdb_sensor` |
| Splunk HEC direct | `dagster_runs_to_splunk_hec_sensor` |
