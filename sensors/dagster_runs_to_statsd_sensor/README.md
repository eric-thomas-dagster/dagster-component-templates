# Dagster Runs → StatsD

Push Dagster run lifecycle events (SUCCESS / FAILURE) as StatsD UDP metrics. **No external Python deps** — just raw datagrams over a socket.

Pairs with `dagster_runs_to_prometheus_sensor` (push gateway / VictoriaMetrics), `dagster_runs_to_influxdb_sensor` (line protocol), and `dagster_runs_to_otlp_sensor` (which emits OTel **logs**, not metrics). Pick whichever wire protocol matches your existing observability stack.

## Metrics emitted per run

| Metric | Type | Value | Tags |
|---|---|---|---|
| `<prefix>.runs.total` | counter | `+1` | `status`, `job` (+ extra_tags) |
| `<prefix>.runs.duration_milliseconds` | timer | `duration_ms` | `status`, `job` (+ extra_tags) |
| `<prefix>.runs.failures` | counter | `+1` | `job`, `step` (+ extra_tags) — failure only |

Wire format (one datagram per metric):

```
dagster.runs.total:1|c|#status:success,job:my_job,env:prod
dagster.runs.duration_milliseconds:1240.5|ms|#status:success,job:my_job,env:prod
```

Set `dogstatsd_tags: false` to drop the `|#…` tag block (plain statsd protocol; only `prefix.runs.total:1|c` is emitted, no tag info).

## When to use which target

| Target | Config |
|---|---|
| **Datadog Agent** (same host) | `statsd_host: 127.0.0.1`, `statsd_port: 8125`, `dogstatsd_tags: true` |
| **Local statsd** (e.g. `graphiteapp/graphite-statsd` Docker image) | `statsd_host: 127.0.0.1`, `statsd_port: 8125`, `dogstatsd_tags: false` |
| **etcd-statsd / Grafana statsd_exporter** | `dogstatsd_tags: true` (both support tag blocks) |
| **Telegraf statsd input** | `dogstatsd_tags: true` (datadog_extensions mode) |

## Example

```yaml
type: dagster_component_templates.DagsterRunsToStatsdSensorComponent
attributes:
  sensor_name: dagster_runs_to_datadog
  statsd_host: 127.0.0.1
  statsd_port: 8125
  metric_prefix: dagster
  dogstatsd_tags: true
  extra_tags:
    env: prod
    team: data-platform
  monitor_jobs: [refresh_analytics_job, daily_ingest_job]  # optional allowlist
  default_status: RUNNING
```

## What you'll see in your tool

- **Datadog**: a metric called `dagster.runs.total` with `status` + `job` tags. Build dashboards on success rate, P50 / P95 duration, failure breakdown by job + step.
- **Grafana (via statsd_exporter → Prometheus)**: same metric names, querable as `dagster_runs_total{status="success"}` etc.
- **Graphite (direct)**: dot-separated metric tree under `dagster.runs.*`.

## When NOT to use this

| Need | Right component |
|---|---|
| Push metrics to a Prometheus pushgateway | `dagster_runs_to_prometheus_sensor` |
| Push to InfluxDB / Telegraf line protocol | `dagster_runs_to_influxdb_sensor` |
| Push as OTel logs (Splunk OTel Collector, Honeycomb logs, etc.) | `dagster_runs_to_otlp_sensor` |
| Push as OTel **metrics** (Datadog OTel ingest, Grafana OTel collector, etc.) | `dagster_runs_to_otlp_metrics_sensor` |
| Failure-only telemetry with rich error context | `dagster_failures_to_otlp_sensor` |
