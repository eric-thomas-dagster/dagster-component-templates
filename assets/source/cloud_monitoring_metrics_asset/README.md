# Cloud Monitoring Metrics Asset

Pull GCP time-series metrics via Cloud Monitoring's [ListTimeSeries](https://cloud.google.com/monitoring/api/ref_v3/rest/v3/projects.timeSeries/list) API into a pandas DataFrame.

```yaml
type: dagster_component_templates.CloudMonitoringMetricsAssetComponent
attributes:
  asset_name: gce_cpu_utilization
  filter: 'metric.type="compute.googleapis.com/instance/cpu/utilization"'
  lookback_minutes: 60
  alignment_period_seconds: 60
  aligner: MEAN
```

## Common metrics

| What | Filter |
|---|---|
| GCE CPU | `metric.type="compute.googleapis.com/instance/cpu/utilization"` |
| Cloud Run req/s | `metric.type="run.googleapis.com/request_count"` |
| Cloud Run latency | `metric.type="run.googleapis.com/request_latencies"` |
| BigQuery slot usage | `metric.type="bigquery.googleapis.com/slots/total_available"` |
| GCS bytes | `metric.type="storage.googleapis.com/storage/total_bytes"` |
| Pub/Sub backlog | `metric.type="pubsub.googleapis.com/subscription/num_undelivered_messages"` |

Browse all at <https://cloud.google.com/monitoring/api/metrics_gcp>.

## Alignment + reduction

- **Aligner** = per-series bucketing (e.g. `MEAN` averages within each `alignment_period_seconds` bucket on each series independently).
- **Cross-series reducer** = optional collapse across all matching series (e.g. `SUM` across every Cloud Run revision).

For total request count across an entire Cloud Run service:
```yaml
aligner: RATE
cross_series_reducer: SUM
group_by_fields: ["resource.labels.service_name"]
```

## Auth

Service account needs `roles/monitoring.viewer`.
