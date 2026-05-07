# DataFrame → Prometheus

Push DataFrame rows as Prometheus metrics via a pushgateway. Each row becomes a metric sample with labels from configured columns. Useful for batch-job metrics that surface in operational dashboards.

## Wide-to-long pattern (when you have multiple metric columns)

If your DataFrame has multiple metric columns (one column per metric), use the existing `unpivot` transform first to get a long-form (label, metric_name, value) DataFrame, then sink each metric_name as a separate label:

```
raw_metrics (wide: timestamp, cpu, memory, requests)
   |
   v  unpivot (id_columns=[timestamp], value_columns=[cpu, memory, requests])
   |
   v  long_metrics (timestamp, metric_name, value)
   |
   v  dataframe_to_prometheus (label_columns=[metric_name], value_column=value)
```

No dedicated Prometheus transform needed — exposition format is simple enough that the sink handles it inline. Compare to OCSF where the schema's complexity warrants a separate `ocsf_normalizer` transform.

## Companion

- `dataframe_from_prometheus` — query Prometheus → DataFrame
- `prometheus_resource` (existing) — pushgateway resource for custom ops

## Validated

Local Prometheus + pushgateway: 4 samples pushed (orders_total{region, category}), Prometheus scraped, query returned all 4 with correct values.
