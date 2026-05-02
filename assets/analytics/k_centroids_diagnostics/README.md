# KCentroidsDiagnostics

Sweeps k from k_min to k_max, fits k-means at each k, and emits a row per k with: inertia (elbow), silhouette score, Calinski-Harabasz score, Davies-Bouldin score. Use the elbow + silhouette to pick the right k.

## Example

```yaml
type: dagster_component_templates.KCentroidsDiagnosticsComponent
attributes:
  asset_name: rfm_k_diagnostics
  upstream_asset_key: customer_rfm_metrics
  feature_columns: [recency, frequency, monetary]
  k_min: 2
  k_max: 10
  group_name: clustering
```


## Requirements

```
pandas
scikit-learn
numpy
```
