# AppendCluster

Trains a k-means model in-place on the input DataFrame and appends a `cluster` column with the assigned cluster ID per row. Lighter alternative to `k_means_clustering` when you don't need the centroid distances and just want the label as a feature.

## Example

```yaml
type: dagster_component_templates.AppendClusterComponent
attributes:
  asset_name: customers_with_cluster
  upstream_asset_key: customer_rfm_metrics
  feature_columns: [recency, frequency, monetary]
  n_clusters: 5
  output_column: cluster
  group_name: clustering
```


## Requirements

```
pandas
scikit-learn
numpy
```
