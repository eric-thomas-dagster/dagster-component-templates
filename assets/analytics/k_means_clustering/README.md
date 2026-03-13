# k_means_clustering

Cluster records in a DataFrame using K-means and assign a cluster ID to each row. Supports optional feature standardization and an optional column containing each record's distance to its cluster centroid. Cluster sizes and inertia are logged to asset metadata.

## Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `asset_name` | `str` | required | Output Dagster asset name |
| `upstream_asset_key` | `str` | required | Upstream asset key providing a DataFrame |
| `feature_columns` | `List[str]` | required | Feature column names used for clustering |
| `n_clusters` | `int` | `5` | Number of clusters (K) |
| `output_column` | `str` | `"cluster"` | Column name for cluster assignment |
| `normalize` | `bool` | `true` | Standardize features with StandardScaler |
| `random_state` | `int` | `42` | Random seed for reproducibility |
| `max_iter` | `int` | `300` | Maximum K-means iterations |
| `include_distance` | `bool` | `false` | Add `<output_column>_distance` column |
| `group_name` | `Optional[str]` | `null` | Dagster asset group name |

## Example YAML

```yaml
component_type: k_means_clustering
description: Segment customers into 6 behavioral clusters.

asset_name: customer_segments
upstream_asset_key: customer_features_enriched
feature_columns:
  - recency_days
  - frequency_90d
  - monetary_value_90d
n_clusters: 6
output_column: segment_cluster
normalize: true
include_distance: true
group_name: segmentation
```

## Metadata Logged

- `n_clusters` — Number of clusters configured
- `inertia` — Sum of squared distances to cluster centers
- `n_iterations` — Number of K-means iterations run
- `cluster_sizes` — Row count per cluster
- `rows` — Total input rows

## IO Manager Note

This component uses Dagster's IO manager to pass DataFrames between assets. No IO manager configuration is required for local development — Dagster's default FilesystemIOManager handles serialization automatically.

## Requirements

- `dagster`
- `pandas`
- `scikit-learn`
