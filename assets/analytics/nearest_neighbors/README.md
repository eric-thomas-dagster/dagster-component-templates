# nearest_neighbors

For each record in the DataFrame, find the K nearest neighbors by feature similarity using scikit-learn's `NearestNeighbors`. Appends neighbor index and/or distance columns to the output. Supports multiple distance metrics and optional feature standardization.

## Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `asset_name` | `str` | required | Output Dagster asset name |
| `upstream_asset_key` | `str` | required | Upstream asset key providing a DataFrame |
| `feature_columns` | `List[str]` | required | Feature column names for distance computation |
| `n_neighbors` | `int` | `5` | Number of nearest neighbors per record |
| `algorithm` | `str` | `"auto"` | Search algorithm: `"auto"`, `"ball_tree"`, `"kd_tree"`, `"brute"` |
| `metric` | `str` | `"euclidean"` | Distance metric (e.g. `"euclidean"`, `"manhattan"`, `"cosine"`) |
| `normalize` | `bool` | `true` | Standardize features before computing distances |
| `output_distances` | `bool` | `true` | Add `neighbor_N_dist` columns |
| `output_indices` | `bool` | `true` | Add `neighbor_N_idx` columns |
| `group_name` | `Optional[str]` | `null` | Dagster asset group name |

## Output Columns

For each neighbor `N` from 1 to `n_neighbors`:
- `neighbor_N_idx` — row index of the Nth nearest neighbor (when `output_indices: true`)
- `neighbor_N_dist` — distance to the Nth nearest neighbor (when `output_distances: true`)

## Example YAML

```yaml
component_type: nearest_neighbors
description: Find the 5 most similar products based on pricing and behavioral features.

asset_name: product_neighbors
upstream_asset_key: product_features_clean
feature_columns:
  - price
  - avg_rating
  - review_count
n_neighbors: 5
metric: euclidean
normalize: true
group_name: recommendations
```

## Metadata Logged

- `n_neighbors` — K value used
- `algorithm` — Algorithm used for neighbor search
- `metric` — Distance metric used
- `rows` — Total input rows

## IO Manager Note

This component uses Dagster's IO manager to pass DataFrames between assets. No IO manager configuration is required for local development — Dagster's default FilesystemIOManager handles serialization automatically.

## Requirements

- `dagster`
- `pandas`
- `scikit-learn`
