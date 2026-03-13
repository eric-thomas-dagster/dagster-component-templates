# pca

Reduce DataFrame dimensionality using Principal Component Analysis (PCA). Replaces or augments a set of high-dimensional feature columns with a smaller number of principal component columns. Explained variance ratios are logged to Dagster asset metadata.

## Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `asset_name` | `str` | required | Output Dagster asset name |
| `upstream_asset_key` | `str` | required | Upstream asset key providing a DataFrame |
| `feature_columns` | `List[str]` | required | Input feature column names |
| `n_components` | `int` | `2` | Number of principal components to retain |
| `output_prefix` | `str` | `"pc_"` | Prefix for output columns (`pc_1`, `pc_2`, ...) |
| `normalize` | `bool` | `true` | Standardize features with StandardScaler before PCA |
| `keep_original` | `bool` | `false` | Retain original feature columns in output |
| `include_explained_variance` | `bool` | `true` | Log per-component explained variance to metadata |
| `group_name` | `Optional[str]` | `null` | Dagster asset group name |

## Output

When `keep_original: false` (default), the output DataFrame drops `feature_columns` and adds `n_components` new columns named `<output_prefix>1`, `<output_prefix>2`, etc.

When `keep_original: true`, the PC columns are appended alongside the original feature columns.

## Example YAML

```yaml
component_type: pca
description: Reduce product features to 3 principal components.

asset_name: product_features_pca
upstream_asset_key: product_features_scaled
feature_columns:
  - price
  - weight
  - rating
  - review_count
n_components: 3
output_prefix: pc_
normalize: true
keep_original: false
group_name: dimensionality_reduction
```

## Metadata Logged

- `n_components` — Number of components retained
- `total_explained_variance` — Cumulative explained variance ratio
- `input_features` — Number of input feature columns
- `pc_N_explained_variance` — Per-component explained variance ratio (when `include_explained_variance: true`)

## IO Manager Note

This component uses Dagster's IO manager to pass DataFrames between assets. No IO manager configuration is required for local development — Dagster's default FilesystemIOManager handles serialization automatically.

## Requirements

- `dagster`
- `pandas`
- `scikit-learn`
