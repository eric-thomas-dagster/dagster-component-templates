# Tile Binning Component

Assign records to bins or buckets based on a numeric column's value range. Supports equal-width intervals, equal-frequency quantile bins, and custom bin edges. Useful for segmentation, scoring tiers, and feature engineering.

This component uses Dagster's IO manager to pass DataFrames between assets. No IO manager configuration is required for local development — Dagster's default FilesystemIOManager handles serialization automatically.

## Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `asset_name` | `str` | required | Output Dagster asset name |
| `upstream_asset_key` | `str` | required | Upstream asset key providing a DataFrame |
| `column` | `str` | required | Numeric column to bin |
| `n_bins` | `int` | `10` | Number of bins |
| `method` | `str` | `"equal_width"` | Binning method: `"equal_width"`, `"equal_freq"`, or `"custom"` |
| `bin_edges` | `Optional[List[float]]` | `None` | Custom bin edges for `method="custom"` |
| `labels` | `Optional[List[str]]` | `None` | Labels for bins (length must equal `n_bins` or `len(bin_edges) - 1`) |
| `output_column` | `Optional[str]` | `None` | Output column name (defaults to `{column}_bin`) |
| `include_numeric_label` | `bool` | `False` | Also add `{column}_bin_num` with 0-indexed bin number |
| `group_name` | `Optional[str]` | `None` | Dagster asset group name |

## Binning Methods

| Method | Description | pandas function |
|---|---|---|
| `equal_width` | Divide value range into equal-width intervals | `pd.cut` |
| `equal_freq` | Divide into bins with equal numbers of records | `pd.qcut` |
| `custom` | Use explicit bin edges provided in `bin_edges` | `pd.cut` |

## Example YAML

```yaml
type: dagster_component_templates.TileBinningComponent
attributes:
  asset_name: customers_with_spend_tier
  upstream_asset_key: customer_spend
  column: annual_spend
  n_bins: 5
  method: equal_freq
  labels:
    - bronze
    - silver
    - gold
    - platinum
    - diamond
  output_column: spend_tier
  include_numeric_label: true
  group_name: segmentation
```

## Custom Bin Edges

When `method="custom"`, provide `bin_edges` as a list of N+1 boundary values defining N bins. For example, `bin_edges: [0, 100, 500, 1000]` creates three bins: `(0, 100]`, `(100, 500]`, `(500, 1000]`.

## Numeric Label

When `include_numeric_label: true`, an additional integer column named `{column}_bin_num` is added with 0-indexed bin assignments. This is useful for downstream models that require numeric features.

## Notes

- `equal_freq` uses `duplicates="drop"` to handle repeated values that would create identical bin edges.
- When `labels` is omitted, pandas generates interval-notation labels (e.g. `(0.0, 10.0]`).
- The bin label column has a pandas `Categorical` dtype.
