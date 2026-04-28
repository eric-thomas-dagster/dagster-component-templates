# Feature Scaler Component

Rescale numeric DataFrame columns using one of four strategies. Implemented in pandas/numpy directly — no scikit-learn dependency.

## Strategies

| Strategy | Formula | When to Use |
|---|---|---|
| `standard` | `(x − mean) / std` | Linear models, neural nets — assumes roughly Gaussian distribution |
| `minmax` | `(x − min) / (max − min)` mapped to `[feature_range_min, feature_range_max]` | Bounded features, image-pixel-like inputs, distance-based models |
| `robust` | `(x − median) / IQR` | Heavy-tailed or outlier-prone columns — resistant to extreme values |
| `maxabs` | `x / max(\|x\|)` → `[-1, 1]` | Sparse data — preserves zero entries |

## Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `asset_name` | `str` | required | Output Dagster asset name |
| `upstream_asset_key` | `str` | required | Upstream asset key providing a DataFrame |
| `strategy` | `str` | `"standard"` | One of `standard`, `minmax`, `robust`, `maxabs` |
| `columns` | `Optional[List[str]]` | `null` | Columns to scale. None = all numeric columns |
| `feature_range_min` | `float` | `0.0` | Lower bound for `minmax` |
| `feature_range_max` | `float` | `1.0` | Upper bound for `minmax` |
| `suffix` | `Optional[str]` | `null` | If set, write to `<col><suffix>` (e.g. `_scaled`); otherwise overwrite |

## Example YAML

```yaml
type: dagster_component_templates.FeatureScalerComponent
attributes:
  asset_name: scaled_customer_features
  upstream_asset_key: encoded_customer_features
  strategy: standard
  columns:
    - lifetime_value
    - days_since_signup
  suffix: _scaled
  group_name: feature_engineering
```

## Notes

- **Constant columns** (zero variance / zero range) are passed through unchanged — the divisor is replaced with `1.0` to avoid `inf` / `NaN`.
- **Non-numeric values** in target columns are coerced via `pd.to_numeric(errors="coerce")`; unparseable cells become NaN before scaling.
- **NaN propagation**: NaNs are preserved in the output. Run an `imputation` component upstream if you need them filled first.
- **Statistics emitted as metadata**: every materialization records the per-column mean/std (or min/max, median/IQR, etc.) under `scaling_stats` so you can audit drift between runs.

## Use Together With

- `imputation` (fill nulls before scaling)
- `one_hot_encoding` (categorical preprocessing) — typical pipeline: cleanse → impute → encode → scale
- `outlier_clipper` (handle extremes before `standard` scaling)
