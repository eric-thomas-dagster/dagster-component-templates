# linear_regression_model

Fit an ordinary least squares linear regression model and output predictions added to the original DataFrame, a coefficients table, or both. Evaluation metrics (R2, MAE) are logged to Dagster asset metadata from the held-out test split.

## Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `asset_name` | `str` | required | Output Dagster asset name |
| `upstream_asset_key` | `str` | required | Upstream asset key providing a DataFrame |
| `target_column` | `str` | required | Column name of the target variable |
| `feature_columns` | `List[str]` | required | Feature column names |
| `test_size` | `float` | `0.2` | Fraction of data for test evaluation |
| `random_state` | `int` | `42` | Random seed for reproducibility |
| `output_mode` | `str` | `"predictions"` | `"predictions"`, `"coefficients"`, or `"both"` |
| `prediction_column` | `str` | `"predicted"` | Column name for predictions |
| `normalize` | `bool` | `false` | Standardize features with StandardScaler before fitting |
| `group_name` | `Optional[str]` | `null` | Dagster asset group name |

## Output Modes

- **`predictions`**: Returns the original DataFrame with a new prediction column appended.
- **`coefficients`**: Returns a DataFrame with `feature`, `coefficient`, and `intercept` columns.
- **`both`**: Returns predictions DataFrame; R2 and MAE are logged to metadata.

## Example YAML

```yaml
component_type: linear_regression_model
description: Predict house sale prices from structural features.

asset_name: house_price_predictions
upstream_asset_key: house_features_clean
target_column: sale_price
feature_columns:
  - sqft_living
  - bedrooms
  - bathrooms
  - yr_built
output_mode: predictions
prediction_column: predicted_sale_price
normalize: true
group_name: real_estate_models
```

## Metadata Logged

- `r2_score` — R-squared on test split
- `mean_absolute_error` — MAE on test split
- `n_features` — Number of feature columns
- `train_rows` / `test_rows` — Split sizes

## IO Manager Note

This component uses Dagster's IO manager to pass DataFrames between assets. No IO manager configuration is required for local development — Dagster's default FilesystemIOManager handles serialization automatically.

## Requirements

- `dagster`
- `pandas`
- `scikit-learn`
