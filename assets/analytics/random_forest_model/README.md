# random_forest_model

Fit a random forest ensemble model for classification or regression using scikit-learn's `RandomForestClassifier` or `RandomForestRegressor`. Evaluation metrics are logged to Dagster asset metadata. Output can be predictions appended to the original DataFrame or a ranked feature importance table.

## Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `asset_name` | `str` | required | Output Dagster asset name |
| `upstream_asset_key` | `str` | required | Upstream asset key providing a DataFrame |
| `target_column` | `str` | required | Column name of the target variable |
| `feature_columns` | `List[str]` | required | Feature column names |
| `task_type` | `str` | `"classification"` | `"classification"` or `"regression"` |
| `n_estimators` | `int` | `100` | Number of trees in the forest |
| `max_depth` | `Optional[int]` | `null` | Maximum tree depth (null = unlimited) |
| `test_size` | `float` | `0.2` | Fraction of data for evaluation |
| `random_state` | `int` | `42` | Random seed for reproducibility |
| `output_mode` | `str` | `"predictions"` | `"predictions"` or `"feature_importance"` |
| `n_jobs` | `int` | `-1` | Parallel jobs (-1 = all CPUs) |
| `group_name` | `Optional[str]` | `null` | Dagster asset group name |

## Output Modes

- **`predictions`**: Returns the original DataFrame with a `predicted` column appended.
- **`feature_importance`**: Returns a DataFrame with `feature` and `importance` columns sorted descending.

## Example YAML

```yaml
component_type: random_forest_model
description: Predict customer lifetime value using random forest regression.

asset_name: customer_ltv_predictions
upstream_asset_key: customer_features_enriched
target_column: ltv_12m
feature_columns:
  - total_orders
  - avg_order_value
  - days_since_first_order
task_type: regression
n_estimators: 200
output_mode: predictions
group_name: ltv_models
```

## Metadata Logged

Classification: `accuracy`, `classification_report`, `n_estimators`, `train_rows`, `test_rows`

Regression: `r2_score`, `mean_absolute_error`, `n_estimators`, `train_rows`, `test_rows`

## IO Manager Note

This component uses Dagster's IO manager to pass DataFrames between assets. No IO manager configuration is required for local development — Dagster's default FilesystemIOManager handles serialization automatically.

## Requirements

- `dagster`
- `pandas`
- `scikit-learn`
