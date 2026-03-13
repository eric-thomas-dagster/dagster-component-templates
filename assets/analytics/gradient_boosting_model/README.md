# gradient_boosting_model

Fit a gradient boosting model for classification or regression using scikit-learn's `GradientBoostingClassifier` or `GradientBoostingRegressor`. Evaluation metrics are logged to Dagster asset metadata. Output can be predictions appended to the original DataFrame or a ranked feature importance table.

## Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `asset_name` | `str` | required | Output Dagster asset name |
| `upstream_asset_key` | `str` | required | Upstream asset key providing a DataFrame |
| `target_column` | `str` | required | Target column to predict |
| `feature_columns` | `List[str]` | required | Feature column names |
| `task_type` | `str` | `"classification"` | `"classification"` or `"regression"` |
| `n_estimators` | `int` | `100` | Number of boosting stages |
| `learning_rate` | `float` | `0.1` | Learning rate (shrinks each tree's contribution) |
| `max_depth` | `int` | `3` | Maximum depth of individual estimators |
| `test_size` | `float` | `0.2` | Fraction of data for evaluation |
| `random_state` | `int` | `42` | Random seed for reproducibility |
| `output_mode` | `str` | `"predictions"` | `"predictions"` or `"feature_importance"` |
| `group_name` | `Optional[str]` | `null` | Dagster asset group name |

## Output Modes

- **`predictions`**: Returns the original DataFrame with a `predicted` column appended.
- **`feature_importance`**: Returns a DataFrame with `feature` and `importance` columns sorted descending.

## Example YAML

```yaml
component_type: gradient_boosting_model
description: Predict customer churn using gradient boosting classification.

asset_name: churn_gbm_predictions
upstream_asset_key: customer_features_enriched
target_column: churned
feature_columns:
  - days_since_last_order
  - total_orders
task_type: classification
n_estimators: 200
learning_rate: 0.05
output_mode: predictions
group_name: churn_models
```

## Metadata Logged

Classification: `accuracy`, `n_estimators`, `train_rows`, `test_rows`

Regression: `r2_score`, `n_estimators`, `train_rows`, `test_rows`

## Requirements

- `dagster`
- `pandas`
- `scikit-learn`
