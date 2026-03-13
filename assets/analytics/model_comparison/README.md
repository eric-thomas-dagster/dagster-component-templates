# model_comparison

Train and evaluate multiple sklearn models side-by-side using k-fold cross-validation. Returns a comparison DataFrame sorted by the primary metric so you can quickly identify the best model for your dataset.

## Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `asset_name` | `str` | required | Output Dagster asset name |
| `upstream_asset_key` | `str` | required | Upstream asset key providing a DataFrame |
| `target_column` | `str` | required | Target column name |
| `feature_columns` | `List[str]` | required | Feature column names |
| `task_type` | `str` | `"classification"` | `"classification"` or `"regression"` |
| `models` | `List[str]` | (all 5) | Models to compare (see table below) |
| `test_size` | `float` | `0.2` | Fraction of data for evaluation |
| `cv_folds` | `int` | `5` | Number of cross-validation folds |
| `random_state` | `int` | `42` | Random seed for reproducibility |
| `group_name` | `Optional[str]` | `null` | Dagster asset group name |

## Supported Models

`logistic_regression`, `random_forest`, `gradient_boosting`, `decision_tree`, `naive_bayes`

Note: `naive_bayes` is only supported for `task_type: classification`.

## Output Columns (classification)

`model`, `accuracy`, `f1_weighted`, `precision_weighted`, `recall_weighted`, `fit_time_mean`

## Output Columns (regression)

`model`, `r2`, `neg_mean_absolute_error`, `fit_time_mean`

## Example YAML

```yaml
component_type: model_comparison
description: Compare classifiers on churn data.

asset_name: churn_model_comparison
upstream_asset_key: customer_features_enriched
target_column: churned
feature_columns:
  - days_since_last_order
  - total_orders
models:
  - random_forest
  - gradient_boosting
  - logistic_regression
cv_folds: 5
group_name: model_selection
```

## Metadata Logged

`best_model`, `best_<primary_metric>`, `models_evaluated`, `cv_folds`

## Requirements

- `dagster`
- `pandas`
- `scikit-learn`
