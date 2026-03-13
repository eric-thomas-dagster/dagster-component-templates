# cross_validation

Run k-fold cross-validation on a dataset using a configurable sklearn model. Returns a DataFrame with per-fold train score, test score, and fit time. Useful for model selection and diagnosing overfitting before committing to a final model.

## Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `asset_name` | `str` | required | Output Dagster asset name |
| `upstream_asset_key` | `str` | required | Upstream asset key providing a DataFrame |
| `target_column` | `str` | required | Target column name |
| `feature_columns` | `List[str]` | required | Feature column names |
| `model_type` | `str` | `"random_forest"` | Model to evaluate (see table below) |
| `task_type` | `str` | `"classification"` | `"classification"` or `"regression"` |
| `cv_folds` | `int` | `5` | Number of cross-validation folds |
| `scoring` | `Optional[str]` | `null` | sklearn scoring string (null = auto) |
| `random_state` | `int` | `42` | Random seed for reproducibility |
| `group_name` | `Optional[str]` | `null` | Dagster asset group name |

## Supported Models

| `model_type` | Classification | Regression |
|---|---|---|
| `random_forest` | RandomForestClassifier | RandomForestRegressor |
| `logistic_regression` | LogisticRegression | LinearRegression |
| `gradient_boosting` | GradientBoostingClassifier | GradientBoostingRegressor |
| `decision_tree` | DecisionTreeClassifier | DecisionTreeRegressor |
| `linear_regression` | — | LinearRegression |

## Output Columns

| Column | Description |
|---|---|
| `fold` | Fold number (1-indexed) |
| `train_score` | Score on training split |
| `test_score` | Score on held-out split |
| `fit_time` | Seconds to fit the model |

## Example YAML

```yaml
component_type: cross_validation
description: Evaluate gradient boosting with 10-fold CV.

asset_name: churn_cv_results
upstream_asset_key: customer_features_enriched
target_column: churned
feature_columns:
  - days_since_last_order
  - total_orders
model_type: gradient_boosting
cv_folds: 10
group_name: model_evaluation
```

## Metadata Logged

`mean_test_score`, `std_test_score`, `mean_train_score`, `scoring_metric`, `cv_folds`, `model_type`

## Requirements

- `dagster`
- `pandas`
- `scikit-learn`
