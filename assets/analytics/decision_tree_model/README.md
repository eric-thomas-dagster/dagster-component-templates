# decision_tree_model

Fit a decision tree for classification or regression tasks. Supports both `DecisionTreeClassifier` and `DecisionTreeRegressor` from scikit-learn. Output can be predictions appended to the original DataFrame or a ranked feature importance table.

## Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `asset_name` | `str` | required | Output Dagster asset name |
| `upstream_asset_key` | `str` | required | Upstream asset key providing a DataFrame |
| `target_column` | `str` | required | Column name of the target variable |
| `feature_columns` | `List[str]` | required | Feature column names |
| `task_type` | `str` | `"classification"` | `"classification"` or `"regression"` |
| `max_depth` | `Optional[int]` | `null` | Maximum tree depth (null = unlimited) |
| `min_samples_split` | `int` | `2` | Minimum samples to split a node |
| `test_size` | `float` | `0.2` | Fraction of data for evaluation |
| `random_state` | `int` | `42` | Random seed for reproducibility |
| `output_mode` | `str` | `"predictions"` | `"predictions"` or `"feature_importance"` |
| `group_name` | `Optional[str]` | `null` | Dagster asset group name |

## Output Modes

- **`predictions`**: Returns the original DataFrame with a `predicted` column appended.
- **`feature_importance`**: Returns a DataFrame with `feature` and `importance` columns sorted by importance descending.

## Example YAML

```yaml
component_type: decision_tree_model
description: Classify loan default risk.

asset_name: loan_default_predictions
upstream_asset_key: loan_applications_features
target_column: defaulted
feature_columns:
  - credit_score
  - annual_income
  - loan_amount
task_type: classification
max_depth: 5
output_mode: predictions
group_name: credit_models
```

## Metadata Logged

Classification: `accuracy`, `classification_report`, `tree_depth`, `n_leaves`

Regression: `r2_score`, `mean_absolute_error`, `tree_depth`, `n_leaves`

## IO Manager Note

This component uses Dagster's IO manager to pass DataFrames between assets. No IO manager configuration is required for local development — Dagster's default FilesystemIOManager handles serialization automatically.

## Requirements

- `dagster`
- `pandas`
- `scikit-learn`
