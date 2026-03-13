# logistic_regression_model

Fit a logistic regression classifier and append predicted class labels and/or per-class probability columns to the output DataFrame. Accuracy and a full classification report are logged to Dagster asset metadata from the held-out test split.

## Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `asset_name` | `str` | required | Output Dagster asset name |
| `upstream_asset_key` | `str` | required | Upstream asset key providing a DataFrame |
| `target_column` | `str` | required | Column name of the target class label |
| `feature_columns` | `List[str]` | required | Feature column names |
| `test_size` | `float` | `0.2` | Fraction of data for test evaluation |
| `random_state` | `int` | `42` | Random seed for reproducibility |
| `max_iter` | `int` | `1000` | Maximum solver iterations |
| `output_predictions` | `bool` | `true` | Add `predicted_class` column |
| `output_probabilities` | `bool` | `true` | Add `predicted_proba_<class>` columns per class |
| `normalize` | `bool` | `true` | Standardize features with StandardScaler |
| `group_name` | `Optional[str]` | `null` | Dagster asset group name |

## Output Columns

- `predicted_class` — predicted label (when `output_predictions: true`)
- `predicted_proba_<label>` — probability per class (when `output_probabilities: true`)

## Example YAML

```yaml
component_type: logistic_regression_model
description: Classify customer churn risk from behavioral features.

asset_name: churn_predictions
upstream_asset_key: customer_features_clean
target_column: churned
feature_columns:
  - days_since_last_login
  - total_orders
  - avg_order_value
  - support_tickets_30d
output_predictions: true
output_probabilities: true
normalize: true
group_name: churn_models
```

## Metadata Logged

- `accuracy` — Classification accuracy on test split
- `classification_report` — Full precision/recall/F1 report
- `n_classes` — Number of distinct target classes
- `train_rows` / `test_rows` — Split sizes

## IO Manager Note

This component uses Dagster's IO manager to pass DataFrames between assets. No IO manager configuration is required for local development — Dagster's default FilesystemIOManager handles serialization automatically.

## Requirements

- `dagster`
- `pandas`
- `scikit-learn`
