# naive_bayes_model

Fit a Naive Bayes classifier using scikit-learn. Supports Gaussian (continuous features), Multinomial (count data), and Bernoulli (binary features) variants. Appends predicted class labels and/or per-class probabilities to the output DataFrame.

## Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `asset_name` | `str` | required | Output Dagster asset name |
| `upstream_asset_key` | `str` | required | Upstream asset key providing a DataFrame |
| `target_column` | `str` | required | Target column to predict |
| `feature_columns` | `List[str]` | required | Feature column names |
| `variant` | `str` | `"gaussian"` | `"gaussian"`, `"multinomial"`, or `"bernoulli"` |
| `test_size` | `float` | `0.2` | Fraction of data for evaluation |
| `random_state` | `int` | `42` | Random seed for reproducibility |
| `output_predictions` | `bool` | `true` | Append `predicted_class` column |
| `output_probabilities` | `bool` | `true` | Append `proba_<class>` columns |
| `group_name` | `Optional[str]` | `null` | Dagster asset group name |

## Output Columns

- `predicted_class` (if `output_predictions: true`): predicted label for each row
- `proba_<class>` (if `output_probabilities: true`): one column per class with predicted probability

## Example YAML

```yaml
component_type: naive_bayes_model
description: Classify lead quality using Gaussian Naive Bayes.

asset_name: lead_quality_nb_predictions
upstream_asset_key: lead_features_enriched
target_column: lead_quality
feature_columns:
  - company_size
  - engagement_score
variant: gaussian
output_predictions: true
output_probabilities: true
group_name: lead_scoring_models
```

## Metadata Logged

`accuracy`, `variant`, `n_classes`, `train_rows`, `test_rows`

## Variant Guide

| Variant | Best for |
|---|---|
| `gaussian` | Continuous features (default) |
| `multinomial` | Word counts, frequency data |
| `bernoulli` | Binary / boolean features |

## Requirements

- `dagster`
- `pandas`
- `scikit-learn`
