# lift_chart

Compute a decile lift/gains chart from actual binary labels and predicted probabilities. Useful for evaluating the performance of a binary classification model and understanding how well it ranks customers or events. Requires no sklearn — only pandas.

## Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `asset_name` | `str` | required | Output Dagster asset name |
| `upstream_asset_key` | `str` | required | Upstream DataFrame with actual labels and predicted probabilities |
| `actual_column` | `str` | required | Binary target column (0/1) |
| `predicted_proba_column` | `str` | required | Predicted probability column |
| `n_bins` | `int` | `10` | Number of decile bins |
| `group_name` | `Optional[str]` | `null` | Dagster asset group name |

## Output Columns

| Column | Description |
|---|---|
| `bin` | Bin number (1 = lowest score, n = highest) |
| `bin_min_score` | Minimum predicted probability in the bin |
| `bin_max_score` | Maximum predicted probability in the bin |
| `n_records` | Total records in the bin |
| `n_positive` | Positive events in the bin |
| `response_rate` | Positives / total in bin |
| `lift` | Response rate / overall response rate |
| `cumulative_lift` | Cumulative lift at this bin |
| `cumulative_capture_rate` | Fraction of all positives captured up to this bin |

## Example YAML

```yaml
component_type: lift_chart
description: Decile lift chart for churn propensity scores.

asset_name: churn_lift_chart
upstream_asset_key: churn_propensity_scores
actual_column: churned
predicted_proba_column: churn_probability
n_bins: 10
group_name: model_evaluation
```

## Metadata Logged

`overall_response_rate`, `top_bin_lift`, `total_records`, `total_positives`, `n_bins`

## Requirements

- `dagster`
- `pandas`
