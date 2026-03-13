# neural_network_model

Fit a multi-layer perceptron (MLP) neural network using scikit-learn's `MLPClassifier` or `MLPRegressor`. Supports configurable hidden layer architecture, activation functions, and optional feature normalization. Output can include predictions and per-class probabilities.

## Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `asset_name` | `str` | required | Output Dagster asset name |
| `upstream_asset_key` | `str` | required | Upstream asset key providing a DataFrame |
| `target_column` | `str` | required | Target column to predict |
| `feature_columns` | `List[str]` | required | Feature column names |
| `task_type` | `str` | `"classification"` | `"classification"` or `"regression"` |
| `hidden_layer_sizes` | `List[int]` | `[100]` | Neurons per hidden layer, e.g. `[128, 64]` |
| `activation` | `str` | `"relu"` | `"relu"`, `"tanh"`, or `"logistic"` |
| `max_iter` | `int` | `500` | Maximum training iterations |
| `learning_rate_init` | `float` | `0.001` | Initial learning rate |
| `normalize` | `bool` | `true` | Apply StandardScaler before training |
| `test_size` | `float` | `0.2` | Fraction of data for evaluation |
| `random_state` | `int` | `42` | Random seed for reproducibility |
| `output_mode` | `str` | `"predictions"` | `"predictions"` or `"probabilities"` |
| `group_name` | `Optional[str]` | `null` | Dagster asset group name |

## Output Modes

- **`predictions`**: Returns the original DataFrame with a `predicted` column appended.
- **`probabilities`**: Also appends `proba_<class>` columns (classification only).

## Example YAML

```yaml
component_type: neural_network_model
description: Predict subscription renewal with a two-layer MLP.

asset_name: renewal_mlp_predictions
upstream_asset_key: subscription_features
target_column: renewed
feature_columns:
  - months_active
  - avg_monthly_usage
hidden_layer_sizes:
  - 128
  - 64
task_type: classification
output_mode: probabilities
group_name: renewal_models
```

## Metadata Logged

Classification: `accuracy`, `n_iter`, `train_rows`, `test_rows`

Regression: `r2_score`, `n_iter`, `train_rows`, `test_rows`

## Requirements

- `dagster`
- `pandas`
- `scikit-learn`
