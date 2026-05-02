# SVM

Fits a Support Vector Classifier or Regressor (sklearn). Configurable kernel, regularization (C), and gamma. Emits per-row predictions plus, for classification, predicted probabilities when the kernel supports them.

## Example

```yaml
type: dagster_component_templates.SVMComponent
attributes:
  asset_name: iris_svm_predictions
  upstream_asset_key: iris_scaled
  target_column: species
  feature_columns: [sepal_length, sepal_width, petal_length, petal_width]
  task_type: classification
  kernel: rbf
  C: 1.0
  gamma: scale
  group_name: model
```


## Requirements

```
pandas
scikit-learn
numpy
```
