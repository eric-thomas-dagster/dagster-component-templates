# ModelScore

Loads a pickled scikit-learn estimator from disk and runs `predict` (and `predict_proba` for classifiers) on the input DataFrame. Lets you keep model fitting and scoring as separate Dagster assets — fit once, score many times.

## Example

```yaml
type: dagster_component_templates.ModelScoreComponent
attributes:
  asset_name: today_predictions
  upstream_asset_key: new_customers_features
  model_path: /tmp/customer_churn_model.joblib
  feature_columns: [tenure, monthly_charges, total_charges]
  output_column: churn_predicted
  include_proba: true
  group_name: scoring
```


## Requirements

```
pandas
scikit-learn
joblib
```
