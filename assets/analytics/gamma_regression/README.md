# GammaRegression

Fits a Gamma GLM (log link) for strictly-positive continuous outcomes like dollar amounts or durations. Emits per-row predictions. Better than OLS when residual variance scales with the mean.

## Example

```yaml
type: dagster_component_templates.GammaRegressionComponent
attributes:
  asset_name: claim_amount_predictions
  upstream_asset_key: insurance_claims
  target_column: claim_amount
  feature_columns: [age, vehicle_age, prior_claims, deductible]
  output_mode: predictions
  group_name: model
```


## Requirements

```
pandas
statsmodels
numpy
```
