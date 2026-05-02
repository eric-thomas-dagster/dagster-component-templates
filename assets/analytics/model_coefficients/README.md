# ModelCoefficients

Fits an sklearn linear or logistic regression and emits a tidy DataFrame of coefficients with intercept. For OLS it also reports standard errors and p-values via statsmodels. Useful for explainability and quick model diagnostics.

## Example

```yaml
type: dagster_component_templates.ModelCoefficientsComponent
attributes:
  asset_name: revenue_coefficients
  upstream_asset_key: features_with_target
  target_column: revenue
  feature_columns: [ad_spend, season_index, n_signups]
  model_kind: ols
  group_name: explainability
```


## Requirements

```
pandas
scikit-learn
statsmodels
numpy
```
