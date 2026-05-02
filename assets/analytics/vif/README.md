# Vif

Variance Inflation Factor for each feature in a design matrix. VIF > 5 typically signals multicollinearity worth addressing before fitting a linear/logistic regression. Output is one row per feature with its VIF.

## Example

```yaml
type: dagster_component_templates.VifComponent
attributes:
  asset_name: housing_vif
  upstream_asset_key: housing_features
  feature_columns:
    - sqft
    - bedrooms
    - bathrooms
    - lot_size
    - year_built
  group_name: diagnostics
```


## Requirements

```
pandas
statsmodels
numpy
```
