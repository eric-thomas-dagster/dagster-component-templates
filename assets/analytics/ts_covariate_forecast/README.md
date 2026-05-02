# TSCovariateForecast

Fits a SARIMAX model (statsmodels) with optional exogenous covariates. Forecasts `n_periods` ahead and emits per-step predicted values + 95% CI. Use when you have known external drivers (price, marketing spend, holiday flag) that influence the series.

## Example

```yaml
type: dagster_component_templates.TSCovariateForecastComponent
attributes:
  asset_name: revenue_forecast
  upstream_asset_key: revenue_with_covariates
  date_column: month
  value_column: revenue
  covariate_columns: [marketing_spend, holiday_flag]
  n_periods: 6
  order: [1, 1, 1]
  seasonal_order: [1, 1, 1, 12]
  group_name: forecast
```


## Requirements

```
pandas
statsmodels
numpy
```
