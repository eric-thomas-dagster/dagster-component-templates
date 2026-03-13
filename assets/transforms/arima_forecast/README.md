# arima_forecast

Fit an ARIMA (AutoRegressive Integrated Moving Average) time series model and generate forecasts with optional confidence intervals. Supports seasonal ARIMA (SARIMA) via `seasonal_order`.

## Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `asset_name` | `str` | required | Output Dagster asset name |
| `upstream_asset_key` | `str` | required | Upstream asset key providing a time series DataFrame |
| `date_column` | `str` | required | Column containing datetime values |
| `value_column` | `str` | required | Column containing numeric values to forecast |
| `forecast_periods` | `int` | `12` | Number of periods to forecast ahead |
| `order` | `List[int]` | `[1, 1, 1]` | ARIMA (p, d, q) order |
| `seasonal_order` | `Optional[List[int]]` | `null` | Seasonal (P, D, Q, S) order |
| `output_mode` | `str` | `"forecast"` | `"forecast"`, `"append"`, or `"fitted"` |
| `confidence_level` | `float` | `0.95` | Confidence level for prediction intervals |
| `group_name` | `Optional[str]` | `null` | Dagster asset group name |

## Output Modes

- **`forecast`**: Returns only the forecasted future rows with confidence intervals.
- **`append`**: Returns the original data concatenated with the forecasted rows.
- **`fitted`**: Returns the original data with `fitted_value` and `residual` columns added.

## Example YAML

```yaml
component_type: arima_forecast
description: Forecast monthly retail sales 12 periods ahead.

asset_name: retail_sales_forecast
upstream_asset_key: retail_sales_monthly
date_column: sale_date
value_column: revenue
forecast_periods: 12
order: [1, 1, 1]
seasonal_order: [1, 1, 1, 12]
output_mode: append
confidence_level: 0.95
group_name: forecasting
```

## Metadata Logged

- `aic` — Akaike Information Criterion
- `bic` — Bayesian Information Criterion
- `order` — ARIMA order used
- `observations` — Number of observations in the training series

## IO Manager Note

This component uses Dagster's IO manager to pass DataFrames between assets. No IO manager configuration is required for local development — Dagster's default FilesystemIOManager handles serialization automatically.

## Requirements

- `dagster`
- `pandas`
- `statsmodels`
