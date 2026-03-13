# ets_forecast

Fit an ETS (Error, Trend, Seasonal) model using the Holt-Winters Exponential Smoothing method and generate forecasts. Supports additive and multiplicative trend and seasonal components with optional trend damping.

## Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `asset_name` | `str` | required | Output Dagster asset name |
| `upstream_asset_key` | `str` | required | Upstream asset key providing a time series DataFrame |
| `date_column` | `str` | required | Column containing datetime values |
| `value_column` | `str` | required | Column containing numeric values to forecast |
| `forecast_periods` | `int` | `12` | Number of periods to forecast ahead |
| `trend` | `Optional[str]` | `"add"` | Trend component: `null`, `"add"`, or `"mul"` |
| `seasonal` | `Optional[str]` | `"add"` | Seasonal component: `null`, `"add"`, or `"mul"` |
| `seasonal_periods` | `Optional[int]` | `12` | Seasonal period length (12=monthly, 52=weekly, 7=daily) |
| `damped_trend` | `bool` | `false` | Whether to dampen the trend component |
| `output_mode` | `str` | `"forecast"` | `"forecast"` or `"append"` |
| `group_name` | `Optional[str]` | `null` | Dagster asset group name |

## Output Modes

- **`forecast`**: Returns only the forecasted future rows.
- **`append`**: Returns the original data concatenated with the forecasted rows.

## Example YAML

```yaml
component_type: ets_forecast
description: Forecast weekly e-commerce orders using additive Holt-Winters ETS model.

asset_name: ecommerce_orders_forecast
upstream_asset_key: ecommerce_orders_weekly
date_column: week_start
value_column: order_count
forecast_periods: 12
trend: add
seasonal: add
seasonal_periods: 52
damped_trend: false
output_mode: append
group_name: forecasting
```

## Metadata Logged

- `aic` — Akaike Information Criterion
- `bic` — Bayesian Information Criterion
- `sse` — Sum of Squared Errors
- `observations` — Number of observations in the training series
- `trend` — Trend component type used
- `seasonal` — Seasonal component type used

## IO Manager Note

This component uses Dagster's IO manager to pass DataFrames between assets. No IO manager configuration is required for local development — Dagster's default FilesystemIOManager handles serialization automatically.

## Requirements

- `dagster`
- `pandas`
- `statsmodels`
