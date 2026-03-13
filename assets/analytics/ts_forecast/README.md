# Time Series Forecast Component

Generate time series forecasts using ARIMA, ETS (Holt-Winters), or automatic model selection based on AIC score.

## Overview

The Time Series Forecast Component fits statistical forecasting models to historical time series data and produces future-period predictions. When `model=auto`, both ARIMA and ETS are fitted and the model with the lower AIC is selected automatically.

## Features

- **Three Models**: ARIMA, ETS (Holt-Winters), or automatic selection
- **AIC-based Auto Selection**: Picks the better-fitting model automatically
- **Append Mode**: Return history + forecast in a single DataFrame
- **Configurable Seasonality**: Set seasonal periods for ETS
- **`is_forecast` Flag**: Distinguish historical vs forecasted rows

## Use Cases

1. **Sales Forecasting**: Predict next N months of revenue
2. **Demand Planning**: Forecast inventory requirements
3. **Capacity Planning**: Anticipate resource needs
4. **Financial Projections**: Budget planning

## Prerequisites

- `statsmodels>=0.14.0`, `pandas>=1.5.0`

## Configuration

### Auto Model Selection (default)

```yaml
type: dagster_component_templates.TsForecastComponent
attributes:
  asset_name: sales_forecast
  upstream_asset_key: sales_history
  date_column: date
  value_column: sales
  forecast_periods: 12
  model: auto
```

### ARIMA Only

```yaml
type: dagster_component_templates.TsForecastComponent
attributes:
  asset_name: arima_forecast
  upstream_asset_key: sales_history
  date_column: date
  value_column: sales
  forecast_periods: 6
  model: arima
  arima_order: [2, 1, 2]
```

### ETS with Seasonality

```yaml
type: dagster_component_templates.TsForecastComponent
attributes:
  asset_name: seasonal_forecast
  upstream_asset_key: monthly_data
  date_column: month
  value_column: units
  forecast_periods: 12
  model: ets
  ets_trend: "add"
  ets_seasonal: "add"
  ets_seasonal_periods: 12
  output_mode: append
```

## Configuration Options

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `asset_name` | string | required | Output asset name |
| `upstream_asset_key` | string | required | Upstream DataFrame asset key |
| `date_column` | string | required | Date column name |
| `value_column` | string | required | Value column name |
| `forecast_periods` | int | `12` | Periods to forecast |
| `model` | string | `auto` | Model: auto, arima, ets |
| `arima_order` | list | `[1,1,1]` | ARIMA (p,d,q) order |
| `ets_trend` | string | `add` | ETS trend: None, add, mul |
| `ets_seasonal` | string | `add` | ETS seasonal: None, add, mul |
| `ets_seasonal_periods` | int | `12` | Seasonal cycle length |
| `output_mode` | string | `forecast_only` | forecast_only or append |
| `group_name` | string | `None` | Asset group |

## Output Schema

| Column | Description |
|--------|-------------|
| `date_column` | Date index of forecast |
| `value_column` | Forecasted value |
| `is_forecast` | True for forecast rows |

## Troubleshooting

- **Convergence warnings**: Try different ARIMA orders or increase max iterations
- **ETS seasonal errors**: Ensure enough data covers at least 2 seasonal cycles
- **ImportError**: Run `pip install statsmodels`
