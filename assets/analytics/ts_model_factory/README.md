# Time Series Model Factory Component

Fit and forecast a separate time series model for each group in a DataFrame. Returns all group forecasts concatenated into a single output DataFrame.

## Overview

The Time Series Model Factory Component is designed for multi-entity forecasting scenarios — e.g., one model per product, store, or customer. It iterates over each group, fits an independent ARIMA or ETS model, and concatenates all forecasts. Groups that fail to fit are skipped with a warning rather than failing the entire asset.

## Features

- **Per-group Models**: Independent model per group entity
- **ARIMA and ETS**: Choose the model type
- **Fault Tolerant**: Skips failing groups without stopping execution
- **Single Output**: All forecasts in one DataFrame
- **Group Tracking**: `group_column` preserved in output

## Use Cases

1. **Multi-SKU Demand Forecasting**: One model per product
2. **Store-level Revenue Forecasting**: Independent store forecasts
3. **Regional Projections**: Forecast per geographic region
4. **Per-customer Metrics**: Individual customer lifetime value projections

## Prerequisites

- `statsmodels>=0.14.0`, `pandas>=1.5.0`

## Configuration

### Per-product Forecasting

```yaml
type: dagster_component_templates.TsModelFactoryComponent
attributes:
  asset_name: product_level_forecasts
  upstream_asset_key: product_sales
  date_column: month
  value_column: revenue
  group_column: product_id
  forecast_periods: 6
  model: ets
  ets_seasonal_periods: 12
```

### Per-store ARIMA

```yaml
type: dagster_component_templates.TsModelFactoryComponent
attributes:
  asset_name: store_forecasts
  upstream_asset_key: store_sales
  date_column: week
  value_column: sales
  group_column: store_id
  forecast_periods: 13
  model: arima
  arima_order: [1, 1, 1]
```

## Configuration Options

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `asset_name` | string | required | Output asset name |
| `upstream_asset_key` | string | required | Upstream DataFrame asset key |
| `date_column` | string | required | Date column name |
| `value_column` | string | required | Value column name |
| `group_column` | string | required | Group identifier column |
| `forecast_periods` | int | `12` | Periods per group |
| `model` | string | `ets` | Model: ets or arima |
| `arima_order` | list | `[1,1,1]` | ARIMA (p,d,q) order |
| `ets_trend` | string | `add` | ETS trend |
| `ets_seasonal` | string | `add` | ETS seasonal |
| `ets_seasonal_periods` | int | `12` | Seasonal cycle length |
| `group_name` | string | `None` | Asset group |

## Output Schema

| Column | Description |
|--------|-------------|
| `date_column` | Forecast date |
| `value_column` | Forecasted value |
| `group_column` | Group identifier |
| `is_forecast` | Always True |

## Troubleshooting

- **Many groups failing**: Check that each group has enough observations (ETS needs >= 2x seasonal periods)
- **All groups failing**: Verify date and value column names
- **ImportError**: Run `pip install statsmodels`
