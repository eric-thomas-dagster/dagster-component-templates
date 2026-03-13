# Time Series Compare Component

Compare ARIMA and ETS model performance on a hold-out test set. Returns a metrics DataFrame with AIC, MAE, RMSE, and MAPE for each model.

## Overview

The Time Series Compare Component performs a train/test evaluation of both ARIMA and ETS models to help you select the best model before committing to production forecasting. It splits the time series at a configurable hold-out boundary, fits each model on the training data, and evaluates predictions against the test set.

## Features

- **Side-by-side Comparison**: ARIMA vs ETS on equal footing
- **Multiple Metrics**: AIC (in-sample fit), MAE, RMSE, MAPE (out-of-sample accuracy)
- **Fault Tolerant**: One model failing doesn't fail the asset
- **Clear Output**: One row per model in the output DataFrame

## Use Cases

1. **Model Selection**: Determine the best model before production
2. **Forecast Auditing**: Regular accuracy checks on deployed pipelines
3. **Hyperparameter Search**: Feed results into optimization loops

## Prerequisites

- `statsmodels>=0.14.0`, `pandas>=1.5.0`, `numpy>=1.23.0`

## Configuration

```yaml
type: dagster_component_templates.TsCompareComponent
attributes:
  asset_name: model_comparison
  upstream_asset_key: sales_history
  date_column: date
  value_column: sales
  test_periods: 6
  arima_order: [1, 1, 1]
  ets_trend: "add"
  ets_seasonal: "add"
  ets_seasonal_periods: 12
```

## Configuration Options

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `asset_name` | string | required | Output asset name |
| `upstream_asset_key` | string | required | Upstream DataFrame asset key |
| `date_column` | string | required | Date column name |
| `value_column` | string | required | Value column name |
| `arima_order` | list | `[1,1,1]` | ARIMA (p,d,q) order |
| `ets_trend` | string | `add` | ETS trend component |
| `ets_seasonal` | string | `add` | ETS seasonal component |
| `ets_seasonal_periods` | int | `12` | Seasonal cycle length |
| `test_periods` | int | `6` | Hold-out test periods |
| `group_name` | string | `None` | Asset group |

## Output Schema

| Column | Description |
|--------|-------------|
| `model` | Model name (ARIMA or ETS) |
| `aic` | Akaike Information Criterion (lower is better) |
| `mae` | Mean Absolute Error on test set |
| `rmse` | Root Mean Squared Error on test set |
| `mape` | Mean Absolute Percentage Error (%) |
| `error` | Error message if model failed (otherwise absent) |

## Metric Guide

| Metric | Description | Prefer |
|--------|-------------|--------|
| AIC | In-sample model fit | Lower |
| MAE | Average absolute error | Lower |
| RMSE | Penalises large errors more | Lower |
| MAPE | Percentage error | Lower |

## Troubleshooting

- **`test_periods` too large**: Must be less than total series length
- **ETS seasonal errors**: Need >= 2x seasonal periods of data
- **MAPE = NaN**: Test values contain zeros; use MAE or RMSE instead
- **ImportError**: Run `pip install statsmodels numpy`
