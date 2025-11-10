# Time Series Generator

Generate synthetic time-series data with configurable patterns, frequencies, and noise levels.

## Overview

This component creates realistic time-series data perfect for demonstrating analytics, forecasting, anomaly detection, and time-series transformations. No external data sources or APIs needed.

## Features

- **7 Pattern Types**: Trend, seasonal, random walk, sine wave, step function, spike, complex
- **Flexible Frequency**: From 1-minute to daily intervals
- **Configurable Date Range**: Generate historical or recent data
- **Adjustable Noise**: Add realistic variation to patterns
- **Reproducible**: Optional random seed for consistent generation
- **DataFrame Output**: Returns pandas DataFrame with timestamp and value columns

## Pattern Types

### Trend
Linear upward or downward movement over time.
- **Use for**: Growth metrics, degradation patterns
- **Example**: Daily active users growing over time

### Seasonal
Recurring daily, weekly, or monthly patterns.
- **Use for**: Business hours patterns, weekly cycles
- **Example**: Website traffic higher during business hours

### Random Walk
Incremental random changes, like stock prices.
- **Use for**: Financial data, sensor drift
- **Example**: Stock price movements

### Sine Wave
Smooth periodic oscillation.
- **Use for**: Cyclical patterns, wave-like behavior
- **Example**: Temperature cycles, tidal patterns

### Step Function
Sudden level changes at random points.
- **Use for**: System upgrades, policy changes
- **Example**: Performance after optimization

### Spike
Mostly flat with occasional random spikes.
- **Use for**: Error rates, anomaly detection demos
- **Example**: Server errors, alert triggers

### Complex (Default)
Combination of trend + seasonal + noise.
- **Use for**: Realistic business metrics
- **Example**: Revenue with growth trend and daily seasonality

## Configuration

### Required Fields

- **asset_name** (string): Name of the asset
- **pattern_type** (enum): Pattern to generate
  - Options: `trend`, `seasonal`, `random_walk`, `sine_wave`, `step_function`, `spike`, `complex`

### Optional Fields

- **start_date** (string): Start date in YYYY-MM-DD format (default: 30 days ago)
- **end_date** (string): End date in YYYY-MM-DD format (default: now)
- **frequency** (enum): Data point frequency (default: `1h`)
  - Options: `1min`, `5min`, `15min`, `30min`, `1h`, `1d`
- **base_value** (number): Starting/baseline value (default: 100.0)
- **noise_level** (number): Random noise amount, 0.0-1.0 (default: 0.1)
- **random_seed** (integer, optional): Seed for reproducible data
- **metric_name** (string): Name of the value column (default: "value")
- **description** (string): Asset description
- **group_name** (string): Asset group for organization

## Example Usage

### Basic Usage - Last 30 Days
```yaml
type: time_series_generator.TimeSeriesGeneratorComponent

attributes:
  asset_name: server_metrics
  pattern_type: complex
  frequency: "1h"
  base_value: 100.0
  metric_name: cpu_usage_pct
```

### Specific Date Range with Seasonality
```yaml
type: time_series_generator.TimeSeriesGeneratorComponent

attributes:
  asset_name: daily_sales
  pattern_type: seasonal
  start_date: "2024-01-01"
  end_date: "2024-12-31"
  frequency: "1d"
  base_value: 50000.0
  noise_level: 0.2
  random_seed: 42
  metric_name: revenue
  description: "Daily sales with business hour patterns"
```

### High-Frequency Sensor Data
```yaml
type: time_series_generator.TimeSeriesGeneratorComponent

attributes:
  asset_name: temperature_readings
  pattern_type: sine_wave
  start_date: "2024-11-10"
  end_date: "2024-11-11"
  frequency: "1min"
  base_value: 72.0
  noise_level: 0.05
  metric_name: temperature_f
  description: "Minute-by-minute temperature readings"
```

### Anomaly Detection Demo
```yaml
type: time_series_generator.TimeSeriesGeneratorComponent

attributes:
  asset_name: error_rates
  pattern_type: spike
  frequency: "5min"
  base_value: 10.0
  noise_level: 0.15
  metric_name: errors_per_min
  description: "Error rates with occasional spikes for anomaly detection"
```

## Output Schema

The component returns a pandas DataFrame with two columns:

| Column | Type | Description |
|--------|------|-------------|
| timestamp | datetime64 | Timestamp for each data point |
| {metric_name} | float64 | The generated metric value |

Example output:
```
        timestamp           value
0 2024-01-01 00:00:00   98.234
1 2024-01-01 01:00:00  102.456
2 2024-01-01 02:00:00   99.871
...
```

## Demo Pipeline Example

```
Time Series Generator (hourly CPU metrics, complex pattern)
  → DuckDB Writer (persist to metrics.duckdb)
    → DuckDB Query Reader (SELECT * WHERE value > threshold)
      → DataFrame Transformer (calculate moving average)
        → Data Quality Checks (range, anomaly detection)
```

## Use Cases

### 1. Forecasting Demos
Generate historical data with trends → Train forecasting models → Predict future values

### 2. Anomaly Detection
Generate normal patterns with spikes → Test anomaly detection algorithms

### 3. Aggregation Pipelines
High-frequency data → Resample to hourly/daily → Calculate statistics

### 4. Real-time Simulation
Generate recent data → Update dashboards → Demonstrate streaming patterns

### 5. A/B Test Scenarios
Generate two series with different patterns → Compare metrics → Test analysis pipelines

## Tips

1. **Match Real Patterns**: Choose pattern types that match your real use case
2. **Start with Complex**: The complex pattern is most realistic for demos
3. **Use Reproducibility**: Set `random_seed` for consistent demos
4. **Adjust Noise**: Lower noise (0.05) for smooth patterns, higher (0.3) for volatile data
5. **Frequency Matters**: Match your actual data frequency for realistic demos
6. **Date Ranges**: Use recent dates for "live" demos, historical for analysis

## Technical Notes

- All timestamps are generated using pandas date_range
- Patterns are generated using numpy for efficiency
- Noise is normally distributed around zero
- Seasonal patterns are based on hour of day (14:00 peak, 02:00 low)
- Complex pattern combines linear trend with daily seasonality
- Random seed affects both pattern generation and noise
