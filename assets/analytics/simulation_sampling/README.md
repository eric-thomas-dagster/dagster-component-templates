# Simulation Sampling Component

Run Monte Carlo simulations by sampling from statistical distributions defined in an upstream Dagster asset DataFrame.

## Overview

The `SimulationSamplingComponent` reads a distribution definition DataFrame where each row specifies a variable name and its distribution parameters, then generates N simulation iterations. Results can be returned as raw samples or summary statistics.

## Supported Distributions

| Distribution | param1 | param2 |
|---|---|---|
| `normal` | mean | std |
| `uniform` | low | high |
| `triangular` | left | right (mode = midpoint) |
| `lognormal` | mean (log scale) | std (log scale) |
| `poisson` | lambda | (unused) |
| `exponential` | scale (1/lambda) | (unused) |
| `beta` | alpha | beta |

## Use Cases

- **Demand forecasting**: Simulate uncertain demand across scenarios
- **Risk analysis**: Monte Carlo risk modeling
- **Financial modeling**: Simulate portfolio returns
- **Supply chain**: Model lead time and capacity variability

## Input Requirements

The upstream DataFrame should have one row per variable:

| Column | Required | Description |
|--------|----------|-------------|
| `variable_column` | Yes | Variable name |
| `distribution_column` | Yes | Distribution type |
| `param1_column` | Yes | First parameter |
| `param2_column` | Yes | Second parameter |

## Output Schema

### `samples` mode
One column per variable, N rows (one per simulation).

### `statistics` mode

| Column | Description |
|--------|-------------|
| `variable` | Variable name |
| `mean` | Sample mean |
| `std` | Standard deviation |
| `p5`, `p25`, `p50`, `p75`, `p95` | Percentiles |
| `min`, `max` | Range |

## Configuration

```yaml
type: dagster_component_templates.SimulationSamplingComponent
attributes:
  asset_name: demand_simulation
  upstream_asset_key: demand_distributions
  variable_column: variable
  distribution_column: distribution
  param1_column: param1
  param2_column: param2
  n_simulations: 10000
  output_mode: statistics
  group_name: analytics
```

## Dependencies

- `pandas>=1.5.0`
- `numpy>=1.23.0`
