# Optimization Component

Solve linear programming (LP) and mixed-integer optimization problems using SciPy's HiGHS solver, reading the problem definition from an upstream Dagster asset DataFrame.

## Overview

The `OptimizationComponent` takes a DataFrame where each row represents a decision variable, and columns specify objective coefficients, bounds, and constraint coefficients. It calls SciPy's `linprog` to find the optimal solution and returns a DataFrame with results appended.

## Use Cases

- **Portfolio optimization**: Maximize expected return subject to risk constraints
- **Resource allocation**: Minimize cost given supply/demand constraints
- **Production planning**: Optimize production mix
- **Logistics**: Minimize transportation cost

## Input Requirements

The upstream DataFrame should have one row per decision variable:

| Column | Type | Required | Description |
|--------|------|----------|-------------|
| `objective_column` | float | Yes | Objective function coefficient |
| `bounds_lower_column` | float | No | Lower bound for variable |
| `bounds_upper_column` | float | No | Upper bound for variable |
| `constraint_columns` | float | No | One column per inequality constraint |
| `constraint_rhs_column` | float | No | Right-hand side for constraints |

## Output Schema

| Column | Type | Description |
|--------|------|-------------|
| `[all input columns]` | various | Original data preserved |
| `optimal_value` | float | Optimal variable value |
| `status` | string | Solver status message |

## Configuration

### Minimize cost example

```yaml
type: dagster_component_templates.OptimizationComponent
attributes:
  asset_name: production_plan
  upstream_asset_key: production_problem
  objective_column: unit_cost
  bounds_lower_column: min_production
  bounds_upper_column: max_production
  maximize: false
  group_name: analytics
```

### Maximize return example

```yaml
type: dagster_component_templates.OptimizationComponent
attributes:
  asset_name: portfolio_weights
  upstream_asset_key: portfolio_problem
  objective_column: expected_return
  bounds_lower_column: min_weight
  bounds_upper_column: max_weight
  maximize: true
  group_name: finance
```

## Dependencies

- `pandas>=1.5.0`
- `scipy>=1.9.0`
- `numpy>=1.23.0`
