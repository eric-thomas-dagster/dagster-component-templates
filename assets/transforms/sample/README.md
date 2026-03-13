# SampleComponent

Sample rows from a DataFrame using a fixed count (`n`) or a fraction (`frac`).
Supports weighted sampling and sampling with replacement for bootstrapping.

## Use Cases

- Create smaller representative subsets for exploratory analysis
- Bootstrap resampling for statistical estimates
- Reduce dataset size for faster iteration during development
- Weight-biased sampling to oversample rare classes

## Fields

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `asset_name` | `str` | Yes | — | Output Dagster asset name |
| `upstream_asset_key` | `str` | Yes | — | Upstream asset key providing a DataFrame |
| `n` | `int` | No* | `None` | Number of rows to sample. Mutually exclusive with `frac`. |
| `frac` | `float` | No* | `None` | Fraction of rows to sample, e.g. `0.1` = 10%. Mutually exclusive with `n`. |
| `random_state` | `int` | No | `42` | Random seed for reproducibility |
| `replace` | `bool` | No | `False` | Sample with replacement (allows duplicate rows) |
| `weights` | `str` | No | `None` | Column name to use as sampling weights |
| `group_name` | `str` | No | `None` | Dagster asset group name |

*Exactly one of `n` or `frac` must be provided.

## Example YAML

```yaml
type: dagster_component_templates.SampleComponent
attributes:
  asset_name: transactions_sample
  upstream_asset_key: raw_transactions
  frac: 0.1
  random_state: 42
  replace: false
  group_name: data_exploration
```

### Fixed Count Example

```yaml
type: dagster_component_templates.SampleComponent
attributes:
  asset_name: events_1000
  upstream_asset_key: raw_events
  n: 1000
  random_state: 7
  group_name: dev_samples
```

## Inputs and Outputs

| | Type | Description |
|-|------|-------------|
| Input | `pd.DataFrame` | The upstream DataFrame to sample from |
| Output | `pd.DataFrame` | Sampled DataFrame with reset integer index |

## IO Manager Note

This component returns a plain `pd.DataFrame`. Ensure your Dagster environment
has an IO manager configured that can handle DataFrames.

## Notes

- Exactly one of `n` or `frac` must be specified; a `ValueError` is raised otherwise.
- The output index is always reset to a clean integer range.
- `weights` must refer to a numeric column in the upstream DataFrame.
- Use `replace=True` with `frac > 1.0` for bootstrap samples larger than the input.
