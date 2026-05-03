# PandasDataframeCheck

Wraps `dagster-pandas` constraints to validate column existence, dtypes, and value bounds against the upstream DataFrame asset. Lighter-weight than Pandera for simple shape checks.

Wraps the official `pandas` package.

## Example

```yaml
type: dagster_component_templates.PandasDataframeCheckComponent
attributes:
  asset_key: <fill in>
  required_columns: <fill in>
  column_types: <fill in>
  blocking: <fill in>
```

## Requirements

```
dagster
pandas
```
