# PanderaAssetCheck

Wraps `dagster-pandera` so a Pandera DataFrameSchema becomes a Dagster asset check. Define schemas in a Python file the component imports, point at the upstream asset, and the check runs the schema against the DataFrame on every materialization.

Wraps the official `dagster-pandera` package.

## Example

```yaml
type: dagster_component_templates.PanderaAssetCheckComponent
attributes:
  asset_key: <fill in>
  schema_module: <fill in>
  schema_name: <fill in>
  blocking: <fill in>
  description: <fill in>
```

## Requirements

```
dagster
dagster-pandera
pandera
```
