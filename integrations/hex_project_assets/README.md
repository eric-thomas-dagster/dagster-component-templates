# HexProjectAssets

Wraps `dagster-hex` to model a Hex project as a Dagster asset — materializing the Dagster asset triggers a Hex project run via the Hex API.

## Example

```yaml
type: dagster_component_templates.HexProjectAssetsComponent
attributes:
  api_key_env_var: <fill in>
  project_id: <fill in>
  asset_name: <fill in>
  group_name: <fill in>
```

## Requirements

```
dagster
dagster-hex
```
