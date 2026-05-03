# CensusSyncAsset

Wraps `dagster-census` to expose a Census sync as a Dagster asset. Materializing the Dagster asset triggers a Census run that pushes warehouse data to your downstream SaaS.

## Example

```yaml
type: dagster_component_templates.CensusSyncAssetComponent
attributes:
  api_key_env_var: <fill in>
  sync_id: <fill in>
  asset_name: <fill in>
  group_name: <fill in>
```

## Requirements

```
dagster
dagster-census
```
