# FivetranAssets

Wraps `dagster-fivetran`'s `FivetranWorkspace` to materialize one Dagster asset per Fivetran connector. Pair with `fivetran_sync_sensor` (already in the registry) to trigger downstream runs when a sync completes.

## Example

```yaml
type: dagster_component_templates.FivetranAssetsComponent
attributes:
  account_id: <fill in>
  api_key_env_var: <fill in>
  api_secret_env_var: <fill in>
  connector_filter: <fill in>
  group_name: <fill in>
```

## Requirements

```
dagster
dagster-fivetran
```
