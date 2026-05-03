# AirbyteAssets

Wraps `dagster-airbyte`'s `load_assets_from_airbyte_instance` (or `AirbyteCloudWorkspace`) to materialize one Dagster asset per Airbyte connection. Pair with `airbyte_sync_sensor` (already in the registry) to trigger Dagster runs when an Airbyte sync completes.

## Example

```yaml
type: dagster_component_templates.AirbyteAssetsComponent
attributes:
  workspace_id: <fill in>
  client_id_env_var: <fill in>
  client_secret_env_var: <fill in>
  connection_filter: <fill in>
  group_name: <fill in>
```

## Requirements

```
dagster
dagster-airbyte
```
