# LookerAssets

Wraps `dagster-looker`'s LookerResource to surface Looker explores, dashboards, and looks as Dagster external assets. Combine with a warehouse asset graph for full lineage from raw tables → LookML → dashboards.

## Example

```yaml
type: dagster_component_templates.LookerAssetsComponent
attributes:
  base_url: <fill in>
  client_id_env_var: <fill in>
  client_secret_env_var: <fill in>
  project: <fill in>
```

## Requirements

```
dagster
dagster-looker
```
