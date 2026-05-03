# TableauAssets

Wraps `dagster-tableau`'s workspace clients to materialize Tableau workbooks, dashboards, and sheets as Dagster external assets — full lineage from warehouse → semantic layer → Tableau dashboards.

## Example

```yaml
type: dagster_component_templates.TableauAssetsComponent
attributes:
  server_url: <fill in>
  site_name: <fill in>
  token_name_env_var: <fill in>
  token_value_env_var: <fill in>
  online: <fill in>
```

## Requirements

```
dagster
dagster-tableau
```
