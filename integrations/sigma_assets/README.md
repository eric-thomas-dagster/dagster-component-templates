# SigmaAssets

Wraps `dagster-sigma`'s `SigmaOrganization` to surface Sigma workbooks and dataset definitions as Dagster external assets — they show up in the asset graph alongside their upstream warehouse tables, giving you BI-tier lineage.

## Example

```yaml
type: dagster_component_templates.SigmaAssetsComponent
attributes:
  base_url: <fill in>
  client_id_env_var: <fill in>
  client_secret_env_var: <fill in>
```

## Requirements

```
dagster
dagster-sigma
```
