# DltPipelineAssets

Wraps `dagster-dlt`'s `build_dlt_asset_specs` to surface dlt pipeline resources as Dagster assets. Each dlt resource becomes a Dagster asset; materialization runs the dlt pipeline. The official path for dlt + Dagster integration.

Wraps the official `dagster-dlt` package.

## Example

```yaml
type: dagster_component_templates.DltPipelineAssetsComponent
attributes:
  pipeline_module: <fill in>
  pipeline_name: <fill in>
  source_module: <fill in>
  source_name: <fill in>
  group_name: <fill in>
```

## Requirements

```
dagster
dagster-dlt
dlt
```
