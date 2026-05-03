# DagstermillNotebookAsset

Wraps `dagstermill.define_dagstermill_asset` so a Jupyter notebook becomes a real Dagster asset — papermill-execute the notebook on materialization, capture outputs, surface lineage. Useful for analyses + reports that should be scheduled and tracked.

Wraps the official `dagstermill` package.

## Example

```yaml
type: dagster_component_templates.DagstermillNotebookAssetComponent
attributes:
  asset_name: <fill in>
  notebook_path: <fill in>
  group_name: <fill in>
  description: <fill in>
  deps: <fill in>
```

## Requirements

```
dagster
dagstermill
ipykernel
```
