# SlingReplicationAsset

Wraps `dagster-sling` to run Sling replication YAMLs as Dagster assets — one asset per stream in the replication. Each materialization runs the configured stream from source to target.

Wraps the official `dagster-sling` package.

## Example

```yaml
type: dagster_component_templates.SlingReplicationAssetComponent
attributes:
  replication_config_path: <fill in>
  group_name: <fill in>
  name: <fill in>
```

## Requirements

```
dagster
dagster-sling
pyyaml
```
