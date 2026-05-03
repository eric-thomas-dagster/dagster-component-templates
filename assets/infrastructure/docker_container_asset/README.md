# DockerContainerAsset

Wraps `dagster-docker` to run a container per materialization, wait for completion, and stream logs into the Dagster run log. Lighter-weight than k8s for local-dev or single-host pipelines.

Wraps the official `dagster-docker` package.

## Example

```yaml
type: dagster_component_templates.DockerContainerAssetComponent
attributes:
  asset_name: <fill in>
  image: <fill in>
  command: <fill in>
  env_vars: <fill in>
  network: <fill in>
  group_name: <fill in>
  deps: <fill in>
```

## Requirements

```
dagster
dagster-docker
```
