# K8sJobAsset

Wraps `dagster-k8s` to launch a Kubernetes Job on materialization, wait for completion, and surface stdout/stderr in the Dagster run log. Useful for compute that needs to run in-cluster (training jobs, batch jobs, anything containerized).

Wraps the official `dagster-k8s` package.

## Example

```yaml
type: dagster_component_templates.K8sJobAssetComponent
attributes:
  asset_name: <fill in>
  image: <fill in>
  command: <fill in>
  namespace: <fill in>
  env_vars: <fill in>
  cpu_limit: <fill in>
  memory_limit: <fill in>
  group_name: <fill in>
  deps: <fill in>
```

## Requirements

```
dagster
dagster-k8s
```
