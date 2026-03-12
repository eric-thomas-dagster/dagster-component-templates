# Helm Deploy

Deploys or upgrades a Helm chart as the **first step in a Dagster asset graph**. Downstream assets declare a dependency on this asset and will not execute until the Helm release has been deployed and all pods are ready.

## Dependencies

```
dagster
```

Helm must be installed separately and available on `PATH`, or provide the full path via `helm_bin`. No Python Helm SDK is required — this component shells out to the CLI directly using `helm upgrade --install --output json`.

## Use case: provision before pipeline

The canonical pattern is a Helm deploy asset that brings up a Kubernetes workload required by the rest of your pipeline before any computation assets run.

### ML serving example

Deploy a model serving container, then run batch inference — ensuring the serving endpoint is live and ready before any inference asset attempts to call it:

```
deploy_ml_serving       ← HelmDeployComponent (chart: my-org/ml-serving, namespace: ml-serving)
        │
        ├── run_batch_inference_gpu    ← calls the serving endpoint
        ├── run_batch_inference_cpu    ← calls the serving endpoint
        └── generate_inference_report ← depends on inference results
```

### Database / data platform example

Deploy a database operator or message queue chart before data loading assets:

```
deploy_kafka            ← HelmDeployComponent (chart: bitnami/kafka, namespace: data-platform)
        │
        ├── load_raw_events       ← publishes to Kafka
        └── consume_events        ← subscribes to Kafka topics
```

Downstream assets declare the dependency in their own component YAML:

```yaml
# downstream inference asset (any component type)
type: dagster_component_templates.SomeOtherAssetComponent
attributes:
  asset_name: run_batch_inference_gpu
  deps:
    - deploy_ml_serving
```

Dagster enforces the ordering: `deploy_ml_serving` must materialize successfully (all pods ready) before any dependent asset is allowed to run. If the Helm deployment fails or pods do not become ready within `timeout`, the entire downstream pipeline is skipped automatically.

## Quick start

```yaml
type: dagster_component_templates.HelmDeployComponent
attributes:
  asset_name: deploy_ml_serving
  release_name: ml-serving
  chart: my-org/ml-serving
  repo_url: https://charts.my-company.com
  repo_name: my-org
  namespace: ml-serving
  set_values:
    image.tag: "latest"
    replicas: "3"
  group_name: infrastructure
  description: Deploy ML model serving before inference pipeline runs
```

## Chart reference formats

| Format | Example | Use case |
|--------|---------|----------|
| Repo alias + chart name | `my-org/ml-serving` | Charts from a registered Helm repository |
| Local path | `./charts/ml-serving` | Charts committed alongside your code |
| OCI reference | `oci://registry.example.com/charts/ml-serving` | Charts stored in an OCI registry (e.g. ECR, GHCR) |

## Adding a Helm repository automatically

Set `repo_url` and `repo_name` to register the repository before installing. This is equivalent to running `helm repo add my-org https://charts.my-company.com && helm repo update` before `helm upgrade --install`:

```yaml
attributes:
  chart: my-org/ml-serving
  repo_url: https://charts.my-company.com
  repo_name: my-org
```

Omit `repo_url` when the chart is a local path, an OCI reference, or a repository already registered on the host.

## Pinning a chart version

Set `version` to lock the deployment to a specific chart version. Without this field, Helm uses the latest available version from the repository:

```yaml
attributes:
  version: "1.4.2"
```

## Values files and --set overrides

Pass `values.yaml` files and inline overrides:

```yaml
attributes:
  values_files:
    - ./helm/values-base.yaml
    - ./helm/values-production.yaml
  set_values:
    image.tag: "sha-abc123"
    autoscaling.enabled: "true"
    autoscaling.maxReplicas: "10"
  set_string_values:
    annotations."app.kubernetes.io/managed-by": dagster
```

`set_values` maps to `--set` (Helm auto-coerces types). `set_string_values` maps to `--set-string` and always passes the value as a string — useful for values that look like numbers but should remain strings (e.g. version tags, annotation values).

## Atomic deploys with automatic rollback

`atomic: true` (the default) passes `--atomic` to Helm, which automatically rolls back the release to the previous known-good state if the deployment fails or times out. This ensures the release never gets stuck in a broken intermediate state:

```yaml
attributes:
  atomic: true   # default
  timeout: 15m   # extend for slow cluster or large images
```

To disable automatic rollback (e.g. when you want to inspect a failed release):

```yaml
attributes:
  atomic: false
```

## Waiting for pod readiness

`wait: true` (the default) passes `--wait` to Helm, which makes the command block until all pods, persistent volumes, and services report as ready before returning. Combined with `atomic`, this means the Dagster asset materializes only when the workload is genuinely healthy and accepting traffic:

```yaml
attributes:
  wait: true     # default
  timeout: 10m   # default
```

## Dry run

Set `dry_run: true` to render templates and validate the manifest against the Kubernetes API server without actually deploying:

```yaml
attributes:
  dry_run: true
```

Useful in CI to catch template errors or schema validation failures before a real deployment.

## Multi-cluster support via kube_context / kubeconfig

Deploy to a specific cluster or context:

```yaml
attributes:
  kube_context: arn:aws:eks:us-east-1:123456789012:cluster/data-platform-prod
  kubeconfig: /home/dagster/.kube/data-platform.yaml
```

Omit both to use the default kubeconfig context (`~/.kube/config`).

## Environment variables for cloud auth

Pass cloud provider credentials or proxy settings via `env_vars`:

```yaml
attributes:
  env_vars:
    AWS_PROFILE: data-platform-prod
    HELM_KUBEAPISERVER: https://kube-api.internal.example.com
```

These are merged with the current process environment before the subprocess is spawned.

## Release metadata as Dagster asset metadata

After a successful deployment, the following are recorded as Dagster asset metadata and visible in the Dagster UI:

| Metadata key | Source |
|---|---|
| `release` | Helm release name |
| `chart` | Chart reference used |
| `chart_version` | Version from `helm upgrade` JSON output |
| `app_version` | Application version from chart metadata |
| `namespace` | Kubernetes namespace |
| `status` | Final release status from `helm status` |
| `resources` | Resource summary from `helm status --output json` |

## All fields

| Field | Type | Default | Description |
|---|---|---|---|
| `asset_name` | `str` | required | Dagster asset name |
| `release_name` | `str` | required | Helm release name |
| `chart` | `str` | required | Chart reference (repo/chart, local path, or OCI) |
| `namespace` | `str` | `"default"` | Kubernetes namespace |
| `create_namespace` | `bool` | `true` | Create namespace if it does not exist |
| `values_files` | `list[str] \| null` | `null` | Values files passed as `-f` |
| `set_values` | `dict[str, str] \| null` | `null` | Overrides passed as `--set` |
| `set_string_values` | `dict[str, str] \| null` | `null` | Overrides passed as `--set-string` |
| `version` | `str \| null` | `null` | Chart version to pin (`--version`) |
| `repo_url` | `str \| null` | `null` | Helm repo URL to add before install |
| `repo_name` | `str \| null` | `null` | Alias to register the repo as |
| `kube_context` | `str \| null` | `null` | Kubernetes context (`--kube-context`) |
| `kubeconfig` | `str \| null` | `null` | Path to kubeconfig file (`--kubeconfig`) |
| `atomic` | `bool` | `true` | Auto-rollback on failure (`--atomic`) |
| `wait` | `bool` | `true` | Wait for pods to be ready (`--wait`) |
| `timeout` | `str` | `"10m"` | Max wait time (`--timeout`) |
| `dry_run` | `bool` | `false` | Dry run — no changes applied (`--dry-run`) |
| `helm_bin` | `str` | `"helm"` | Path to the helm binary |
| `env_vars` | `dict[str, str] \| null` | `null` | Extra subprocess environment variables |
| `group_name` | `str` | `"infrastructure"` | Dagster asset group name |
| `description` | `str \| null` | `null` | Asset description |
| `deps` | `list[str] \| null` | `null` | Upstream Dagster asset keys |
