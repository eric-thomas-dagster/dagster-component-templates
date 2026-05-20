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

<!-- FIELDS:START - auto-generated by tools/regen_readme_fields.py -->

## Fields

### Required

| Field | Type | Description |
|---|---|---|
| `asset_name` | `str` | Dagster asset name |
| `release_name` | `str` | Helm release name |
| `chart` | `str` | Chart reference: repository alias + name (e.g. 'my-org/ml-serving'), local path (e.g. './charts/ml-serving'), or OCI reference (e.g. 'oci://registry.example.com/charts/ml-serving'). |

### Execution

| Field | Type | Default | Description |
|---|---|---|---|
| `timeout` | `str` | `"10m"` | Maximum time to wait for resources to become ready (--timeout). Format: '10m', '300s', etc. |

### Catalog metadata

| Field | Type | Default | Description |
|---|---|---|---|
| `group_name` | `str` | `"infrastructure"` | Dagster asset group name. |
| `description` | `str` | — | Human-readable description of what this Helm release deploys. |
| `deps` | `list[str]` | — | Upstream Dagster asset keys this asset depends on (e.g. ['raw_orders', 'schema/orders']). |
| `owners` | `List[str]` | — | Asset owners — team names ('team:analytics') or email addresses. |
| `asset_tags` | `Dict[str, str]` | — | Additional key-value tags applied to the asset in the Dagster catalog. |
| `kinds` | `List[str]` | — | Asset kinds for the catalog (e.g. ['snowflake', 'python']). Auto-inferred from component name when unset. |
| `column_lineage` | `Dict[str, List[str]]` | — | Column-level lineage: output column → list of upstream columns it derives from, e.g. {'revenue': ['price', 'quantity']}. |

### Freshness

| Field | Type | Default | Description |
|---|---|---|---|
| `freshness_max_lag_minutes` | `int` | — | Maximum acceptable lag in minutes before the asset is considered stale. Builds a FreshnessPolicy when set. |
| `freshness_cron` | `str` | — | Cron schedule string for the freshness policy, e.g. '0 9 * * 1-5' (weekdays 9am). |

### Partitions

| Field | Type | Default | Description |
|---|---|---|---|
| `partition_type` | `str` | — | Partition type: 'daily', 'weekly', 'monthly', 'hourly', 'static', 'multi', or None for unpartitioned. |
| `partition_start` | `str` | — | Partition start date in ISO format (e.g. '2024-01-01'). Required for time-based partition types. |
| `partition_date_column` | `str` | — | Column used to filter the upstream DataFrame to the current date partition key. |
| `partition_values` | `str` | — | Comma-separated values for static or multi partitioning, e.g. 'acme,globex,initech'. |
| `partition_static_dim` | `str` | — | Dimension name for the static axis in multi-partitioning, e.g. 'customer'. |
| `partition_static_column` | `str` | — | Column used to filter the upstream DataFrame to the current static partition value. |

### Retry policy

| Field | Type | Default | Description |
|---|---|---|---|
| `retry_policy_max_retries` | `int` | — | Max retries on failure. Defines a RetryPolicy when set. |
| `retry_policy_delay_seconds` | `int` | — | Seconds between retries (default 1). |
| `retry_policy_backoff` | `str` | `"exponential"` | Backoff strategy: 'linear' or 'exponential'. |

### Other

| Field | Type | Default | Description |
|---|---|---|---|
| `namespace` | `str` | `"default"` | Kubernetes namespace to deploy into. |
| `create_namespace` | `bool` | `true` | Create the namespace if it does not already exist (--create-namespace). |
| `values_files` | `list[str]` | — | List of values.yaml files passed as -f to helm upgrade. |
| `set_values` | `dict[str, str]` | — | Key-value pairs passed as --set. Values are coerced to their YAML type by Helm. |
| `set_string_values` | `dict[str, str]` | — | Key-value pairs passed as --set-string. Values are always treated as strings. |
| `version` | `str` | — | Pin a specific chart version (--version). Omit to use the latest. |
| `repo_url` | `str` | — | Helm repository URL to add before installing (e.g. https://charts.my-company.com). Required when using a repo alias in `chart` and the repo is not already added. |
| `repo_name` | `str` | — | Name to register the repository as (e.g. 'my-org'). Must match the alias prefix in `chart` when using a repository reference. |
| `kube_context` | `str` | — | Kubernetes context to use (--kube-context). Uses the current context if omitted. |
| `kubeconfig` | `str` | — | Path to a kubeconfig file (--kubeconfig). Uses ~/.kube/config if omitted. |
| `atomic` | `bool` | `true` | Roll back automatically on failure (--atomic). Ensures the release never gets stuck in a broken state. |
| `wait` | `bool` | `true` | Wait for all pods and resources to be ready before considering the deploy successful (--wait). |
| `dry_run` | `bool` | `false` | Perform a dry run — render templates and validate but do not apply (--dry-run). |
| `helm_bin` | `str` | `"helm"` | Path to the helm binary (default: 'helm', assumed to be on PATH). |
| `env_vars` | `dict[str, str]` | — | Extra environment variables for the Helm subprocess (e.g. KUBECONFIG, HELM_KUBEAPISERVER, cloud provider auth vars). |

<!-- FIELDS:END -->
