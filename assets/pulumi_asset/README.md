# Pulumi Asset

Deploys a Pulumi stack as the **first step in a Dagster asset graph**. Downstream data assets declare a dependency on this asset and will not execute until the stack has been deployed successfully.

## Dependencies

```
dagster
```

Pulumi must be installed separately and available on `PATH`, or provide the full path via `pulumi_bin`. No Pulumi Automation API SDK is required — this component shells out to the CLI directly using `pulumi up --json`.

## Use case: provision before pipeline

The canonical pattern is a Pulumi asset that stands up the cloud infrastructure required by the rest of your pipeline (object storage buckets, IAM roles, managed databases, Kubernetes clusters, etc.) before any data assets run:

```
provision_cloud_resources   ← PulumiAssetComponent (Pulumi stack: my-org/data-platform/production)
        │
        ├── load_raw_events      ← depends on S3 bucket / GCS bucket being provisioned
        ├── load_raw_users       ← depends on managed database endpoint being available
        └── run_dbt              ← depends on IAM roles and networking being in place
```

Downstream assets declare the dependency in their own component YAML:

```yaml
# downstream asset (any component type)
type: dagster_component_templates.SomeOtherAssetComponent
attributes:
  asset_name: load_raw_events
  deps:
    - provision_cloud_resources
```

Dagster enforces the ordering: `provision_cloud_resources` must materialize successfully before any dependent asset is allowed to run. If the Pulumi deployment fails, the entire downstream pipeline is skipped automatically.

## Quick start

```yaml
type: dagster_component_templates.PulumiAssetComponent
attributes:
  asset_name: provision_cloud_resources
  stack_name: my-org/data-platform/production
  working_dir: "{{ project_root }}/infrastructure"
  operation: up
  refresh: true
  group_name: infrastructure
  description: Provision S3 buckets, IAM roles, and RDS instance before pipeline runs
```

## Stack name formats

| Format | Backend |
|--------|---------|
| `my-org/data-platform/production` | Pulumi Cloud (organization/project/stack) |
| `my-org/data-platform/staging` | Pulumi Cloud — staging stack |
| `production` | Local or self-hosted backend |

## Operations

Set `operation` to control what Pulumi does when the asset is materialized:

| Value | Behavior |
|-------|----------|
| `up` (default) | Create or update resources to match the desired state |
| `destroy` | Tear down all resources in the stack |
| `preview` | Show what would change without making any modifications |

```yaml
attributes:
  operation: preview   # safe — read-only, no changes applied
```

## Refresh before apply

Set `refresh: true` to run `pulumi refresh` before `up` or `destroy`. This reconciles the Pulumi state file with the actual state of cloud resources, catching any out-of-band changes before attempting to apply:

```yaml
attributes:
  refresh: true
```

Refresh is skipped automatically when `operation: preview`.

## Drift detection with expect_no_changes

Set `expect_no_changes: true` to make the asset fail if Pulumi detects any resource changes. This is useful for a scheduled drift-detection pipeline that should alert when someone has manually modified infrastructure:

```yaml
attributes:
  operation: up
  expect_no_changes: true
```

When changes are detected, the asset raises an exception with the full change breakdown, which surfaces in the Dagster run log and can trigger alerting.

## Targeting specific resources

Limit the operation to specific resource URNs with `target_resources`:

```yaml
attributes:
  target_resources:
    - urn:pulumi:production::data-platform::aws:s3/bucket:Bucket::data-lake
    - urn:pulumi:production::data-platform::aws:iam/role:Role::pipeline-executor
```

This maps to `pulumi up --target <urn>` and is useful for partial updates in large stacks.

## Stack config overrides

Pass config key-value pairs to override stack config at runtime without modifying `Pulumi.<stack>.yaml`:

```yaml
attributes:
  config:
    aws:region: us-west-2
    myApp:instanceType: t3.large
```

Each entry becomes `--config key=value` on the Pulumi command line.

## Custom secrets provider

Specify an alternative secrets provider for the stack:

```yaml
attributes:
  secrets_provider: awskms://alias/my-pulumi-key
```

Supports any provider string accepted by the Pulumi CLI: `awskms://`, `azurekeyvault://`, `gcpkms://`, `hashivault://`, or `passphrase`.

## Cloud authentication via env_vars

Pass cloud provider credentials or profiles as environment variables:

```yaml
attributes:
  env_vars:
    AWS_PROFILE: data-platform-prod
    AWS_REGION: us-east-1
    PULUMI_ACCESS_TOKEN: "{{ env.PULUMI_ACCESS_TOKEN }}"
```

These are merged with the current process environment before the subprocess is spawned. For Pulumi Cloud, `PULUMI_ACCESS_TOKEN` must be set (either here or in the process environment) for authentication.

## Stack outputs as metadata

After a successful `up`, Pulumi stack outputs are parsed from `--json` and surfaced as Dagster asset metadata. This makes it easy to inspect provisioned resource identifiers (bucket names, endpoint URLs, ARNs, connection strings) in the Dagster UI without navigating to the cloud console.

Resource change counts (`create`, `update`, `delete`, `same`) are also recorded, giving an at-a-glance summary of what the deployment actually did.

## All fields

| Field | Type | Default | Description |
|---|---|---|---|
| `asset_name` | `str` | required | Dagster asset name |
| `stack_name` | `str` | required | Fully qualified stack: `org/project/stack` or just `stack` for local |
| `working_dir` | `str` | required | Directory containing `Pulumi.yaml` |
| `operation` | `str` | `"up"` | `"up"`, `"destroy"`, or `"preview"` |
| `expect_no_changes` | `bool` | `false` | Fail if changes are detected (drift detection) |
| `refresh` | `bool` | `false` | Run `pulumi refresh` before `up`/`destroy` |
| `target_resources` | `list[str] \| null` | `null` | Resource URNs to pass as `--target` |
| `config` | `dict[str, str] \| null` | `null` | Config key-value pairs passed as `--config` |
| `secrets_provider` | `str \| null` | `null` | Secrets provider URL (`--secrets-provider`) |
| `pulumi_bin` | `str` | `"pulumi"` | Path to the pulumi binary |
| `env_vars` | `dict[str, str] \| null` | `null` | Extra subprocess environment variables |
| `group_name` | `str` | `"infrastructure"` | Dagster asset group name |
| `description` | `str \| null` | `null` | Asset description |
| `deps` | `list[str] \| null` | `null` | Upstream Dagster asset keys |
