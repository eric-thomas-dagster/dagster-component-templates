# Terraform Asset

Provisions infrastructure with Terraform as the **first step in a Dagster asset graph**. Downstream data assets declare a dependency on this asset and will not execute until Terraform has applied successfully.

## Dependencies

```
dagster
```

Terraform must be installed separately and available on `PATH`, or provide the full path via `terraform_bin`. No Python Terraform SDK is required — this component shells out to the CLI directly.

## Use case: provision before pipeline

The canonical pattern is a Terraform asset that stands up the infrastructure required by the rest of your pipeline (S3 buckets, IAM roles, databases, VPC resources, etc.) before any data assets run:

```
provision_data_infrastructure  ← TerraformAssetComponent
        │
        ├── load_raw_events    ← depends on S3 bucket existing
        ├── load_raw_users     ← depends on RDS instance existing
        └── dbt_run            ← depends on IAM roles being in place
```

Downstream assets declare the dependency in their own component YAML:

```yaml
# downstream asset
type: dagster_component_templates.SomeOtherAssetComponent
attributes:
  asset_name: load_raw_events
  deps:
    - provision_data_infrastructure
```

## Quick start

```yaml
type: dagster_component_templates.TerraformAssetComponent
attributes:
  asset_name: provision_data_infrastructure
  working_dir: "{{ project_root }}/terraform/data_platform"
  workspace: production
  var_files:
    - vars/production.tfvars
  group_name: infrastructure
  description: Provision S3 buckets, IAM roles, and RDS instance before pipeline runs
```

## Workspace selection

Set `workspace` to automatically select (or create) a Terraform workspace before running:

```yaml
attributes:
  workspace: staging   # runs: terraform workspace select staging
```

If the workspace does not exist it is created with `terraform workspace new` and then selected. Omit `workspace` (or leave it `null`) to use the default workspace.

## Var files and inline vars

Pass `.tfvars` files or inline variable overrides:

```yaml
attributes:
  var_files:
    - vars/common.tfvars
    - vars/production.tfvars
  vars:
    db_instance_class: db.t3.medium
    enable_deletion_protection: "true"
```

Both are translated to the appropriate `-var-file` and `-var` flags on the `plan` and `destroy` commands.

## plan_only mode for CI

Set `plan_only: true` to run `terraform plan` without applying. Use this in CI pipelines to detect drift without modifying infrastructure:

```yaml
attributes:
  plan_only: true
```

Exit code semantics from `terraform plan -detailed-exitcode` are respected:
- `0` — no changes
- `2` — changes present (apply will run when `plan_only: false`)
- `1` — error (always raises an exception)

## Targeting specific resources

Limit the apply scope with `-target`:

```yaml
attributes:
  targets:
    - aws_s3_bucket.data_lake
    - aws_iam_role.pipeline_executor
```

## Terraform outputs as metadata

After a successful apply, `terraform output -json` is parsed and the output values are surfaced as Dagster asset metadata. This makes it easy to inspect provisioned resource identifiers (bucket names, endpoint URLs, ARNs, etc.) in the Dagster UI without navigating to the cloud console.

## Environment variables for cloud auth

Pass cloud provider credentials or profiles via `env_vars`:

```yaml
attributes:
  env_vars:
    AWS_PROFILE: data-platform-prod
    AWS_REGION: us-east-1
```

These are merged with the current process environment before the subprocess is spawned.

## Destroy mode

Set `destroy: true` to run `terraform destroy` instead of `terraform apply`:

```yaml
attributes:
  destroy: true
  auto_approve: true
```

## All fields

| Field | Type | Default | Description |
|---|---|---|---|
| `asset_name` | `str` | required | Dagster asset name |
| `working_dir` | `str` | required | Path to Terraform root module directory |
| `workspace` | `str \| null` | `null` | Terraform workspace to select |
| `var_files` | `list[str] \| null` | `null` | `.tfvars` files passed as `-var-file` |
| `vars` | `dict[str, str] \| null` | `null` | Inline vars passed as `-var key=value` |
| `targets` | `list[str] \| null` | `null` | `-target` resources to limit apply scope |
| `terraform_bin` | `str` | `"terraform"` | Path to terraform binary |
| `init_on_run` | `bool` | `true` | Run `terraform init` before apply |
| `plan_only` | `bool` | `false` | Only plan, do not apply |
| `destroy` | `bool` | `false` | Run destroy instead of apply |
| `parallelism` | `int` | `10` | `-parallelism` flag |
| `auto_approve` | `bool` | `true` | Pass `-auto-approve` to apply/destroy |
| `env_vars` | `dict[str, str] \| null` | `null` | Extra subprocess environment variables |
| `group_name` | `str` | `"infrastructure"` | Dagster asset group name |
| `description` | `str \| null` | `null` | Asset description |
| `deps` | `list[str] \| null` | `null` | Upstream Dagster asset keys |
