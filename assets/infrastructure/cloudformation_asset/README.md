# CloudFormation Asset

Deploy or update an AWS CloudFormation stack as a Dagster asset. Use this as the **first step in a pipeline** so that downstream data assets — S3 buckets, RDS instances, Glue databases, IAM roles, and any other AWS resources provisioned by the stack — are guaranteed to exist before data processing begins.

## Key Features

- **Create-or-update idempotency via change sets.** If the stack does not exist it is created. If it already exists, a CloudFormation change set is used to apply only the diff. If there are no changes, the asset completes successfully without touching the stack — making it safe to re-run at any time.
- **Stack outputs as Dagster metadata.** After every materialization, all CloudFormation stack outputs (e.g. bucket ARNs, database endpoints, queue URLs) are captured and surfaced as asset metadata visible in the Dagster UI asset catalog.
- **Pipeline first-step pattern.** Declare downstream data assets as `deps` so Dagster's scheduler and the asset graph enforce that infrastructure is ready before any downstream computation runs.

## Example

```yaml
type: dagster_component_templates.CloudFormationAssetComponent
attributes:
  asset_name: provision_data_lake
  stack_name: my-company-data-platform
  template_file: "{{ project_root }}/infra/data_platform.yaml"
  parameters:
    Environment: production
    BucketName: my-company-data-lake
  region_name: us-east-1
  group_name: infrastructure
  description: Provision S3 data lake, Glue catalog, and IAM roles
```

## Template: `template_file` vs `template_url`

| Field | When to use |
|---|---|
| `template_file` | Template lives in your repo. The path is read at materialization time. Supports `{{ project_root }}` for project-relative paths. |
| `template_url` | Template is stored in S3 (e.g. `https://s3.amazonaws.com/my-bucket/template.yaml`). Required for templates larger than 51,200 bytes. |

Exactly one of the two must be provided; specifying neither (or both) raises an error at materialization time.

## Using as a Pipeline First Step

Declare the CloudFormation asset and then reference it as a dep on downstream assets:

```yaml
# Component 1 — infrastructure
type: dagster_component_templates.CloudFormationAssetComponent
attributes:
  asset_name: provision_data_lake
  stack_name: my-company-data-platform
  template_file: "{{ project_root }}/infra/data_platform.yaml"
  group_name: infrastructure

---
# Component 2 — downstream data asset that must wait for infra
type: dagster_component_templates.SomeOtherComponent
attributes:
  asset_name: ingest_raw_events
  deps:
    - provision_data_lake
```

Dagster will refuse to run `ingest_raw_events` until `provision_data_lake` has materialized successfully, enforcing the infrastructure-first ordering in both scheduled runs and manual re-materializations.

## Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `asset_name` | `str` | required | Dagster asset name |
| `stack_name` | `str` | required | CloudFormation stack name |
| `template_file` | `str` | `null` | Local path to CF template |
| `template_url` | `str` | `null` | S3 URL to CF template |
| `parameters` | `dict[str, str]` | `null` | Stack parameters |
| `capabilities` | `list[str]` | `["CAPABILITY_IAM", "CAPABILITY_NAMED_IAM"]` | CF capabilities |
| `region_name` | `str` | `"us-east-1"` | AWS region |
| `aws_profile` | `str` | `null` | AWS CLI profile |
| `role_arn` | `str` | `null` | IAM role for CloudFormation to assume |
| `on_failure` | `str` | `"ROLLBACK"` | CREATE failure behavior |
| `timeout_minutes` | `int` | `60` | Max wait time |
| `poll_interval_seconds` | `int` | `15` | Status poll interval |
| `tags` | `dict[str, str]` | `null` | AWS resource tags |
| `group_name` | `str` | `"infrastructure"` | Dagster group name |
| `description` | `str` | `null` | Asset description |
| `deps` | `list[str]` | `null` | Upstream Dagster asset keys |

## IAM Permissions

The IAM principal running Dagster (or the role specified in `aws_profile`) needs at minimum:

```json
{
  "Effect": "Allow",
  "Action": [
    "cloudformation:CreateStack",
    "cloudformation:UpdateStack",
    "cloudformation:DescribeStacks",
    "cloudformation:CreateChangeSet",
    "cloudformation:DescribeChangeSet",
    "cloudformation:ExecuteChangeSet",
    "cloudformation:DeleteChangeSet"
  ],
  "Resource": "*"
}
```

If you set `role_arn`, the calling principal also needs:

```json
{
  "Effect": "Allow",
  "Action": "iam:PassRole",
  "Resource": "<role_arn>"
}
```

Additionally, the principal (or the assumed role) must have permissions for every AWS resource type that the CloudFormation template creates (e.g. `s3:CreateBucket`, `rds:CreateDBInstance`, etc.).

## Stack Outputs in the Dagster UI

After each successful materialization, all CloudFormation stack outputs are attached as asset metadata. In the Dagster UI's asset catalog you will see entries such as:

```
stack_name   →  my-company-data-platform
status       →  CREATE_COMPLETE
outputs      →  {
                  "DataLakeBucketArn": "arn:aws:s3:::my-company-data-lake",
                  "GlueDatabaseName": "my_catalog",
                  "IamRoleArn": "arn:aws:iam::123456789:role/DataEngineerRole"
                }
```

These values can be referenced by downstream assets to avoid hard-coding ARNs or endpoints.

## Dependencies

```
boto3>=1.26.0
```
