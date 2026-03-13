# AWSCDKAssetComponent

Deploy AWS CDK stacks as the first step in a Dagster pipeline. This component creates the cloud infrastructure — S3 buckets, RDS databases, Glue catalogs, VPCs, IAM roles, and so on — that downstream data assets depend on. Infrastructure provisioning and data processing live in the same lineage graph, making dependencies explicit and auditable.

## Prerequisites

### CDK Bootstrap

Every AWS account/region combination must be bootstrapped before CDK can deploy resources. Run this once from your CDK app directory:

```bash
cdk bootstrap aws://123456789012/us-east-1
```

Without bootstrapping, deployments will fail with a missing CDKToolkit stack error.

### CDK CLI

The `cdk` binary is installed separately via npm and is **not** included in `requirements.txt`:

```bash
npm install -g aws-cdk
```

Verify the installation:

```bash
cdk --version
```

If the `cdk` binary lives at a non-standard path, set `cdk_bin` to the full path.

## Provision-Before-Pipeline Pattern

The core use case is ensuring infrastructure exists before data flows through it. Define the CDK asset, then declare downstream assets as dependents:

```yaml
# component.yaml — infrastructure provisioning
type: dagster_component_templates.AWSCDKAssetComponent
attributes:
  asset_name: provision_data_platform
  working_dir: /opt/pipelines/cdk
  stacks:
    - DataLakeStack
    - GlueCatalogStack
  require_approval: never
  group_name: infrastructure
```

```python
# downstream_assets.py — data assets that depend on infrastructure
@dg.asset(deps=["provision_data_platform"])
def raw_events(context):
    # S3 bucket is guaranteed to exist because provision_data_platform ran first
    ...
```

Dagster's asset graph enforces the ordering. If the CDK deployment fails, downstream assets are never triggered.

## Key Configuration Options

### `require_approval: never`

For automated pipeline runs, always set `require_approval: never`. The default CDK behavior prompts for confirmation on security-sensitive changes (broadening IAM policies, security group rules), which will block an unattended run indefinitely.

```yaml
require_approval: never   # fully automated — no prompts
require_approval: any-change  # prompt on any diff
require_approval: broadening  # prompt only on permission widening (CDK default)
```

### `hotswap` for Lambda-Heavy Stacks in Development

When your stack contains Lambda functions or ECS task definitions, `hotswap: true` bypasses CloudFormation for those resources and updates them directly. Deployments that would otherwise take minutes complete in seconds.

```yaml
hotswap: true   # fast path — skips CloudFormation for supported resource types
```

**Do not use `hotswap` in production.** It skips drift detection and can leave CloudFormation state out of sync with actual AWS resources.

### `exclusively` to Skip Dependency Stacks

By default CDK deploys a stack's dependency stacks automatically. Set `exclusively: true` when you only want to touch the stacks listed in `stacks` and trust that their dependencies are already up to date:

```yaml
stacks:
  - ApplicationStack
exclusively: true   # do not redeploy NetworkStack even if it is a dependency
```

### `context` Variables

Pass CDK context values that your app reads via `this.node.tryGetContext()`:

```yaml
context:
  environment: production
  account: "123456789012"
  region: us-east-1
```

### CloudFormation `parameters`

Override CloudFormation parameters at deploy time:

```yaml
parameters:
  DataLakeStack:BucketName: my-company-data-lake
  DataLakeStack:RetentionDays: "90"
```

## Stack Outputs as Dagster Metadata

After a successful deployment the component reads the CDK outputs file and attaches the parsed JSON to the asset as metadata. In the Dagster UI, open the materialization event to find:

- **stacks** — list of stack names that were deployed
- **outputs** — full CloudFormation outputs map (e.g. bucket names, endpoint URLs, ARNs)
- **working_dir** — the CDK app directory used
- **cdk_command** — the exact command that was executed

This makes it straightforward to trace which infrastructure version was active for any pipeline run.

### Passing Outputs to Downstream Assets

Downstream assets can read stack outputs from the outputs file directly:

```python
import json

@dg.asset(deps=["provision_data_platform"])
def raw_events(context):
    with open("/tmp/provision_data_platform_outputs.json") as f:
        outputs = json.load(f)
    bucket = outputs["DataLakeStack"]["RawEventsBucketName"]
    ...
```

Or use the `outputs_file` field to write to a stable, known path:

```yaml
outputs_file: /opt/pipelines/cdk_outputs.json
```

## Full Example

```yaml
type: dagster_component_templates.AWSCDKAssetComponent
attributes:
  asset_name: provision_data_platform
  working_dir: "{{ project_root }}/cdk"
  stacks:
    - DataLakeStack
    - GlueCatalogStack
  context:
    environment: production
    account: "123456789012"
  parameters:
    DataLakeStack:RetentionDays: "90"
  require_approval: never
  exclusively: false
  hotswap: false
  rollback: true
  outputs_file: /opt/pipelines/cdk_outputs.json
  profile: data-platform-deploy
  region: us-east-1
  group_name: infrastructure
  description: Deploy S3 data lake and Glue catalog stacks before pipeline runs
  deps: []
```

## Destroy Operation

To tear down stacks, set `operation: destroy`. This is useful for ephemeral environments:

```yaml
operation: destroy
stacks:
  - EphemeralTestStack
require_approval: never
```

## Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| `CDKToolkit` stack not found | Account not bootstrapped | Run `cdk bootstrap` |
| `cdk: command not found` | CDK CLI not on PATH | `npm install -g aws-cdk` or set `cdk_bin` |
| Deployment hangs waiting for input | `require_approval` not set to `never` | Set `require_approval: never` |
| Stack stuck in `UPDATE_ROLLBACK_FAILED` | Previous failed deploy left broken state | Manually resolve in AWS Console, then re-run |
| Hotswap fails for a resource type | Resource type not supported by hotswap | Remove `hotswap: true` or update CDK version |
