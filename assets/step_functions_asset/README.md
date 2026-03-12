# AWS Step Functions Asset

Connects to AWS Step Functions at component prepare time, discovers all matching state machines, and creates **one Dagster asset per state machine**. At execution time each asset starts a Step Functions execution and (optionally) polls until it reaches a terminal status.

## How it works

### StateBackedComponent caching

Discovery is performed in `write_state_to_path`, which pages through `sfn.list_state_machines`, applies any `name_prefix` / `exclude_names` filters, and writes the result to a local JSON cache. `build_defs_from_state` reads that cache and builds asset definitions with zero network calls — this is what makes code-server reloads instant.

Populate or refresh the cache with:

```bash
# In development — runs automatically on `dagster dev`
dagster dev

# In CI/CD or during image build
dg utils refresh-defs-state
```

If no cache exists yet the component returns empty `Definitions` and logs a warning.

## Required AWS IAM permissions

The IAM identity (or assumed role) needs these permissions:

| Permission | Purpose |
|---|---|
| `states:ListStateMachines` | Discover state machines at prepare time |
| `states:StartExecution` | Trigger an execution during materialization |
| `states:DescribeExecution` | Poll execution status when `wait_for_completion: true` |

For cross-account access via `role_arn`, the calling identity also needs `sts:AssumeRole` on that role.

## Fields

| Field | Required | Default | Description |
|---|---|---|---|
| `region_name` | Yes | — | AWS region where state machines live (e.g. `us-east-1`) |
| `aws_profile` | No | `null` | AWS named profile; uses default credential chain if omitted |
| `role_arn` | No | `null` | IAM Role ARN to assume for cross-account access |
| `name_prefix` | No | `null` | Only include state machines whose names start with this string |
| `exclude_names` | No | `null` | List of state machine names to exclude from asset generation |
| `group_name` | No | `step_functions` | Dagster asset group name for all generated assets |
| `key_prefix` | No | `null` | Asset key prefix prepended to every generated asset key |
| `wait_for_completion` | No | `true` | Poll until execution reaches SUCCEEDED/FAILED/TIMED_OUT/ABORTED |
| `poll_interval_seconds` | No | `10` | Seconds between `describe_execution` polls |
| `execution_timeout_seconds` | No | `3600` | Max seconds to wait before raising a timeout error |

## Example YAML

### Basic — all state machines in a region

```yaml
type: dagster_component_templates.StepFunctionsAssetComponent
attributes:
  region_name: us-east-1
  group_name: step_functions
  wait_for_completion: true
```

### Filtered — only state machines with a shared prefix

```yaml
type: dagster_component_templates.StepFunctionsAssetComponent
attributes:
  region_name: us-east-1
  name_prefix: "data-pipeline-"
  exclude_names:
    - data-pipeline-deprecated
  group_name: step_functions
  wait_for_completion: true
  poll_interval_seconds: 15
```

### Cross-account role assumption

```yaml
type: dagster_component_templates.StepFunctionsAssetComponent
attributes:
  region_name: eu-west-1
  role_arn: "arn:aws:iam::123456789012:role/DagsterStepFunctionsCrossAccount"
  name_prefix: "prod-"
  group_name: step_functions_prod
  wait_for_completion: true
  execution_timeout_seconds: 7200
```

### Fire-and-forget (no polling)

```yaml
type: dagster_component_templates.StepFunctionsAssetComponent
attributes:
  region_name: us-west-2
  group_name: step_functions
  wait_for_completion: false
```

## Refreshing state

To force re-discovery of state machines (e.g. after adding new pipelines):

```bash
dg utils refresh-defs-state
```

Or simply restart `dagster dev` — state refresh runs automatically on startup.

## Cross-account role assumption

Set `role_arn` to the ARN of a role in the target account. The component calls `sts:AssumeRole` and uses the resulting temporary credentials to build the Step Functions client. The calling identity needs `sts:AssumeRole` permission, and the target role's trust policy must allow the calling account or role.
