# `AwsBatchWorkspaceComponent`

Discover AWS Batch job definitions via `DescribeJobDefinitions` and materialize one Dagster asset per job definition by submitting a real AWS Batch job (`SubmitJob`) through **Dagster Pipes**, polling `DescribeJobs` to completion. No component in this registry wrapped AWS Batch before this one, and `dagster_aws.pipes` (confirmed: dagster-aws 0.29.x) ships no `PipesBatchClient` -- `PipesBatchClient` here is this component's own, modeled closely on the real, installed `dagster_aws.pipes.PipesECSClient`.

The closest existing component, `ecs_pipes_asset`, doesn't fit: it calls ECS's `RunTask`/`DescribeTasks` against a cluster + task definition, is one asset per component instance, and has no enumeration or observation sensor. This component is workspace-shaped instead (same convention as `ssis_workspace`/`snowflake_workspace`/`aws_glue`): `StateBackedComponent` enumeration, per-entry `assets_by_job_definition` overrides, a `translation:` callable, a `@public get_asset_spec(props)` hook, and an opt-in observation sensor.

## What's different from ECS, and how it was confirmed

| | ECS (`PipesECSClient`) | AWS Batch (`PipesBatchClient`, this component) |
|---|---|---|
| Launch | `RunTask` against a cluster + task definition | `SubmitJob` against a job queue + job definition -- Batch itself owns placement, Spot capacity, and retries |
| Poll | `DescribeTasks`, via ECS's `tasks_stopped` **waiter** | `DescribeJobs`, polled manually -- Batch's boto3 client ships **no completion waiter** (confirmed: `boto3.client("batch").waiter_names == []`) |
| Interrupt | `StopTask` | `TerminateJob` |
| Bootstrap env | `containerOverrides.environment` (`[{name, value}]`) | Same shape -- confirmed against the installed botocore Batch service model |
| Tags | `[{key, value}]` list, no native tagging in some calls | `tags` is a plain `{str: str}` map (confirmed via the botocore service model: `tags: map (TagrisTagsMap)`) -- `dagster/run-id` merges straight in |
| Log group | Constructed from the task definition's `awslogs-stream-prefix` + container name + task id | `DescribeJobs`'s response already includes `container.logStreamName` and `container.logConfiguration.options` directly -- no reconstruction needed |
| Default log group | N/A | `aws/batch/job` (no leading slash) -- confirmed via the AWS Batch user guide's "Use the awslogs log driver" page, not assumed |

## User-code contract

Same Pipes contract as every other component in this registry:

```python
# batch_task/run_batch.py
from dagster_pipes import open_dagster_pipes

with open_dagster_pipes() as pipes:
    pipes.log.info("running batch job")
    pipes.report_asset_materialization(metadata={"rows_processed": 1842})
```

## Demo mode

`demo_mode: true` (the default) does **not** swap `PipesBatchClient` for a different transport, and does not abstractly fake Batch API responses. It builds the exact same `PipesBatchClient`/`PipesCloudWatchMessageReader` classes used in production, injecting a pair of internal fake boto3 `batch`/`logs` clients instead of real ones -- the same convention `ecs_pipes_asset`'s own demo mode uses:

- The fake `submit_job` call launches `local_script_path` as a real local subprocess, with the container-override environment Batch would have injected.
- The fake `logs` client serves the subprocess's growing stdout as paginated CloudWatch log events, so Pipes messages and live logs stream back exactly as they would from a real Batch job.
- Enumeration (`write_state_to_path`) synthesizes a single job definition named after `local_script_path`'s filename, since there's no real AWS Batch account to enumerate -- enough to exercise the whole workspace shape (`assets_by_job_definition` overrides, partitions, the sensor) with no real AWS account available.

Only the actual AWS API calls are faked -- `PipesBatchClient`'s own request-building, polling, and message-reading code runs unmodified in both modes. Set `demo_mode: false` and configure `job_queue`/credentials for a real account to run against real AWS Batch.

## Per-job-definition overrides

```yaml
assets_by_job_definition:
  my-etl-job:
    key: raw/orders
    owners: ["team:data-engineering"]
    job_queue: priority-queue          # overrides the default job_queue for this job definition only
    container_environment:
      MODE: incremental                 # merged over the component-level container_environment template
```

## Observation sensor

`generate_sensor: true` adds a sensor polling `ListJobs` per `job_queues_to_poll` (default: `[job_queue]`), emitting an `AssetObservation` for every `SUCCEEDED` job that does **not** carry a `dagster/run-id` tag -- i.e. a job someone (or something) submitted outside Dagster. Jobs Dagster itself submits are tagged with `dagster/run-id` by `PipesBatchClient.run` (merged from `session.default_remote_invocation_info`, the same mechanism every other Pipes client in this registry uses for its own run-correlation tags), so the sensor never double-reports them. The sensor is a no-op (and says so via its skip reason) when `demo_mode: true` -- there's no real job queue to poll.

Observations resolve to the SAME asset key the executable assets use (via `get_asset_spec`), not an independently reconstructed one -- the same bug class found and fixed in `aws_glue`/`ssis_workspace`/`snowflake_workspace`/`azure_data_factory` elsewhere in this registry, avoided here from the start.

## Auth

Uses the default boto3 credential chain (IAM role, `~/.aws/credentials`, env vars) by default. Set `aws_access_key_id` + `aws_secret_access_key` (+ optional `aws_session_token`) to override.

## Required IAM permissions

```json
{
  "Effect": "Allow",
  "Action": [
    "batch:DescribeJobDefinitions", "batch:SubmitJob", "batch:DescribeJobs",
    "batch:TerminateJob", "batch:ListJobs",
    "logs:GetLogEvents", "logs:DescribeLogStreams"
  ],
  "Resource": "*"
}
```

[//]: # (FIELDS:START - auto-generated by tools/regen_readme_fields.py)

## Fields

### Required

| Field | Type | Description |
|---|---|---|
| `region_name` | `str` | AWS region for the batch/logs clients. |
| `job_queue` | `str` | Default AWS Batch job queue jobs submit to. Per-entry override via assets_by_job_definition.<name>.job_queue. |

### Execution

| Field | Type | Default | Description |
|---|---|---|---|
| `timeout_seconds` | `int` | — | timeout.attemptDurationSeconds. |
| `poll_interval_seconds` | `float` | `6.0` | Seconds between DescribeJobs polls (Batch's boto3 client ships no completion waiter). |

### Catalog metadata

| Field | Type | Default | Description |
|---|---|---|---|
| `group_name` | `str` | `"aws_batch"` | Dagster asset group name. |
| `key_prefix` | `Union[str, List[str]]` | — | Prefix for every emitted asset key. Default: ['aws_batch']. |
| `kinds` | `List[str]` | — | Asset kinds. Default always includes ['batch', 'aws', 'pipes']. |
| `tags` | `Union[Dict[str, Any], List[str]]` | — | Additional asset tags. |
| `owners` | `List[str]` | — | Asset owners. |
| `backfill_policy` | `str` | — | 'single_run' or 'multi_run'. Only honored when the asset is actually partitioned. |

### Partitions

| Field | Type | Default | Description |
|---|---|---|---|
| `partition_type` | `str` | — | 'daily'\|'weekly'\|'monthly'\|'hourly'\|'cron'\|'static'\|'dynamic', applied to every job definition's asset. |
| `partition_start` | `str` | — | — |
| `partition_values` | `Any` | — | — |
| `partition_cron_schedule` | `str` | — | — |
| `partition_cron_fmt` | `str` | `"%Y-%m-%d"` | — |
| `partition_end_offset` | `int` | `0` | — |
| `partition_timezone` | `str` | — | — |

### Retry policy

| Field | Type | Default | Description |
|---|---|---|---|
| `retry_attempts` | `int` | — | retryStrategy.attempts -- number of times Batch itself retries a failed job. |

### Source / target

| Field | Type | Default | Description |
|---|---|---|---|
| `filter_by_name_pattern` | `str` | — | Regex -- only include job definitions whose name matches. |

### Other

| Field | Type | Default | Description |
|---|---|---|---|
| `aws_access_key_id` | `str` | — | — |
| `aws_secret_access_key` | `str` | — | — |
| `aws_session_token` | `str` | — | — |
| `job_definition_name_prefix` | `str` | — | Only include job definitions whose name starts with this prefix. |
| `exclude_job_definitions` | `List[str]` | — | Explicit list of job definition names to skip. |
| `exclude_name_pattern` | `str` | — | Regex -- exclude job definitions whose name matches. |
| `status_filter` | `str` | `"ACTIVE"` | DescribeJobDefinitions status filter (e.g. 'ACTIVE'). |
| `container_command` | `List[str]` | — | Command override for the container. |
| `container_environment` | `Dict[str, str]` | — | Environment variable overrides for the container, as a plain {name: value} dict -- converted internally to Batch's [{name, value}] list shape. |
| `vcpus_override` | `float` | — | containerOverrides.vcpus. |
| `memory_override` | `int` | — | containerOverrides.memory (MiB). |
| `submit_job_params_overrides` | `Dict[str, Any]` | — | Raw overrides deep-merged on top of the submit_job params this component builds, for anything not covered by an explicit field above. |
| `log_group` | `str` | — | Fallback CloudWatch log group when a job definition's own logConfiguration doesn't specify one. Batch's real default is 'aws/batch/job' (used when this is unset). |
| `extras` | `Dict[str, Any]` | — | Arbitrary extra data passed to PipesBatchClient.run(extras=...). |
| `assets_by_job_definition` | `Dict[str, Dict[str, Any]]` | — | Override the asset spec (and job_queue/arguments) for specific job definitions, keyed by job definition name. Mirrors aws_glue's assets_by_job_name / the official dagster-databricks assets_by_task_key shape. Per-entry ke… _(full docs in schema.json + component README)_ |
| `translation` | `TranslationFn[BatchJobDefinitionProps]` | — | Optional per-asset translation callable. Receives a BatchJobDefinitionProps and returns a dict of AssetSpec kwargs (key / group_name / tags / owners / kinds / metadata / description). Follows the same convention as ssis_… _(full docs in schema.json + component README)_ |
| `demo_mode` | `bool` | `true` | Run jobs as a real local subprocess against fake Batch/CloudWatch clients instead of a real Batch job queue. The real PipesBatchClient/PipesCloudWatchMessageReader code runs unmodified in both modes -- only the boto3 API… _(full docs in schema.json + component README)_ |
| `local_script_path` | `str` | — | Script demo mode launches locally (via sys.executable) in place of a real Batch job. Required when demo_mode=true. |
| `generate_sensor` | `bool` | `false` | Opt-in sensor polling ListJobs for jobs Dagster did not itself submit (no dagster/run-id tag). |
| `observation_interval_seconds` | `int` | `300` | Sensor tick interval. Only used when generate_sensor=true. |
| `job_queues_to_poll` | `List[str]` | — | Job queues the observation sensor polls via ListJobs. Default: [job_queue]. |
| `dynamic_partition_name` | `str` | — | — |
| `backfill_policy_max_partitions_per_run` | `int` | — | — |
| `defs_state` | `ResolvedDefsStateConfig` | `DefsStateConfigArgs.local_filesystem()` | — |

[//]: # (FIELDS:END)

## Example

```yaml
type: dagster_community_components.AwsBatchWorkspaceComponent
attributes:
  region_name: us-east-1
  job_queue: my-job-queue
  log_group: /batch/my-jobs
  group_name: batch_workspace
  generate_sensor: true
  observation_interval_seconds: 300
  demo_mode: true
  local_script_path: batch_task/run_batch.py
```

## Composes with

- `ecs_pipes_asset` -- same Pipes-based observability pattern for a single ECS task, not a job-definition workspace.
- `aws_glue` -- the sibling workspace component for AWS Glue jobs/crawlers/workflows.

## What doesn't work in v1

- **Multi-node parallel jobs** -- `PipesBatchClient` targets single-container Batch jobs only; multi-node parallel jobs (`nodeProperties`) aren't wired in.
- **EKS/Fargate-specific container properties** -- `containerOverrides` covers the common EC2/Fargate single-container shape; `ecsPropertiesOverride`/`eksPropertiesOverride` aren't exposed as dedicated fields (use `submit_job_params_overrides`).
- **Per-partition checks on a `single_run` backfill range** -- same limitation as `ecs_pipes_asset`: a backfill range folds all partitions' metadata into one `MaterializeResult`.
