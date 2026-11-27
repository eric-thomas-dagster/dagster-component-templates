# EMR Resource

Register an `EmrResource` for use by any op or asset you write -- the Airflow
`EmrAddStepsOperator` + `EmrStepSensor` equivalent exposed as an injectable
resource method, `submit_step_and_wait(...)`, rather than baked into a single
pre-built asset component.

Real boto3 EMR API: `add_job_flow_steps`, `describe_step` polling to a
terminal state, and `list_clusters` to resolve `cluster_name` -> cluster id
when `cluster_id` isn't given directly. `jar` defaults to EMR's bundled
`command-runner.jar`, the standard way to run an arbitrary shell command
(such as `spark-submit`) as a step without a custom jar -- `args` then starts
with the command itself.

## Usage

```python
import dagster as dg
from dagster_community_components import EmrResource

@dg.asset
def my_asset(emr: EmrResource):
    result = emr.submit_step_and_wait(
        cluster_name="analytics-cluster",
        args=["spark-submit", "--deploy-mode", "cluster", "s3://bucket/job.py"],
    )
    return result["state"]
```

## Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `resource_key` | `str` | `emr_resource` | Key used to register this resource. Other components reference it via resource_key. |
| `region_name` | `Optional[str]` | — | AWS region, e.g. 'us-east-1'. |
| `profile_name` | `Optional[str]` | — | Named AWS CLI/SDK profile. Omit to use the default boto3 credential chain (env vars, instance role, etc.). |
| `aws_access_key_id_env_var` | `Optional[str]` | — | Env var holding the AWS access key ID. |
| `aws_secret_access_key_env_var` | `Optional[str]` | — | Env var holding the AWS secret access key. |
| `aws_session_token_env_var` | `Optional[str]` | — | Env var holding a temporary AWS session token, paired with the access key/secret above. |

## `EmrResource.submit_step_and_wait(...)` arguments

`cluster_id` / `cluster_name` (one required), `step_name`, `action_on_failure`
(`TERMINATE_JOB_FLOW`|`TERMINATE_CLUSTER`|`CANCEL_AND_WAIT`|`CONTINUE`), `jar`,
`main_class`, `args`, `properties`, `wait_for_completion` (default `True`),
`poll_interval_seconds`, `timeout_seconds`, `fail_on_step_failure` (default
`True`), `log` (optional object with `.info(msg)`, e.g. `context.log`).

Returns `{"cluster_id", "step_id"}` immediately when
`wait_for_completion=False`, or `{"cluster_id", "step_id", "state"}` once
terminal. Raises `RuntimeError` on a `CANCELLED`/`FAILED`/`INTERRUPTED` step
unless `fail_on_step_failure=False`.
