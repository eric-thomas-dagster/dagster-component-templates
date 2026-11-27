# Spark Submit Resource

Register a `SparkSubmitResource` for use by any op or asset you write -- the
Airflow `SparkSubmitOperator` equivalent exposed as an injectable resource
method, `submit(...)`, rather than baked into a single pre-built asset
component. Requires a `spark-submit` binary on PATH (or
`spark_submit_binary` pointed at one).

Builds the real `spark-submit` command line (`--master`, `--deploy-mode`,
`--class`, `--conf key=value` per entry, `--packages`/`--exclude-packages`/
`--repositories`/`--jars`/`--py-files`/`--files`/`--archives` as comma-joined
lists, `--driver-class-path`, `--executor-cores`, `--verbose`,
`--properties-file`, then the application path and its args), streams
stdout/stderr through the parent process, and raises `dg.Failure` on a
non-zero exit code (unless it's in `success_exit_codes`) or a timeout.

## Usage

```python
import dagster as dg
from dagster_community_components import SparkSubmitResource

@dg.asset
def my_asset(spark: SparkSubmitResource):
    result = spark.submit(
        application="s3://bucket/job.py",
        master="yarn",
        deploy_mode="cluster",
    )
    return result["exit_code"]
```

## Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `resource_key` | `str` | `spark_submit_resource` | Key used to register this resource. Other components reference it via resource_key. |
| `spark_submit_binary` | `str` | `spark-submit` | Path to the spark-submit binary. Defaults to 'spark-submit' on PATH. |

## `SparkSubmitResource.submit(...)` arguments

`application` (required), `master`, `deploy_mode` (`client`|`cluster`),
`class_name`, `name`, `driver_memory`, `executor_memory`, `num_executors`,
`total_executor_cores`, `executor_cores`, `queue`, `packages`,
`exclude_packages`, `repositories`, `jars`, `py_files`, `files`, `archives`,
`driver_class_path`, `properties_file`, `verbose`, `conf`,
`extra_args`, `application_args`, `env_vars`, `timeout_seconds`,
`success_exit_codes`, `log` (optional object with `.info(msg)`, e.g.
`context.log`).

Kerberos/impersonation (real `spark-submit` flags, confirmed via
`spark-submit --help`): `keytab`, `principal` (mutually exclusive with
`proxy_user` -- spark-submit itself rejects the combination), `proxy_user`.
Standalone cluster-mode driver supervision: `supervise` (`--supervise`,
restarts the driver on failure).

Cluster-mode completion: `wait_for_cluster_completion` (only meaningful with
`deploy_mode="cluster"` -- standalone cluster-mode submission returns as soon
as the driver is accepted, with the real job still running on the cluster;
this parses the `driver-...` submission id from spark-submit's stdout and
polls the real, separate `spark-submit --status SUBMISSION_ID --master
MASTER` command at `status_poll_interval_seconds` until the driver reaches a
terminal state, raising `dg.Failure`/`TimeoutError` as appropriate), along
with `status_poll_interval_seconds` and `status_poll_timeout_seconds`. A
no-op for client-mode/YARN/Kubernetes submissions, which already block until
completion.

Returns `{"exit_code", "command", "stdout_preview"}`, plus `"submission_id"`/
`"driver_state"` when `wait_for_cluster_completion=True` was honored.
