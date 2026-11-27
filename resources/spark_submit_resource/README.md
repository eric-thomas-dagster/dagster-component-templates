# Spark Submit Resource

Register a `SparkSubmitResource` for use by any op or asset you write -- the
Airflow `SparkSubmitOperator` equivalent exposed as an injectable resource
method, `submit(...)`, rather than baked into a single pre-built asset
component. Requires a `spark-submit` binary on PATH (or
`spark_submit_binary` pointed at one).

Builds the real `spark-submit` command line (`--master`, `--deploy-mode`,
`--class`, `--conf key=value` per entry, `--packages`/`--jars`/`--py-files`
as comma-joined lists, then the application path and its args), streams
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
`total_executor_cores`, `queue`, `packages`, `jars`, `py_files`, `conf`,
`extra_args`, `application_args`, `env_vars`, `timeout_seconds`,
`success_exit_codes`, `log` (optional object with `.info(msg)`, e.g.
`context.log`).

Returns `{"exit_code", "command", "stdout_preview"}`.
