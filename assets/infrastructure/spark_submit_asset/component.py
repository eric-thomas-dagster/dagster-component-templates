"""SparkSubmitAssetComponent.

Shells out to the real `spark-submit` CLI binary and runs it as a Dagster
asset -- the Airflow `SparkSubmitOperator` equivalent, for a Spark cluster
reached by a local `spark-submit` install (standalone master URL, YARN,
Kubernetes master URL, or `local[*]`), as distinct from `spark_livy_batch_asset`
(submits over Livy's REST API) and `spark_k8s_operator_asset` (submits via
the Kubernetes Spark Operator CRD, no `spark-submit` binary involved).

Builds the real `spark-submit` command line (`--master`, `--deploy-mode`,
`--class`, `--conf key=value` per entry, `--packages`/`--jars`/`--py-files`
as comma-joined lists, then the application path and its args), matching
`spark-submit --help`'s documented flags -- not guessed. Streams stdout/
stderr through the parent process (so the Dagster ComputeLogManager
captures them, same convention as `shell_command_asset`) and raises
`dg.Failure` with the captured stderr on a non-zero exit code.
"""
from typing import Any, Dict, List, Optional, Union

import dagster as dg
from pydantic import Field, model_validator

# BEGIN field-shape normalization (inlined per component; keep copies identical — see tests/test_field_shapes.py)
from typing import Any, Dict, List, Optional, Union  # noqa: E402,F401
from pydantic import model_validator  # noqa: E402,F401


def coerce_field_shapes(data, *, lists=(), csv=(), tags=()):
    """Accept the shapes people (and LLMs) naturally write for common boilerplate fields."""
    if not isinstance(data, dict):
        return data
    out = dict(data)
    for k in lists:  # List[str]: "a" / "a, b" -> ["a", "b"]; strip + drop empties
        v = out.get(k)
        if isinstance(v, str):
            v = v.split(",")
        if isinstance(v, (list, tuple)):
            out[k] = [x.strip() if isinstance(x, str) else x for x in v if not (isinstance(x, str) and not x.strip())]
    for k in csv:  # comma-separated str given as a list -> "a,b"
        v = out.get(k)
        if isinstance(v, (list, tuple, set)):
            out[k] = ",".join(str(x).strip() for x in v)
    for k in tags:  # Dict[str, str]: scalars -> str, ["k=v"] / ["k:v"] -> {k: v}
        v = out.get(k)
        if isinstance(v, (list, tuple)) and v and all(isinstance(x, str) and ("=" in x or ":" in x) for x in v):
            v = dict((p.strip() for p in (x.split("=", 1) if "=" in x else x.split(":", 1))) for x in v)
        if isinstance(v, dict):
            out[k] = {str(a).strip(): (str(b).lower() if isinstance(b, bool) else str(b)) for a, b in v.items() if b is not None}
    return out
# END field-shape normalization


# BEGIN freshness-policy compat (inlined per component; keep copies identical, see tests/test_freshness_policy_compat.py)
def _freshness_policy_compat(max_lag_minutes, cron_schedule=None):
    """Build a FreshnessPolicy on the current Dagster API (time_window / cron) and on the older constructor."""
    from datetime import timedelta

    from dagster import FreshnessPolicy

    lag = timedelta(minutes=max_lag_minutes)
    if hasattr(FreshnessPolicy, "time_window"):
        if cron_schedule:
            return FreshnessPolicy.cron(deadline_cron=cron_schedule, lower_bound_delta=lag)
        return FreshnessPolicy.time_window(fail_window=lag)
    return FreshnessPolicy(maximum_lag_minutes=max_lag_minutes, cron_schedule=cron_schedule)
# END freshness-policy compat


def _build_partitions_def(
    partition_type,
    partition_start,
    partition_values,
    dynamic_partition_name,
    partition_dimensions,
    cron_schedule=None,
    cron_fmt="%Y-%m-%d",
    end_offset=0,
    timezone=None,
):
    """Construct a Dagster partitions_def from the canonical partition fields.

    Strict: raises ValueError on misconfigured combinations rather than
    silently picking a default. Specifically:
      - time-based partition_type without partition_start
      - partition_type=multi without partition_values
      - partition_type=dynamic without dynamic_partition_name
      - both partition_dimensions AND flat fields set (ambiguous intent)
    """
    from dagster import (
        DailyPartitionsDefinition, WeeklyPartitionsDefinition,
        MonthlyPartitionsDefinition, HourlyPartitionsDefinition,
        StaticPartitionsDefinition, MultiPartitionsDefinition,
        DynamicPartitionsDefinition,
    )

    # Both shapes set: ambiguous. Pick one.
    if partition_dimensions and partition_type:
        raise ValueError(
            "Set either partition_type (flat-fields shape) or "
            "partition_dimensions (multi-axis shape), not both."
        )

    def _build_axis(spec):
        t = spec.get("type")
        _end_offset = spec.get("end_offset", 0)
        _timezone = spec.get("timezone")
        if t in ("daily", "weekly", "monthly", "hourly") and not spec.get("start"):
            raise ValueError(f"partition dimension type={t!r} requires 'start' (ISO date)")
        if t == "daily":
            return DailyPartitionsDefinition(start_date=spec["start"], end_offset=_end_offset, timezone=_timezone)
        if t == "weekly":
            return WeeklyPartitionsDefinition(start_date=spec["start"], end_offset=_end_offset, timezone=_timezone)
        if t == "monthly":
            return MonthlyPartitionsDefinition(start_date=spec["start"], end_offset=_end_offset, timezone=_timezone)
        if t == "hourly":
            return HourlyPartitionsDefinition(start_date=spec["start"], end_offset=_end_offset, timezone=_timezone)
        if t == "cron":
            from dagster import TimeWindowPartitionsDefinition
            if not spec.get("start"):
                raise ValueError(f"partition dimension type={t!r} requires 'start' (ISO date)")
            if not spec.get("cron_schedule"):
                raise ValueError("partition dimension type='cron' requires 'cron_schedule'")
            return TimeWindowPartitionsDefinition(start=spec["start"], fmt=spec.get("fmt") or "%Y-%m-%d", cron_schedule=spec["cron_schedule"], end_offset=_end_offset, timezone=_timezone)
        if t == "static":
            vals = spec.get("values") or []
            if isinstance(vals, str):
                vals = [v.strip() for v in vals.split(",") if v.strip()]
            if not vals:
                raise ValueError("partition dimension type='static' requires non-empty 'values'")
            return StaticPartitionsDefinition(list(vals))
        if t == "dynamic":
            name = spec.get("dynamic_partition_name") or spec.get("name")
            if not name:
                raise ValueError("partition dimension type='dynamic' requires a name")
            return DynamicPartitionsDefinition(name=name)
        raise ValueError(f"unknown partition type: {t!r}")

    if partition_dimensions:
        if len(partition_dimensions) == 1:
            return _build_axis(partition_dimensions[0])
        axes = {d["name"]: _build_axis(d) for d in partition_dimensions}
        return MultiPartitionsDefinition(axes)

    if not partition_type:
        return None
    if isinstance(partition_values, (list, tuple)):
        _values = [str(v).strip() for v in partition_values if str(v).strip()]
    else:
        _values = [v.strip() for v in (str(partition_values) if partition_values else "").split(",") if v.strip()]
    if partition_type in ("daily", "weekly", "monthly", "hourly", "cron") and not partition_start:
        raise ValueError(
            f"partition_type={partition_type!r} requires partition_start (ISO date, e.g. '2024-01-01')."
        )
    if partition_type == "daily":
        return DailyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone)
    if partition_type == "weekly":
        return WeeklyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone)
    if partition_type == "monthly":
        return MonthlyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone)
    if partition_type == "hourly":
        return HourlyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone)
    if partition_type == "cron":
        from dagster import TimeWindowPartitionsDefinition
        if not cron_schedule:
            raise ValueError(
                "partition_type='cron' requires cron_schedule, e.g. '0 0 * * 1-5' for weekdays only."
            )
        return TimeWindowPartitionsDefinition(start=partition_start, fmt=cron_fmt, cron_schedule=cron_schedule, end_offset=end_offset, timezone=timezone)
    if partition_type == "static":
        if not _values:
            raise ValueError("partition_type='static' requires partition_values (comma-separated).")
        return StaticPartitionsDefinition(_values)
    if partition_type == "dynamic":
        if not dynamic_partition_name:
            raise ValueError(
                "partition_type='dynamic' requires dynamic_partition_name."
            )
        return DynamicPartitionsDefinition(name=dynamic_partition_name)
    if partition_type == "multi":
        if not _values:
            raise ValueError("partition_type='multi' requires partition_values (comma-separated).")
        if not partition_start:
            raise ValueError("partition_type='multi' requires partition_start (the date axis start).")
        return MultiPartitionsDefinition({
            "date": DailyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone),
            "static_dim": StaticPartitionsDefinition(_values),
        })
    raise ValueError(f"unknown partition_type: {partition_type!r}")


def _build_spark_submit_command(
    spark_submit_binary: str, application: str, master: Optional[str], deploy_mode: Optional[str],
    class_name: Optional[str], name: Optional[str], driver_memory: Optional[str], executor_memory: Optional[str],
    num_executors: Optional[int], total_executor_cores: Optional[int], queue: Optional[str],
    packages: Optional[List[str]], jars: Optional[List[str]], py_files: Optional[List[str]],
    conf: Optional[Dict[str, str]], extra_args: Optional[List[str]], application_args: Optional[List[str]],
) -> List[str]:
    cmd: List[str] = [spark_submit_binary]
    if master:
        cmd += ["--master", master]
    if deploy_mode:
        cmd += ["--deploy-mode", deploy_mode]
    if class_name:
        cmd += ["--class", class_name]
    if name:
        cmd += ["--name", name]
    if driver_memory:
        cmd += ["--driver-memory", driver_memory]
    if executor_memory:
        cmd += ["--executor-memory", executor_memory]
    if num_executors is not None:
        cmd += ["--num-executors", str(num_executors)]
    if total_executor_cores is not None:
        cmd += ["--total-executor-cores", str(total_executor_cores)]
    if queue:
        cmd += ["--queue", queue]
    if packages:
        cmd += ["--packages", ",".join(packages)]
    if jars:
        cmd += ["--jars", ",".join(jars)]
    if py_files:
        cmd += ["--py-files", ",".join(py_files)]
    for k, v in (conf or {}).items():
        cmd += ["--conf", f"{k}={v}"]
    cmd += list(extra_args or [])
    cmd.append(application)
    cmd += list(application_args or [])
    return cmd


class SparkSubmitAssetComponent(dg.Component, dg.Model, dg.Resolvable):
    """Run `spark-submit` as a Dagster asset -- the Airflow `SparkSubmitOperator` equivalent.

    Requires a `spark-submit` binary on PATH (or `spark_submit_binary` pointed
    at one). Use `spark_livy_batch_asset` instead when the cluster is fronted
    by Apache Livy's REST API, or `spark_k8s_operator_asset` for the
    Kubernetes Spark Operator CRD.
    """

    asset_name: str = Field(description="Dagster asset key.")
    application: str = Field(description="Path to the application file (.jar or .py), e.g. 's3://bucket/job.py' or 'local:///opt/spark-apps/job.jar'.")
    master: Optional[str] = Field(default=None, description="Spark master URL, e.g. 'yarn', 'spark://host:7077', 'k8s://https://host:443', or 'local[*]'. Omit to use spark-submit's own default/config.")
    deploy_mode: Optional[str] = Field(default=None, description="'client' or 'cluster'.")
    class_name: Optional[str] = Field(default=None, description="Application Java/Scala main class. Required for a JVM (jar) job; omit for a PySpark file.")
    spark_submit_binary: str = Field(default="spark-submit", description="Path to the spark-submit binary. Defaults to 'spark-submit' on PATH.")

    name: Optional[str] = Field(default=None, description="Spark application name (--name). Defaults to asset_name if unset.")
    driver_memory: Optional[str] = Field(default=None, description="Driver process memory, e.g. '2g'.")
    executor_memory: Optional[str] = Field(default=None, description="Executor process memory, e.g. '4g'.")
    num_executors: Optional[int] = Field(default=None, description="Number of executors to launch.")
    total_executor_cores: Optional[int] = Field(default=None, description="Total cores across all executors (standalone/Mesos mode).")
    queue: Optional[str] = Field(default=None, description="YARN queue to submit to.")
    packages: Optional[List[str]] = Field(default=None, description="Maven coordinates of packages to include, e.g. ['org.apache.spark:spark-avro_2.12:3.5.0'].")
    jars: Optional[List[str]] = Field(default=None, description="Extra jars to include on the driver/executor classpath.")
    py_files: Optional[List[str]] = Field(default=None, description="Extra Python files (.py/.zip/.egg) to place on PYTHONPATH.")
    conf: Optional[Dict[str, str]] = Field(default=None, description="Extra Spark configuration, passed as one --conf key=value per entry, e.g. {'spark.executor.memoryOverhead': '512m'}.")
    extra_args: Optional[List[str]] = Field(default=None, description="Raw extra spark-submit flags inserted verbatim before the application path, for anything not covered by a dedicated field.")
    application_args: Optional[List[str]] = Field(default=None, description="Command line arguments passed to the application itself (after the application path).")
    env_vars: Optional[Dict[str, str]] = Field(default=None, description="Extra environment variables for the spark-submit subprocess, merged over the current environment.")

    timeout_seconds: Optional[int] = Field(default=None, ge=1, description="Kill the process if it runs longer than this. Omit for no timeout.")
    success_exit_codes: Optional[List[int]] = Field(default=None, description="Exit codes other than 0 that should still count as success.")

    group_name: Optional[str] = Field(default=None, description="Dagster asset group name.")
    description: Optional[str] = Field(default=None, description="Asset description.")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners.")
    asset_tags: Optional[Union[Dict[str, Any], List[str]]] = Field(default=None, description="Extra asset tags.")
    kinds: Optional[List[str]] = Field(default=None, description="Defaults to ['spark'].")
    deps: Optional[List[str]] = Field(default=None, description="Lineage-only upstream asset keys.")

    partition_type: Optional[str] = Field(default=None, description="'daily' | 'weekly' | 'monthly' | 'hourly' | 'static' | 'dynamic' | 'cron' | None.")
    partition_start: Optional[str] = Field(default=None)
    partition_values: Optional[Union[str, List[str]]] = Field(default=None)
    dynamic_partition_name: Optional[str] = Field(default=None)
    partition_dimensions: Optional[List[Dict[str, Any]]] = Field(default=None)
    partition_cron_schedule: Optional[str] = Field(
        default=None,
        description="Required when partition_type='cron'. A cron expression restricting the date axis to a custom cadence, e.g. '0 0 * * 1-5' for weekdays only.",
    )
    partition_cron_fmt: Optional[str] = Field(default="%Y-%m-%d", description="strftime format for partition keys produced by partition_type='cron'.")
    partition_end_offset: int = Field(
        default=0,
        description=(
            "Passed through to the underlying Daily/Weekly/Monthly/Hourly/cron "
            "partitions definition. 0 (the Dagster default) means a time-based "
            "partition set only ever holds COMPLETED windows -- e.g. a daily set "
            "never includes today until tomorrow. Set to 1 to include the "
            "current, still-in-progress window."
        ),
    )
    partition_timezone: Optional[str] = Field(
        default=None,
        description="IANA timezone for the underlying Daily/Weekly/Monthly/Hourly/cron partitions definition (defaults to UTC when unset, matching Dagster's own default).",
    )

    freshness_max_lag_minutes: Optional[int] = Field(default=None)
    freshness_cron: Optional[str] = Field(default=None)

    retry_policy_max_retries: Optional[int] = Field(default=None)
    retry_policy_delay_seconds: Optional[int] = Field(default=None)
    retry_policy_backoff: str = Field(default="exponential")

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(
            data,
            lists=("owners", "kinds", "deps", "packages", "jars", "py_files", "extra_args", "application_args", "success_exit_codes"),
            csv=("partition_values",), tags=("asset_tags",),
        )

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label='Spark Submit Asset', editable=True)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        if self.deploy_mode is not None and self.deploy_mode not in ("client", "cluster"):
            raise ValueError(f"deploy_mode must be 'client' or 'cluster', got {self.deploy_mode!r}")

        _self = self
        partitions_def = _build_partitions_def(
            self.partition_type, self.partition_start, self.partition_values,
            self.dynamic_partition_name, self.partition_dimensions,
            cron_schedule=self.partition_cron_schedule,
            cron_fmt=self.partition_cron_fmt or "%Y-%m-%d", end_offset=self.partition_end_offset, timezone=self.partition_timezone,
        )

        _inferred_kinds = list(self.kinds or ["spark"])
        _all_tags = dict(self.asset_tags or {})
        for _kind in _inferred_kinds:
            _all_tags[f"dagster/kind/{_kind}"] = ""

        _freshness_policy = None
        if self.freshness_max_lag_minutes is not None:
            _freshness_policy = _freshness_policy_compat(int(self.freshness_max_lag_minutes), self.freshness_cron)

        _retry_policy = None
        if self.retry_policy_max_retries is not None:
            from dagster import Backoff, RetryPolicy
            _retry_policy = RetryPolicy(
                max_retries=self.retry_policy_max_retries,
                delay=self.retry_policy_delay_seconds or 1,
                backoff=Backoff[self.retry_policy_backoff.upper()],
            )

        @dg.asset(
            name=self.asset_name,
            group_name=self.group_name,
            description=self.description or f"Run `spark-submit` for {self.application!r}.",
            owners=self.owners or [],
            tags=_all_tags,
            kinds=set(_inferred_kinds),
            deps=[dg.AssetKey.from_user_string(d) for d in (self.deps or [])],
            partitions_def=partitions_def,
            freshness_policy=_freshness_policy,
            retry_policy=_retry_policy,
        )
        def _spark_submit_asset(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
            import os
            import subprocess
            import sys
            import threading

            self = _self
            cmd = _build_spark_submit_command(
                self.spark_submit_binary, self.application, self.master, self.deploy_mode,
                self.class_name, self.name or self.asset_name, self.driver_memory, self.executor_memory,
                self.num_executors, self.total_executor_cores, self.queue,
                self.packages, self.jars, self.py_files, self.conf, self.extra_args, self.application_args,
            )
            merged_env = {**os.environ, **(self.env_vars or {})}
            context.log.info(f"spark_submit_asset: $ {' '.join(cmd)}")

            proc = subprocess.Popen(
                cmd, env=merged_env,
                stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                text=True, bufsize=1,
            )

            timed_out = {"flag": False}
            timer = None
            if self.timeout_seconds:
                def _kill():
                    timed_out["flag"] = True
                    proc.kill()
                timer = threading.Timer(self.timeout_seconds, _kill)
                timer.start()

            try:
                stdout_lines = []
                for line in proc.stdout or []:
                    sys.stdout.write(line)
                    sys.stdout.flush()
                    stdout_lines.append(line)
                stderr_data = (proc.stderr.read() if proc.stderr else "") or ""
                if stderr_data:
                    sys.stderr.write(stderr_data)
                    sys.stderr.flush()
                proc.wait()
            finally:
                if timer:
                    timer.cancel()

            if timed_out["flag"]:
                raise dg.Failure(
                    description=f"spark-submit timed out after {self.timeout_seconds}s and was killed.",
                    metadata={"stderr": dg.MetadataValue.text(stderr_data[-2000:])},
                )

            success_codes = set(self.success_exit_codes or []) | {0}
            if proc.returncode not in success_codes:
                raise dg.Failure(
                    description=f"spark-submit exited {proc.returncode}",
                    metadata={"stderr": dg.MetadataValue.text(stderr_data[-2000:])},
                )

            return dg.MaterializeResult(metadata={
                "exit_code": dg.MetadataValue.int(proc.returncode),
                "command": dg.MetadataValue.text(" ".join(cmd)),
                "stdout_preview": dg.MetadataValue.text("".join(stdout_lines)[-2000:]),
            })

        return dg.Definitions(assets=[_spark_submit_asset])
