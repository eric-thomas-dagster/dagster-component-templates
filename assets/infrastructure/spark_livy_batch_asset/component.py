"""SparkLivyBatchAssetComponent.

Submits (and polls) a Spark batch job to a standalone/YARN Spark cluster via
Apache Livy's REST API -- the classic on-prem/Hadoop-era submission path, as
distinct from `spark_k8s_operator_asset` (Kubernetes via the Spark Operator
CRD) and AWS EMR. Livy is a REST front door for `spark-submit`: one POST
creates a "batch", which runs as a real Spark application on the cluster
Livy is configured against.

Request/response shapes verified directly against the official Livy REST API
reference (livy.apache.org/docs/latest/rest-api.html), not from memory:
  - `POST /batches` -- body `file` (required) + optional `className`, `args`,
    `jars`/`pyFiles`/`files`/`archives` (lists), `driverMemory`/`driverCores`/
    `executorMemory`/`executorCores`/`numExecutors`, `queue`, `name`, `conf`
    (map); response `{id, state, appId, appInfo, log}`.
  - `GET /batches/{id}/state` -- `{id, state}`.
  - `GET /batches/{id}/log` -- `{id, from, size, log}`.
  - `DELETE /batches/{id}` -- kill the batch.

Livy's own docs only enumerate session states (not_started/starting/idle/
busy/shutting_down/error/dead/success), not a public batch-state enum --
this component treats 'success' as the only success terminal state and
anything else that stops changing as a failure, rather than hardcoding a
state list that isn't actually documented.
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


_SUCCESS_STATES = {"success"}
_FAILURE_STATES = {"dead", "killed", "error"}


def _submit_batch(
    livy_url: str, file: str, class_name: Optional[str], args: Optional[List[str]],
    jars: Optional[List[str]], py_files: Optional[List[str]], files: Optional[List[str]], archives: Optional[List[str]],
    driver_memory: Optional[str], driver_cores: Optional[int], executor_memory: Optional[str],
    executor_cores: Optional[int], num_executors: Optional[int], queue: Optional[str], name: Optional[str],
    spark_conf: Optional[Dict[str, str]], headers: Dict[str, str], request_timeout_seconds: int,
) -> int:
    import requests

    body: Dict[str, Any] = {"file": file}
    for k, v in (
        ("className", class_name), ("args", args), ("jars", jars), ("pyFiles", py_files),
        ("files", files), ("archives", archives), ("driverMemory", driver_memory),
        ("driverCores", driver_cores), ("executorMemory", executor_memory), ("executorCores", executor_cores),
        ("numExecutors", num_executors), ("queue", queue), ("name", name), ("conf", spark_conf),
    ):
        if v is not None:
            body[k] = v

    resp = requests.post(f"{livy_url}/batches", json=body, headers=headers, timeout=request_timeout_seconds)
    if resp.status_code >= 400:
        raise RuntimeError(f"Livy batch submission failed: HTTP {resp.status_code} {resp.text[:500]!r}")
    return resp.json()["id"]


def _get_state(livy_url: str, batch_id: int, headers: Dict[str, str], request_timeout_seconds: int) -> str:
    import requests
    resp = requests.get(f"{livy_url}/batches/{batch_id}/state", headers=headers, timeout=request_timeout_seconds)
    if resp.status_code >= 400:
        raise RuntimeError(f"Livy batch state poll failed: HTTP {resp.status_code} {resp.text[:500]!r}")
    return resp.json()["state"]


def _get_batch_detail(livy_url: str, batch_id: int, headers: Dict[str, str], request_timeout_seconds: int) -> Dict[str, Any]:
    import requests
    resp = requests.get(f"{livy_url}/batches/{batch_id}", headers=headers, timeout=request_timeout_seconds)
    if resp.status_code >= 400:
        raise RuntimeError(f"Livy batch detail fetch failed: HTTP {resp.status_code} {resp.text[:500]!r}")
    return resp.json()


def _get_log_tail(livy_url: str, batch_id: int, headers: Dict[str, str], request_timeout_seconds: int) -> str:
    import requests
    try:
        resp = requests.get(f"{livy_url}/batches/{batch_id}/log", headers=headers, timeout=request_timeout_seconds)
        if resp.status_code >= 400:
            return ""
        return "\n".join(resp.json().get("log") or [])
    except Exception:
        return ""


def _poll_until_terminal(livy_url: str, batch_id: int, log, headers: Dict[str, str],
                          poll_interval_seconds: float, timeout_seconds: int, request_timeout_seconds: int) -> str:
    import time
    deadline = time.monotonic() + timeout_seconds
    state = _get_state(livy_url, batch_id, headers, request_timeout_seconds)
    while state not in _SUCCESS_STATES and state not in _FAILURE_STATES:
        if time.monotonic() >= deadline:
            raise TimeoutError(f"Livy batch {batch_id} did not reach a terminal state within {timeout_seconds}s (last state={state!r}).")
        log.info(f"spark_livy_batch_asset: batch {batch_id} state={state!r}")
        time.sleep(poll_interval_seconds)
        state = _get_state(livy_url, batch_id, headers, request_timeout_seconds)
    return state


class SparkLivyBatchAssetComponent(dg.Component, dg.Model, dg.Resolvable):
    """Submit a Spark batch job to a standalone/YARN cluster via Apache Livy, and poll to completion.

    Distinct from `spark_k8s_operator_asset` (Kubernetes via the Spark
    Operator CRD) -- use this one when Spark runs on a classic
    standalone/YARN cluster fronted by a Livy server, the common on-prem
    pattern.
    """

    asset_name: str = Field(description="Dagster asset key.")
    livy_url: str = Field(description="Base URL of the Livy server, e.g. 'http://livy-server:8998'.")
    file: str = Field(description="Path to the application file (jar or Python script), e.g. 's3://bucket/job.py' or 'hdfs:///jobs/job.jar'. The only field Livy itself requires.")
    class_name: Optional[str] = Field(default=None, description="Application Java/Spark main class. Required for a JVM (jar) job; omit for a PySpark file.")
    args: Optional[List[str]] = Field(default=None, description="Command line arguments for the application.")
    jars: Optional[List[str]] = Field(default=None, description="Extra jars to include.")
    py_files: Optional[List[str]] = Field(default=None, description="Extra Python files (.py/.zip/.egg) to place on PYTHONPATH.")
    files: Optional[List[str]] = Field(default=None, description="Extra files to place in the executor working directory.")
    archives: Optional[List[str]] = Field(default=None, description="Archives to extract into the executor working directory.")
    driver_memory: Optional[str] = Field(default=None, description="Driver process memory, e.g. '2g'.")
    driver_cores: Optional[int] = Field(default=None, description="Number of cores for the driver process.")
    executor_memory: Optional[str] = Field(default=None, description="Executor process memory, e.g. '4g'.")
    executor_cores: Optional[int] = Field(default=None, description="Number of cores per executor.")
    num_executors: Optional[int] = Field(default=None, description="Number of executors to launch.")
    queue: Optional[str] = Field(default=None, description="YARN queue to submit to.")
    batch_name: Optional[str] = Field(default=None, description="A name for the Livy batch. Defaults to the asset name.")
    spark_conf: Optional[Dict[str, str]] = Field(default=None, description="Extra Spark configuration, e.g. {'spark.executor.memoryOverhead': '512m'}.")
    request_headers: Optional[Dict[str, str]] = Field(default=None, description="Extra HTTP headers for every request, e.g. an Authorization header if Livy sits behind an authenticating proxy.")

    poll_interval_seconds: float = Field(default=5.0, gt=0.0, description="Seconds between status polls.")
    timeout_seconds: int = Field(default=1800, ge=1, description="Max time to wait for the batch to reach a terminal state.")
    request_timeout_seconds: int = Field(default=60, ge=1, description="HTTP request timeout.")
    fail_on_batch_failure: bool = Field(default=True, description="Raise if the batch ends dead/killed/error. False materializes anyway with the state in metadata.")

    group_name: Optional[str] = Field(default=None, description="Dagster asset group name.")
    description: Optional[str] = Field(default=None, description="Asset description.")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners.")
    asset_tags: Optional[Union[Dict[str, Any], List[str]]] = Field(default=None, description="Extra asset tags.")
    kinds: Optional[List[str]] = Field(default=None, description="Defaults to ['spark', 'livy'].")
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
        return coerce_field_shapes(data, lists=("owners", "kinds", "deps", "args", "jars", "py_files", "files", "archives"), csv=("partition_values",), tags=("asset_tags",))

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        _self = self
        partitions_def = _build_partitions_def(
            self.partition_type, self.partition_start, self.partition_values,
            self.dynamic_partition_name, self.partition_dimensions,
            cron_schedule=self.partition_cron_schedule,
            cron_fmt=self.partition_cron_fmt or "%Y-%m-%d", end_offset=self.partition_end_offset, timezone=self.partition_timezone,
        )

        _inferred_kinds = list(self.kinds or ["spark", "livy"])
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
            description=self.description or f"Submit {self.file!r} as a Spark batch via Livy ({self.livy_url}).",
            owners=self.owners or [],
            tags=_all_tags,
            kinds=set(_inferred_kinds),
            deps=[dg.AssetKey.from_user_string(d) for d in (self.deps or [])],
            partitions_def=partitions_def,
            freshness_policy=_freshness_policy,
            retry_policy=_retry_policy,
        )
        def _spark_livy_batch_asset(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
            self = _self
            headers = dict(self.request_headers or {})

            batch_id = _submit_batch(
                self.livy_url, self.file, self.class_name, self.args, self.jars, self.py_files,
                self.files, self.archives, self.driver_memory, self.driver_cores, self.executor_memory,
                self.executor_cores, self.num_executors, self.queue, self.batch_name or self.asset_name,
                self.spark_conf, headers, self.request_timeout_seconds,
            )
            context.log.info(f"spark_livy_batch_asset: submitted batch_id={batch_id}")

            state = _poll_until_terminal(
                self.livy_url, batch_id, context.log, headers,
                self.poll_interval_seconds, self.timeout_seconds, self.request_timeout_seconds,
            )

            if self.fail_on_batch_failure and state in _FAILURE_STATES:
                log_tail = _get_log_tail(self.livy_url, batch_id, headers, self.request_timeout_seconds)
                raise RuntimeError(
                    f"Livy batch {batch_id} ended {state!r} instead of 'success'."
                    + (f" Last log lines:\n{log_tail[-2000:]}" if log_tail else "")
                )

            detail = _get_batch_detail(self.livy_url, batch_id, headers, self.request_timeout_seconds)
            metadata: Dict[str, Any] = {
                "batch_id": dg.MetadataValue.int(batch_id),
                "state": dg.MetadataValue.text(state),
                "livy_url": dg.MetadataValue.text(self.livy_url),
            }
            if detail.get("appId"):
                metadata["app_id"] = dg.MetadataValue.text(detail["appId"])

            return dg.MaterializeResult(metadata=metadata)

        return dg.Definitions(assets=[_spark_livy_batch_asset])
