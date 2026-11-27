"""DataprocJobAssetComponent.

Submit a PySpark/Spark/Hive/Pig job to an existing Google Cloud Dataproc
cluster and wait for completion -- the `DataprocSubmitJobOperator`
equivalent from Airflow's Dataproc provider.

Request/response shapes verified directly against the installed
`google-cloud-dataproc` v5 client (`google.cloud.dataproc_v1`), not from
memory: `JobControllerClient.submit_job` takes `project_id`/`region`/`job`
(a `Job(reference=..., placement=JobPlacement(cluster_name=...), <type>_job=...)`)
and returns the accepted `Job` synchronously (not yet complete -- poll
`get_job(project_id, region, job_id)` until `job.status.state` is terminal);
`PySparkJob` fields are `main_python_file_uri`/`args`/`python_file_uris`/
`jar_file_uris`/`file_uris`/`archive_uris`/`properties`; `SparkJob` fields are
`main_jar_file_uri`/`main_class`/`args`/`jar_file_uris`/`file_uris`/
`archive_uris`/`properties`; `HiveJob`/`PigJob` fields are `query_file_uri`/
`query_list`/`continue_on_failure`/`script_variables`/`properties`/
`jar_file_uris`. Terminal `JobStatus.State` values: `DONE` (success),
`ERROR`/`CANCELLED` (failure). The regional client requires
`client_options={"api_endpoint": f"{region}-dataproc.googleapis.com:443"}`.
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


_TERMINAL_SUCCESS = {"DONE"}
_TERMINAL_FAILURE = {"ERROR", "CANCELLED"}


def _make_job_client(region: str, gcp_credentials_env_var: Optional[str]):
    from google.cloud import dataproc_v1

    client_kwargs: Dict[str, Any] = {"client_options": {"api_endpoint": f"{region}-dataproc.googleapis.com:443"}}
    if gcp_credentials_env_var:
        import json
        import os

        from google.oauth2 import service_account

        raw = os.environ.get(gcp_credentials_env_var)
        if not raw:
            raise ValueError(f"Env var {gcp_credentials_env_var!r} (gcp_credentials_env_var) is not set.")
        client_kwargs["credentials"] = service_account.Credentials.from_service_account_info(json.loads(raw))
    return dataproc_v1.JobControllerClient(**client_kwargs)


def _build_job_body(self) -> Dict[str, Any]:
    job: Dict[str, Any] = {"placement": {"cluster_name": self.cluster_name}}
    if self.labels:
        job["labels"] = dict(self.labels)

    if self.job_type == "pyspark":
        if not self.main_python_file_uri:
            raise ValueError("job_type='pyspark' requires main_python_file_uri.")
        job["pyspark_job"] = {
            "main_python_file_uri": self.main_python_file_uri,
            "args": self.args or [],
            "python_file_uris": self.python_file_uris or [],
            "jar_file_uris": self.jar_file_uris or [],
            "file_uris": self.file_uris or [],
            "archive_uris": self.archive_uris or [],
            "properties": self.properties or {},
        }
    elif self.job_type == "spark":
        if not self.main_jar_file_uri and not self.main_class:
            raise ValueError("job_type='spark' requires main_jar_file_uri or main_class.")
        spark_job: Dict[str, Any] = {
            "args": self.args or [],
            "jar_file_uris": self.jar_file_uris or [],
            "file_uris": self.file_uris or [],
            "archive_uris": self.archive_uris or [],
            "properties": self.properties or {},
        }
        if self.main_jar_file_uri:
            spark_job["main_jar_file_uri"] = self.main_jar_file_uri
        if self.main_class:
            spark_job["main_class"] = self.main_class
        job["spark_job"] = spark_job
    elif self.job_type in ("hive", "pig"):
        if not self.query_file_uri and not self.query_list:
            raise ValueError(f"job_type={self.job_type!r} requires query_file_uri or query_list.")
        type_job: Dict[str, Any] = {
            "continue_on_failure": self.continue_on_failure,
            "script_variables": self.script_variables or {},
            "properties": self.properties or {},
            "jar_file_uris": self.jar_file_uris or [],
        }
        if self.query_file_uri:
            type_job["query_file_uri"] = self.query_file_uri
        else:
            type_job["query_list"] = {"queries": list(self.query_list)}
        job[f"{self.job_type}_job"] = type_job
    else:
        raise ValueError(f"job_type must be 'pyspark', 'spark', 'hive', or 'pig', got {self.job_type!r}")
    return job


class DataprocJobAssetComponent(dg.Component, dg.Model, dg.Resolvable):
    """Submit a PySpark/Spark/Hive/Pig job to an existing Dataproc cluster and wait for completion."""

    asset_name: str = Field(description="Dagster asset key.")
    project_id: str = Field(description="GCP project ID.")
    region: str = Field(description="Dataproc region, e.g. 'us-central1'.")
    cluster_name: str = Field(description="The existing Dataproc cluster to submit this job to.")
    gcp_credentials_env_var: Optional[str] = Field(default=None, description="Env var containing a GCP service account JSON string. Omit to use Application Default Credentials.")

    job_type: str = Field(description="'pyspark' | 'spark' | 'hive' | 'pig'.")

    main_python_file_uri: Optional[str] = Field(default=None, description="Required for job_type='pyspark'. GCS URI of the main .py file.")
    main_jar_file_uri: Optional[str] = Field(default=None, description="For job_type='spark': GCS/HDFS URI of the main jar. Provide this or main_class.")
    main_class: Optional[str] = Field(default=None, description="For job_type='spark': fully-qualified main class name (jar_file_uris must include the jar providing it). Provide this or main_jar_file_uri.")
    python_file_uris: Optional[List[str]] = Field(default=None, description="pyspark only: additional .py/.zip/.egg files.")
    jar_file_uris: Optional[List[str]] = Field(default=None, description="pyspark/spark/hive/pig: additional jars on the classpath.")
    file_uris: Optional[List[str]] = Field(default=None, description="pyspark/spark: files to place in each executor's working directory.")
    archive_uris: Optional[List[str]] = Field(default=None, description="pyspark/spark: archives (.jar/.tar/.tar.gz/.tgz/.zip) to extract into each executor's working directory.")
    args: Optional[List[str]] = Field(default=None, description="pyspark/spark: command-line arguments passed to the driver.")
    properties: Optional[Dict[str, str]] = Field(default=None, description="Job-level Spark/Hadoop properties, e.g. {'spark.executor.memory': '4g'}.")

    query_file_uri: Optional[str] = Field(default=None, description="hive/pig: GCS URI of the query file. Provide this or query_list.")
    query_list: Optional[List[str]] = Field(default=None, description="hive/pig: inline list of queries/statements to run in order. Provide this or query_file_uri.")
    continue_on_failure: bool = Field(default=False, description="hive/pig: keep running subsequent queries after one fails.")
    script_variables: Optional[Dict[str, str]] = Field(default=None, description="hive/pig: Hive/Pig variables substituted into the query text.")

    labels: Optional[Dict[str, str]] = Field(default=None, description="Dataproc job labels.")
    poll_interval_seconds: float = Field(default=10.0, gt=0.0, description="Seconds between status polls.")
    timeout_seconds: int = Field(default=3600, ge=1, description="Max time to wait for the job to reach a terminal state.")

    group_name: Optional[str] = Field(default=None, description="Dagster asset group name.")
    description: Optional[str] = Field(default=None, description="Asset description.")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners.")
    asset_tags: Optional[Union[Dict[str, Any], List[str]]] = Field(default=None, description="Extra asset tags.")
    kinds: Optional[List[str]] = Field(default=None, description="Defaults to ['gcp', 'dataproc'].")
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
            lists=("owners", "kinds", "deps", "python_file_uris", "jar_file_uris", "file_uris", "archive_uris", "args", "query_list"),
            csv=("partition_values",),
            tags=("properties", "script_variables", "labels"),
        )

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label='Dataproc Job Asset', editable=True)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        if self.job_type not in ("pyspark", "spark", "hive", "pig"):
            raise ValueError(f"job_type must be 'pyspark', 'spark', 'hive', or 'pig', got {self.job_type!r}")
        # Validate the job body shape eagerly so a misconfiguration fails at load time, not at materialize time.
        _build_job_body(self)

        _self = self
        partitions_def = _build_partitions_def(
            self.partition_type, self.partition_start, self.partition_values,
            self.dynamic_partition_name, self.partition_dimensions,
            cron_schedule=self.partition_cron_schedule,
            cron_fmt=self.partition_cron_fmt or "%Y-%m-%d", end_offset=self.partition_end_offset, timezone=self.partition_timezone,
        )

        _inferred_kinds = list(self.kinds or ["gcp", "dataproc"])
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
            description=self.description or f"Submit a {self.job_type} job to Dataproc cluster {self.cluster_name!r} and wait for completion.",
            owners=self.owners or [],
            tags=_all_tags,
            kinds=set(_inferred_kinds),
            deps=[dg.AssetKey.from_user_string(d) for d in (self.deps or [])],
            partitions_def=partitions_def,
            freshness_policy=_freshness_policy,
            retry_policy=_retry_policy,
        )
        def _dataproc_job_asset(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
            import time

            self = _self
            client = _make_job_client(self.region, self.gcp_credentials_env_var)
            job_body = _build_job_body(self)

            submitted = client.submit_job(project_id=self.project_id, region=self.region, job=job_body)
            job_id = submitted.reference.job_id
            context.log.info(f"dataproc_job_asset: submitted {self.job_type} job {job_id!r} to cluster {self.cluster_name!r}.")

            deadline = time.monotonic() + self.timeout_seconds
            job = submitted
            while job.status.state.name not in (_TERMINAL_SUCCESS | _TERMINAL_FAILURE):
                if time.monotonic() >= deadline:
                    raise TimeoutError(
                        f"Dataproc job {job_id!r} did not reach a terminal state within {self.timeout_seconds}s "
                        f"(last status={job.status.state.name!r})."
                    )
                time.sleep(self.poll_interval_seconds)
                job = client.get_job(project_id=self.project_id, region=self.region, job_id=job_id)
                context.log.info(f"dataproc_job_asset: job {job_id!r} status={job.status.state.name!r}")

            state = job.status.state.name
            if state in _TERMINAL_FAILURE:
                details = job.status.details or "(no details)"
                raise RuntimeError(f"Dataproc job {job_id!r} finished with status={state!r}: {details}")

            metadata: Dict[str, Any] = {
                "project_id": dg.MetadataValue.text(self.project_id),
                "cluster_name": dg.MetadataValue.text(self.cluster_name),
                "job_id": dg.MetadataValue.text(job_id),
                "job_type": dg.MetadataValue.text(self.job_type),
                "job_status": dg.MetadataValue.text(state),
            }
            if job.driver_output_resource_uri:
                metadata["driver_output_uri"] = dg.MetadataValue.path(job.driver_output_resource_uri)
            return dg.MaterializeResult(metadata=metadata)

        return dg.Definitions(assets=[_dataproc_job_asset])
