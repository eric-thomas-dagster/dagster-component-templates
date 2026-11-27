"""EmrStepAssetComponent.

Adds a step to a running (or named-and-resolved) AWS EMR cluster and polls it
to completion as a Dagster asset -- the Airflow `EmrAddStepsOperator` +
`EmrStepSensor` equivalent in one asset.

Real boto3 EMR API, verified against the botocore reference:
  - `add_job_flow_steps(JobFlowId=..., Steps=[{'Name', 'ActionOnFailure',
    'HadoopJarStep': {'Jar', 'MainClass', 'Args', 'Properties'}}])` ->
    `{'StepIds': [...]}`.
  - `describe_step(ClusterId=..., StepId=...)` ->
    `{'Step': {'Status': {'State', 'FailureDetails': {'Reason', 'Message',
    'LogFile'}, 'Timeline': {...}}}}`. States: PENDING, CANCEL_PENDING,
    RUNNING, COMPLETED, CANCELLED, FAILED, INTERRUPTED.
  - `list_clusters(ClusterStates=[...])` -> `{'Clusters': [{'Id', 'Name',
    ...}]}`, used to resolve `cluster_name` to a cluster id when `cluster_id`
    isn't given directly.

`jar` defaults to EMR's bundled `command-runner.jar`, the standard way to run
an arbitrary shell command (including `spark-submit`) as a step without a
custom JAR -- `args` then starts with the command itself, e.g.
`["spark-submit", "--deploy-mode", "cluster", "s3://bucket/job.py"]`.
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


_SUCCESS_STATES = {"COMPLETED"}
_FAILURE_STATES = {"CANCELLED", "FAILED", "INTERRUPTED"}


def _make_emr_client(region_name: str, aws_profile: Optional[str]):
    import boto3
    session = boto3.Session(profile_name=aws_profile, region_name=region_name)
    return session.client("emr")


def _resolve_cluster_id(client, cluster_id: Optional[str], cluster_name: Optional[str]) -> str:
    if cluster_id:
        return cluster_id
    if not cluster_name:
        raise ValueError("Set either cluster_id or cluster_name.")
    matches = []
    paginator = client.get_paginator("list_clusters")
    for page in paginator.paginate(ClusterStates=["STARTING", "BOOTSTRAPPING", "RUNNING", "WAITING"]):
        matches.extend(c for c in page["Clusters"] if c["Name"] == cluster_name)
    if not matches:
        raise RuntimeError(f"No active EMR cluster found with name {cluster_name!r}.")
    if len(matches) > 1:
        ids = ", ".join(c["Id"] for c in matches)
        raise RuntimeError(f"Multiple active EMR clusters named {cluster_name!r}: {ids}. Use cluster_id instead.")
    return matches[0]["Id"]


def _add_step(
    client, cluster_id: str, step_name: str, action_on_failure: str,
    jar: str, main_class: Optional[str], args: List[str], properties: Optional[Dict[str, str]],
) -> str:
    hadoop_jar_step: Dict[str, Any] = {"Jar": jar, "Args": list(args)}
    if main_class:
        hadoop_jar_step["MainClass"] = main_class
    if properties:
        hadoop_jar_step["Properties"] = [{"Key": k, "Value": v} for k, v in properties.items()]

    response = client.add_job_flow_steps(
        JobFlowId=cluster_id,
        Steps=[{"Name": step_name, "ActionOnFailure": action_on_failure, "HadoopJarStep": hadoop_jar_step}],
    )
    return response["StepIds"][0]


def _describe_step(client, cluster_id: str, step_id: str) -> Dict[str, Any]:
    return client.describe_step(ClusterId=cluster_id, StepId=step_id)["Step"]


def _poll_until_terminal(client, cluster_id: str, step_id: str, log, poll_interval_seconds: float, timeout_seconds: int) -> Dict[str, Any]:
    import time
    deadline = time.monotonic() + timeout_seconds
    step = _describe_step(client, cluster_id, step_id)
    state = step["Status"]["State"]
    while state not in _SUCCESS_STATES and state not in _FAILURE_STATES:
        if time.monotonic() >= deadline:
            raise TimeoutError(f"EMR step {step_id} on cluster {cluster_id} did not reach a terminal state within {timeout_seconds}s (last state={state!r}).")
        log.info(f"emr_step_asset: step {step_id} state={state!r}")
        time.sleep(poll_interval_seconds)
        step = _describe_step(client, cluster_id, step_id)
        state = step["Status"]["State"]
    return step


class EmrStepAssetComponent(dg.Component, dg.Model, dg.Resolvable):
    """Add a step to an AWS EMR cluster and poll it to completion as a Dagster asset.

    The Airflow `EmrAddStepsOperator` + `EmrStepSensor` equivalent in one
    asset. `jar` defaults to `command-runner.jar`, EMR's bundled runner for
    an arbitrary shell command (such as `spark-submit`) as a step -- `args`
    then starts with the command itself.
    """

    asset_name: str = Field(description="Dagster asset key.")
    region_name: str = Field(description="AWS region, e.g. 'us-east-1'.")
    aws_profile: Optional[str] = Field(default=None, description="Named AWS profile. Omit to use the default boto3 credential chain (env vars, instance role, etc.).")

    cluster_id: Optional[str] = Field(default=None, description="Existing EMR cluster id, e.g. 'j-XXXXXXXXXXXXX'. Set this or cluster_name.")
    cluster_name: Optional[str] = Field(default=None, description="Existing EMR cluster name, resolved to a cluster id via list_clusters (must match exactly one STARTING/BOOTSTRAPPING/RUNNING/WAITING cluster). Set this or cluster_id.")

    step_name: Optional[str] = Field(default=None, description="Step name shown in the EMR console. Defaults to asset_name.")
    action_on_failure: str = Field(default="CONTINUE", description="'TERMINATE_JOB_FLOW' | 'TERMINATE_CLUSTER' | 'CANCEL_AND_WAIT' | 'CONTINUE'.")
    jar: str = Field(default="command-runner.jar", description="Jar for the step. Defaults to EMR's bundled command-runner.jar, which runs args as a shell command (e.g. spark-submit) without a custom jar.")
    main_class: Optional[str] = Field(default=None, description="Main class, for a custom jar step. Omit when using command-runner.jar.")
    args: List[str] = Field(description="Step arguments, e.g. ['spark-submit', '--deploy-mode', 'cluster', 's3://bucket/job.py'].")
    properties: Optional[Dict[str, str]] = Field(default=None, description="Extra Hadoop job properties for the step.")

    wait_for_completion: bool = Field(default=True, description="Poll the step to a terminal state. False fires the step and materializes immediately with just the step id.")
    poll_interval_seconds: float = Field(default=15.0, gt=0.0, description="Seconds between status polls.")
    timeout_seconds: int = Field(default=3600, ge=1, description="Max time to wait for the step to reach a terminal state.")
    fail_on_step_failure: bool = Field(default=True, description="Raise if the step ends CANCELLED/FAILED/INTERRUPTED. False materializes anyway with the state in metadata.")

    group_name: Optional[str] = Field(default=None, description="Dagster asset group name.")
    description: Optional[str] = Field(default=None, description="Asset description.")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners.")
    asset_tags: Optional[Union[Dict[str, Any], List[str]]] = Field(default=None, description="Extra asset tags.")
    kinds: Optional[List[str]] = Field(default=None, description="Defaults to ['aws', 'emr'].")
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
        return coerce_field_shapes(data, lists=("owners", "kinds", "deps", "args"), csv=("partition_values",), tags=("asset_tags",))

    @classmethod
    def get_form_config(cls):
        from dagster.components.resolved.form_config import ComponentFormConfig
        return ComponentFormConfig(label='EMR Step Asset', editable=True)

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        if not self.cluster_id and not self.cluster_name:
            raise ValueError("Set either cluster_id or cluster_name.")

        _self = self
        partitions_def = _build_partitions_def(
            self.partition_type, self.partition_start, self.partition_values,
            self.dynamic_partition_name, self.partition_dimensions,
            cron_schedule=self.partition_cron_schedule,
            cron_fmt=self.partition_cron_fmt or "%Y-%m-%d", end_offset=self.partition_end_offset, timezone=self.partition_timezone,
        )

        _inferred_kinds = list(self.kinds or ["aws", "emr"])
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
            description=self.description or f"Run an EMR step on cluster {self.cluster_id or self.cluster_name!r}.",
            owners=self.owners or [],
            tags=_all_tags,
            kinds=set(_inferred_kinds),
            deps=[dg.AssetKey.from_user_string(d) for d in (self.deps or [])],
            partitions_def=partitions_def,
            freshness_policy=_freshness_policy,
            retry_policy=_retry_policy,
        )
        def _emr_step_asset(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
            self = _self
            client = _make_emr_client(self.region_name, self.aws_profile)
            cluster_id = _resolve_cluster_id(client, self.cluster_id, self.cluster_name)

            step_id = _add_step(
                client, cluster_id, self.step_name or self.asset_name, self.action_on_failure,
                self.jar, self.main_class, self.args, self.properties,
            )
            context.log.info(f"emr_step_asset: added step_id={step_id} to cluster={cluster_id}")

            metadata: Dict[str, Any] = {
                "cluster_id": dg.MetadataValue.text(cluster_id),
                "step_id": dg.MetadataValue.text(step_id),
            }

            if not self.wait_for_completion:
                return dg.MaterializeResult(metadata=metadata)

            step = _poll_until_terminal(client, cluster_id, step_id, context.log, self.poll_interval_seconds, self.timeout_seconds)
            state = step["Status"]["State"]
            metadata["state"] = dg.MetadataValue.text(state)

            if state in _FAILURE_STATES and self.fail_on_step_failure:
                failure = step["Status"].get("FailureDetails") or {}
                raise RuntimeError(
                    f"EMR step {step_id} on cluster {cluster_id} ended {state!r} instead of 'COMPLETED'. "
                    f"Reason: {failure.get('Reason')!r} Message: {failure.get('Message')!r} LogFile: {failure.get('LogFile')!r}"
                )

            return dg.MaterializeResult(metadata=metadata)

        return dg.Definitions(assets=[_emr_step_asset])
