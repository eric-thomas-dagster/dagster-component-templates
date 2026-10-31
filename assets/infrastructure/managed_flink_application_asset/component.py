"""ManagedFlinkApplicationAssetComponent.

Declaratively ensures an AWS Managed Service for Apache Flink application
(the managed runtime for Apache Flink -- `kinesisanalyticsv2` in boto3) is
running or stopped, as a Dagster asset: `describe_application` to read the
current `ApplicationStatus`, `start_application`/`stop_application` if it
isn't already in `desired_state`, then polls `describe_application` until
it settles.

Request/response shapes verified directly against the boto3
`kinesisanalyticsv2` client reference (not from memory): `start_application`
takes `ApplicationName` + an optional `RunConfiguration` (`FlinkRunConfiguration.
AllowNonRestoredState`, `ApplicationRestoreConfiguration.{ApplicationRestoreType,
SnapshotName}`) and returns `{'OperationId': str}`; `stop_application` takes
`ApplicationName` + `Force`, same response shape; `describe_application`
returns `{'ApplicationDetail': {'ApplicationStatus': ..., 'ApplicationVersionId':
..., 'ApplicationARN': ...}}`, with `ApplicationStatus` one of DELETING |
STARTING | STOPPING | READY | RUNNING | UPDATING | AUTOSCALING |
FORCE_STOPPING | ROLLING_BACK | MAINTENANCE | ROLLED_BACK.

A Managed Service for Apache Flink application is a long-running stream
processor, not a batch job that completes -- "materializing" this asset
means "ensure the stream processor is in the desired running/stopped
state", not "wait for it to finish."
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


_ACTIVE_STATUSES = {"STARTING", "UPDATING", "AUTOSCALING", "MAINTENANCE"}
_BLOCKED_STATUSES = {"DELETING", "STOPPING", "FORCE_STOPPING", "ROLLING_BACK"}


def _make_flink_client(region_name: str, aws_profile: Optional[str]):
    import boto3
    session = boto3.Session(profile_name=aws_profile, region_name=region_name)
    return session.client("kinesisanalyticsv2")


def _describe(client, application_name: str) -> Dict[str, Any]:
    return client.describe_application(ApplicationName=application_name)["ApplicationDetail"]


def _poll_until(client, application_name: str, target_statuses, log, poll_interval_seconds: float, timeout_seconds: int) -> Dict[str, Any]:
    import time
    deadline = time.monotonic() + timeout_seconds
    detail = _describe(client, application_name)
    while detail["ApplicationStatus"] not in target_statuses:
        if detail["ApplicationStatus"] == "ROLLED_BACK":
            raise RuntimeError(
                f"Flink application {application_name!r} rolled back while waiting for "
                f"{target_statuses}; last status=ROLLED_BACK."
            )
        if time.monotonic() >= deadline:
            raise TimeoutError(
                f"Flink application {application_name!r} did not reach {target_statuses} within "
                f"{timeout_seconds}s (last status={detail['ApplicationStatus']!r})."
            )
        log.info(f"managed_flink_application_asset: {application_name!r} status={detail['ApplicationStatus']!r}, waiting for {target_statuses}")
        time.sleep(poll_interval_seconds)
        detail = _describe(client, application_name)
    return detail


def _ensure_state(
    client,
    application_name: str,
    desired_state: str,
    allow_non_restored_state: bool,
    application_restore_type: Optional[str],
    snapshot_name: Optional[str],
    force_stop: bool,
    log,
    poll_interval_seconds: float,
    timeout_seconds: int,
) -> Dict[str, Any]:
    detail = _describe(client, application_name)
    status = detail["ApplicationStatus"]
    operation_id = None

    if desired_state == "running":
        if status == "RUNNING":
            log.info(f"managed_flink_application_asset: {application_name!r} already RUNNING.")
            return {"detail": detail, "operation_id": None, "action": "none"}
        if status in _BLOCKED_STATUSES:
            raise RuntimeError(
                f"Flink application {application_name!r} is {status!r}; cannot start from this state "
                "(it must finish its current transition first)."
            )
        if status == "READY":
            run_configuration: Dict[str, Any] = {
                "FlinkRunConfiguration": {"AllowNonRestoredState": allow_non_restored_state},
            }
            if application_restore_type:
                restore_cfg = {"ApplicationRestoreType": application_restore_type}
                if snapshot_name:
                    restore_cfg["SnapshotName"] = snapshot_name
                run_configuration["ApplicationRestoreConfiguration"] = restore_cfg
            resp = client.start_application(ApplicationName=application_name, RunConfiguration=run_configuration)
            operation_id = resp.get("OperationId")
            log.info(f"managed_flink_application_asset: started {application_name!r} (operation_id={operation_id}).")
        # else: already STARTING/UPDATING/AUTOSCALING/MAINTENANCE -- already heading toward RUNNING.
        detail = _poll_until(client, application_name, {"RUNNING"}, log, poll_interval_seconds, timeout_seconds)
        return {"detail": detail, "operation_id": operation_id, "action": "started"}

    if desired_state == "stopped":
        if status == "READY":
            log.info(f"managed_flink_application_asset: {application_name!r} already READY (stopped).")
            return {"detail": detail, "operation_id": None, "action": "none"}
        if status in _BLOCKED_STATUSES:
            raise RuntimeError(
                f"Flink application {application_name!r} is {status!r}; cannot stop from this state "
                "(it must finish its current transition first)."
            )
        resp = client.stop_application(ApplicationName=application_name, Force=force_stop)
        operation_id = resp.get("OperationId")
        log.info(f"managed_flink_application_asset: stopping {application_name!r} (operation_id={operation_id}).")
        detail = _poll_until(client, application_name, {"READY"}, log, poll_interval_seconds, timeout_seconds)
        return {"detail": detail, "operation_id": operation_id, "action": "stopped"}

    raise ValueError(f"desired_state must be 'running' or 'stopped', got {desired_state!r}")


class ManagedFlinkApplicationAssetComponent(dg.Component, dg.Model, dg.Resolvable):
    """Ensure an AWS Managed Service for Apache Flink application is running or stopped.

    A Managed Service for Apache Flink application is a long-running stream
    processor, not a batch job -- materializing this asset means "ensure the
    stream processor is in `desired_state`", polling `describe_application`
    until it settles rather than waiting for a job to finish.
    """

    asset_name: str = Field(description="Dagster asset key.")
    application_name: str = Field(description="The Managed Service for Apache Flink application name.")
    region_name: str = Field(description="AWS region, e.g. 'us-east-1'.")
    aws_profile: Optional[str] = Field(default=None, description="Named AWS profile. Omit to use the default boto3 credential chain.")

    desired_state: str = Field(default="running", description="'running' or 'stopped'. Idempotent: a no-op if the application is already in this state.")
    allow_non_restored_state: bool = Field(default=False, description="When starting from a snapshot, allow the runtime to skip state that can't be mapped to the updated program.")
    application_restore_type: Optional[str] = Field(
        default=None,
        description="'SKIP_RESTORE_FROM_SNAPSHOT' | 'RESTORE_FROM_LATEST_SNAPSHOT' | 'RESTORE_FROM_CUSTOM_SNAPSHOT'. Omit to let AWS pick its own default restore behavior.",
    )
    snapshot_name: Optional[str] = Field(default=None, description="Required when application_restore_type='RESTORE_FROM_CUSTOM_SNAPSHOT'.")
    force_stop: bool = Field(default=False, description="Force-stop without taking a snapshot first. May cause data loss or duplicate processing on next start -- see AWS docs.")
    poll_interval_seconds: float = Field(default=5.0, gt=0.0, description="Seconds between status polls.")
    timeout_seconds: int = Field(default=300, ge=1, description="Max time to wait for the application to reach the target state.")

    group_name: Optional[str] = Field(default=None, description="Dagster asset group name.")
    description: Optional[str] = Field(default=None, description="Asset description.")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners.")
    asset_tags: Optional[Union[Dict[str, Any], List[str]]] = Field(default=None, description="Extra asset tags.")
    kinds: Optional[List[str]] = Field(default=None, description="Defaults to ['aws', 'flink'].")
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
        return coerce_field_shapes(data, lists=("owners", "kinds", "deps"), csv=("partition_values",), tags=("asset_tags",))

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        if self.desired_state not in ("running", "stopped"):
            raise ValueError(f"desired_state must be 'running' or 'stopped', got {self.desired_state!r}")
        if self.application_restore_type == "RESTORE_FROM_CUSTOM_SNAPSHOT" and not self.snapshot_name:
            raise ValueError("application_restore_type='RESTORE_FROM_CUSTOM_SNAPSHOT' requires snapshot_name.")

        _self = self
        partitions_def = _build_partitions_def(
            self.partition_type, self.partition_start, self.partition_values,
            self.dynamic_partition_name, self.partition_dimensions,
            cron_schedule=self.partition_cron_schedule,
            cron_fmt=self.partition_cron_fmt or "%Y-%m-%d", end_offset=self.partition_end_offset, timezone=self.partition_timezone,
        )

        _inferred_kinds = list(self.kinds or ["aws", "flink"])
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
            description=self.description or f"Ensure Flink application {self.application_name!r} is {self.desired_state}.",
            owners=self.owners or [],
            tags=_all_tags,
            kinds=set(_inferred_kinds),
            deps=[dg.AssetKey.from_user_string(d) for d in (self.deps or [])],
            partitions_def=partitions_def,
            freshness_policy=_freshness_policy,
            retry_policy=_retry_policy,
        )
        def _managed_flink_application_asset(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
            self = _self
            client = _make_flink_client(self.region_name, self.aws_profile)
            result = _ensure_state(
                client, self.application_name, self.desired_state,
                self.allow_non_restored_state, self.application_restore_type, self.snapshot_name,
                self.force_stop, context.log, self.poll_interval_seconds, self.timeout_seconds,
            )
            detail = result["detail"]

            metadata: Dict[str, Any] = {
                "application_name": dg.MetadataValue.text(self.application_name),
                "application_status": dg.MetadataValue.text(detail["ApplicationStatus"]),
                "application_version_id": dg.MetadataValue.int(detail["ApplicationVersionId"]),
                "action": dg.MetadataValue.text(result["action"]),
            }
            if detail.get("ApplicationARN"):
                metadata["application_arn"] = dg.MetadataValue.text(detail["ApplicationARN"])
            if result["operation_id"]:
                metadata["operation_id"] = dg.MetadataValue.text(result["operation_id"])

            return dg.MaterializeResult(metadata=metadata)

        return dg.Definitions(assets=[_managed_flink_application_asset])
