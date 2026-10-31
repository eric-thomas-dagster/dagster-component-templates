"""DatabricksCleanRoomNotebookRunComponent.

Submits and runs a Databricks Clean Room notebook as a Dagster asset.

Clean Rooms has NO "submit a run" REST endpoint of its own -- confirmed via
the real Clean Rooms API reference (docs.databricks.com/api/clean-rooms/v1):
it only exposes read-only listing endpoints (`GET .../task-runs`,
`GET .../runs`). A clean room notebook is actually executed the same way any
other notebook is -- through the standard Databricks Jobs API -- using a
dedicated task type, `clean_rooms_notebook_task`
(`databricks.sdk.service.jobs.CleanRoomsNotebookTask`, confirmed directly
against the installed `databricks-sdk`: fields `clean_room_name`,
`notebook_name`, optional `etag` + `notebook_base_parameters`, no cluster
spec needed since it runs inside the clean room's own isolated compute).

Uses the `databricks-sdk` WorkspaceClient (matching this registry's
`databricks_asset_bundle` sibling), not raw REST calls: `client.jobs.submit(
tasks=[SubmitTask(clean_rooms_notebook_task=...)])` then
`client.jobs.wait_get_run_job_terminated_or_skipped(run_id=...)`.
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


def _make_workspace_client(host_env_var: str, token_env_var: str):
    import os
    from databricks.sdk import WorkspaceClient

    host = os.environ.get(host_env_var, "").rstrip("/")
    if not host:
        raise RuntimeError(f"{host_env_var!r} not set.")
    token = os.environ.get(token_env_var)
    if not token:
        raise RuntimeError(f"{token_env_var!r} not set.")
    return WorkspaceClient(host=host, token=token)


def _run_clean_room_notebook(
    client,
    run_name: str,
    clean_room_name: str,
    notebook_name: str,
    etag: Optional[str],
    notebook_base_parameters: Optional[Dict[str, str]],
    timeout_seconds: int,
) -> Dict[str, Any]:
    """Submit the clean room notebook task and poll until terminal. Returns a plain dict."""
    from datetime import timedelta
    from databricks.sdk.service import jobs

    clean_room_task = jobs.CleanRoomsNotebookTask(
        clean_room_name=clean_room_name,
        notebook_name=notebook_name,
        etag=etag,
        notebook_base_parameters=notebook_base_parameters,
    )
    run = client.jobs.submit(
        run_name=run_name,
        tasks=[jobs.SubmitTask(task_key="clean_room_run", clean_rooms_notebook_task=clean_room_task)],
    )
    final_run = client.jobs.wait_get_run_job_terminated_or_skipped(
        run_id=run.run_id, timeout=timedelta(seconds=timeout_seconds),
    )

    state = final_run.state
    return {
        "run_id": final_run.run_id,
        "run_page_url": final_run.run_page_url,
        "life_cycle_state": state.life_cycle_state.value if state and state.life_cycle_state else None,
        "result_state": state.result_state.value if state and state.result_state else None,
        "state_message": state.state_message if state else None,
    }


class DatabricksCleanRoomNotebookRunComponent(dg.Component, dg.Model, dg.Resolvable):
    """Run a Databricks Clean Room notebook as a Dagster asset.

    Clean Rooms has no dedicated "submit a run" REST endpoint -- a clean room
    notebook runs through the standard Databricks Jobs API with a
    `clean_rooms_notebook_task`, which requires no cluster (it runs inside
    the clean room's own isolated compute). The notebook must already exist
    in the clean room and already be approved by its collaborators, and this
    component's caller must be the clean room's owner or hold the
    `EXECUTE CLEAN ROOM TASK` privilege.
    """

    asset_name: str = Field(description="Dagster asset key.")
    host_env_var: str = Field(default="DATABRICKS_HOST", description="Env var with the workspace URL (`https://<workspace>.cloud.databricks.com`).")
    token_env_var: str = Field(default="DATABRICKS_TOKEN", description="Env var with a Databricks PAT or OAuth bearer token.")

    clean_room_name: str = Field(description="The clean room the notebook belongs to.")
    notebook_name: str = Field(description="Name of the notebook to run, as it appears in the clean room.")
    etag: Optional[str] = Field(default=None, description="Checksum confirming the notebook being run is the latest version. Omit to always run the current version.")
    notebook_base_parameters: Optional[Dict[str, str]] = Field(default=None, description="Base parameters passed to the clean room notebook job.")
    run_name: Optional[str] = Field(default=None, description="Databricks run name. Defaults to the asset name.")

    timeout_seconds: int = Field(default=1200, ge=1, description="Max time to wait for the run to reach a terminal state.")
    fail_on_run_failure: bool = Field(default=True, description="Raise if the run's result_state isn't SUCCESS. False materializes anyway with the result in metadata.")

    group_name: Optional[str] = Field(default=None, description="Dagster asset group name.")
    description: Optional[str] = Field(default=None, description="Asset description.")
    owners: Optional[List[str]] = Field(default=None, description="Asset owners.")
    asset_tags: Optional[Union[Dict[str, Any], List[str]]] = Field(default=None, description="Extra asset tags.")
    kinds: Optional[List[str]] = Field(default=None, description="Defaults to ['databricks', 'clean_rooms'].")
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
        _self = self
        partitions_def = _build_partitions_def(
            self.partition_type, self.partition_start, self.partition_values,
            self.dynamic_partition_name, self.partition_dimensions,
            cron_schedule=self.partition_cron_schedule,
            cron_fmt=self.partition_cron_fmt or "%Y-%m-%d", end_offset=self.partition_end_offset, timezone=self.partition_timezone,
        )

        _inferred_kinds = list(self.kinds or ["databricks", "clean_rooms"])
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
            description=self.description or f"Run Databricks Clean Room notebook {self.notebook_name!r} in {self.clean_room_name!r}.",
            owners=self.owners or [],
            tags=_all_tags,
            kinds=set(_inferred_kinds),
            deps=[dg.AssetKey.from_user_string(d) for d in (self.deps or [])],
            partitions_def=partitions_def,
            freshness_policy=_freshness_policy,
            retry_policy=_retry_policy,
        )
        def _databricks_clean_room_notebook_run(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
            self = _self
            client = _make_workspace_client(self.host_env_var, self.token_env_var)
            result = _run_clean_room_notebook(
                client,
                self.run_name or self.asset_name,
                self.clean_room_name,
                self.notebook_name,
                self.etag,
                self.notebook_base_parameters,
                self.timeout_seconds,
            )

            context.log.info(
                f"databricks_clean_room_notebook_run: run_id={result['run_id']} "
                f"life_cycle_state={result['life_cycle_state']!r} result_state={result['result_state']!r}"
            )

            if self.fail_on_run_failure and result["result_state"] != "SUCCESS":
                raise RuntimeError(
                    f"Clean room notebook run {result['run_id']} did not succeed "
                    f"(life_cycle_state={result['life_cycle_state']!r}, result_state={result['result_state']!r}): "
                    f"{result['state_message']}"
                )

            metadata: Dict[str, Any] = {
                "run_id": dg.MetadataValue.int(result["run_id"]),
                "clean_room_name": dg.MetadataValue.text(self.clean_room_name),
                "notebook_name": dg.MetadataValue.text(self.notebook_name),
            }
            if result["life_cycle_state"]:
                metadata["life_cycle_state"] = dg.MetadataValue.text(result["life_cycle_state"])
            if result["result_state"]:
                metadata["result_state"] = dg.MetadataValue.text(result["result_state"])
            if result["state_message"]:
                metadata["state_message"] = dg.MetadataValue.text(result["state_message"])
            if result["run_page_url"]:
                metadata["run_page_url"] = dg.MetadataValue.url(result["run_page_url"])

            return dg.MaterializeResult(metadata=metadata)

        return dg.Definitions(assets=[_databricks_clean_room_notebook_run])
