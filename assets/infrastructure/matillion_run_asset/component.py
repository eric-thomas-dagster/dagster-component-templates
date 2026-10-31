"""Matillion Run Asset Component.

Triggers a Matillion ETL job on demand and waits for it to complete.
Dagster owns the schedule; Matillion executes the transformation.

Includes MatillionResource for shared connection config across components.

Matillion ETL REST API: https://{instance}/rest/v1
"""
import time
from typing import Dict, List, Literal, Optional, Type, Union
import dagster as dg
from dagster import AssetExecutionContext, ConfigurableResource, MaterializeResult
from pydantic import Field


class MatillionResource(ConfigurableResource):
    """Resource for connecting to the Matillion ETL REST API.

    Example:
        ```python
        MatillionResource(
            instance_url=EnvVar("MATILLION_INSTANCE_URL"),
            username=EnvVar("MATILLION_USERNAME"),
            password=EnvVar("MATILLION_PASSWORD"),
        )
        ```
    """

    instance_url: str = Field(description="Matillion instance URL (e.g. https://matillion.mycompany.com)")
    username: str = Field(description="Matillion username")
    password: str = Field(description="Matillion password")

    def _headers(self) -> dict:
        import base64
        credentials = base64.b64encode(f"{self.username}:{self.password}".encode()).decode()
        return {"Authorization": f"Basic {credentials}", "Accept": "application/json"}

    def run_job(self, project: str, version: str, job_name: str, variables: dict | None = None) -> int:
        """Trigger a job run and return the run ID."""
        import requests
        base = self.instance_url.rstrip("/")
        resp = requests.post(
            f"{base}/rest/v1/project/name/{project}/version/name/{version}/job/name/{job_name}/run",
            headers={**self._headers(), "Content-Type": "application/json"},
            json={"variables": variables or {}},
            timeout=30,
        )
        resp.raise_for_status()
        return resp.json().get("id", resp.json())  # returns run ID (int)

    def get_run_status(self, project: str, version: str, job_name: str, run_id: int) -> dict:
        """Return status dict for a specific run."""
        import requests
        base = self.instance_url.rstrip("/")
        resp = requests.get(
            f"{base}/rest/v1/project/name/{project}/version/name/{version}/job/name/{job_name}/run/id/{run_id}",
            headers=self._headers(),
            timeout=30,
        )
        resp.raise_for_status()
        return resp.json()

    def get_latest_run(self, project: str, version: str, job_name: str) -> dict | None:
        """Return the most recent run for a job."""
        import requests
        base = self.instance_url.rstrip("/")
        resp = requests.get(
            f"{base}/rest/v1/project/name/{project}/version/name/{version}/job/name/{job_name}/runs",
            headers=self._headers(),
            params={"limit": 1},
            timeout=30,
        )
        resp.raise_for_status()
        runs = resp.json()
        if isinstance(runs, list) and runs:
            return runs[0]
        return runs.get("runs", [None])[0] if isinstance(runs, dict) else None



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


class MatillionRunAssetComponent(dg.Component, dg.Model, dg.Resolvable):
    """Trigger a Matillion ETL job on demand and surface results as a Dagster asset.

    Example (env vars):
        ```yaml
        type: dagster_community_components.MatillionRunAssetComponent
        attributes:
          asset_key: matillion/transforms/load_orders
          project: MyProject
          version: default
          job_name: Load Orders
          instance_url_env_var: MATILLION_INSTANCE_URL
          username_env_var: MATILLION_USERNAME
          password_env_var: MATILLION_PASSWORD
        ```

    Example (resource):
        ```yaml
        type: dagster_community_components.MatillionRunAssetComponent
        attributes:
          asset_key: matillion/transforms/load_orders
          project: MyProject
          version: default
          job_name: Load Orders
          resource_key: matillion
        ```
    """

    asset_key: str = Field(description="Dagster asset key (e.g. 'matillion/transforms/load_orders')")
    project: str = Field(description="Matillion project name")
    version: str = Field(default="default", description="Matillion project version name")
    job_name: str = Field(description="Matillion job name to run")
    instance_url_env_var: Optional[str] = Field(default=None, description="Env var with Matillion instance URL")
    username_env_var: Optional[str] = Field(default=None, description="Env var with Matillion username")
    password_env_var: Optional[str] = Field(default=None, description="Env var with Matillion password")
    resource_key: Optional[str] = Field(default=None, description="Key of a MatillionResource")
    variables: Optional[dict] = Field(default=None, description="Job variable overrides passed at runtime")
    poll_interval_seconds: float = Field(default=10.0, description="Seconds between status polls")
    timeout_seconds: int = Field(default=3600, description="Max seconds to wait for job completion")
    group_name: Optional[str] = Field(default="matillion", description="Dagster asset group name")
    deps: Optional[list[str]] = Field(default=None, description="Upstream asset keys this asset depends on (e.g. ['raw_orders', 'schema/asset'])")


    description: Optional[str] = Field(
        default=None,
        description="Asset description shown in the Dagster catalog.",
    )

    owners: Optional[List[str]] = Field(
        default=None,
        description="Asset owners — team names ('team:analytics') or email addresses.",
    )

    asset_tags: Optional[Union[Dict[str, Any], List[str]]] = Field(
        default=None,
        description="Additional key-value tags applied to the asset in the Dagster catalog.",
    )

    kinds: Optional[List[str]] = Field(
        default=None,
        description="Asset kinds for the catalog (e.g. ['snowflake', 'python']). Auto-inferred from component name when unset.",
    )

    freshness_max_lag_minutes: Optional[int] = Field(
        default=None,
        description="Maximum acceptable lag in minutes before the asset is considered stale. Builds a FreshnessPolicy when set.",
    )

    freshness_cron: Optional[str] = Field(
        default=None,
        description="Cron schedule string for the freshness policy, e.g. '0 9 * * 1-5' (weekdays 9am).",
    )

    column_lineage: Optional[Dict[str, List[str]]] = Field(
        default=None,
        description="Column-level lineage, emitted as `dagster/column_lineage` metadata on every materialization: {output column: [upstream columns]}, e.g. {'revenue': ['price', 'quantity']}. The upstream asset is `upstream_asset_key`, or the only entry of `deps`; with several upstream assets, name each column '<asset_key>:<column>'. Ignored with a warning when no upstream asset can be resolved.",
    )

    partition_type: Optional[str] = Field(
        default=None,
        description="Partition type: 'daily', 'weekly', 'monthly', 'hourly', 'static', 'multi', or None for unpartitioned.",
    )

    partition_cron_schedule: Optional[str] = Field(
        default=None,
        description=(
            "Required when partition_type='cron'. A cron expression restricting the date axis to a "
            "custom cadence, e.g. '0 0 * * 1-5' for weekdays only (business-day cadence). Built on "
            "Dagster's TimeWindowPartitionsDefinition(cron_schedule=...)."
        ),
    )
    partition_cron_fmt: Optional[str] = Field(
        default="%Y-%m-%d",
        description="strftime format for partition keys produced by partition_type='cron'. Only change this for a sub-daily cron_schedule.",
    )
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

    partition_start: Optional[str] = Field(
        default=None,
        description="Partition start date in ISO format (e.g. '2024-01-01'). Required for time-based partition types.",
    )

    partition_date_column: Optional[Union[str, int]] = Field(
        default=None,
        description="Not used by this component, which has no input rows to filter. Use {partition_key}, {partition_date}, {partition_start} and {partition_end} in `variables` instead.",
    )

    partition_values: Optional[Union[str, List[str]]] = Field(
        default=None,
        description="Comma-separated values for static or multi partitioning, e.g. 'acme,globex,initech'.",
    )

    partition_static_dim: Optional[str] = Field(
        default=None,
        description="Dimension name for the static axis in multi-partitioning, e.g. 'customer'.",
    )

    partition_static_column: Optional[Union[str, int]] = Field(
        default=None,
        description="Not used by this component, which has no input rows to filter. Use {partition_key}, {partition_date}, {partition_start} and {partition_end} in `variables` instead.",
    )

    retry_policy_max_retries: Optional[int] = Field(
        default=None,
        description="Max retries on failure. Defines a RetryPolicy when set.",
    )

    retry_policy_delay_seconds: Optional[int] = Field(
        default=None,
        description="Seconds between retries (default 1).",
    )

    retry_policy_backoff: str = Field(
        default="exponential",
        description="Backoff strategy: 'linear' or 'exponential'.",
    )

    backfill_policy: Optional[Literal["single_run", "multi_run"]] = Field(
        default=None,
        description=(
            "Backfill execution strategy for a partitioned asset. 'single_run' "
            "fires ONE external call covering the whole backfill range -- the "
            "partition_key/partition_start/partition_end tokens this component "
            "already supports are comma-joined/range-spanning rather than a "
            "single key -- instead of one call per partition. 'multi_run' "
            "launches one run per partition (optionally capped via "
            "backfill_policy_max_partitions_per_run). Leave unset for Dagster's "
            "default (one run per partition, uncapped). Ignored when "
            "partition_type is not set."
        ),
    )
    backfill_policy_max_partitions_per_run: Optional[int] = Field(
        default=None,
        description="For backfill_policy='multi_run' only: maximum partitions materialized per run. Leave unset for Dagster's default (1 partition per run).",
    )

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=('deps', 'owners', 'kinds'), csv=('partition_values',), tags=('asset_tags',))

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        # freshness_max_lag_minutes / freshness_cron -> FreshnessPolicy (applied to the asset(s) below)
        # retry_policy_* settings -> RetryPolicy (applied to the asset(s) below)
        _column_lineage_md = _column_lineage_metadata(self)
        partitions_def = _build_partitions_def(
            self.partition_type,
            self.partition_start,
            self.partition_values,
            None,  # dynamic_partition_name: not a field of this component
            None,  # partition_dimensions: not a field of this component
        
    cron_schedule=self.partition_cron_schedule,
    cron_fmt=self.partition_cron_fmt or "%Y-%m-%d", end_offset=self.partition_end_offset, timezone=self.partition_timezone,
)
        partition_date_column = self.partition_date_column
        partition_static_column = self.partition_static_column
        partition_static_dim = self.partition_static_dim

        _retry_policy_obj = None
        if self.retry_policy_max_retries is not None:
            from dagster import Backoff, RetryPolicy
        
            _retry_policy_obj = RetryPolicy(
                max_retries=self.retry_policy_max_retries,
                delay=self.retry_policy_delay_seconds or 1,
                backoff=Backoff[self.retry_policy_backoff.upper()],
            )
        _freshness_policy_obj = (
            _freshness_policy_compat(self.freshness_max_lag_minutes, self.freshness_cron)
            if self.freshness_max_lag_minutes is not None else None
        )
        _self = self

        backfill_policy = None
        if partitions_def is not None and self.backfill_policy == "single_run":
            backfill_policy = dg.BackfillPolicy.single_run()
        elif partitions_def is not None and self.backfill_policy == "multi_run":
            backfill_policy = dg.BackfillPolicy.multi_run(
                max_partitions_per_run=self.backfill_policy_max_partitions_per_run or 1
            )

        @dg.asset(partitions_def=partitions_def, retry_policy=_retry_policy_obj, freshness_policy=_freshness_policy_obj, backfill_policy=backfill_policy, 
            key=dg.AssetKey(self.asset_key.split("/")),
            description=self.description or (f"Run Matillion job '{self.job_name}' in project {self.project}"),
            group_name=self.group_name,
            kinds={"matillion"},
            required_resource_keys={self.resource_key} if self.resource_key else set(),
            deps=[dg.AssetKey.from_user_string(k) for k in (self.deps or [])],
        )
        def matillion_run_asset(context: AssetExecutionContext) -> MaterializeResult:
            if _column_lineage_md:
                context.add_output_metadata(_column_lineage_md)
            _tokens = _partition_vars(context)
            import os, base64, requests

            if _self.resource_key:
                resource: MatillionResource = getattr(context.resources, _self.resource_key)
                run_id = resource.run_job(_self.project, _self.version, _self.job_name, _substitute_partition(_self.variables, _tokens))
            else:
                instance_url = os.environ.get(_self.instance_url_env_var or "", "").rstrip("/")
                username = os.environ.get(_self.username_env_var or "", "")
                password = os.environ.get(_self.password_env_var or "", "")
                credentials = base64.b64encode(f"{username}:{password}".encode()).decode()
                headers = {"Authorization": f"Basic {credentials}", "Content-Type": "application/json", "Accept": "application/json"}
                resp = requests.post(
                    f"{instance_url}/rest/v1/project/name/{_self.project}/version/name/{_self.version}/job/name/{_self.job_name}/run",
                    headers=headers,
                    json={"variables": _substitute_partition(_self.variables, _tokens) or {}},
                    timeout=30,
                )
                resp.raise_for_status()
                run_id = resp.json().get("id", resp.json())

            context.log.info(f"Matillion job triggered. project={_self.project} job={_self.job_name} run_id={run_id}")

            elapsed = 0.0
            while elapsed < _self.timeout_seconds:
                time.sleep(_self.poll_interval_seconds)
                elapsed += _self.poll_interval_seconds
                try:
                    if _self.resource_key:
                        resource = getattr(context.resources, _self.resource_key)
                        status_data = resource.get_run_status(_self.project, _self.version, _self.job_name, run_id)
                    else:
                        instance_url = os.environ.get(_self.instance_url_env_var or "", "").rstrip("/")
                        credentials = base64.b64encode(
                            f"{os.environ.get(_self.username_env_var or '', '')}:{os.environ.get(_self.password_env_var or '', '')}".encode()
                        ).decode()
                        resp = requests.get(
                            f"{instance_url}/rest/v1/project/name/{_self.project}/version/name/{_self.version}/job/name/{_self.job_name}/run/id/{run_id}",
                            headers={"Authorization": f"Basic {credentials}", "Accept": "application/json"},
                            timeout=30,
                        )
                        resp.raise_for_status()
                        status_data = resp.json()
                    status = (status_data.get("state") or status_data.get("status") or "").upper()
                    context.log.info(f"Run {run_id} status: {status}")
                except Exception as e:
                    context.log.warning(f"Poll error: {e}")
                    continue

                if status == "SUCCESS":
                    return MaterializeResult(metadata={
                        "run_id": run_id,
                        "project": _self.project,
                        "job_name": _self.job_name,
                        "status": status,
                        "start_time": status_data.get("startTime", ""),
                        "end_time": status_data.get("endTime", ""),
                        "row_count": status_data.get("rowCount", 0),
                    })
                elif status in ("FAILED", "CANCELLED", "ABORTED"):
                    raise Exception(f"Matillion run {run_id} {status}. message={status_data.get('message', '')}")

            raise Exception(f"Matillion run {run_id} timed out after {_self.timeout_seconds}s")

        return dg.Definitions(assets=[matillion_run_asset])


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


# BEGIN partitions-def builder (inlined per component; keep copies identical, see tests/test_partition_port.py)
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
# END partitions-def builder


# BEGIN partition-vars helper (inlined per component; keep copies identical, see tests/test_partition_vars.py)
def _partition_vars(context):
    """Tokens a partitioned run can use in its variables: {partition_key}, and for time partitions {partition_date}
    (YYYY-MM-DD), {partition_start} and {partition_end} (ISO datetimes); for a multi-partition also
    {partition_<dimension>}. Empty for an unpartitioned run."""
    import re as _re

    if not (context.has_partition_key or context.has_partition_key_range):
        return {}
    keys = list(context.partition_keys)
    tokens = {"partition_key": ",".join(str(k) for k in keys)}
    try:
        window = context.partition_time_window
        tokens["partition_start"] = window.start.strftime("%Y-%m-%dT%H:%M:%S")
        tokens["partition_end"] = window.end.strftime("%Y-%m-%dT%H:%M:%S")
        tokens["partition_date"] = window.start.strftime("%Y-%m-%d")
    except Exception:
        pass  # static / dynamic partitions have no time window
    if len(keys) == 1 and hasattr(keys[0], "keys_by_dimension"):
        for dimension, value in keys[0].keys_by_dimension.items():
            tokens["partition_" + _re.sub(r"\W", "_", str(dimension))] = str(value)
    return tokens


def _partition_env(tokens):
    """DAGSTER_PARTITION_* environment variables for a partitioned run (empty when unpartitioned)."""
    if not tokens:
        return {}
    env = {"DAGSTER_PARTITION_KEY": tokens["partition_key"]}
    for token, name in (("partition_date", "DAGSTER_PARTITION_DATE"), ("partition_start", "DAGSTER_PARTITION_START"),
                        ("partition_end", "DAGSTER_PARTITION_END")):
        if token in tokens:
            env[name] = tokens[token]
    return env


def _substitute_partition(value, tokens):
    """Replace {partition_*} tokens inside strings, recursively through dicts and lists. Only known tokens are
    replaced, so other braces (JSON, shell, templates) are left alone. A no-op for an unpartitioned run."""
    import re as _re

    if not tokens:
        return value
    if isinstance(value, str):
        return _re.sub(r"\{(partition_\w+)\}", lambda m: tokens.get(m.group(1), m.group(0)), value)
    if isinstance(value, dict):
        return {k: _substitute_partition(v, tokens) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return type(value)(_substitute_partition(v, tokens) for v in value)
    return value
# END partition-vars helper

# BEGIN column-lineage helper (inlined per component; keep copies identical, see tests/test_column_lineage.py)
def _column_lineage_metadata(component):
    """`dagster/column_lineage` metadata from the component's `column_lineage` mapping ({output column: [upstream columns]}).

    The upstream asset is `upstream_asset_key`, or the only entry of `deps`. With several upstream assets, name each
    column as "<asset_key>:<column>". Returns {} when the mapping is unset, or (with a warning) when no upstream asset
    can be resolved, since lineage needs an upstream asset to point at."""
    import warnings

    import dagster as _dg

    mapping = getattr(component, "column_lineage", None)
    if not mapping:
        return {}
    ups = getattr(component, "upstream_asset_key", None) or getattr(component, "deps", None) or []
    ups = [ups] if isinstance(ups, (str, _dg.AssetKey)) else list(ups)
    ups = [u.to_user_string() if isinstance(u, _dg.AssetKey) else str(u) for u in ups]
    deps = {}
    for out_col, in_cols in mapping.items():
        refs = []
        for ic in [in_cols] if isinstance(in_cols, str) else in_cols:
            key, _, col = str(ic).rpartition(":")
            if key in ups and col:
                refs.append(_dg.TableColumnDep(asset_key=_dg.AssetKey.from_user_string(key), column_name=col))
            elif len(ups) == 1:
                refs.append(_dg.TableColumnDep(asset_key=_dg.AssetKey.from_user_string(ups[0]), column_name=str(ic)))
            else:
                warnings.warn(
                    f"column_lineage ignored: cannot tell which upstream asset {ic!r} belongs to "
                    f"(upstream assets: {ups or 'none'}; use '<asset_key>:<column>' with several upstream assets)"
                )
                return {}
        deps[str(out_col)] = refs
    return {"dagster/column_lineage": _dg.MetadataValue.column_lineage(_dg.TableColumnLineage(deps))}
# END column-lineage helper
