import os
from typing import Any, Dict, List, Optional

import dagster as dg
from pydantic import Field


@dg.definitions
def _build_partitions_def(
    partition_type,
    partition_start,
    partition_values,
    dynamic_partition_name,
    partition_dimensions,
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
        if t in ("daily", "weekly", "monthly", "hourly") and not spec.get("start"):
            raise ValueError(f"partition dimension type={t!r} requires 'start' (ISO date)")
        if t == "daily":
            return DailyPartitionsDefinition(start_date=spec["start"])
        if t == "weekly":
            return WeeklyPartitionsDefinition(start_date=spec["start"])
        if t == "monthly":
            return MonthlyPartitionsDefinition(start_date=spec["start"])
        if t == "hourly":
            return HourlyPartitionsDefinition(start_date=spec["start"])
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
    _values = [v.strip() for v in (partition_values or "").split(",") if v.strip()]
    if partition_type in ("daily", "weekly", "monthly", "hourly") and not partition_start:
        raise ValueError(
            f"partition_type={partition_type!r} requires partition_start (ISO date, e.g. '2024-01-01')."
        )
    if partition_type == "daily":
        return DailyPartitionsDefinition(start_date=partition_start)
    if partition_type == "weekly":
        return WeeklyPartitionsDefinition(start_date=partition_start)
    if partition_type == "monthly":
        return MonthlyPartitionsDefinition(start_date=partition_start)
    if partition_type == "hourly":
        return HourlyPartitionsDefinition(start_date=partition_start)
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
            "date": DailyPartitionsDefinition(start_date=partition_start),
            "static_dim": StaticPartitionsDefinition(_values),
        })
    raise ValueError(f"unknown partition_type: {partition_type!r}")


class TectonAssetComponent(dg.Component, dg.Model, dg.Resolvable):
    """Trigger Tecton feature view materialization jobs via the Tecton SDK.

    Authenticates against a Tecton workspace using an API key, then triggers
    online and/or offline materialization jobs for the specified feature views
    (or all views in the workspace when none are listed).  Job IDs are logged
    and the total count of triggered jobs is surfaced as asset metadata.
    """

    # --- Tecton connection config ---------------------------------------------
    workspace: str = Field(
        description="Tecton workspace name (e.g. 'production', 'staging')."
    )
    tecton_api_key_env_var: str = Field(
        default="TECTON_API_KEY",
        description="Name of the environment variable holding the Tecton API key.",
    )
    tecton_url_env_var: Optional[str] = Field(
        default=None,
        description=(
            "Name of the environment variable holding the Tecton cluster URL. "
            "Defaults to https://app.tecton.ai when not set."
        ),
    )

    # --- Feature view selection -----------------------------------------------
    feature_views: Optional[list[str]] = Field(
        default=None,
        description=(
            "Specific feature view names to trigger materialization for. "
            "When None all feature views in the workspace are triggered."
        ),
    )

    # --- Materialization targets -----------------------------------------------
    online: bool = Field(
        default=True,
        description="Materialize to the online store.",
    )
    offline: bool = Field(
        default=True,
        description="Materialize to the offline store.",
    )

    # --- Asset metadata -------------------------------------------------------
    group_name: Optional[str] = Field(
        default="feature_store",
        description="Dagster asset group name.",
    )
    partition_type: Optional[str] = Field(
        default=None,
        description="Partition type: 'daily', 'weekly', 'monthly', 'hourly', 'static', 'multi', or None for unpartitioned",
    )
    partition_start: Optional[str] = Field(
        default=None,
        description="Partition start date in ISO format, e.g. '2024-01-01'. Required for time-based partition types.",
    )
    partition_date_column: Optional[str] = Field(
        default=None,
        description="Column used to filter upstream DataFrame to the current date partition key.",
    )
    dynamic_partition_name: Optional[str] = Field(
        default=None,
        description="Name for DynamicPartitionsDefinition (when partition_type='dynamic'), e.g. 'tenants'.",
    )

    partition_dimensions: Optional[List[Dict[str, Any]]] = Field(
        default=None,
        description="Multi-axis partition spec: list of {name, type, start, values, dynamic_partition_name} dicts. Overrides flat fields when set.",
    )

    partition_values: Optional[str] = Field(
        default=None,
        description="Comma-separated values for static or multi partitioning, e.g. 'customer_a,customer_b,customer_c'.",
    )
    partition_static_dim: Optional[str] = Field(
        default=None,
        description="Dimension name for the static axis in multi-partitioning, e.g. 'customer' or 'region'.",
    )
    partition_static_column: Optional[str] = Field(
        default=None,
        description="Column used to filter upstream DataFrame to the current static partition dimension (e.g. 'customer_id').",
    )
    asset_name: str = Field(
        default="tecton_features",
        description="Dagster asset key for this component.",
    )
    deps: Optional[list[str]] = Field(
        default=None,
        description="Upstream asset keys for lineage.",
    )

    # -------------------------------------------------------------------------
    # build_defs
    # -------------------------------------------------------------------------

    retry_policy_max_retries: Optional[int] = Field(

        default=None,

        description="Max retries on asset failure. Defines a RetryPolicy. Useful for transient network failures, rate limits, etc.",

    )

    retry_policy_delay_seconds: Optional[int] = Field(

        default=None,

        description="Seconds between retries (default 1).",

    )

    retry_policy_backoff: str = Field(

        default="exponential",

        description="Backoff strategy: 'linear' or 'exponential'.",

    )



    description: Optional[str] = Field(
        default=None,
        description="Asset description shown in the Dagster catalog.",
    )

    owners: Optional[List[str]] = Field(
        default=None,
        description="Asset owners — team names ('team:analytics') or email addresses.",
    )

    asset_tags: Optional[Dict[str, str]] = Field(
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
        description="Column-level lineage: output column → list of upstream columns it derives from, e.g. {'revenue': ['price', 'quantity']}.",
    )

    def build_defs(self, load_context: dg.ComponentLoadContext) -> dg.Definitions:
        component = self

        dep_keys = [dg.AssetKey.from_user_string(k) for k in (component.deps or [])]


        # Build partition definition (auto-generated; supports daily, weekly, monthly,

        # hourly partitions out of the box).
        partitions_def = _build_partitions_def(
            self.partition_type,
            self.partition_start,
            self.partition_values,
            self.dynamic_partition_name,
            self.partition_dimensions,
        )


        # Build retry policy (auto-generated; opt-in via retry_policy_max_retries).


        _retry_policy = None


        if self.retry_policy_max_retries is not None:


            from dagster import Backoff, RetryPolicy


            _retry_policy = RetryPolicy(


                max_retries=self.retry_policy_max_retries,


                delay=self.retry_policy_delay_seconds or 1,


                backoff=Backoff[self.retry_policy_backoff.upper()],


            )



        @dg.asset(retry_policy=_retry_policy, partitions_def=partitions_def, 
            name=component.asset_name,
            group_name=component.group_name,
            deps=dep_keys,
            kinds={"tecton", "feature_store"},
        )
        def _tecton_asset(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
            import tecton

            api_key = os.environ.get(component.tecton_api_key_env_var)
            if not api_key:
                raise ValueError(
                    f"Environment variable '{component.tecton_api_key_env_var}' "
                    "is not set or is empty."
                )

            tecton_url = "https://app.tecton.ai"
            if component.tecton_url_env_var:
                url_from_env = os.environ.get(component.tecton_url_env_var)
                if url_from_env:
                    tecton_url = url_from_env

            context.log.info(
                f"Authenticating with Tecton at {tecton_url} "
                f"(workspace: {component.workspace})"
            )
            tecton.set_credentials(tecton_url=tecton_url, tecton_api_key=api_key)

            ws = tecton.get_workspace(component.workspace)

            if component.feature_views is not None:
                fv_names = component.feature_views
            else:
                context.log.info(
                    "No feature_views specified — fetching all feature views "
                    f"from workspace '{component.workspace}'."
                )
                fv_names = [fv.name for fv in ws.list_feature_views()]

            context.log.info(
                f"Triggering materialization for {len(fv_names)} feature view(s): "
                f"{fv_names}"
            )

            triggered_jobs = []
            for fv_name in fv_names:
                fv = ws.get_feature_view(fv_name)
                job = fv.run_materialization_job(
                    online=component.online,
                    offline=component.offline,
                )
                context.log.info(
                    f"Triggered materialization job {job.id} for feature view '{fv_name}'"
                )
                triggered_jobs.append({"feature_view": fv_name, "job_id": job.id})

            return dg.MaterializeResult(
                metadata={
                    "workspace": component.workspace,
                    "feature_views_count": len(fv_names),
                    "feature_views": str(fv_names),
                    "online": component.online,
                    "offline": component.offline,
                    "triggered_jobs": str(triggered_jobs),
                }
            )

        return dg.Definitions(assets=[_tecton_asset])
