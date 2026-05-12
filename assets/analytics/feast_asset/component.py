import os
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import dagster as dg
from pydantic import Field


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
    if isinstance(partition_values, (list, tuple)):
        _values = [str(v).strip() for v in partition_values if str(v).strip()]
    else:
        _values = [v.strip() for v in (str(partition_values) if partition_values else "").split(",") if v.strip()]
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


class FeastAssetComponent(dg.Component, dg.Model, dg.Resolvable):
    """Materialize features from a source table into a Feast feature store.

    Wraps ``store.materialize_incremental`` (default) or ``store.materialize``
    for a full date-range materialization, depending on the ``incremental``
    flag.  The resulting Dagster asset surfaces feature-view names, end date,
    and materialization mode as asset metadata for observability.
    """

    # --- Feature store config -------------------------------------------------
    feature_store_repo_path: str = Field(
        default=".",
        description=(
            "Path to the Feast repository directory that contains "
            "``feature_store.yaml``."
        ),
    )
    feature_views: Optional[list[str]] = Field(
        default=None,
        description=(
            "Specific feature view names to materialize. "
            "When None all feature views in the store are materialized."
        ),
    )
    incremental: bool = Field(
        default=True,
        description=(
            "When True (default) use ``store.materialize_incremental``, which "
            "materializes from the last materialization timestamp to ``end_date``. "
            "When False use ``store.materialize`` with an explicit start date."
        ),
    )
    start_date_env_var: Optional[str] = Field(
        default=None,
        description=(
            "Name of an environment variable whose value is an ISO-8601 date/datetime "
            "string used as ``start_date`` for non-incremental materialization. "
            "Required when ``incremental`` is False."
        ),
    )
    end_date_offset_days: int = Field(
        default=0,
        description=(
            "Number of days before UTC now to use as ``end_date``. "
            "0 means use the current UTC time."
        ),
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
        default="feast_features",
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

    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
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
            kinds={"feast", "feature_store"},
        )
        def _feast_asset(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
            from feast import FeatureStore

            store = FeatureStore(repo_path=component.feature_store_repo_path)

            end_date = datetime.utcnow() - timedelta(days=component.end_date_offset_days)

            if component.incremental:
                context.log.info(
                    f"Running incremental materialization up to {end_date.isoformat()}"
                )
                store.materialize_incremental(
                    end_date=end_date,
                    feature_views=component.feature_views,
                )
                mode = "incremental"
            else:
                if not component.start_date_env_var:
                    raise ValueError(
                        "start_date_env_var must be set when incremental=False."
                    )
                start_date_str = os.environ.get(component.start_date_env_var)
                if not start_date_str:
                    raise ValueError(
                        f"Environment variable '{component.start_date_env_var}' "
                        "is not set or is empty."
                    )
                start_date = datetime.fromisoformat(start_date_str)
                context.log.info(
                    f"Running full materialization from {start_date.isoformat()} "
                    f"to {end_date.isoformat()}"
                )
                store.materialize(
                    start_date=start_date,
                    end_date=end_date,
                    feature_views=component.feature_views,
                )
                mode = "full"

            feature_views_materialized = component.feature_views or ["<all>"]
            context.log.info(
                f"Feast materialization complete. mode={mode}, "
                f"feature_views={feature_views_materialized}"
            )

            return dg.MaterializeResult(
                metadata={
                    "feature_views": str(feature_views_materialized),
                    "end_date": end_date.isoformat(),
                    "mode": mode,
                    "feature_store_repo_path": component.feature_store_repo_path,
                }
            )

        return dg.Definitions(assets=[_feast_asset])
