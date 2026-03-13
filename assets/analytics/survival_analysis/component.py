"""Survival Analysis.

Estimate survival functions using Kaplan-Meier or Cox proportional hazards model.
"""
from dataclasses import dataclass
from typing import List, Optional

import pandas as pd
from dagster import (
    AssetExecutionContext,
    AssetIn,
    AssetKey,
    Component,
    ComponentLoadContext,
    Definitions,
    MetadataValue,
    Model,
    Resolvable,
    asset,
)
from pydantic import Field


@dataclass
class SurvivalAnalysisComponent(Component, Model, Resolvable):
    """Estimate survival functions using Kaplan-Meier or Cox proportional hazards model."""

    asset_name: str = Field(description="Output Dagster asset name")
    upstream_asset_key: str = Field(description="Upstream asset key providing a DataFrame")
    duration_column: str = Field(description="Time-to-event column")
    event_column: str = Field(description="Binary event indicator (1=event occurred, 0=censored)")
    group_column: Optional[str] = Field(default=None, description="Column for grouped Kaplan-Meier analysis")
    method: str = Field(default="kaplan_meier", description="'kaplan_meier' or 'cox'")
    covariate_columns: Optional[List[str]] = Field(
        default=None,
        description="Covariate columns for Cox model (required when method='cox')",
    )
    time_points: Optional[List[float]] = Field(
        default=None,
        description="Specific time points at which to evaluate survival probability",
    )
    group_name: Optional[str] = Field(default=None, description="Dagster asset group name")
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

    @classmethod
    def get_description(cls) -> str:
        return "Estimate survival functions using Kaplan-Meier or Cox proportional hazards model."

    def build_defs(self, load_context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        upstream_asset_key = self.upstream_asset_key
        duration_column = self.duration_column
        event_column = self.event_column
        group_column = self.group_column
        method = self.method
        covariate_columns = self.covariate_columns
        time_points = self.time_points
        group_name = self.group_name

        # Build partition definition
        partitions_def = None
        if self.partition_type:
            from dagster import (
                DailyPartitionsDefinition, WeeklyPartitionsDefinition,
                MonthlyPartitionsDefinition, HourlyPartitionsDefinition,
                StaticPartitionsDefinition, MultiPartitionsDefinition,
            )
            _start = self.partition_start or "2020-01-01"
            _values = [v.strip() for v in (self.partition_values or "").split(",") if v.strip()]
            if self.partition_type == "daily":
                partitions_def = DailyPartitionsDefinition(start_date=_start)
            elif self.partition_type == "weekly":
                partitions_def = WeeklyPartitionsDefinition(start_date=_start)
            elif self.partition_type == "monthly":
                partitions_def = MonthlyPartitionsDefinition(start_date=_start)
            elif self.partition_type == "hourly":
                partitions_def = HourlyPartitionsDefinition(start_date=_start)
            elif self.partition_type == "static":
                partitions_def = StaticPartitionsDefinition(_values)
            elif self.partition_type == "multi":
                _dim = self.partition_static_dim or "segment"
                partitions_def = MultiPartitionsDefinition({
                    "date": DailyPartitionsDefinition(start_date=_start),
                    _dim: StaticPartitionsDefinition(_values),
                })
        partition_type = self.partition_type
        partition_date_column = self.partition_date_column
        partition_static_column = self.partition_static_column
        partition_static_dim = self.partition_static_dim

        @asset(
            name=asset_name,
            ins={"upstream": AssetIn(key=AssetKey.from_user_string(upstream_asset_key))},
            partitions_def=partitions_def,
            group_name=group_name,
        )
        def _asset(context: AssetExecutionContext, upstream: pd.DataFrame) -> pd.DataFrame:
            # Filter to current partition if partitioned
            if context.has_partition_key:
                _pk = context.partition_key
                _is_multi = hasattr(_pk, "keys_by_dimension")
                _date_key = _pk.keys_by_dimension.get("date", "") if _is_multi else str(_pk)
                _static_key = _pk.keys_by_dimension.get(partition_static_dim or "segment", "") if _is_multi else None
                if partition_date_column and partition_date_column in upstream.columns and _date_key:
                    upstream = upstream[upstream[partition_date_column].astype(str) == _date_key]
                if partition_static_column and partition_static_column in upstream.columns and _static_key:
                    upstream = upstream[upstream[partition_static_column].astype(str) == _static_key]
                elif partition_static_column and partition_static_column in upstream.columns and not _is_multi:
                    upstream = upstream[upstream[partition_static_column].astype(str) == str(_pk)]
            try:
                from lifelines import KaplanMeierFitter, CoxPHFitter
            except ImportError as e:
                raise ImportError("lifelines is required: pip install lifelines") from e

            if method == "kaplan_meier":
                kmf = KaplanMeierFitter()
                if group_column:
                    results = []
                    for grp, gdf in upstream.groupby(group_column):
                        kmf.fit(gdf[duration_column], gdf[event_column], label=str(grp))
                        sf = kmf.survival_function_.reset_index()
                        sf.columns = ["timeline", "survival_probability"]
                        sf[group_column] = grp
                        if time_points:
                            sf = sf[sf["timeline"].isin(time_points)]
                        results.append(sf)
                    result = pd.concat(results, ignore_index=True)
                else:
                    kmf.fit(upstream[duration_column], upstream[event_column])
                    result = kmf.survival_function_.reset_index()
                    result.columns = ["timeline", "survival_probability"]
                    if time_points:
                        result = result[result["timeline"].isin(time_points)]

                median_survival = float(kmf.median_survival_time_) if not group_column else float("nan")
                context.add_output_metadata({
                    "method": MetadataValue.text("kaplan_meier"),
                    "n_subjects": MetadataValue.int(len(upstream)),
                    "n_events": MetadataValue.int(int(upstream[event_column].sum())),
                    "median_survival": MetadataValue.float(median_survival),
                })
                return result

            elif method == "cox":
                cph = CoxPHFitter()
                cols = [duration_column, event_column] + (covariate_columns or [])
                cph.fit(upstream[cols], duration_col=duration_column, event_col=event_column)
                result = cph.summary.reset_index()
                context.add_output_metadata({
                    "method": MetadataValue.text("cox"),
                    "n_subjects": MetadataValue.int(len(upstream)),
                    "n_events": MetadataValue.int(int(upstream[event_column].sum())),
                    "concordance_index": MetadataValue.float(float(cph.concordance_index_)),
                })
                return result

            else:
                raise ValueError(f"Unknown method: {method}. Use 'kaplan_meier' or 'cox'.")

        return Definitions(assets=[_asset])
