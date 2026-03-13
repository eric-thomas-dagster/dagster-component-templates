"""RunningTotal Component.

Calculate a running/cumulative total on a column. Supports optional grouping to
reset the running total per group, and optional sorting before computation.
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
    Model,
    Resolvable,
    asset,
)
from pydantic import Field


@dataclass
class RunningTotalComponent(Component, Model, Resolvable):
    """Calculate a running/cumulative total on a column."""

    asset_name: str = Field(description="Output Dagster asset name")
    upstream_asset_key: str = Field(description="Upstream asset key providing a DataFrame")
    value_column: str = Field(description="Column to accumulate")
    output_column: Optional[str] = Field(
        default=None,
        description="Name for the cumulative column. Defaults to running_{value_column}",
    )
    group_by: Optional[List[str]] = Field(
        default=None, description="Reset running total per group"
    )
    sort_by: Optional[str] = Field(
        default=None, description="Sort by this column before computing the running total"
    )
    sort_ascending: bool = Field(
        default=True, description="Sort direction when sort_by is set"
    )
    agg_function: str = Field(
        default="sum",
        description="Cumulative function to apply: 'sum', 'count', or 'mean'",
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
        return "Calculate a running/cumulative total on a column."

    def build_defs(self, load_context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        upstream_asset_key = self.upstream_asset_key
        value_column = self.value_column
        output_column = self.output_column
        group_by = self.group_by
        sort_by = self.sort_by
        sort_ascending = self.sort_ascending
        agg_function = self.agg_function
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
            description=RunningTotalComponent.get_description(),
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
            out_col = output_column or f"running_{value_column}"
            df = upstream.copy()

            if sort_by:
                df = df.sort_values(sort_by, ascending=sort_ascending).reset_index(drop=True)

            cum_func_map = {
                "sum": "cumsum",
                "count": "cumcount",
                "mean": "cummean",
            }

            if agg_function == "sum":
                if group_by:
                    df[out_col] = df.groupby(group_by)[value_column].cumsum()
                else:
                    df[out_col] = df[value_column].cumsum()
            elif agg_function == "count":
                if group_by:
                    df[out_col] = df.groupby(group_by)[value_column].cumcount() + 1
                else:
                    df[out_col] = range(1, len(df) + 1)
            elif agg_function == "mean":
                if group_by:
                    df[out_col] = (
                        df.groupby(group_by)[value_column]
                        .expanding()
                        .mean()
                        .reset_index(level=list(range(len(group_by))), drop=True)
                    )
                else:
                    df[out_col] = df[value_column].expanding().mean()
            else:
                raise ValueError(
                    f"Unsupported agg_function '{agg_function}'. "
                    "Use one of: sum, count, mean"
                )

            context.log.info(
                f"Computed running {agg_function} of '{value_column}' "
                f"into '{out_col}' over {len(df)} rows"
            )
            return df

        return Definitions(assets=[_asset])
