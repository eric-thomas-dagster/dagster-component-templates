"""Unique Dedup Asset Component.

Remove duplicate records from a DataFrame, with control over which columns
define uniqueness, which copy to keep, and whether to flag or filter duplicates.
"""
from dataclasses import dataclass
from typing import Optional, List

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
class UniqueDedupComponent(Component, Model, Resolvable):
    """Remove or flag duplicate records from a DataFrame.

    Supports three output modes: returning only unique rows, returning only
    duplicate rows (useful for auditing), or returning all rows with a boolean
    flag column indicating which rows are duplicates.
    """

    asset_name: str = Field(description="Output Dagster asset name")
    upstream_asset_key: str = Field(description="Upstream asset key providing a DataFrame")
    subset: Optional[List[str]] = Field(
        default=None,
        description="Columns to consider for duplication (None = all columns)",
    )
    keep: str = Field(
        default="first",
        description="Which duplicate to keep: 'first', 'last', or 'none' (drop all duplicates including original)",
    )
    output_mode: str = Field(
        default="unique",
        description=(
            "Output mode: 'unique' (keep non-duplicates), "
            "'duplicates' (return only duplicate rows), "
            "'all' (return all rows with is_duplicate bool column)"
        ),
    )
    flag_column: str = Field(
        default="is_duplicate",
        description="Column name for the duplicate flag when output_mode='all'",
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
        return "Remove duplicate records from a DataFrame with configurable deduplication strategy."

    def build_defs(self, load_context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        upstream_asset_key = self.upstream_asset_key
        subset = self.subset
        keep = self.keep
        output_mode = self.output_mode
        flag_column = self.flag_column
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
            description=UniqueDedupComponent.get_description(),
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
            df = upstream.copy()
            keep_val = False if keep == "none" else keep

            rows_before = len(df)

            if output_mode == "unique":
                result = df.drop_duplicates(subset=subset, keep=keep_val)
            elif output_mode == "duplicates":
                result = df[df.duplicated(subset=subset, keep=keep_val)]
            elif output_mode == "all":
                result = df.copy()
                result[flag_column] = df.duplicated(subset=subset, keep=keep_val)
            else:
                raise ValueError(
                    f"Unknown output_mode '{output_mode}'. Use 'unique', 'duplicates', or 'all'."
                )

            rows_after = len(result)
            context.log.info(
                f"Dedup complete. Rows before: {rows_before}, after: {rows_after}, "
                f"mode='{output_mode}', keep='{keep}'"
            )
            return result

        return Definitions(assets=[_asset])
