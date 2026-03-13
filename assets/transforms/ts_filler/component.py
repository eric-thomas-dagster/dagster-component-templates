"""TS Filler.

Fill gaps in a time series by resampling to a regular frequency.
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
class TsFillerComponent(Component, Model, Resolvable):
    """Fill gaps in a time series by resampling to a regular frequency."""

    asset_name: str = Field(description="Output Dagster asset name")
    upstream_asset_key: str = Field(description="Upstream asset key providing a time series DataFrame")
    date_column: str = Field(description="Column name containing datetime values")
    frequency: str = Field(default="D", description="Pandas offset alias: 'D' (daily), 'W' (weekly), 'ME' (month-end), 'h' (hourly), 'min' (minute)")
    fill_method: str = Field(default="forward_fill", description="Fill method: 'forward_fill', 'backward_fill', 'interpolate', 'zero', 'mean'")
    value_columns: Optional[List[str]] = Field(default=None, description="Columns to fill (None = all numeric columns)")
    group_by: Optional[List[str]] = Field(default=None, description="Fill within groups (e.g. per product_id)")
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
        return "Fill gaps in a time series by resampling to a regular frequency."

    def build_defs(self, load_context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        upstream_asset_key = self.upstream_asset_key
        date_column = self.date_column
        frequency = self.frequency
        fill_method = self.fill_method
        value_columns = self.value_columns
        group_by = self.group_by
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
            df = upstream.copy()
            df[date_column] = pd.to_datetime(df[date_column])

            original_rows = len(df)
            cols_to_fill = value_columns or df.select_dtypes(include="number").columns.tolist()

            def apply_fill(sub_df: pd.DataFrame) -> pd.DataFrame:
                sub_df = sub_df.set_index(date_column)
                sub_df = sub_df.resample(frequency).asfreq()
                if fill_method == "forward_fill":
                    sub_df[cols_to_fill] = sub_df[cols_to_fill].ffill()
                elif fill_method == "backward_fill":
                    sub_df[cols_to_fill] = sub_df[cols_to_fill].bfill()
                elif fill_method == "interpolate":
                    sub_df[cols_to_fill] = sub_df[cols_to_fill].interpolate()
                elif fill_method == "zero":
                    sub_df[cols_to_fill] = sub_df[cols_to_fill].fillna(0)
                elif fill_method == "mean":
                    for col in cols_to_fill:
                        sub_df[col] = sub_df[col].fillna(sub_df[col].mean())
                else:
                    raise ValueError(f"Unknown fill_method: {fill_method}")
                return sub_df.reset_index()

            if group_by:
                parts = []
                for keys, group in df.groupby(group_by):
                    filled = apply_fill(group.copy())
                    if isinstance(keys, tuple):
                        for k, v in zip(group_by, keys):
                            filled[k] = v
                    else:
                        filled[group_by[0]] = keys
                    parts.append(filled)
                result = pd.concat(parts, ignore_index=True)
            else:
                result = apply_fill(df)

            context.add_output_metadata({
                "original_rows": MetadataValue.int(original_rows),
                "output_rows": MetadataValue.int(len(result)),
                "rows_added": MetadataValue.int(len(result) - original_rows),
                "frequency": MetadataValue.text(frequency),
                "fill_method": MetadataValue.text(fill_method),
            })

            return result

        return Definitions(assets=[_asset])
