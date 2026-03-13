"""FindReplace.

Look up values in one column against a reference DataFrame and replace with mapped values.
"""
from dataclasses import dataclass
from typing import Optional

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
class FindReplace(Component, Model, Resolvable):
    """Look up values in one column against a reference DataFrame and replace with mapped values."""

    asset_name: str = Field(description="Output Dagster asset name")
    upstream_asset_key: str = Field(description="Main DataFrame asset key")
    lookup_asset_key: str = Field(description="Reference/lookup DataFrame asset key")
    lookup_key_column: str = Field(description="Column in lookup table to match against")
    lookup_value_column: str = Field(description="Column in lookup table with replacement values")
    target_column: str = Field(description="Column in main DataFrame to look up")
    output_column: Optional[str] = Field(default=None, description="Output column name (defaults to overwriting target_column)")
    default_value: Optional[str] = Field(default=None, description="Value when no match found (defaults to original value)")
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
        return "Look up values in one column against a reference DataFrame and replace with mapped values."

    def build_defs(self, load_context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        upstream_asset_key = self.upstream_asset_key
        lookup_asset_key = self.lookup_asset_key
        lookup_key_column = self.lookup_key_column
        lookup_value_column = self.lookup_value_column
        target_column = self.target_column
        output_column = self.output_column
        default_value = self.default_value
        group_name = self.group_name

        ins = {
            "upstream": AssetIn(key=AssetKey.from_user_string(upstream_asset_key)),
            "lookup": AssetIn(key=AssetKey.from_user_string(lookup_asset_key)),
        }

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

        partitions_def=partitions_def,
        @asset(name=asset_name, ins=ins, group_name=group_name)
        def _asset(context: AssetExecutionContext, upstream: pd.DataFrame, lookup: pd.DataFrame) -> pd.DataFrame:
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
            out_col = output_column if output_column else target_column

            mapping = dict(zip(lookup[lookup_key_column], lookup[lookup_value_column]))

            if default_value is not None:
                df[out_col] = df[target_column].map(mapping).fillna(default_value)
            else:
                df[out_col] = df[target_column].map(mapping).fillna(df[target_column])

            return df

        return Definitions(assets=[_asset])
