"""TextToColumns.

Split a text column into multiple columns or rows by a delimiter.
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
class TextToColumns(Component, Model, Resolvable):
    """Split a text column into multiple columns or rows by a delimiter."""

    asset_name: str = Field(description="Output Dagster asset name")
    upstream_asset_key: str = Field(description="Upstream asset key providing a DataFrame")
    column: str = Field(description="Column to split")
    delimiter: str = Field(default=",", description="Delimiter string")
    max_splits: Optional[int] = Field(default=None, description="Max number of splits")
    output_columns: Optional[List[str]] = Field(default=None, description="Names for resulting columns (auto-generated if None)")
    expand_to_rows: bool = Field(default=False, description="If True, split into rows instead of columns")
    strip_whitespace: bool = Field(default=True, description="Strip whitespace from each part")
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
        return "Split a text column into multiple columns or rows by a delimiter."

    def build_defs(self, load_context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        upstream_asset_key = self.upstream_asset_key
        column = self.column
        delimiter = self.delimiter
        max_splits = self.max_splits
        output_columns = self.output_columns
        expand_to_rows = self.expand_to_rows
        strip_whitespace = self.strip_whitespace
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

            if expand_to_rows:
                df[column] = df[column].str.split(delimiter)
                if strip_whitespace:
                    df[column] = df[column].apply(
                        lambda parts: [p.strip() for p in parts] if isinstance(parts, list) else parts
                    )
                df = df.explode(column).reset_index(drop=True)
            else:
                split_df = df[column].str.split(delimiter, expand=True, n=max_splits)

                if strip_whitespace:
                    split_df = split_df.apply(
                        lambda col: col.str.strip() if col.dtype == object else col
                    )

                if output_columns:
                    rename_map = {i: name for i, name in enumerate(output_columns[: len(split_df.columns)])}
                    split_df = split_df.rename(columns=rename_map)
                    # auto-name any remaining columns beyond output_columns
                    remaining = {
                        i: f"{column}_{i}"
                        for i in range(len(output_columns), len(split_df.columns))
                    }
                    split_df = split_df.rename(columns=remaining)
                else:
                    split_df = split_df.rename(columns={i: f"{column}_{i}" for i in split_df.columns})

                df = df.drop(columns=[column])
                df = pd.concat([df, split_df], axis=1)

            return df

        return Definitions(assets=[_asset])
