"""Auto Field.

Automatically optimize DataFrame column dtypes to reduce memory usage.
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
class AutoFieldComponent(Component, Model, Resolvable):
    """Automatically optimize DataFrame column dtypes to reduce memory usage."""

    asset_name: str = Field(description="Output Dagster asset name")
    upstream_asset_key: str = Field(description="Upstream asset key providing a DataFrame")
    columns: Optional[List[str]] = Field(
        default=None,
        description="Columns to optimize. None = optimize all columns.",
    )
    downcast_integers: bool = Field(
        default=True,
        description="Reduce int64 to the smallest integer type that fits",
    )
    downcast_floats: bool = Field(
        default=True,
        description="Reduce float64 to float32 where possible",
    )
    convert_strings_to_category: bool = Field(
        default=True,
        description="Convert low-cardinality string columns to category dtype",
    )
    category_threshold: float = Field(
        default=0.5,
        description="Unique ratio below which a string column is converted to category",
    )
    parse_dates: bool = Field(
        default=False,
        description="Attempt to parse high-cardinality string columns as datetime",
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
        return "Automatically optimize DataFrame column dtypes to reduce memory usage."

    def build_defs(self, load_context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        upstream_asset_key = self.upstream_asset_key
        columns = self.columns
        downcast_integers = self.downcast_integers
        downcast_floats = self.downcast_floats
        convert_strings_to_category = self.convert_strings_to_category
        category_threshold = self.category_threshold
        parse_dates = self.parse_dates
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
            description=AutoFieldComponent.get_description(),
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
            before_mb = upstream.memory_usage(deep=True).sum() / 1e6
            df = upstream.copy()
            target_cols = columns or df.columns.tolist()

            for col in target_cols:
                if col not in df.columns:
                    continue
                dtype = df[col].dtype

                if downcast_integers and pd.api.types.is_integer_dtype(dtype):
                    df[col] = pd.to_numeric(df[col], downcast="integer")

                elif downcast_floats and pd.api.types.is_float_dtype(dtype):
                    df[col] = pd.to_numeric(df[col], downcast="float")

                elif convert_strings_to_category and dtype == object:
                    unique_ratio = df[col].nunique() / max(len(df), 1)
                    if unique_ratio < category_threshold:
                        df[col] = df[col].astype("category")
                    elif parse_dates:
                        try:
                            df[col] = pd.to_datetime(df[col], infer_datetime_format=True)
                        except Exception:
                            pass

            after_mb = df.memory_usage(deep=True).sum() / 1e6
            savings_pct = ((before_mb - after_mb) / before_mb * 100) if before_mb > 0 else 0.0
            context.log.info(
                f"Memory: {before_mb:.2f} MB -> {after_mb:.2f} MB "
                f"(saved {savings_pct:.1f}%)"
            )
            return df

        return Definitions(assets=[_asset])
