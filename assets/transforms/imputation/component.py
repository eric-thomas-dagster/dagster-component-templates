"""Imputation Asset Component.

Fill missing values in a DataFrame using a variety of strategies including
statistical measures, constant values, forward/backward fill, and interpolation.
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
class ImputationComponent(Component, Model, Resolvable):
    """Fill missing values using various imputation strategies.

    Supports statistical imputation (mean, median, mode), constant fill,
    directional fill (forward/backward), and interpolation. Can target
    specific columns or automatically select appropriate columns based on
    the chosen strategy.
    """

    asset_name: str = Field(description="Output Dagster asset name")
    upstream_asset_key: str = Field(description="Upstream asset key providing a DataFrame")
    strategy: str = Field(
        default="mean",
        description=(
            "Imputation strategy: 'mean', 'median', 'mode', 'constant', "
            "'forward_fill', 'backward_fill', or 'interpolate'"
        ),
    )
    fill_value: Optional[str] = Field(
        default=None,
        description="Constant value to fill nulls with when strategy='constant'",
    )
    columns: Optional[List[str]] = Field(
        default=None,
        description=(
            "Columns to impute. None = all numeric columns for mean/median, "
            "all columns for other strategies."
        ),
    )
    limit: Optional[int] = Field(
        default=None,
        description="Maximum number of consecutive fills for forward/backward fill and interpolate",
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
        return "Fill missing values using mean, median, mode, constant, forward/backward fill, or interpolation."

    def build_defs(self, load_context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        upstream_asset_key = self.upstream_asset_key
        strategy = self.strategy
        fill_value = self.fill_value
        columns = self.columns
        limit = self.limit
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
            description=ImputationComponent.get_description(),
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

            target_cols = columns if columns else (
                df.select_dtypes(include="number").columns.tolist()
                if strategy in ("mean", "median")
                else df.columns.tolist()
            )

            for col in target_cols:
                if col not in df.columns:
                    context.log.warning(f"Column '{col}' not found, skipping.")
                    continue

                missing_before = df[col].isna().sum()
                if missing_before == 0:
                    continue

                if strategy == "mean":
                    df[col] = df[col].fillna(df[col].mean())
                elif strategy == "median":
                    df[col] = df[col].fillna(df[col].median())
                elif strategy == "mode":
                    mode_vals = df[col].mode()
                    df[col] = df[col].fillna(mode_vals[0] if not mode_vals.empty else None)
                elif strategy == "constant":
                    df[col] = df[col].fillna(fill_value)
                elif strategy == "forward_fill":
                    df[col] = df[col].ffill(limit=limit)
                elif strategy == "backward_fill":
                    df[col] = df[col].bfill(limit=limit)
                elif strategy == "interpolate":
                    df[col] = df[col].interpolate(limit=limit)

                missing_after = df[col].isna().sum()
                context.log.info(
                    f"Column '{col}': filled {missing_before - missing_after} nulls using '{strategy}'"
                )

            return df

        return Definitions(assets=[_asset])
