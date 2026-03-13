"""Data Cleansing Asset Component.

Fix common data quality issues — nulls, whitespace, case, and punctuation
across string columns in a DataFrame.
"""
import string
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
class DataCleansingComponent(Component, Model, Resolvable):
    """Fix common data quality issues in string columns of a DataFrame.

    Handles null values, leading/trailing whitespace, case normalization,
    and punctuation removal. Can target all string columns or a specific subset.
    """

    asset_name: str = Field(description="Output Dagster asset name")
    upstream_asset_key: str = Field(description="Upstream asset key providing a DataFrame")
    null_handling: str = Field(
        default="keep",
        description="How to handle nulls: 'keep', 'drop' (drop rows with any null), or 'fill' (fill with null_fill_value)",
    )
    null_fill_value: Optional[str] = Field(
        default=None,
        description="Value to fill nulls with when null_handling='fill'",
    )
    trim_whitespace: bool = Field(
        default=True,
        description="Strip leading/trailing whitespace from string columns",
    )
    normalize_case: Optional[str] = Field(
        default=None,
        description="Case normalization: None, 'upper', 'lower', or 'title'",
    )
    remove_punctuation: bool = Field(
        default=False,
        description="Remove punctuation characters from string columns",
    )
    columns: Optional[List[str]] = Field(
        default=None,
        description="Apply transformations only to these columns (None = all string columns)",
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
        return "Fix common data quality issues — nulls, whitespace, case, and punctuation in string columns."

    def build_defs(self, load_context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        upstream_asset_key = self.upstream_asset_key
        null_handling = self.null_handling
        null_fill_value = self.null_fill_value
        trim_whitespace = self.trim_whitespace
        normalize_case = self.normalize_case
        remove_punctuation = self.remove_punctuation
        columns = self.columns
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
            description=DataCleansingComponent.get_description(),
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

            str_cols = columns if columns else df.select_dtypes(include="object").columns.tolist()

            if null_handling == "drop":
                df = df.dropna()
            elif null_handling == "fill":
                df = df.fillna(null_fill_value)

            for col in str_cols:
                if col not in df.columns:
                    context.log.warning(f"Column '{col}' not found in DataFrame, skipping.")
                    continue
                if trim_whitespace:
                    df[col] = df[col].astype(str).str.strip()
                if normalize_case == "upper":
                    df[col] = df[col].str.upper()
                elif normalize_case == "lower":
                    df[col] = df[col].str.lower()
                elif normalize_case == "title":
                    df[col] = df[col].str.title()
                if remove_punctuation:
                    df[col] = df[col].str.translate(str.maketrans("", "", string.punctuation))

            context.log.info(
                f"Data cleansing complete. Rows: {len(df)}, Columns processed: {len(str_cols)}"
            )
            return df

        return Definitions(assets=[_asset])
