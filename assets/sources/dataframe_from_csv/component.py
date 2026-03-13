"""DataframeFromCsv Component.

Read a CSV file and output a DataFrame. Supports environment variable substitution
in the file path for flexible deployment configuration.
"""
import os
from dataclasses import dataclass
from typing import Dict, List, Optional

import pandas as pd
from dagster import (
    AssetExecutionContext,
    Component,
    ComponentLoadContext,
    Definitions,
    Model,
    Resolvable,
    asset,
)
from pydantic import Field


@dataclass
class DataframeFromCsvComponent(Component, Model, Resolvable):
    """Read a CSV file and output a DataFrame."""

    asset_name: str = Field(description="Output Dagster asset name")
    file_path: str = Field(
        description="Path to CSV file. Supports env var substitution, e.g. ${DATA_DIR}/file.csv"
    )
    delimiter: str = Field(default=",", description="Column delimiter character")
    encoding: str = Field(default="utf-8", description="File encoding")
    parse_dates: Optional[List[str]] = Field(
        default=None, description="Columns to parse as dates"
    )
    dtype: Optional[Dict[str, str]] = Field(
        default=None, description="Column dtype overrides, e.g. {id: str, amount: float}"
    )
    skiprows: Optional[int] = Field(
        default=None, description="Number of rows to skip at the start of the file"
    )
    nrows: Optional[int] = Field(default=None, description="Maximum number of rows to read")
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
        return "Read a CSV file and output a DataFrame."

    def build_defs(self, load_context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        file_path = self.file_path
        delimiter = self.delimiter
        encoding = self.encoding
        parse_dates = self.parse_dates
        dtype = self.dtype
        skiprows = self.skiprows
        nrows = self.nrows
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
            partitions_def=partitions_def,
            group_name=group_name,
            description=DataframeFromCsvComponent.get_description(),
        )
        def _asset(context: AssetExecutionContext) -> pd.DataFrame:
            # Log partition key for source components
            if context.has_partition_key:
                context.log.info(f"Running for partition: {context.partition_key}")
            resolved_path = os.path.expandvars(file_path)
            context.log.info(f"Reading CSV from {resolved_path}")

            df = pd.read_csv(
                resolved_path,
                delimiter=delimiter,
                encoding=encoding,
                parse_dates=parse_dates or False,
                dtype=dtype,
                skiprows=skiprows,
                nrows=nrows,
            )

            context.log.info(f"Loaded {len(df)} rows and {len(df.columns)} columns from CSV")
            return df

        return Definitions(assets=[_asset])
