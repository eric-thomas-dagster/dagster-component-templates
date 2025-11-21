"""CSV File Ingestion Asset Component."""

from typing import Optional
import pandas as pd
from pathlib import Path
from datetime import datetime
from dagster import (
    Component,
    Resolvable,
    Model,
    Definitions,
    AssetSpec,
    AssetExecutionContext,
    ComponentLoadContext,
    multi_asset,
)
from pydantic import Field


class CSVFileIngestionComponent(Component, Model, Resolvable):
    """
    Component for ingesting CSV files into Dagster assets.

    This component creates an asset that reads a CSV file and returns
    a pandas DataFrame. It supports various CSV options and can optionally
    cache the data to a parquet file for better performance.
    """

    asset_name: str = Field(description="Name of the asset")
    file_path: str = Field(description="Path to the CSV file to ingest")
    description: Optional[str] = Field(
        default="",
        description="Description of the asset"
    )
    group_name: Optional[str] = Field(
        default="",
        description="Asset group name for organization"
    )

    # CSV Reading Options
    delimiter: str = Field(
        default=",",
        description="CSV delimiter character"
    )
    encoding: str = Field(
        default="utf-8",
        description="File encoding"
    )
    skip_rows: int = Field(
        default=0,
        description="Number of rows to skip at the start"
    )
    header_row: Optional[int] = Field(
        default=0,
        description="Row number to use as column names (0-indexed, None for no header)"
    )

    # Data Processing Options
    columns_to_read: Optional[str] = Field(
        default="",
        description="Comma-separated list of column names to read (empty = all columns)"
    )
    dtype_mapping: Optional[str] = Field(
        default="",
        description="Column type mappings as JSON string, e.g. {\"col1\": \"int64\", \"col2\": \"float64\"}"
    )
    parse_dates: Optional[str] = Field(
        default="",
        description="Comma-separated list of columns to parse as dates"
    )

    # Output Options
    cache_to_parquet: bool = Field(
        default=False,
        description="Whether to cache the data to a parquet file for better performance"
    )
    parquet_path: Optional[str] = Field(
        default="",
        description="Path where parquet cache will be stored (auto-generated if empty)"
    )

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        """Build Dagster definitions for the CSV ingestion asset."""

        # Capture fields for closure
        asset_name = self.asset_name
        file_path = self.file_path
        description = self.description
        group_name = self.group_name or None
        delimiter = self.delimiter
        encoding = self.encoding
        skip_rows = self.skip_rows
        header_row = self.header_row
        cache_to_parquet = self.cache_to_parquet
        parquet_path = self.parquet_path

        # Parse column list
        columns_to_read = None
        if self.columns_to_read:
            columns_to_read = [c.strip() for c in self.columns_to_read.split(",")]

        # Parse dtype mapping
        dtype_mapping = None
        if self.dtype_mapping:
            import json
            try:
                dtype_mapping = json.loads(self.dtype_mapping)
            except json.JSONDecodeError:
                pass

        # Parse date columns
        parse_dates = None
        if self.parse_dates:
            parse_dates = [c.strip() for c in self.parse_dates.split(",")]

        @multi_asset(
            specs=[
                AssetSpec(
                    key=asset_name,
                    description=description or f"CSV data from {file_path}",
                    group_name=group_name,
                    metadata={
                        "source_file": file_path,
                        "delimiter": delimiter,
                        "encoding": encoding,
                    }
                )
            ]
        )
        def csv_ingestion_asset(context: AssetExecutionContext):
            """Asset that ingests CSV file into a pandas DataFrame."""

            # Check if running in partitioned mode
            partition_date = None
            partitioned_file_path = file_path
            if context.has_partition_key:
                # Parse partition key as date (format: YYYY-MM-DD)
                try:
                    partition_date = datetime.strptime(context.partition_key, "%Y-%m-%d")
                    context.log.info(f"Reading CSV for partition {context.partition_key}")

                    # Support partitioned file paths with {partition_date} placeholder
                    # e.g., /data/sales_{partition_date}.csv -> /data/sales_2024-01-01.csv
                    if "{partition_date}" in file_path:
                        partitioned_file_path = file_path.replace(
                            "{partition_date}",
                            partition_date.strftime("%Y-%m-%d")
                        )
                        context.log.info(f"Using partitioned file path: {partitioned_file_path}")
                    elif "{partition_key}" in file_path:
                        partitioned_file_path = file_path.replace(
                            "{partition_key}",
                            context.partition_key
                        )
                        context.log.info(f"Using partitioned file path: {partitioned_file_path}")
                except ValueError:
                    context.log.warning(
                        f"Could not parse partition key '{context.partition_key}' as date, "
                        "using original file path"
                    )
            else:
                context.log.info("Reading CSV (non-partitioned)")

            # Check if we should use cached parquet
            if cache_to_parquet:
                cache_path = parquet_path or f"{partitioned_file_path}.parquet"
                cache_file = Path(cache_path)
                source_file = Path(partitioned_file_path)

                # Use cache if it exists and is newer than source
                if cache_file.exists() and cache_file.stat().st_mtime > source_file.stat().st_mtime:
                    context.log.info(f"Loading from parquet cache: {cache_path}")
                    df = pd.read_parquet(cache_path)
                    return df

            # Read CSV file
            context.log.info(f"Reading CSV file: {partitioned_file_path}")

            try:
                df = pd.read_csv(
                    partitioned_file_path,
                    delimiter=delimiter,
                    encoding=encoding,
                    skiprows=skip_rows,
                    header=header_row,
                    usecols=columns_to_read,
                    dtype=dtype_mapping,
                    parse_dates=parse_dates,
                )

                context.log.info(f"Loaded {len(df)} rows, {len(df.columns)} columns")

                # Log basic statistics
                context.log.info(f"Columns: {list(df.columns)}")
                context.log.info(f"Memory usage: {df.memory_usage(deep=True).sum() / 1024 / 1024:.2f} MB")

                # Cache to parquet if requested
                if cache_to_parquet:
                    cache_path = parquet_path or f"{partitioned_file_path}.parquet"
                    context.log.info(f"Caching to parquet: {cache_path}")
                    df.to_parquet(cache_path, index=False)

                # Add row count metadata
                context.add_output_metadata({
                    "num_rows": len(df),
                    "num_columns": len(df.columns),
                    "columns": list(df.columns),
                    "memory_mb": df.memory_usage(deep=True).sum() / 1024 / 1024,
                })

                return df

            except FileNotFoundError:
                context.log.error(f"CSV file not found: {partitioned_file_path}")
                raise
            except pd.errors.EmptyDataError:
                context.log.error(f"CSV file is empty: {partitioned_file_path}")
                raise
            except Exception as e:
                context.log.error(f"Error reading CSV file: {str(e)}")
                raise

        return Definitions(assets=[csv_ingestion_asset])
