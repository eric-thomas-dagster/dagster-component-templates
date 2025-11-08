"""File Transformer Asset Component.

Transform files between formats (CSV, JSON, Parquet, Excel).
Works with file sensors via run_config to process files automatically.
"""

import os
from typing import Optional
from pathlib import Path

import pandas as pd
from dagster import (
    Component,
    ComponentLoadContext,
    Definitions,
    AssetExecutionContext,
    AssetSpec,
    multi_asset,
    Config,
)
from pydantic import BaseModel, Field


class FileTransformerComponent(Component, BaseModel):
    """Component for transforming files between formats.

    This asset transforms files from one format to another. It can accept file
    information via run_config from sensors (like Filesystem Sensor) or be used
    standalone with a fixed input file path.

    Supported formats: CSV, JSON, Parquet, Excel

    Example:
        ```yaml
        type: dagster_component_templates.FileTransformerComponent
        attributes:
          asset_name: transform_csv_to_parquet
          output_format: parquet
          output_directory: /data/processed
        ```
    """

    asset_name: str = Field(
        description="Name of the asset"
    )

    input_file_path: Optional[str] = Field(
        default=None,
        description="Fixed input file path (optional, can use run_config instead)"
    )

    input_format: Optional[str] = Field(
        default="auto",
        description="Input file format: 'auto', 'csv', 'json', 'parquet', 'excel'"
    )

    output_format: str = Field(
        description="Output file format: 'csv', 'json', 'parquet', 'excel'"
    )

    output_directory: str = Field(
        description="Directory to write transformed files"
    )

    output_filename: Optional[str] = Field(
        default=None,
        description="Output filename (optional, defaults to input filename with new extension)"
    )

    # CSV options
    csv_delimiter: str = Field(
        default=",",
        description="CSV delimiter character"
    )

    csv_encoding: str = Field(
        default="utf-8",
        description="CSV file encoding"
    )

    # JSON options
    json_orient: str = Field(
        default="records",
        description="JSON orientation: 'records', 'split', 'index', 'columns', 'values'"
    )

    # Excel options
    excel_sheet_name: str = Field(
        default="Sheet1",
        description="Excel sheet name for reading/writing"
    )

    # Parquet options
    parquet_compression: str = Field(
        default="snappy",
        description="Parquet compression: 'snappy', 'gzip', 'brotli', None"
    )

    # Data processing
    drop_duplicates: bool = Field(
        default=False,
        description="Whether to drop duplicate rows"
    )

    fill_na_value: Optional[str] = Field(
        default=None,
        description="Value to fill NaN values with (optional)"
    )

    columns_to_keep: Optional[str] = Field(
        default=None,
        description="Comma-separated list of columns to keep (optional)"
    )

    description: Optional[str] = Field(
        default=None,
        description="Asset description"
    )

    group_name: Optional[str] = Field(
        default=None,
        description="Asset group for organization"
    )

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        fixed_input_path = self.input_file_path
        input_format = self.input_format
        output_format = self.output_format
        output_directory = self.output_directory
        output_filename = self.output_filename
        csv_delimiter = self.csv_delimiter
        csv_encoding = self.csv_encoding
        json_orient = self.json_orient
        excel_sheet_name = self.excel_sheet_name
        parquet_compression = self.parquet_compression if self.parquet_compression != "None" else None
        drop_duplicates_flag = self.drop_duplicates
        fill_na_value = self.fill_na_value
        columns_to_keep = self.columns_to_keep
        description = self.description or f"Transform files to {output_format}"
        group_name = self.group_name

        # Define run config schema for file sensors
        class FileConfig(Config):
            """Configuration passed from file sensors via run_config."""
            file_path: Optional[str] = None
            file_name: Optional[str] = None
            file_size: Optional[int] = None
            file_modified_time: Optional[float] = None
            directory_path: Optional[str] = None

        @multi_asset(
            name=f"{asset_name}_asset",
            specs=[
                AssetSpec(
                    key=asset_name,
                    description=description,
                    group_name=group_name,
                )
            ],
        )
        def file_transformer_asset(context: AssetExecutionContext, config: FileConfig):
            """Asset that transforms files between formats."""

            # Determine input file path (from run_config or fixed path)
            if config.file_path:
                input_path = config.file_path
                context.log.info(f"Using file from run_config: {input_path}")
            elif fixed_input_path:
                input_path = fixed_input_path
                context.log.info(f"Using fixed input path: {input_path}")
            else:
                raise ValueError("No input file path provided (via run_config or input_file_path)")

            # Verify input file exists
            if not os.path.exists(input_path):
                raise FileNotFoundError(f"Input file not found: {input_path}")

            # Auto-detect input format if needed
            detected_format = input_format
            if detected_format == "auto":
                ext = Path(input_path).suffix.lower()
                format_map = {
                    '.csv': 'csv',
                    '.json': 'json',
                    '.parquet': 'parquet',
                    '.pq': 'parquet',
                    '.xlsx': 'excel',
                    '.xls': 'excel',
                }
                detected_format = format_map.get(ext)
                if not detected_format:
                    raise ValueError(f"Cannot auto-detect format for extension: {ext}")
                context.log.info(f"Auto-detected input format: {detected_format}")

            # Read input file
            context.log.info(f"Reading {detected_format} file: {input_path}")

            if detected_format == "csv":
                df = pd.read_csv(input_path, delimiter=csv_delimiter, encoding=csv_encoding)
            elif detected_format == "json":
                df = pd.read_json(input_path, orient=json_orient)
            elif detected_format == "parquet":
                df = pd.read_parquet(input_path)
            elif detected_format == "excel":
                df = pd.read_excel(input_path, sheet_name=excel_sheet_name)
            else:
                raise ValueError(f"Unsupported input format: {detected_format}")

            context.log.info(f"Loaded {len(df)} rows, {len(df.columns)} columns")

            # Data processing
            if columns_to_keep:
                cols = [c.strip() for c in columns_to_keep.split(',')]
                missing_cols = set(cols) - set(df.columns)
                if missing_cols:
                    context.log.warning(f"Columns not found: {missing_cols}")
                existing_cols = [c for c in cols if c in df.columns]
                df = df[existing_cols]
                context.log.info(f"Kept {len(existing_cols)} columns")

            if drop_duplicates_flag:
                original_len = len(df)
                df = df.drop_duplicates()
                context.log.info(f"Dropped {original_len - len(df)} duplicate rows")

            if fill_na_value is not None:
                df = df.fillna(fill_na_value)
                context.log.info(f"Filled NaN values with: {fill_na_value}")

            # Determine output filename
            if output_filename:
                out_filename = output_filename
            else:
                base_name = Path(input_path).stem
                ext_map = {
                    'csv': '.csv',
                    'json': '.json',
                    'parquet': '.parquet',
                    'excel': '.xlsx',
                }
                out_filename = f"{base_name}{ext_map.get(output_format, '.out')}"

            # Ensure output directory exists
            os.makedirs(output_directory, exist_ok=True)

            # Write output file
            output_path = os.path.join(output_directory, out_filename)
            context.log.info(f"Writing {output_format} file: {output_path}")

            if output_format == "csv":
                df.to_csv(output_path, index=False, encoding=csv_encoding)
            elif output_format == "json":
                df.to_json(output_path, orient=json_orient, indent=2)
            elif output_format == "parquet":
                df.to_parquet(output_path, compression=parquet_compression, index=False)
            elif output_format == "excel":
                df.to_excel(output_path, sheet_name=excel_sheet_name, index=False)
            else:
                raise ValueError(f"Unsupported output format: {output_format}")

            # Get file size
            output_size = os.path.getsize(output_path)

            # Add metadata
            context.add_output_metadata({
                "input_file": input_path,
                "output_file": output_path,
                "input_format": detected_format,
                "output_format": output_format,
                "num_rows": len(df),
                "num_columns": len(df.columns),
                "columns": list(df.columns),
                "output_size_bytes": output_size,
                "output_size_mb": round(output_size / 1024 / 1024, 2),
            })

            return df

        return Definitions(assets=[file_transformer_asset])
