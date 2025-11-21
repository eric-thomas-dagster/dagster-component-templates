"""S3 to Database Asset Component."""

from typing import Optional
import boto3
import pandas as pd
from io import BytesIO
from datetime import datetime
from sqlalchemy import create_engine
from dagster import (
    Component,
    Resolvable,
    Model,
    Definitions,
    AssetSpec,
    AssetExecutionContext,
    ComponentLoadContext,
    multi_asset,
    Config,
)
from pydantic import Field


class S3ToDatabaseAssetComponent(Component, Model, Resolvable):
    """
    Component for loading files from S3 into a database table.

    This component is designed to work with sensors that pass S3 file information
    via run_config. It reads the file from S3, processes it, and writes to a database.

    Works with CSV, JSON, and Parquet files.
    """

    asset_name: str = Field(description="Name of the asset")

    # Database Configuration
    database_url: str = Field(
        description="Database connection URL (use ${DB_URL} for env var)"
    )
    table_name: str = Field(
        description="Name of the database table to write to"
    )
    schema_name: Optional[str] = Field(
        default="",
        description="Database schema name (optional)"
    )
    if_exists: str = Field(
        default="append",
        description="How to behave if table exists: 'fail', 'replace', 'append'"
    )

    # S3 Configuration
    aws_region: Optional[str] = Field(
        default="",
        description="AWS region (optional, uses default if not specified)"
    )

    # File Processing
    file_format: str = Field(
        default="csv",
        description="Expected file format: 'csv', 'json', 'parquet', 'auto' (detect from extension)"
    )
    csv_delimiter: str = Field(
        default=",",
        description="CSV delimiter character (if format is CSV)"
    )
    json_orient: str = Field(
        default="records",
        description="JSON orientation: 'records', 'split', 'index', 'columns', 'values'"
    )

    # Data Processing
    column_mapping: Optional[str] = Field(
        default="",
        description="JSON string for renaming columns, e.g. {\"old_name\": \"new_name\"}"
    )
    dtype_mapping: Optional[str] = Field(
        default="",
        description="JSON string for specifying column types, e.g. {\"col1\": \"int64\"}"
    )

    # Metadata
    description: Optional[str] = Field(
        default="",
        description="Description of the asset"
    )
    group_name: Optional[str] = Field(
        default="",
        description="Asset group name for organization"
    )

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        """Build Dagster definitions for the S3 to Database asset."""

        # Capture fields for closure
        asset_name = self.asset_name
        database_url = self.database_url
        table_name = self.table_name
        schema_name = self.schema_name or None
        if_exists = self.if_exists
        aws_region = self.aws_region
        file_format = self.file_format
        csv_delimiter = self.csv_delimiter
        json_orient = self.json_orient
        description = self.description
        group_name = self.group_name or None

        # Parse column mapping
        column_mapping = None
        if self.column_mapping:
            import json
            try:
                column_mapping = json.loads(self.column_mapping)
            except json.JSONDecodeError:
                pass

        # Parse dtype mapping
        dtype_mapping = None
        if self.dtype_mapping:
            import json
            try:
                dtype_mapping = json.loads(self.dtype_mapping)
            except json.JSONDecodeError:
                pass

        # Define the run config schema
        class S3FileConfig(Config):
            """Configuration passed from sensor via run_config."""
            s3_bucket: str
            s3_key: str
            s3_size: Optional[int] = None
            s3_last_modified: Optional[str] = None

        @multi_asset(
            specs=[
                AssetSpec(
                    key=asset_name,
                    description=description or f"S3 to Database: {table_name}",
                    group_name=group_name,
                    metadata={
                        "table": table_name,
                        "schema": schema_name,
                        "source": "s3",
                    }
                )
            ]
        )
        def s3_to_database_asset(context: AssetExecutionContext, config: S3FileConfig):
            """Asset that loads S3 files into a database table."""

            bucket = config.s3_bucket
            key = config.s3_key

            # Check if running in partitioned mode
            partition_date = None
            partitioned_key = key
            if context.has_partition_key:
                # Parse partition key as date (format: YYYY-MM-DD)
                try:
                    partition_date = datetime.strptime(context.partition_key, "%Y-%m-%d")
                    context.log.info(f"Processing S3 file for partition {context.partition_key}")

                    # Support partitioned S3 keys with {partition_date} placeholder
                    # e.g., data/sales_{partition_date}.csv -> data/sales_2024-01-01.csv
                    if "{partition_date}" in key:
                        partitioned_key = key.replace(
                            "{partition_date}",
                            partition_date.strftime("%Y-%m-%d")
                        )
                        context.log.info(f"Using partitioned S3 key: {partitioned_key}")
                    elif "{partition_key}" in key:
                        partitioned_key = key.replace(
                            "{partition_key}",
                            context.partition_key
                        )
                        context.log.info(f"Using partitioned S3 key: {partitioned_key}")
                except ValueError:
                    context.log.warning(
                        f"Could not parse partition key '{context.partition_key}' as date, "
                        "using original S3 key"
                    )
            else:
                context.log.info(f"Processing S3 file (non-partitioned)")

            context.log.info(f"Processing s3://{bucket}/{partitioned_key}")

            # Initialize S3 client
            s3_client_kwargs = {}
            if aws_region:
                s3_client_kwargs['region_name'] = aws_region
            s3_client = boto3.client('s3', **s3_client_kwargs)

            try:
                # Download file from S3
                response = s3_client.get_object(Bucket=bucket, Key=partitioned_key)
                file_content = response['Body'].read()

                context.log.info(f"Downloaded {len(file_content)} bytes from S3")

                # Determine file format
                format_to_use = file_format
                if format_to_use == "auto":
                    if partitioned_key.endswith('.csv'):
                        format_to_use = "csv"
                    elif partitioned_key.endswith('.json'):
                        format_to_use = "json"
                    elif partitioned_key.endswith('.parquet'):
                        format_to_use = "parquet"
                    else:
                        raise ValueError(f"Cannot auto-detect format for {partitioned_key}")

                # Parse file into DataFrame
                if format_to_use == "csv":
                    df = pd.read_csv(
                        BytesIO(file_content),
                        delimiter=csv_delimiter,
                        dtype=dtype_mapping
                    )
                elif format_to_use == "json":
                    df = pd.read_json(
                        BytesIO(file_content),
                        orient=json_orient,
                        dtype=dtype_mapping
                    )
                elif format_to_use == "parquet":
                    df = pd.read_parquet(BytesIO(file_content))
                else:
                    raise ValueError(f"Unsupported file format: {format_to_use}")

                context.log.info(f"Loaded {len(df)} rows, {len(df.columns)} columns")

                # Apply column mapping if provided
                if column_mapping:
                    df = df.rename(columns=column_mapping)
                    context.log.info(f"Renamed columns: {list(column_mapping.keys())}")

                # Connect to database
                engine = create_engine(database_url)

                # Write to database
                df.to_sql(
                    name=table_name,
                    con=engine,
                    schema=schema_name,
                    if_exists=if_exists,
                    index=False,
                    method='multi',
                    chunksize=1000
                )

                context.log.info(
                    f"Wrote {len(df)} rows to {schema_name + '.' if schema_name else ''}{table_name}"
                )

                # Add metadata
                context.add_output_metadata({
                    "num_rows": len(df),
                    "num_columns": len(df.columns),
                    "columns": list(df.columns),
                    "s3_bucket": bucket,
                    "s3_key": partitioned_key,
                    "s3_size_bytes": config.s3_size,
                    "table": f"{schema_name + '.' if schema_name else ''}{table_name}",
                    "file_format": format_to_use,
                })

                return df

            except Exception as e:
                context.log.error(f"Error processing s3://{bucket}/{partitioned_key}: {str(e)}")
                raise

        return Definitions(assets=[s3_to_database_asset])
