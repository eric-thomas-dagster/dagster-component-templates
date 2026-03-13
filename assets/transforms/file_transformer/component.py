"""File Transformer Asset Component.

Transform files between formats (CSV, JSON, Parquet, Excel).
Works with file sensors via run_config to process files automatically.
"""

import os
from typing import Dict, List, Optional
from pathlib import Path

import pandas as pd
from dagster import (
    Component,
    ComponentLoadContext,
    Definitions,
    AssetExecutionContext,
    AssetKey,
    asset,
    Config,
    Resolvable,
    Model,
)
from pydantic import Field


class FileTransformerComponent(Component, Model, Resolvable):
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
    owners: Optional[List[str]] = Field(
        default=None,
        description="Asset owners — list of team names or email addresses, e.g. ['team:analytics', 'user@company.com']",
    )
    asset_tags: Optional[Dict[str, str]] = Field(
        default=None,
        description="Additional key-value tags to apply to the asset, e.g. {'domain': 'finance', 'tier': 'gold'}",
    )
    kinds: Optional[List[str]] = Field(
        default=None,
        description="Asset kinds for the Dagster catalog, e.g. ['snowflake', 'python']. Auto-inferred from component name if not set.",
    )
    freshness_max_lag_minutes: Optional[int] = Field(
        default=None,
        description="Maximum acceptable lag in minutes before the asset is considered stale. Defines a FreshnessPolicy.",
    )
    freshness_cron: Optional[str] = Field(
        default=None,
        description="Cron schedule string for the freshness policy, e.g. '0 9 * * 1-5' (weekdays at 9am).",
    )
    column_lineage: Optional[Dict[str, List[str]]] = Field(
        default=None,
        description="Column-level lineage mapping: output column name → list of upstream column names it was derived from, e.g. {'revenue': ['price', 'quantity']}",
    )

    deps: Optional[list[str]] = Field(default=None, description="Upstream asset keys this asset depends on (e.g. ['raw_orders', 'schema/asset'])")

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

        # Infer kinds from component name if not explicitly set
        _comp_name = "file_transformer"  # component directory name
        _kind_map = {
            "snowflake": "snowflake", "bigquery": "bigquery", "redshift": "redshift",
            "postgres": "postgres", "postgresql": "postgres", "mysql": "mysql",
            "s3": "s3", "adls": "azure", "azure": "azure", "gcs": "gcp",
            "google": "gcp", "databricks": "databricks", "dbt": "dbt",
            "kafka": "kafka", "mongodb": "mongodb", "redis": "redis",
            "neo4j": "neo4j", "elasticsearch": "elasticsearch", "pinecone": "pinecone",
            "chromadb": "chromadb", "pgvector": "postgres",
        }
        _inferred_kinds = self.kinds or []
        if not _inferred_kinds:
            _comp_lower = asset_name.lower()
            for keyword, kind in _kind_map.items():
                if keyword in _comp_lower:
                    _inferred_kinds.append(kind)
            if not _inferred_kinds:
                _inferred_kinds = ["python"]

        # Build combined tags: user tags + kind tags
        _all_tags = dict(self.asset_tags or {})
        for _kind in _inferred_kinds:
            _all_tags[f"dagster/kind/{_kind}"] = ""

        # Build freshness policy
        _freshness_policy = None
        if self.freshness_max_lag_minutes is not None:
            from dagster import FreshnessPolicy
            _freshness_policy = FreshnessPolicy(
                maximum_lag_minutes=self.freshness_max_lag_minutes,
                cron_schedule=self.freshness_cron,
            )

        owners = self.owners or []
        column_lineage = self.column_lineage if hasattr(self, 'column_lineage') else None


        @asset(
            name=asset_name,
            description=description,
            partitions_def=partitions_def,
                        owners=owners,
            tags=_all_tags,
            freshness_policy=_freshness_policy,
group_name=group_name,
            deps=[AssetKey.from_user_string(k) for k in (self.deps or [])],
        )
        def file_transformer_asset(context: AssetExecutionContext, config: FileConfig):
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

            # Build column schema metadata
            from dagster import TableSchema, TableColumn, TableColumnLineage, TableColumnDep
            _col_schema = TableSchema(columns=[
                TableColumn(name=str(col), type=str(df.dtypes[col]))
                for col in df.columns
            ])
            _metadata = {
                "dagster/row_count": MetadataValue.int(len(df)),
                "dagster/column_schema": MetadataValue.table_schema(_col_schema),
            }
            if column_lineage:
                _upstream_key = AssetKey.from_user_string(upstream_asset_key) if upstream_asset_key else None
                if _upstream_key:
                    _lineage_deps = {}
                    for out_col, in_cols in column_lineage.items():
                        _lineage_deps[out_col] = [
                            TableColumnDep(asset_key=_upstream_key, column_name=ic)
                            for ic in in_cols
                        ]
                    _metadata["dagster/column_lineage"] = MetadataValue.table_column_lineage(
                        TableColumnLineage(_lineage_deps)
                    )
            context.add_output_metadata(_metadata)

            return df

        from dagster import build_column_schema_change_checks


        _schema_checks = build_column_schema_change_checks(assets=[file_transformer_asset])


        return Definitions(assets=[file_transformer_asset], asset_checks=list(_schema_checks))
