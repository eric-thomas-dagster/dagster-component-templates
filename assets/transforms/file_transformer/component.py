"""File Transformer Asset Component.

Transform files between formats (CSV, JSON, Parquet, Excel).
Works with file sensors via run_config to process files automatically.
"""

import os
from typing import Any, Dict, List, Optional
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
    MetadataValue,
)
from pydantic import Field


def _build_partitions_def(
    partition_type,
    partition_start,
    partition_values,
    dynamic_partition_name,
    partition_dimensions,
):
    """Construct a Dagster partitions_def from the canonical partition fields.

    Strict: raises ValueError on misconfigured combinations rather than
    silently picking a default. Specifically:
      - time-based partition_type without partition_start
      - partition_type=multi without partition_values
      - partition_type=dynamic without dynamic_partition_name
      - both partition_dimensions AND flat fields set (ambiguous intent)
    """
    from dagster import (
        DailyPartitionsDefinition, WeeklyPartitionsDefinition,
        MonthlyPartitionsDefinition, HourlyPartitionsDefinition,
        StaticPartitionsDefinition, MultiPartitionsDefinition,
        DynamicPartitionsDefinition,
    )

    # Both shapes set: ambiguous. Pick one.
    if partition_dimensions and partition_type:
        raise ValueError(
            "Set either partition_type (flat-fields shape) or "
            "partition_dimensions (multi-axis shape), not both."
        )

    def _build_axis(spec):
        t = spec.get("type")
        if t in ("daily", "weekly", "monthly", "hourly") and not spec.get("start"):
            raise ValueError(f"partition dimension type={t!r} requires 'start' (ISO date)")
        if t == "daily":
            return DailyPartitionsDefinition(start_date=spec["start"])
        if t == "weekly":
            return WeeklyPartitionsDefinition(start_date=spec["start"])
        if t == "monthly":
            return MonthlyPartitionsDefinition(start_date=spec["start"])
        if t == "hourly":
            return HourlyPartitionsDefinition(start_date=spec["start"])
        if t == "static":
            vals = spec.get("values") or []
            if isinstance(vals, str):
                vals = [v.strip() for v in vals.split(",") if v.strip()]
            if not vals:
                raise ValueError("partition dimension type='static' requires non-empty 'values'")
            return StaticPartitionsDefinition(list(vals))
        if t == "dynamic":
            name = spec.get("dynamic_partition_name") or spec.get("name")
            if not name:
                raise ValueError("partition dimension type='dynamic' requires a name")
            return DynamicPartitionsDefinition(name=name)
        raise ValueError(f"unknown partition type: {t!r}")

    if partition_dimensions:
        if len(partition_dimensions) == 1:
            return _build_axis(partition_dimensions[0])
        axes = {d["name"]: _build_axis(d) for d in partition_dimensions}
        return MultiPartitionsDefinition(axes)

    if not partition_type:
        return None
    _values = [v.strip() for v in (partition_values or "").split(",") if v.strip()]
    if partition_type in ("daily", "weekly", "monthly", "hourly") and not partition_start:
        raise ValueError(
            f"partition_type={partition_type!r} requires partition_start (ISO date, e.g. '2024-01-01')."
        )
    if partition_type == "daily":
        return DailyPartitionsDefinition(start_date=partition_start)
    if partition_type == "weekly":
        return WeeklyPartitionsDefinition(start_date=partition_start)
    if partition_type == "monthly":
        return MonthlyPartitionsDefinition(start_date=partition_start)
    if partition_type == "hourly":
        return HourlyPartitionsDefinition(start_date=partition_start)
    if partition_type == "static":
        if not _values:
            raise ValueError("partition_type='static' requires partition_values (comma-separated).")
        return StaticPartitionsDefinition(_values)
    if partition_type == "dynamic":
        if not dynamic_partition_name:
            raise ValueError(
                "partition_type='dynamic' requires dynamic_partition_name."
            )
        return DynamicPartitionsDefinition(name=dynamic_partition_name)
    if partition_type == "multi":
        if not _values:
            raise ValueError("partition_type='multi' requires partition_values (comma-separated).")
        if not partition_start:
            raise ValueError("partition_type='multi' requires partition_start (the date axis start).")
        return MultiPartitionsDefinition({
            "date": DailyPartitionsDefinition(start_date=partition_start),
            "static_dim": StaticPartitionsDefinition(_values),
        })
    raise ValueError(f"unknown partition_type: {partition_type!r}")


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

    upstream_asset_key: Optional[str] = Field(
        default=None,
        description=(
            "Upstream DataFrame asset key. When set, the component reads the DataFrame "
            "from the upstream asset (no file I/O on input) and writes it to "
            "output_directory in the requested output_format. Mutually exclusive "
            "with input_file_path / run_config-driven file paths."
        ),
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
    dynamic_partition_name: Optional[str] = Field(
        default=None,
        description="Name for DynamicPartitionsDefinition (when partition_type='dynamic'), e.g. 'tenants'.",
    )

    partition_dimensions: Optional[List[Dict[str, Any]]] = Field(
        default=None,
        description="Multi-axis partition spec: list of {name, type, start, values, dynamic_partition_name} dicts. Overrides flat fields when set.",
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

    include_preview_metadata: bool = Field(
        default=False,
        description=(
            "Include a preview of the output data in metadata (first 25 "
            "rows or a sample) for builder UIs."
        ),
    )

    preview_rows: int = Field(
        default=25,
        ge=1,
        le=500,
        description=(
            "Rows to include in the preview metadata. For long DataFrames "
            "(>10x preview_rows), a random sample is used; otherwise head()."
        ),
    )

    deps: Optional[list[str]] = Field(default=None, description="Upstream asset keys this asset depends on (e.g. ['raw_orders', 'schema/asset'])")


    retry_policy_max_retries: Optional[int] = Field(
        default=None,
        description="Max retries on failure. Defines a RetryPolicy when set.",
    )

    retry_policy_delay_seconds: Optional[int] = Field(
        default=None,
        description="Seconds between retries (default 1).",
    )

    retry_policy_backoff: str = Field(
        default="exponential",
        description="Backoff strategy: 'linear' or 'exponential'.",
    )

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        # Standard catalog fields — phase 2 wiring
        _retry_policy = None
        if self.retry_policy_max_retries is not None:
            from dagster import Backoff, RetryPolicy
            _retry_policy = RetryPolicy(
                max_retries=self.retry_policy_max_retries,
                delay=self.retry_policy_delay_seconds or 1,
                backoff=Backoff[self.retry_policy_backoff.upper()],
            )
        _freshness_policy = None
        if self.freshness_max_lag_minutes is not None:
            from dagster import FreshnessPolicy
            _freshness_policy = FreshnessPolicy(
                maximum_lag_minutes=self.freshness_max_lag_minutes,
                cron_schedule=self.freshness_cron,
            )
        _all_tags = dict(self.asset_tags or {})
        for _k in (self.kinds or []):
            _all_tags[f"dagster/kind/{_k}"] = ""
        asset_name = self.asset_name
        include_preview = self.include_preview_metadata
        preview_rows = self.preview_rows
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

        partitions_def = _build_partitions_def(
            self.partition_type,
            self.partition_start,
            self.partition_values,
            self.dynamic_partition_name,
            self.partition_dimensions,
        )
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


        upstream_asset_key = self.upstream_asset_key
        from dagster import AssetIn
        _ins = (
            {"upstream": AssetIn(key=AssetKey.from_user_string(upstream_asset_key))}
            if upstream_asset_key
            else None
        )

        @asset(
            name=asset_name,
            description=description,
            partitions_def=partitions_def,
                        owners=owners,
            tags=_all_tags,
            freshness_policy=_freshness_policy,
group_name=group_name,
            ins=_ins,
            deps=[AssetKey.from_user_string(k) for k in (self.deps or [])],
            retry_policy=_retry_policy,
        )
        def file_transformer_asset(context: AssetExecutionContext, config: FileConfig, **kwargs):
            """Asset that transforms files between formats, or writes an upstream
            DataFrame to a file in the requested output format."""

            # Path A: upstream-asset mode — skip file read, take DataFrame directly.
            if upstream_asset_key:
                df = kwargs.get("upstream")
                if df is None:
                    raise ValueError(
                        f"upstream_asset_key={upstream_asset_key!r} set but no upstream "
                        "DataFrame was provided (IO manager mis-wiring?)"
                    )
                input_path = None
                detected_format = None  # not applicable
                context.log.info(f"Using upstream asset '{upstream_asset_key}' as input ({len(df)} rows)")
                # Filter to current partition if partitioned (works on the upstream df)
                if context.has_partition_key:
                    _pk = context.partition_key
                    _is_multi = hasattr(_pk, "keys_by_dimension")
                    _date_key = _pk.keys_by_dimension.get("date", "") if _is_multi else str(_pk)
                    _static_key = _pk.keys_by_dimension.get(partition_static_dim or "segment", "") if _is_multi else None
                    if partition_date_column and partition_date_column in df.columns and _date_key:
                        df = df[df[partition_date_column].astype(str) == _date_key]
                    if partition_static_column and partition_static_column in df.columns and _static_key:
                        df = df[df[partition_static_column].astype(str) == _static_key]
                    elif partition_static_column and partition_static_column in df.columns and not _is_multi:
                        df = df[df[partition_static_column].astype(str) == str(_pk)]
                # Skip file-input branch entirely
                input_filename_stem = asset_name
            else:
                # Path B: file-input mode — read input file, transform, write output.
                # Determine input file path (from run_config or fixed path)
                if config.file_path:
                    input_path = config.file_path
                    context.log.info(f"Using file from run_config: {input_path}")
                elif fixed_input_path:
                    input_path = fixed_input_path
                    context.log.info(f"Using fixed input path: {input_path}")
                else:
                    raise ValueError("No input file path provided (via run_config, input_file_path, or upstream_asset_key)")
                df = None
                input_filename_stem = Path(input_path).stem

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
                base_name = input_filename_stem
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
            # Use explicit lineage, or auto-infer passthrough columns at runtime
            _effective_lineage = column_lineage
            if not _effective_lineage:
                try:
                    _upstream_cols = set(df.columns)
                    _effective_lineage = {
                        col: [col] for col in _col_schema.columns_by_name
                        if col in _upstream_cols
                    }
                except Exception:
                    pass
            if _effective_lineage:
                _upstream_key = AssetKey.from_user_string(upstream_asset_key) if upstream_asset_key else None
                if _upstream_key:
                    _lineage_deps = {}
                    for out_col, in_cols in _effective_lineage.items():
                        _lineage_deps[out_col] = [
                            TableColumnDep(asset_key=_upstream_key, column_name=ic)
                            for ic in in_cols
                        ]
                    _metadata["dagster/column_lineage"] = MetadataValue.column_lineage(
                        TableColumnLineage(_lineage_deps)
                    )
            if include_preview and len(df) > 0:
                try:
                    _prev = df.sample(min(preview_rows, len(df))) if len(df) > preview_rows * 10 else df.head(preview_rows)
                    _metadata["preview"] = MetadataValue.md(_prev.to_markdown(index=False))
                except Exception as _e:
                    context.log.warning(f"preview emission failed: {_e}")
            context.add_output_metadata(_metadata)

            return df

        from dagster import build_column_schema_change_checks


        _schema_checks = build_column_schema_change_checks(assets=[file_transformer_asset])


        return Definitions(assets=[file_transformer_asset], asset_checks=list(_schema_checks))
