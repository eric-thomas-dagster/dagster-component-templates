"""CSV File Ingestion Asset Component."""

from typing import Any, Dict, List, Optional
import pandas as pd
from pathlib import Path
from datetime import datetime
from dagster import (
    Component,
    Resolvable,
    Model,
    Definitions,
    AssetExecutionContext,
    ComponentLoadContext,
    AssetKey,
    asset,
    Output,
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

    include_preview_metadata: bool = Field(
        default=False,
        description="Include a preview of the output data in metadata (first 5 rows as markdown table). Used by builder UIs to render asset shape without warehouse access."
    )

    preview_rows: int = Field(
        default=25,
        ge=1,
        le=500,
        description=(
            "Rows to include in the preview metadata when "
            "`include_preview_metadata` is True. For long DataFrames "
            "(>10x preview_rows), a random sample is used so the preview "
            "reflects the data distribution; otherwise head() is used."
        ),
    )

    deps: Optional[list[str]] = Field(default=None, description="Upstream asset keys this asset depends on (e.g. ['raw_orders', 'schema/asset'])")

    partition_type: Optional[str] = Field(

        default=None,

        description="Partition type: 'daily', 'weekly', 'monthly', 'hourly', or None for unpartitioned. With a partition type set, the partition key is exposed via context.partition_key for use in filtering / templating.",

    )

    partition_start: Optional[str] = Field(

        default=None,

        description="Partition start date in ISO format, e.g. '2024-01-01'. Required when partition_type is set.",

    )


    retry_policy_max_retries: Optional[int] = Field(


        default=None,


        description="Max retries on asset failure. Defines a RetryPolicy. Useful for transient network failures, rate limits, etc.",


    )


    retry_policy_delay_seconds: Optional[int] = Field(


        default=None,


        description="Seconds between retries (default 1).",


    )


    retry_policy_backoff: str = Field(


        default="exponential",


        description="Backoff strategy: 'linear' or 'exponential'.",


    )




    column_lineage: Optional[Dict[str, List[str]]] = Field(
        default=None,
        description="Column-level lineage: output column → list of upstream columns it derives from, e.g. {'revenue': ['price', 'quantity']}.",
    )

    partition_date_column: Optional[str] = Field(
        default=None,
        description="Column used to filter the upstream DataFrame to the current date partition key.",
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
        description="Comma-separated values for static or multi partitioning, e.g. 'acme,globex,initech'.",
    )

    partition_static_dim: Optional[str] = Field(
        default=None,
        description="Dimension name for the static axis in multi-partitioning, e.g. 'customer'.",
    )

    partition_static_column: Optional[str] = Field(
        default=None,
        description="Column used to filter the upstream DataFrame to the current static partition value.",
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
        include_preview = self.include_preview_metadata
        preview_rows = self.preview_rows

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

        # Infer kinds from component name if not explicitly set
        _comp_name = "csv_file_ingestion"  # component directory name
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


        # Build partition definition (auto-generated; supports daily, weekly,


        # monthly, hourly partitions out of the box).
        partitions_def = _build_partitions_def(
            self.partition_type,
            self.partition_start,
            self.partition_values,
            self.dynamic_partition_name,
            self.partition_dimensions,
        )



        # Build retry policy (auto-generated; opt-in via retry_policy_max_retries).



        _retry_policy = None



        if self.retry_policy_max_retries is not None:



            from dagster import Backoff, RetryPolicy



            _retry_policy = RetryPolicy(



                max_retries=self.retry_policy_max_retries,



                delay=self.retry_policy_delay_seconds or 1,



                backoff=Backoff[self.retry_policy_backoff.upper()],



            )




        @asset(retry_policy=_retry_policy, partitions_def=partitions_def, 
            name=asset_name,
            description=description or f"CSV data from {file_path}",
                        owners=owners,
            tags=_all_tags,
            freshness_policy=_freshness_policy,
group_name=group_name,
            metadata={
                "source_file": file_path,
                "delimiter": delimiter,
                "encoding": encoding,
            },
            deps=[AssetKey.from_user_string(k) for k in (self.deps or [])],
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

                # Add row count metadata. Cast numpy scalars to native Python
                # types so Dagster's event log serializer can handle them.
                context.add_output_metadata({
                    "num_rows": int(len(df)),
                    "num_columns": int(len(df.columns)),
                    "columns": list(df.columns),
                    "memory_mb": float(df.memory_usage(deep=True).sum()) / 1024 / 1024,
                })

                if include_preview and len(df) > 0:
                    # Return with sample metadata
                    return Output(
                        value=df,
                        metadata={
                            "row_count": len(df),
                            "columns": df.columns.tolist(),
                            "preview": MetadataValue.md(df.head().to_markdown())
                        }
                    )
                else:
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
