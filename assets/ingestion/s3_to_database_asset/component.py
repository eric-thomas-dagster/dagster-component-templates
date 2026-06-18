"""S3 to Database Asset Component."""

from typing import Any, Dict, List, Optional, Union
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
    AssetExecutionContext,
    ComponentLoadContext,
    AssetKey,
    asset,
    Config,
    MaterializeResult,
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
    if isinstance(partition_values, (list, tuple)):
        _values = [str(v).strip() for v in partition_values if str(v).strip()]
    else:
        _values = [v.strip() for v in (str(partition_values) if partition_values else "").split(",") if v.strip()]
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
    column_mapping: Optional[Union[str, int]] = Field(
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

    include_preview_metadata: bool = Field(
        default=False,
        description=(
            "Include a preview of the DataFrame about to be written, in "
            "metadata, so builder UIs can show 'what's being sunk' without "
            "warehouse access."
        ),
    )

    preview_rows: int = Field(
        default=25,
        ge=1,
        le=500,
        description=(
            "Rows in the preview when include_preview_metadata=True. Random "
            "sample if len > 10x preview_rows; else head."
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

    partition_date_column: Optional[Union[str, int]] = Field(
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

    partition_static_column: Optional[Union[str, int]] = Field(
        default=None,
        description="Column used to filter the upstream DataFrame to the current static partition value.",
    )

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        """Build Dagster definitions for the S3 to Database asset."""

        # Capture fields for closure
        asset_name = self.asset_name
        include_preview = self.include_preview_metadata
        preview_rows = self.preview_rows
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

        # Infer kinds from component name if not explicitly set
        _comp_name = "s3_to_database_asset"  # component directory name
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
            key=AssetKey.from_user_string(asset_name),
            description=description or f"S3 to Database: {table_name}",
                        owners=owners,
            tags=_all_tags,
            freshness_policy=_freshness_policy,
group_name=group_name,
            metadata={
                "table": table_name,
                "schema": schema_name,
                "source": "s3",
            },
            deps=[AssetKey.from_user_string(k) for k in (self.deps or [])],
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

                return MaterializeResult(
                    metadata={
                        "row_count": MetadataValue.int(len(df)),
                        "table": MetadataValue.text(table_name),
                    
                        "dagster/row_count": MetadataValue.int(len(df)),
                        **({"preview": MetadataValue.md((df.sample(preview_rows) if len(df) > preview_rows * 10 else df.head(preview_rows)).to_markdown(index=False))} if include_preview and len(df) > 0 else {}),
                    }
                )

            except Exception as e:
                context.log.error(f"Error processing s3://{bucket}/{partitioned_key}: {str(e)}")
                raise

        return Definitions(assets=[s3_to_database_asset])
