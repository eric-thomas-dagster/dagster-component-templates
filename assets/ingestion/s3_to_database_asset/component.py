"""S3 to Database Asset Component."""

from typing import Any, Dict, List, Literal, Optional, Union
import boto3
import pandas as pd
from io import BytesIO
from datetime import datetime
from sqlalchemy import create_engine
from dagster import (
    BackfillPolicy,
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
    cron_schedule=None,
    cron_fmt="%Y-%m-%d",
    end_offset=0,
    timezone=None,
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
        _end_offset = spec.get("end_offset", 0)
        _timezone = spec.get("timezone")
        if t in ("daily", "weekly", "monthly", "hourly") and not spec.get("start"):
            raise ValueError(f"partition dimension type={t!r} requires 'start' (ISO date)")
        if t == "daily":
            return DailyPartitionsDefinition(start_date=spec["start"], end_offset=_end_offset, timezone=_timezone)
        if t == "weekly":
            return WeeklyPartitionsDefinition(start_date=spec["start"], end_offset=_end_offset, timezone=_timezone)
        if t == "monthly":
            return MonthlyPartitionsDefinition(start_date=spec["start"], end_offset=_end_offset, timezone=_timezone)
        if t == "hourly":
            return HourlyPartitionsDefinition(start_date=spec["start"], end_offset=_end_offset, timezone=_timezone)
        if t == "cron":
            from dagster import TimeWindowPartitionsDefinition
            if not spec.get("start"):
                raise ValueError(f"partition dimension type={t!r} requires 'start' (ISO date)")
            if not spec.get("cron_schedule"):
                raise ValueError("partition dimension type='cron' requires 'cron_schedule'")
            return TimeWindowPartitionsDefinition(start=spec["start"], fmt=spec.get("fmt") or "%Y-%m-%d", cron_schedule=spec["cron_schedule"], end_offset=_end_offset, timezone=_timezone)
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
    if partition_type in ("daily", "weekly", "monthly", "hourly", "cron") and not partition_start:
        raise ValueError(
            f"partition_type={partition_type!r} requires partition_start (ISO date, e.g. '2024-01-01')."
        )
    if partition_type == "daily":
        return DailyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone)
    if partition_type == "weekly":
        return WeeklyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone)
    if partition_type == "monthly":
        return MonthlyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone)
    if partition_type == "hourly":
        return HourlyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone)
    if partition_type == "cron":
        from dagster import TimeWindowPartitionsDefinition
        if not cron_schedule:
            raise ValueError(
                "partition_type='cron' requires cron_schedule, e.g. '0 0 * * 1-5' for weekdays only."
            )
        return TimeWindowPartitionsDefinition(start=partition_start, fmt=cron_fmt, cron_schedule=cron_schedule, end_offset=end_offset, timezone=timezone)
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
            "date": DailyPartitionsDefinition(start_date=partition_start, end_offset=end_offset, timezone=timezone),
            "static_dim": StaticPartitionsDefinition(_values),
        })
    raise ValueError(f"unknown partition_type: {partition_type!r}")


def _resolve_db_url(context, resource_key, database_url):
    """Resolve a SQLAlchemy-compatible database URL.

    Precedence: a registered resource (WHERE to connect, e.g. postgres_resource)
    if resource_key is set and the resource exposes a 'connection_string'
    property, otherwise the literal database_url. The write mechanics (HOW)
    are unchanged either way -- this only resolves the URL that
    sqlalchemy.create_engine() then connects with.
    """
    if resource_key:
        resource = getattr(context.resources, resource_key, None)
        if resource is None:
            raise ValueError(f"Resource {resource_key!r} is not registered in this project's Definitions.")
        url = getattr(resource, "connection_string", None)
        if not url:
            raise ValueError(
                f"Resource {resource_key!r} ({type(resource).__name__}) does not expose a "
                "'connection_string' property, so it can't be used to determine the database "
                "URL automatically. Use database_url instead."
            )
        return url
    if database_url:
        return database_url
    raise ValueError("Set 'resource_key' or 'database_url'.")



# BEGIN field-shape normalization (inlined per component; keep copies identical — see tests/test_field_shapes.py)
from typing import Any, Dict, List, Optional, Union  # noqa: E402,F401
from pydantic import model_validator  # noqa: E402,F401


def coerce_field_shapes(data, *, lists=(), csv=(), tags=()):
    """Accept the shapes people (and LLMs) naturally write for common boilerplate fields."""
    if not isinstance(data, dict):
        return data
    out = dict(data)
    for k in lists:  # List[str]: "a" / "a, b" -> ["a", "b"]; strip + drop empties
        v = out.get(k)
        if isinstance(v, str):
            v = v.split(",")
        if isinstance(v, (list, tuple)):
            out[k] = [x.strip() if isinstance(x, str) else x for x in v if not (isinstance(x, str) and not x.strip())]
    for k in csv:  # comma-separated str given as a list -> "a,b"
        v = out.get(k)
        if isinstance(v, (list, tuple, set)):
            out[k] = ",".join(str(x).strip() for x in v)
    for k in tags:  # Dict[str, str]: scalars -> str, ["k=v"] / ["k:v"] -> {k: v}
        v = out.get(k)
        if isinstance(v, (list, tuple)) and v and all(isinstance(x, str) and ("=" in x or ":" in x) for x in v):
            v = dict((p.strip() for p in (x.split("=", 1) if "=" in x else x.split(":", 1))) for x in v)
        if isinstance(v, dict):
            out[k] = {str(a).strip(): (str(b).lower() if isinstance(b, bool) else str(b)) for a, b in v.items() if b is not None}
    return out
# END field-shape normalization


class S3ToDatabaseAssetComponent(Component, Model, Resolvable):
    """
    Component for loading files from S3 into a database table.

    This component is designed to work with sensors that pass S3 file information
    via run_config. It reads the file from S3, processes it, and writes to a database.

    Works with CSV, JSON, and Parquet files.
    """

    asset_name: str = Field(description="Name of the asset")

    # Database Configuration
    database_url: Optional[str] = Field(
        default=None,
        description="Database connection URL (use ${DB_URL} for env var). Set this OR resource_key.",
    )
    resource_key: Optional[str] = Field(
        default=None,
        description="Key of a registered SQL resource (e.g. postgres_resource) to use for the destination connection instead of database_url. The resource must expose a connection_string property (e.g. PostgresResource, MongoDBResource); not all resource types do.",
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
    asset_tags: Optional[Union[Dict[str, Any], List[str]]] = Field(
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

    partition_cron_schedule: Optional[str] = Field(
        default=None,
        description=(
            "Required when partition_type='cron'. A cron expression restricting the date axis to a "
            "custom cadence, e.g. '0 0 * * 1-5' for weekdays only (business-day cadence). Built on "
            "Dagster's TimeWindowPartitionsDefinition(cron_schedule=...)."
        ),
    )
    partition_cron_fmt: Optional[str] = Field(
        default="%Y-%m-%d",
        description="strftime format for partition keys produced by partition_type='cron'. Only change this for a sub-daily cron_schedule.",
    )
    partition_end_offset: int = Field(
        default=0,
        description=(
            "Passed through to the underlying Daily/Weekly/Monthly/Hourly/cron "
            "partitions definition. 0 (the Dagster default) means a time-based "
            "partition set only ever holds COMPLETED windows -- e.g. a daily set "
            "never includes today until tomorrow. Set to 1 to include the "
            "current, still-in-progress window."
        ),
    )
    partition_timezone: Optional[str] = Field(
        default=None,
        description="IANA timezone for the underlying Daily/Weekly/Monthly/Hourly/cron partitions definition (defaults to UTC when unset, matching Dagster's own default).",
    )

    partition_start: Optional[str] = Field(

        default=None,

        description="Partition start date in ISO format, e.g. '2024-01-01'. Required when partition_type is set.",

    )

    backfill_policy: Optional[Literal["single_run", "multi_run"]] = Field(
        default=None,
        description=(
            "Backfill execution strategy for a partitioned asset. 'single_run' "
            "materializes an entire backfill range in one run (reads and "
            "concatenates one S3 file per day in the range, then writes once). "
            "'multi_run' launches one run per partition (optionally capped via "
            "backfill_policy_max_partitions_per_run). Leave unset for Dagster's "
            "default (one run per partition, uncapped). Ignored when "
            "partition_type is not set. Unlike dlt-based ingestion connectors, "
            "this asset writes directly to SQL and returns no IO-managed value, "
            "so 'single_run' works with any IO manager."
        ),
    )
    backfill_policy_max_partitions_per_run: Optional[int] = Field(
        default=None,
        description="For backfill_policy='multi_run' only: maximum partitions materialized per run. Leave unset for Dagster's default (1 partition per run).",
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
        description="Column-level lineage, emitted as `dagster/column_lineage` metadata on every materialization: {output column: [upstream columns]}, e.g. {'revenue': ['price', 'quantity']}. The upstream asset is `upstream_asset_key`, or the only entry of `deps`; with several upstream assets, name each column '<asset_key>:<column>'. Ignored with a warning when no upstream asset can be resolved.",
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

    partition_values: Optional[Union[str, List[str]]] = Field(
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

    @model_validator(mode="before")
    @classmethod
    def _coerce_field_shapes(cls, data):
        return coerce_field_shapes(data, lists=('owners', 'kinds', 'deps'), csv=('partition_values',), tags=('asset_tags',))

    def build_defs(self, context: ComponentLoadContext) -> Definitions:
        """Build Dagster definitions for the S3 to Database asset."""

        # Capture fields for closure
        _column_lineage_md = _column_lineage_metadata(self)
        asset_name = self.asset_name
        include_preview = self.include_preview_metadata
        preview_rows = self.preview_rows
        database_url = self.database_url
        resource_key = self.resource_key
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
            _freshness_policy = _freshness_policy_compat(self.freshness_max_lag_minutes, self.freshness_cron)

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
        
    cron_schedule=self.partition_cron_schedule,
    cron_fmt=self.partition_cron_fmt or "%Y-%m-%d", end_offset=self.partition_end_offset, timezone=self.partition_timezone,
)

        backfill_policy = None
        if partitions_def is not None and self.backfill_policy == "single_run":
            backfill_policy = BackfillPolicy.single_run()
        elif partitions_def is not None and self.backfill_policy == "multi_run":
            backfill_policy = BackfillPolicy.multi_run(
                max_partitions_per_run=self.backfill_policy_max_partitions_per_run or 1
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
            backfill_policy=backfill_policy,
            required_resource_keys={resource_key} if resource_key else set(),
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

            if _column_lineage_md:
                context.add_output_metadata(_column_lineage_md)
            bucket = config.s3_bucket
            key = config.s3_key

            # Partition key(s) this call must process: a normal tick gives
            # exactly one; a single_run backfill gives the whole range (via
            # context.partition_keys, which returns a 1-element list for a
            # normal tick too -- the single, uniform way to read "which
            # partition(s) am I reading a file for"). This asset writes
            # directly to SQL (df.to_sql below) and returns a bare
            # MaterializeResult with no value, bypassing Dagster's IO
            # manager entirely -- unlike the dlt-based connectors, a
            # single_run backfill here has no multi-partition-output IO
            # manager constraint to worry about.
            partition_keys = [None]
            if context.has_partition_key or context.has_partition_key_range:
                partition_keys = list(context.partition_keys)

            def _resolve_key(partition_key):
                """Support partitioned S3 keys with a {partition_date} placeholder for
                time-based partitions (e.g. data/sales_{partition_date}.csv ->
                data/sales_2024-01-01.csv), independent of whether the key also
                uses {partition_key} -- static/dynamic partition keys aren't dates,
                so that substitution must not depend on the date parse succeeding."""
                if partition_key is None:
                    context.log.info("Processing S3 file (non-partitioned)")
                    return key
                context.log.info(f"Processing S3 file for partition {partition_key}")
                partitioned_key = key
                if "{partition_date}" in key:
                    try:
                        partition_date = datetime.strptime(partition_key, "%Y-%m-%d")
                        partitioned_key = key.replace(
                            "{partition_date}",
                            partition_date.strftime("%Y-%m-%d")
                        )
                        context.log.info(f"Using partitioned S3 key: {partitioned_key}")
                    except ValueError:
                        context.log.warning(
                            f"Could not parse partition key '{partition_key}' as date, "
                            "using original S3 key"
                        )
                if "{partition_key}" in partitioned_key:
                    partitioned_key = partitioned_key.replace("{partition_key}", partition_key)
                    context.log.info(f"Using partitioned S3 key: {partitioned_key}")
                return partitioned_key

            # Initialize S3 client
            s3_client_kwargs = {}
            if aws_region:
                s3_client_kwargs['region_name'] = aws_region
            s3_client = boto3.client('s3', **s3_client_kwargs)

            def _read_one(partition_key):
                partitioned_key = _resolve_key(partition_key)
                context.log.info(f"Processing s3://{bucket}/{partitioned_key}")

                response = s3_client.get_object(Bucket=bucket, Key=partitioned_key)
                file_content = response['Body'].read()
                context.log.info(f"Downloaded {len(file_content)} bytes from S3")

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
                return df

            try:
                dfs = [_read_one(pk) for pk in partition_keys]
                df = pd.concat(dfs, ignore_index=True) if len(dfs) > 1 else dfs[0]

                # Apply column mapping if provided
                if column_mapping:
                    df = df.rename(columns=column_mapping)
                    context.log.info(f"Renamed columns: {list(column_mapping.keys())}")

                # Connect to database
                engine = create_engine(_resolve_db_url(context, resource_key, database_url))

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
                context.log.error(f"Error processing s3://{bucket}/{key}: {str(e)}")
                raise

        return Definitions(assets=[s3_to_database_asset])


# BEGIN freshness-policy compat (inlined per component; keep copies identical, see tests/test_freshness_policy_compat.py)
def _freshness_policy_compat(max_lag_minutes, cron_schedule=None):
    """Build a FreshnessPolicy on the current Dagster API (time_window / cron) and on the older constructor."""
    from datetime import timedelta

    from dagster import FreshnessPolicy

    lag = timedelta(minutes=max_lag_minutes)
    if hasattr(FreshnessPolicy, "time_window"):
        if cron_schedule:
            return FreshnessPolicy.cron(deadline_cron=cron_schedule, lower_bound_delta=lag)
        return FreshnessPolicy.time_window(fail_window=lag)
    return FreshnessPolicy(maximum_lag_minutes=max_lag_minutes, cron_schedule=cron_schedule)
# END freshness-policy compat

# BEGIN column-lineage helper (inlined per component; keep copies identical, see tests/test_column_lineage.py)
def _column_lineage_metadata(component):
    """`dagster/column_lineage` metadata from the component's `column_lineage` mapping ({output column: [upstream columns]}).

    The upstream asset is `upstream_asset_key`, or the only entry of `deps`. With several upstream assets, name each
    column as "<asset_key>:<column>". Returns {} when the mapping is unset, or (with a warning) when no upstream asset
    can be resolved, since lineage needs an upstream asset to point at."""
    import warnings

    import dagster as _dg

    mapping = getattr(component, "column_lineage", None)
    if not mapping:
        return {}
    ups = getattr(component, "upstream_asset_key", None) or getattr(component, "deps", None) or []
    ups = [ups] if isinstance(ups, (str, _dg.AssetKey)) else list(ups)
    ups = [u.to_user_string() if isinstance(u, _dg.AssetKey) else str(u) for u in ups]
    deps = {}
    for out_col, in_cols in mapping.items():
        refs = []
        for ic in [in_cols] if isinstance(in_cols, str) else in_cols:
            key, _, col = str(ic).rpartition(":")
            if key in ups and col:
                refs.append(_dg.TableColumnDep(asset_key=_dg.AssetKey.from_user_string(key), column_name=col))
            elif len(ups) == 1:
                refs.append(_dg.TableColumnDep(asset_key=_dg.AssetKey.from_user_string(ups[0]), column_name=str(ic)))
            else:
                warnings.warn(
                    f"column_lineage ignored: cannot tell which upstream asset {ic!r} belongs to "
                    f"(upstream assets: {ups or 'none'}; use '<asset_key>:<column>' with several upstream assets)"
                )
                return {}
        deps[str(out_col)] = refs
    return {"dagster/column_lineage": _dg.MetadataValue.column_lineage(_dg.TableColumnLineage(deps))}
# END column-lineage helper
