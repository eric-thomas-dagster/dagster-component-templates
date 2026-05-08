"""Dataframe To S3.

Write a DataFrame to Amazon S3 as Parquet, CSV, or JSON.
"""
import os
from typing import Any, Dict, List, Optional

import pandas as pd
from dagster import (
    AssetExecutionContext,
    AssetIn,
    AssetKey,
    Component,
    ComponentLoadContext,
    Definitions,
    MaterializeResult,
    MetadataValue,
    Model,
    Resolvable,
    asset,
)
from pydantic import Field


def _build_partitions_def(
    partition_type,
    partition_start,
    partition_values,
    dynamic_partition_name,
    partition_dimensions,
):
    """Construct a Dagster partitions_def from the canonical partition fields."""
    from dagster import (
        DailyPartitionsDefinition, WeeklyPartitionsDefinition,
        MonthlyPartitionsDefinition, HourlyPartitionsDefinition,
        StaticPartitionsDefinition, MultiPartitionsDefinition,
        DynamicPartitionsDefinition,
    )

    def _build_axis(spec):
        t = spec.get("type")
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
            return StaticPartitionsDefinition(list(vals))
        if t == "dynamic":
            name = spec.get("dynamic_partition_name") or spec.get("name")
            if not name:
                raise ValueError("dynamic partition dimension requires a name")
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
    if partition_type == "daily":
        return DailyPartitionsDefinition(start_date=partition_start or "2024-01-01")
    if partition_type == "weekly":
        return WeeklyPartitionsDefinition(start_date=partition_start or "2024-01-01")
    if partition_type == "monthly":
        return MonthlyPartitionsDefinition(start_date=partition_start or "2024-01-01")
    if partition_type == "hourly":
        return HourlyPartitionsDefinition(start_date=partition_start or "2024-01-01")
    if partition_type == "static":
        return StaticPartitionsDefinition(_values)
    if partition_type == "dynamic":
        if not dynamic_partition_name:
            raise ValueError(
                "partition_type='dynamic' requires dynamic_partition_name."
            )
        return DynamicPartitionsDefinition(name=dynamic_partition_name)
    if partition_type == "multi":
        return MultiPartitionsDefinition({
            "date": DailyPartitionsDefinition(start_date=partition_start or "2024-01-01"),
            "static_dim": StaticPartitionsDefinition(_values),
        })
    raise ValueError(f"unknown partition_type: {partition_type!r}")


class DataframeToS3Component(Component, Model, Resolvable):
    """Write a DataFrame to Amazon S3 as Parquet, CSV, or JSON."""

    asset_name: str = Field(description="Output Dagster asset name")
    upstream_asset_key: str = Field(description="Upstream asset key providing a DataFrame")
    bucket_env_var: str = Field(default="S3_BUCKET", description="Env var containing the S3 bucket name")
    key: str = Field(description="S3 object key / path within bucket e.g. data/output/results.parquet")
    format: str = Field(default="parquet", description="Output format: 'parquet', 'csv', or 'json'")
    aws_access_key_env_var: Optional[str] = Field(
        default=None,
        description="Env var with AWS access key ID. None = use IAM role or ~/.aws/credentials",
    )
    aws_secret_key_env_var: Optional[str] = Field(
        default=None,
        description="Env var with AWS secret access key",
    )
    aws_region: Optional[str] = Field(default=None, description="AWS region e.g. 'us-east-1'")
    compression: Optional[str] = Field(
        default=None,
        description="Compression codec. For parquet: 'snappy', 'gzip'. For csv: 'gzip'. None = default.",
    )
    partition_cols: Optional[List[str]] = Field(
        default=None,
        description="Column names to partition by (parquet only)",
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


    description: Optional[str] = Field(
        default=None,
        description="Asset description shown in the Dagster catalog.",
    )

    column_lineage: Optional[Dict[str, List[str]]] = Field(
        default=None,
        description="Column-level lineage: output column → list of upstream columns it derives from, e.g. {'revenue': ['price', 'quantity']}.",
    )

    deps: Optional[List[str]] = Field(
        default=None,
        description="Lineage-only upstream asset keys (no data passed at runtime).",
    )

    @classmethod
    def get_description(cls) -> str:
        return "Write a DataFrame to Amazon S3 as Parquet, CSV, or JSON."

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


    def build_defs(self, load_context: ComponentLoadContext) -> Definitions:
        asset_name = self.asset_name
        include_preview = self.include_preview_metadata
        preview_rows = self.preview_rows
        upstream_asset_key = self.upstream_asset_key
        bucket_env_var = self.bucket_env_var
        key = self.key
        format = self.format
        aws_access_key_env_var = self.aws_access_key_env_var
        aws_secret_key_env_var = self.aws_secret_key_env_var
        aws_region = self.aws_region
        compression = self.compression
        partition_cols = self.partition_cols
        group_name = self.group_name

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
        _comp_name = "dataframe_to_s3"  # component directory name
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


        # Build retry policy (auto-generated; opt-in via retry_policy_max_retries).


        _retry_policy = None


        if self.retry_policy_max_retries is not None:


            from dagster import Backoff, RetryPolicy


            _retry_policy = RetryPolicy(


                max_retries=self.retry_policy_max_retries,


                delay=self.retry_policy_delay_seconds or 1,


                backoff=Backoff[self.retry_policy_backoff.upper()],


            )



        @asset(retry_policy=_retry_policy, 
            name=asset_name,
            ins={"upstream": AssetIn(key=AssetKey.from_user_string(upstream_asset_key))},
            partitions_def=partitions_def,
                        owners=owners,
            tags=_all_tags,
            freshness_policy=_freshness_policy,
group_name=group_name,
            description=DataframeToS3Component.get_description(),
        )
        def _asset(context: AssetExecutionContext, upstream: pd.DataFrame) -> MaterializeResult:
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
            try:
                import boto3
                import s3fs
            except ImportError:
                raise ImportError(
                    "boto3 and s3fs required: pip install boto3 s3fs"
                )

            bucket = os.environ[bucket_env_var]
            s3_path = f"s3://{bucket}/{key}"

            storage_options: dict = {}
            if aws_access_key_env_var:
                storage_options = {
                    "key": os.environ[aws_access_key_env_var],
                    "secret": os.environ[aws_secret_key_env_var],
                }
            if aws_region:
                storage_options["client_kwargs"] = {"region_name": aws_region}

            if format == "parquet":
                upstream.to_parquet(
                    s3_path,
                    compression=compression or "snappy",
                    partition_cols=partition_cols,
                    storage_options=storage_options or None,
                )
            elif format == "csv":
                upstream.to_csv(
                    s3_path,
                    index=False,
                    compression=compression,
                    storage_options=storage_options or None,
                )
            elif format == "json":
                upstream.to_json(
                    s3_path,
                    orient="records",
                    lines=True,
                    storage_options=storage_options or None,
                )
            else:
                raise ValueError(f"Unsupported format '{format}'. Must be 'parquet', 'csv', or 'json'.")

            context.log.info(f"Wrote {len(upstream)} rows to {s3_path} as {format}")
            return MaterializeResult(
                metadata={
                    "row_count": MetadataValue.int(len(upstream)),
                    "column_count": MetadataValue.int(len(upstream.columns)),
                    "s3_path": MetadataValue.text(s3_path),
                    "format": MetadataValue.text(format),
                
                    "dagster/row_count": MetadataValue.int(len(upstream)),
                    **({"preview": MetadataValue.md((upstream.sample(preview_rows) if len(upstream) > preview_rows * 10 else upstream.head(preview_rows)).to_markdown(index=False))} if include_preview and len(upstream) > 0 else {}),
                }
            )

        return Definitions(assets=[_asset])
